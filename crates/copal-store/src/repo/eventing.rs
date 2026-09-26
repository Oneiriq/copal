//! Eventing repository: endpoints, the outbox, and deliveries.
//!
//! The outbox rows come from the engine event on `file`; nothing here
//! creates them. Fan-out and delivery both follow the CAS discipline:
//! marking an event dispatched, claiming a delivery, and settling an
//! attempt are all guarded updates, so replicas cooperate without a
//! coordinator and a crash at any point retries instead of losing or
//! duplicating work.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{CopalError, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const ENDPOINT_TABLE: &str = "webhook_endpoint";
const EVENT_TABLE: &str = "file_event";
const DELIVERY_TABLE: &str = "webhook_delivery";

/// Delivery attempts before a delivery is marked failed for good.
pub const MAX_ATTEMPTS: i64 = 8;

fn rid(table: &str, id: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(table, id).map_err(|e| map_store_err("eventing id", e))
}

/// A webhook endpoint as the dispatcher sees it (sealed secret included;
/// the API listing uses [`list_endpoints`], which never selects it).
#[derive(Debug, Clone, Deserialize)]
pub struct EndpointRow {
    pub id: String,
    pub tenant_id: String,
    pub target_url: String,
    pub events: String,
    pub secret_sealed: String,
    pub active: bool,
    pub created_at: String,
}

impl EndpointRow {
    /// The bare endpoint id.
    pub fn endpoint_id(&self) -> String {
        strip_record_prefix(&self.id, ENDPOINT_TABLE).to_owned()
    }

    /// Whether this endpoint wants `action` (empty filter means all).
    pub fn wants(&self, action: &str) -> bool {
        self.events.is_empty() || self.events.split(',').any(|e| e.trim() == action)
    }
}

/// Register an endpoint. The secret arrives already sealed.
pub async fn create_endpoint(
    store: &Store,
    tenant: &TenantId,
    target_url: &str,
    events: &str,
    secret_sealed: &str,
) -> copal_core::Result<EndpointRow> {
    if !target_url.starts_with("http://") && !target_url.starts_with("https://") {
        return Err(CopalError::validation("target_url must be http(s)"));
    }
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "target_url": target_url,
        "events": events,
        "secret_sealed": secret_sealed,
    });
    let created = create_record(
        store.client(),
        &rid(ENDPOINT_TABLE, &id)?.to_string(),
        payload,
    )
    .await
    .map_err(|e| map_store_err("create_endpoint", e))?;
    let row = created
        .record
        .ok_or_else(|| CopalError::Store("create returned no record".into()))?;
    serde_json::from_value(row).map_err(|e| CopalError::Store(format!("endpoint row: {e}")))
}

/// List a tenant's endpoints; sealed secrets never leave the repo here.
pub async fn list_endpoints(store: &Store, tenant: &TenantId) -> copal_core::Result<Vec<Value>> {
    let query = Query::new()
        .select(Some(vec![
            "id".to_owned(),
            "target_url".to_owned(),
            "events".to_owned(),
            "active".to_owned(),
            "created_at".to_owned(),
        ]))
        .from_table(ENDPOINT_TABLE)
        .map_err(|e| map_store_err("list_endpoints", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .order_by("created_at", "ASC")
        .map_err(|e| map_store_err("list_endpoints", e))?;
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_endpoints", e))?;
    Ok(rows
        .into_iter()
        .map(|mut row| {
            if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                let bare = strip_record_prefix(id, ENDPOINT_TABLE).to_owned();
                row["id"] = json!(bare);
            }
            row
        })
        .collect())
}

/// A tenant's active endpoints, sealed secrets included, for fan-out
/// and delivery.
pub async fn active_endpoints(store: &Store, tenant: &str) -> copal_core::Result<Vec<EndpointRow>> {
    let query = Query::new()
        .select(None)
        .from_table(ENDPOINT_TABLE)
        .map_err(|e| map_store_err("active_endpoints", e))?
        .where_(eq("tenant_id", tenant))
        .where_(eq("active", true));
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("active_endpoints", e))
}

/// Deactivate an endpoint, tenant-scoped, once. Pending deliveries for
/// it settle as failed when the dispatcher finds it inactive.
pub async fn deactivate_endpoint(
    store: &Store,
    tenant: &TenantId,
    endpoint_id: &str,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(rid(ENDPOINT_TABLE, endpoint_id)?.to_string())
        .map_err(|e| map_store_err("deactivate_endpoint", e))?
        .set("active", Value::from(false))
        .map_err(|e| map_store_err("deactivate_endpoint", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(eq("active", true))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("deactivate_endpoint", e))?;
    Ok(!rows.is_empty())
}

/// An outbox row.
#[derive(Debug, Clone, Deserialize)]
pub struct EventRow {
    pub id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub file: Option<String>,
    pub action: String,
    pub payload: Value,
    pub dispatched: bool,
    pub created_at: String,
}

impl EventRow {
    /// The bare event id.
    pub fn event_id(&self) -> String {
        strip_record_prefix(&self.id, EVENT_TABLE).to_owned()
    }

    /// The bare linked file id, when the link survives.
    pub fn file_id(&self) -> Option<String> {
        self.file
            .as_deref()
            .map(|raw| strip_record_prefix(raw, "file").to_owned())
    }
}

/// Fetch one outbox row by id.
pub async fn fetch_event(store: &Store, event_id: &str) -> copal_core::Result<Option<EventRow>> {
    let Some(row) = get_record(store.client(), &rid(EVENT_TABLE, event_id)?)
        .await
        .map_err(|e| map_store_err("fetch_event", e))?
    else {
        return Ok(None);
    };
    serde_json::from_value(row)
        .map(Some)
        .map_err(|e| CopalError::Store(format!("event row: {e}")))
}

/// Fetch one endpoint by id (sealed secret included, dispatcher only).
pub async fn fetch_endpoint(
    store: &Store,
    endpoint_id: &str,
) -> copal_core::Result<Option<EndpointRow>> {
    let Some(row) = get_record(store.client(), &rid(ENDPOINT_TABLE, endpoint_id)?)
        .await
        .map_err(|e| map_store_err("fetch_endpoint", e))?
    else {
        return Ok(None);
    };
    serde_json::from_value(row)
        .map(Some)
        .map_err(|e| CopalError::Store(format!("endpoint row: {e}")))
}

/// Undispatched outbox rows, oldest first.
pub async fn undispatched_events(store: &Store, limit: i64) -> copal_core::Result<Vec<EventRow>> {
    let query = Query::new()
        .select(None)
        .from_table(EVENT_TABLE)
        .map_err(|e| map_store_err("undispatched_events", e))?
        .where_(eq("dispatched", false))
        .order_by("created_at", "ASC")
        .map_err(|e| map_store_err("undispatched_events", e))?
        .limit(limit)
        .map_err(|e| map_store_err("undispatched_events", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("undispatched_events", e))
}

/// A tenant's recent events, newest first (API surface and tests).
/// `action` narrows the feed to one dotted verb.
pub async fn list_events(
    store: &Store,
    tenant: &TenantId,
    action: Option<&str>,
    limit: i64,
) -> copal_core::Result<Vec<EventRow>> {
    let mut query = Query::new()
        .select(None)
        .from_table(EVENT_TABLE)
        .map_err(|e| map_store_err("list_events", e))?
        .where_(eq("tenant_id", tenant.as_str()));
    if let Some(action) = action {
        query = query.where_(eq("action", action));
    }
    let query = query
        .order_by("created_at", "DESC")
        .map_err(|e| map_store_err("list_events", e))?
        .limit(limit)
        .map_err(|e| map_store_err("list_events", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_events", e))
}

/// The cursor one page hands the next: the last row's timestamp and
/// id, joined opaquely. Outbox rows are minted by an engine event,
/// which assigns random record ids, so the id alone carries no time
/// order; the keyset is `(created_at, id)`, with the id breaking
/// ties inside one timestamp.
pub fn event_cursor(row: &EventRow) -> String {
    format!("{}~{}", row.created_at, row.event_id())
}

/// One page of a tenant's events, keyset-paged over
/// `(created_at, id)`. Ascending from a cursor replays forward (the
/// indexer resume); the default descending pages backward through
/// history. `idx_event_tenant (tenant_id, created_at)` carries the
/// scan, and a consumer resuming from a saved cursor sees every
/// later event at least once.
pub async fn list_events_page(
    store: &Store,
    tenant: &TenantId,
    action: Option<&str>,
    limit: i64,
    cursor: Option<&str>,
    ascending: bool,
) -> copal_core::Result<Vec<EventRow>> {
    let mut query = Query::new()
        .select(None)
        .from_table(EVENT_TABLE)
        .map_err(|e| map_store_err("events_page", e))?
        .where_(eq("tenant_id", tenant.as_str()));
    if let Some(action) = action {
        query = query.where_(eq("action", action));
    }
    if let Some(cursor) = cursor {
        let (at, id) = cursor
            .split_once('~')
            .ok_or_else(|| CopalError::validation("malformed cursor"))?;
        if !at.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b':' | b'.' | b'+' | b'Z' | b'T')
        }) {
            return Err(CopalError::validation("malformed cursor"));
        }
        let rid = RecordID::<()>::new(EVENT_TABLE, id)
            .map_err(|_| CopalError::validation("malformed cursor"))?;
        let comparator = if ascending { ">" } else { "<" };
        query = query.where_str(format!(
            "(created_at {comparator} d'{at}' OR (created_at = d'{at}' AND id {comparator} {rid}))",
        ));
    }
    let direction = if ascending { "ASC" } else { "DESC" };
    let query = query
        .order_by("created_at", direction)
        .map_err(|e| map_store_err("events_page", e))?
        .order_by("id", direction)
        .map_err(|e| map_store_err("events_page", e))?
        .limit(limit)
        .map_err(|e| map_store_err("events_page", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("events_page", e))
}

/// One event by id, tenant-checked.
pub async fn get_event(
    store: &Store,
    tenant: &TenantId,
    event_id: &str,
) -> copal_core::Result<Option<EventRow>> {
    let rid = RecordID::<()>::new(EVENT_TABLE, event_id)
        .map_err(|_| CopalError::validation("malformed event id"))?;
    let query = Query::new()
        .select(None)
        .from_table(EVENT_TABLE)
        .map_err(|e| map_store_err("get_event", e))?
        .where_str(format!("id = {rid}"))
        .where_(eq("tenant_id", tenant.as_str()));
    let rows: Vec<EventRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("get_event", e))?;
    Ok(rows.into_iter().next())
}

/// A live stream of a tenant's events, narrowed to one dotted verb when
/// `action` is given. The engine applies both conditions, so the tenant
/// scope is never a check this code can forget.
pub async fn watch_events(
    store: &Store,
    tenant: &TenantId,
    action: Option<&str>,
) -> copal_core::Result<impl futures::Stream<Item = copal_core::Result<EventRow>> + Send + Unpin> {
    use futures::StreamExt as _;
    let mut conditions = vec![surql::query::Condition::from(eq(
        "tenant_id",
        tenant.as_str(),
    ))];
    if let Some(action) = action {
        conditions.push(surql::query::Condition::from(eq("action", action)));
    }
    let rows = store.watch_rows(EVENT_TABLE, conditions).await?;
    // The outbox is append-only, so a row's creation IS the event. Later
    // writes to it are bookkeeping (the webhook dispatcher marks it
    // dispatched, retention deletes it), and relaying them handed the
    // subscriber the same event again.
    Ok(rows.filter_map(|item| {
        futures::future::ready(match item {
            Ok((crate::RowChange::Created, row)) => {
                Some(serde_json::from_value::<EventRow>(row).map_err(|e| {
                    copal_core::CopalError::Store(format!(
                        "watch_events: outbox row did not decode: {e}"
                    ))
                }))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
    }))
}

/// Mark an event dispatched, exactly once. Runs AFTER its deliveries
/// exist, so a crash in between retries into idempotent conflicts.
pub async fn mark_dispatched(store: &Store, event_id: &str) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(rid(EVENT_TABLE, event_id)?.to_string())
        .map_err(|e| map_store_err("mark_dispatched", e))?
        .set("dispatched", Value::from(true))
        .map_err(|e| map_store_err("mark_dispatched", e))?
        .where_(eq("dispatched", false))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("mark_dispatched", e))?;
    Ok(!rows.is_empty())
}

/// Create the delivery for (event, endpoint). Returns false when it
/// already exists (the unique pair absorbs fan-out retries).
pub async fn create_delivery(
    store: &Store,
    tenant: &str,
    event_id: &str,
    endpoint_id: &str,
) -> copal_core::Result<bool> {
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let event_rid = rid(EVENT_TABLE, event_id)?;
    let endpoint_rid = rid(ENDPOINT_TABLE, endpoint_id)?;
    let payload = json!({ "tenant_id": tenant });
    create_record(
        store.client(),
        &rid(DELIVERY_TABLE, &id)?.to_string(),
        payload,
    )
    .await
    .map_err(|e| map_store_err("create_delivery", e))?;
    // Record links arm via UPDATE, never CREATE. The unique index on
    // the pair lands here: a duplicate arming conflicts and the caller
    // treats it as already-fanned-out, deleting the orphan shell.
    let arm = Query::new()
        .update_set(rid(DELIVERY_TABLE, &id)?.to_string())
        .map_err(|e| map_store_err("create_delivery", e))?
        .set_expr("event_row", raw(event_rid.to_string()))
        .map_err(|e| map_store_err("create_delivery", e))?
        .set_expr("endpoint", raw(endpoint_rid.to_string()))
        .map_err(|e| map_store_err("create_delivery", e))?
        .return_after();
    match query_records::<Value>(store.client(), &arm).await {
        Ok(_) => Ok(true),
        Err(err) => {
            let mapped = map_store_err("create_delivery", err);
            if matches!(mapped, CopalError::Conflict(_)) {
                let cleanup = Query::new()
                    .delete(rid(DELIVERY_TABLE, &id)?.to_string())
                    .map_err(|e| map_store_err("create_delivery", e))?;
                query_records::<Value>(store.client(), &cleanup)
                    .await
                    .map_err(|e| map_store_err("create_delivery", e))?;
                Ok(false)
            } else {
                Err(mapped)
            }
        }
    }
}

/// A delivery row.
#[derive(Debug, Clone, Deserialize)]
pub struct DeliveryRow {
    pub id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub event_row: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    pub state: String,
    pub attempts: i64,
    #[serde(default)]
    pub next_attempt_at: Option<String>,
    #[serde(default)]
    pub last_status: Option<i64>,
    pub created_at: String,
}

impl DeliveryRow {
    /// The bare delivery id.
    pub fn delivery_id(&self) -> String {
        strip_record_prefix(&self.id, DELIVERY_TABLE).to_owned()
    }

    /// The bare linked event id.
    pub fn event_id(&self) -> Option<String> {
        self.event_row
            .as_deref()
            .map(|raw| strip_record_prefix(raw, EVENT_TABLE).to_owned())
    }

    /// The bare linked endpoint id.
    pub fn endpoint_id(&self) -> Option<String> {
        self.endpoint
            .as_deref()
            .map(|raw| strip_record_prefix(raw, ENDPOINT_TABLE).to_owned())
    }
}

/// Pending deliveries that are due (first attempt or backoff elapsed),
/// oldest first.
pub async fn due_deliveries(store: &Store, limit: i64) -> copal_core::Result<Vec<DeliveryRow>> {
    let query = Query::new()
        .select(None)
        .from_table(DELIVERY_TABLE)
        .map_err(|e| map_store_err("due_deliveries", e))?
        .where_(eq("state", "pending"))
        .where_str("(next_attempt_at IS NONE OR next_attempt_at <= time::now())")
        .order_by("created_at", "ASC")
        .map_err(|e| map_store_err("due_deliveries", e))?
        .limit(limit)
        .map_err(|e| map_store_err("due_deliveries", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("due_deliveries", e))
}

/// Claim one delivery for an attempt. Refuses when another deliverer
/// holds a live claim or the row settled.
pub async fn claim_delivery(
    store: &Store,
    delivery_id: &str,
    owner: &str,
    lease_secs: u32,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(rid(DELIVERY_TABLE, delivery_id)?.to_string())
        .map_err(|e| map_store_err("claim_delivery", e))?
        .set("claim_owner", Value::from(owner))
        .map_err(|e| map_store_err("claim_delivery", e))?
        .set_expr(
            "claim_expires_at",
            raw(format!("time::now() + {lease_secs}s")),
        )
        .map_err(|e| map_store_err("claim_delivery", e))?
        .where_(eq("state", "pending"))
        .where_str("(claim_owner IS NONE OR claim_expires_at < time::now())")
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("claim_delivery", e))?;
    Ok(!rows.is_empty())
}

/// Settle a claimed delivery as delivered.
pub async fn complete_delivery(
    store: &Store,
    delivery_id: &str,
    status: i64,
) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(DELIVERY_TABLE, delivery_id)?.to_string())
        .map_err(|e| map_store_err("complete_delivery", e))?
        .set("state", Value::from("delivered"))
        .map_err(|e| map_store_err("complete_delivery", e))?
        .set("last_status", Value::from(status))
        .map_err(|e| map_store_err("complete_delivery", e))?
        .set_expr("claim_owner", raw("NONE"))
        .map_err(|e| map_store_err("complete_delivery", e))?
        .set_expr("claim_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("complete_delivery", e))?
        .where_(eq("state", "pending"))
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("complete_delivery", e))?;
    Ok(())
}

/// Record a failed attempt: bump the counter, either schedule the next
/// try (exponential backoff) or settle as failed past the cap.
pub async fn record_attempt_failure(
    store: &Store,
    delivery_id: &str,
    attempts_now: i64,
    status: Option<i64>,
) -> copal_core::Result<()> {
    let next = attempts_now + 1;
    let mut query = Query::new()
        .update_set(rid(DELIVERY_TABLE, delivery_id)?.to_string())
        .map_err(|e| map_store_err("record_attempt_failure", e))?
        .set("attempts", Value::from(next))
        .map_err(|e| map_store_err("record_attempt_failure", e))?
        .set_expr("claim_owner", raw("NONE"))
        .map_err(|e| map_store_err("record_attempt_failure", e))?
        .set_expr("claim_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("record_attempt_failure", e))?;
    if let Some(status) = status {
        query = query
            .set("last_status", Value::from(status))
            .map_err(|e| map_store_err("record_attempt_failure", e))?;
    }
    query = if next >= MAX_ATTEMPTS {
        query
            .set("state", Value::from("failed"))
            .map_err(|e| map_store_err("record_attempt_failure", e))?
    } else {
        // 30s, 60s, 120s, ... capped at one hour.
        let backoff = (30i64 << (next - 1).min(7)).min(3_600);
        query
            .set_expr("next_attempt_at", raw(format!("time::now() + {backoff}s")))
            .map_err(|e| map_store_err("record_attempt_failure", e))?
    };
    let query = query.where_(eq("state", "pending")).return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("record_attempt_failure", e))?;
    Ok(())
}

/// A tenant's deliveries, newest first (API surface and tests).
/// `endpoint` narrows to one webhook's history, `state` to one
/// outcome.
pub async fn list_deliveries(
    store: &Store,
    tenant: &TenantId,
    endpoint: Option<&str>,
    state: Option<&str>,
    limit: i64,
) -> copal_core::Result<Vec<DeliveryRow>> {
    let mut query = Query::new()
        .select(None)
        .from_table(DELIVERY_TABLE)
        .map_err(|e| map_store_err("list_deliveries", e))?
        .where_(eq("tenant_id", tenant.as_str()));
    if let Some(endpoint) = endpoint {
        let target = rid(ENDPOINT_TABLE, endpoint)?;
        query = query.where_str(format!("endpoint = {target}"));
    }
    if let Some(state) = state {
        query = query.where_(eq("state", state));
    }
    let query = query
        .order_by("created_at", "DESC")
        .map_err(|e| map_store_err("list_deliveries", e))?
        .limit(limit)
        .map_err(|e| map_store_err("list_deliveries", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_deliveries", e))
}

/// Test support: pull a delivery's next attempt into the past so a
/// pass retries it now. Never called by production code.
pub async fn force_due_for_test(store: &Store, delivery_id: &str) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(DELIVERY_TABLE, delivery_id)?.to_string())
        .map_err(|e| map_store_err("force_due", e))?
        .set_expr("next_attempt_at", raw("time::now() - 1h"))
        .map_err(|e| map_store_err("force_due", e))?
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("force_due", e))?;
    Ok(())
}

/// Emit one application-side outbox event. The engine mints file
/// lifecycle events itself; compliance actions happen in handlers,
/// so they write here, land in the same feed, and fan out through
/// the same webhook dispatcher. The record link is set after the
/// create because links cannot ride a JSON payload.
pub async fn emit_event(
    store: &Store,
    tenant: &TenantId,
    file: Option<&str>,
    action: &str,
    payload: Value,
) -> copal_core::Result<()> {
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let event_rid = rid(EVENT_TABLE, &id)?;
    let body = serde_json::json!({
        "tenant_id": tenant.as_str(),
        "action": action,
        "payload": payload,
    });
    create_record(store.client(), &event_rid.to_string(), body)
        .await
        .map_err(|e| map_store_err("emit_event", e))?;
    if let Some(file_id) = file {
        let file_rid = rid("file", file_id)?;
        let update = Query::new()
            .update_set(event_rid.to_string())
            .map_err(|e| map_store_err("emit_event", e))?
            .set_expr("file", raw(file_rid.to_string()))
            .map_err(|e| map_store_err("emit_event", e))?
            .return_after();
        query_records::<Value>(store.client(), &update)
            .await
            .map_err(|e| map_store_err("emit_event", e))?;
    }
    Ok(())
}
