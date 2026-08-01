//! API-key repository: mint rows, fetch for verification, revoke.
//!
//! The verification path is a record fetch by key id (the token
//! carries it): no index scan, no tenant input, and the caller does
//! the constant-time hash compare. Listing never returns hashes.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::{eq, is_none};
use surql::types::RecordID;

use copal_core::{CopalError, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const TABLE: &str = "api_key";

fn key_rid(key_id: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, key_id).map_err(|e| map_store_err("key id", e))
}

/// A key row as verification sees it.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyRow {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub key_hash: String,
    /// Comma-joined scope names; empty means unscoped.
    #[serde(default)]
    pub scopes: String,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub revoked_at: Option<String>,
    pub created_at: String,
}

impl ApiKeyRow {
    /// The bare key id (record prefix stripped).
    pub fn key_id(&self) -> String {
        strip_record_prefix(&self.id, TABLE).to_owned()
    }
}

/// Create a key row. A duplicate (tenant, name) surfaces as `Conflict`
/// via the unique index.
pub async fn create_key(
    store: &Store,
    tenant: &TenantId,
    name: &str,
    key_id: &str,
    key_hash: &str,
    scopes: &[String],
) -> copal_core::Result<ApiKeyRow> {
    if name.trim().is_empty() {
        return Err(CopalError::validation("key name must not be empty"));
    }
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "name": name,
        "key_hash": key_hash,
        "scopes": scopes.join(","),
    });
    let created = create_record(store.client(), &key_rid(key_id)?.to_string(), payload)
        .await
        .map_err(|e| map_store_err("create_key", e))?;
    let row = created
        .record
        .ok_or_else(|| CopalError::Store("create returned no record".into()))?;
    serde_json::from_value(row).map_err(|e| CopalError::Store(format!("key row shape: {e}")))
}

/// Arm a key's expiry, server-side so client clock skew cannot
/// lengthen a lifetime. Runs after create; a caller that fails here
/// revokes the fresh key rather than leaving an unexpiring one.
pub async fn arm_key_expiry(store: &Store, key_id: &str, ttl_secs: u32) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(key_rid(key_id)?.to_string())
        .map_err(|e| map_store_err("arm_key_expiry", e))?
        .set_expr("expires_at", raw(format!("time::now() + {ttl_secs}s")))
        .map_err(|e| map_store_err("arm_key_expiry", e))?
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("arm_key_expiry", e))?;
    if rows.is_empty() {
        return Err(CopalError::Store("expiry armed no row".into()));
    }
    Ok(())
}

/// Test support: push a key's expiry into the past, so expiry
/// refusals are testable without sleeping. Never called by production
/// code.
pub async fn force_expire_key_for_test(store: &Store, key_id: &str) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(key_rid(key_id)?.to_string())
        .map_err(|e| map_store_err("force_expire_key", e))?
        .set_expr("expires_at", raw("time::now() - 1h"))
        .map_err(|e| map_store_err("force_expire_key", e))?
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("force_expire_key", e))?;
    Ok(())
}

/// Fetch one key by id ONLY if it has not expired; the engine's clock
/// decides, matching the grant discipline. An expired key reads as
/// absent, which the caller's timing burn already makes
/// indistinguishable from an unknown one.
pub async fn fetch_live_key(store: &Store, key_id: &str) -> copal_core::Result<Option<ApiKeyRow>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("fetch_live_key", e))?
        .where_str(format!("id = {}", key_rid(key_id)?))
        .where_str("(expires_at IS NONE OR expires_at > time::now())");
    let mut rows: Vec<ApiKeyRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("fetch_live_key", e))?;
    Ok(rows.pop())
}

/// Fetch one key by id, for verification. Unscoped on purpose: the
/// tenant comes OUT of the row; the caller compares hashes.
pub async fn fetch_key(store: &Store, key_id: &str) -> copal_core::Result<Option<ApiKeyRow>> {
    let Some(row) = get_record(store.client(), &key_rid(key_id)?)
        .await
        .map_err(|e| map_store_err("fetch_key", e))?
    else {
        return Ok(None);
    };
    serde_json::from_value(row)
        .map(Some)
        .map_err(|e| CopalError::Store(format!("key row shape: {e}")))
}

/// List a tenant's keys; hashes never leave the repo here.
pub async fn list_keys(store: &Store, tenant: &TenantId) -> copal_core::Result<Vec<Value>> {
    let query = Query::new()
        .select(Some(vec![
            "id".to_owned(),
            "name".to_owned(),
            "scopes".to_owned(),
            "expires_at".to_owned(),
            "created_at".to_owned(),
            "revoked_at".to_owned(),
        ]))
        .from_table(TABLE)
        .map_err(|e| map_store_err("list_keys", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .order_by("created_at", "ASC")
        .map_err(|e| map_store_err("list_keys", e))?;
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_keys", e))?;
    Ok(rows
        .into_iter()
        .map(|mut row| {
            if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                let bare = strip_record_prefix(id, TABLE).to_owned();
                row["id"] = json!(bare);
            }
            row
        })
        .collect())
}

/// Append one audit event. The row is engine-immutable (THROW on
/// UPDATE/DELETE); callers pass `admin` or the tenant id as the actor.
pub async fn record_audit(
    store: &Store,
    tenant: &TenantId,
    actor: &str,
    action: &str,
    subject: &str,
    origin: Option<&str>,
    detail: Option<Value>,
) -> copal_core::Result<()> {
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid =
        RecordID::<()>::new("audit_event", id.as_str()).map_err(|e| map_store_err("audit", e))?;
    let mut payload = serde_json::Map::new();
    payload.insert("tenant_id".into(), json!(tenant.as_str()));
    payload.insert("actor".into(), json!(actor));
    payload.insert("action".into(), json!(action));
    payload.insert("subject".into(), json!(subject));
    if let Some(origin) = origin {
        payload.insert("origin".into(), json!(origin));
    }
    if let Some(detail) = detail {
        payload.insert("detail".into(), detail);
    }
    create_record(store.client(), &rid.to_string(), Value::Object(payload))
        .await
        .map_err(|e| map_store_err("audit", e))?;
    Ok(())
}

/// List a tenant's audit trail, newest first, bounded.
pub async fn list_audit(
    store: &Store,
    tenant: &TenantId,
    limit: i64,
) -> copal_core::Result<Vec<Value>> {
    let query = Query::new()
        .select(None)
        .from_table("audit_event")
        .map_err(|e| map_store_err("audit", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .order_by("created_at", "DESC")
        .map_err(|e| map_store_err("audit", e))?
        .limit(limit)
        .map_err(|e| map_store_err("audit", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("audit", e))
}

/// Test support: attempt to rewrite an audit event, so integration
/// tests can prove the engine-level THROW rather than trusting the
/// schema text. Never called by production code.
pub async fn tamper_audit_for_test(store: &Store, tenant: &TenantId) -> copal_core::Result<()> {
    #[derive(serde::Deserialize)]
    struct IdRow {
        id: String,
    }
    let find = Query::new()
        .select(Some(vec!["id".to_owned()]))
        .from_table("audit_event")
        .map_err(|e| map_store_err("tamper", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .limit(1)
        .map_err(|e| map_store_err("tamper", e))?;
    let rows: Vec<IdRow> = query_records(store.client(), &find)
        .await
        .map_err(|e| map_store_err("tamper", e))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| CopalError::not_found("audit event"))?;
    let bare = strip_record_prefix(&row.id, "audit_event");
    let rid = RecordID::<()>::new("audit_event", bare).map_err(|e| map_store_err("tamper", e))?;
    let query = Query::new()
        .update_set(rid.to_string())
        .map_err(|e| map_store_err("tamper", e))?
        .set("action", Value::from("forged"))
        .map_err(|e| map_store_err("tamper", e))?
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tamper", e))?;
    Ok(())
}

/// Revoke a key, tenant-scoped; idempotent-safe via the NONE guard.
/// Returns whether this call performed the revocation.
pub async fn revoke_key(
    store: &Store,
    tenant: &TenantId,
    key_id: &str,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(key_rid(key_id)?.to_string())
        .map_err(|e| map_store_err("revoke_key", e))?
        .set_expr("revoked_at", raw("time::now()"))
        .map_err(|e| map_store_err("revoke_key", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(is_none("revoked_at"))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("revoke_key", e))?;
    Ok(!rows.is_empty())
}
