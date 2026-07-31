//! Flow repository: runs, claims, and the step journal.
//!
//! The claim protocol mirrors uploads: a CAS with a server-computed
//! lease is the only door into `running`, expired leases are stealable
//! by the reaper (back to `pending`, journal intact), and completion
//! clears the lease with the terminal status atomically.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::{eq, is_none, is_not_none};
use surql::types::RecordID;

use copal_core::{CopalError, FileId, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const RUN_TABLE: &str = "workflow_run";
const STEP_TABLE: &str = "workflow_step";
const LEASE_EXPIRED: &str = "lease_expires_at < time::now()";

fn run_rid(run_id: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(RUN_TABLE, run_id).map_err(|e| map_store_err("run id", e))
}

/// A run row.
#[derive(Debug, Clone, Deserialize)]
pub struct RunRow {
    pub id: String,
    pub tenant_id: String,
    pub workflow_key: String,
    pub status: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub run_error: Option<String>,
    /// Subject file link, serialized as a record id string.
    #[serde(default)]
    pub file: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub ended_at: Option<String>,
}

impl RunRow {
    /// The bare run id (record prefix stripped).
    pub fn run_id(&self) -> String {
        strip_record_prefix(&self.id, RUN_TABLE).to_owned()
    }

    /// The subject file id, when a subject link is armed.
    pub fn file_id(&self) -> Option<copal_core::Result<FileId>> {
        self.file
            .as_deref()
            .map(|raw| FileId::parse(strip_record_prefix(raw, "file")))
    }
}

/// A journal row.
#[derive(Debug, Clone, Deserialize)]
pub struct StepRow {
    pub step_key: String,
    pub attempt: i64,
    pub status: String,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub step_error: Option<String>,
    pub started_at: String,
    #[serde(default)]
    pub ended_at: Option<String>,
}

/// Enqueue a run in `pending`. An idempotency-key replay returns the
/// original run id with `created == false`, mirroring file creation.
pub async fn enqueue(
    store: &Store,
    tenant: &TenantId,
    workflow_key: &str,
    input: Value,
    subject: Option<&FileId>,
    idempotency_key: Option<&str>,
) -> copal_core::Result<(String, bool)> {
    let run_id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let mut payload = serde_json::Map::new();
    payload.insert("tenant_id".into(), json!(tenant.as_str()));
    payload.insert("workflow_key".into(), json!(workflow_key));
    payload.insert(
        "input".into(),
        if input.is_null() { json!({}) } else { input },
    );
    if let Some(key) = idempotency_key {
        payload.insert("idempotency_key".into(), json!(key));
    }
    match create_record(
        store.client(),
        &run_rid(&run_id)?.to_string(),
        Value::Object(payload),
    )
    .await
    {
        Ok(_) => {}
        Err(err) => {
            let mapped = map_store_err("enqueue", err);
            if let (CopalError::Conflict(_), Some(key)) = (&mapped, idempotency_key) {
                if let Some(existing) = find_by_idempotency_key(store, tenant, key).await? {
                    return Ok((existing.run_id(), false));
                }
            }
            return Err(mapped);
        }
    }
    // Arm the subject link when present; a crash between create and arm
    // leaves a runnable run without a subject pointer, which the
    // workflow reads from input anyway.
    if let Some(file) = subject {
        let file_rid =
            RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("enqueue", e))?;
        let query = Query::new()
            .update_set(run_rid(&run_id)?.to_string())
            .map_err(|e| map_store_err("enqueue", e))?
            .set_expr("file", raw(file_rid.to_string()))
            .map_err(|e| map_store_err("enqueue", e))?
            .return_after();
        query_records::<Value>(store.client(), &query)
            .await
            .map_err(|e| map_store_err("enqueue", e))?;
    }
    Ok((run_id, true))
}

async fn find_by_idempotency_key(
    store: &Store,
    tenant: &TenantId,
    key: &str,
) -> copal_core::Result<Option<RunRow>> {
    let query = Query::new()
        .select(None)
        .from_table(RUN_TABLE)
        .map_err(|e| map_store_err("find_run", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(eq("idempotency_key", key))
        .limit(1)
        .map_err(|e| map_store_err("find_run", e))?;
    let mut rows: Vec<RunRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("find_run", e))?;
    Ok(rows.pop())
}

/// Fetch one run, tenant-scoped.
pub async fn get_run(
    store: &Store,
    tenant: &TenantId,
    run_id: &str,
) -> copal_core::Result<Option<RunRow>> {
    let Some(row) = get_record(store.client(), &run_rid(run_id)?)
        .await
        .map_err(|e| map_store_err("get_run", e))?
    else {
        return Ok(None);
    };
    let row: RunRow = serde_json::from_value(row)
        .map_err(|e| CopalError::Store(format!("run row shape: {e}")))?;
    if row.tenant_id != tenant.as_str() {
        return Ok(None);
    }
    Ok(Some(row))
}

/// Keyset position for run listings, mirroring the file listing shape.
#[derive(Debug, Clone)]
pub struct RunListPosition {
    pub created_at: String,
    pub id: String,
}

/// List a tenant's runs, keyset-paginated; newest first unless
/// `ascending`. A status filter rides idx_run_ops
/// (tenant_id, status, created_at).
pub async fn list_runs(
    store: &Store,
    tenant: &TenantId,
    limit: i64,
    after: Option<&RunListPosition>,
    ascending: bool,
    status: Option<&str>,
) -> copal_core::Result<Vec<RunRow>> {
    let (cmp, dir) = if ascending {
        (">", "ASC")
    } else {
        ("<", "DESC")
    };
    let mut query = Query::new()
        .select(None)
        .from_table(RUN_TABLE)
        .map_err(|e| map_store_err("list_runs", e))?
        .where_(eq("tenant_id", tenant.as_str()));
    if let Some(status) = status {
        query = query.where_(eq("status", status));
    }
    if let Some(position) = after {
        let position_rid =
            run_rid(&position.id).map_err(|e| CopalError::validation(format!("cursor: {e}")))?;
        query = query.where_str(format!(
            "(created_at {cmp} d'{ts}' OR (created_at = d'{ts}' AND id {cmp} {id}))",
            ts = position.created_at,
            id = position_rid,
        ));
    }
    let query = query
        .order_by("created_at", dir)
        .map_err(|e| map_store_err("list_runs", e))?
        .order_by("id", dir)
        .map_err(|e| map_store_err("list_runs", e))?
        .limit(limit)
        .map_err(|e| map_store_err("list_runs", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_runs", e))
}

/// Claim the oldest pending run for `owner`, or `None` when the queue
/// is empty. Select-then-CAS: losing the CAS to a rival just means
/// trying the next candidate.
pub async fn claim_next_pending(
    store: &Store,
    owner: &str,
    lease_secs: u32,
) -> copal_core::Result<Option<RunRow>> {
    for _ in 0..8 {
        let find = Query::new()
            .select(None)
            .from_table(RUN_TABLE)
            .map_err(|e| map_store_err("claim", e))?
            .where_(eq("status", "pending"))
            .order_by("created_at", "ASC")
            .map_err(|e| map_store_err("claim", e))?
            .limit(1)
            .map_err(|e| map_store_err("claim", e))?;
        let mut candidates: Vec<RunRow> = query_records(store.client(), &find)
            .await
            .map_err(|e| map_store_err("claim", e))?;
        let Some(candidate) = candidates.pop() else {
            return Ok(None);
        };

        let query = Query::new()
            .update_set(run_rid(&candidate.run_id())?.to_string())
            .map_err(|e| map_store_err("claim", e))?
            .set("status", Value::from("running"))
            .map_err(|e| map_store_err("claim", e))?
            .set("lease_owner", Value::from(owner))
            .map_err(|e| map_store_err("claim", e))?
            .set_expr(
                "lease_expires_at",
                raw(format!("time::now() + {lease_secs}s")),
            )
            .map_err(|e| map_store_err("claim", e))?
            .set_expr("started_at", raw("time::now()"))
            .map_err(|e| map_store_err("claim", e))?
            .where_(eq("status", "pending"))
            .return_after();
        let rows: Vec<RunRow> = query_records(store.client(), &query)
            .await
            .map_err(|e| map_store_err("claim", e))?;
        if let Some(claimed) = rows.into_iter().next() {
            return Ok(Some(claimed));
        }
        // Lost the race for this candidate; try the next.
    }
    Ok(None)
}

/// Finish a run with a terminal status, clearing the lease atomically.
pub async fn finish_run(
    store: &Store,
    run_id: &str,
    status: &str,
    output: Option<&Value>,
    error: Option<&str>,
) -> copal_core::Result<()> {
    let mut query = Query::new()
        .update_set(run_rid(run_id)?.to_string())
        .map_err(|e| map_store_err("finish_run", e))?
        .set("status", Value::from(status))
        .map_err(|e| map_store_err("finish_run", e))?
        .set_expr("lease_owner", raw("NONE"))
        .map_err(|e| map_store_err("finish_run", e))?
        .set_expr("lease_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("finish_run", e))?
        .set_expr("ended_at", raw("time::now()"))
        .map_err(|e| map_store_err("finish_run", e))?;
    if let Some(output) = output {
        query = query
            .set("output", output.clone())
            .map_err(|e| map_store_err("finish_run", e))?;
    }
    if let Some(error) = error {
        query = query
            .set("run_error", Value::from(error))
            .map_err(|e| map_store_err("finish_run", e))?;
    }
    let query = query.where_(eq("status", "running")).return_after();
    let rows: Vec<RunRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("finish_run", e))?;
    if rows.is_empty() {
        return Err(CopalError::conflict(format!(
            "run {run_id} is not running (lost lease or already terminal)",
        )));
    }
    Ok(())
}

/// Retry a FAILED run: CAS it back to `pending` with its journal
/// intact. Returns false when the run is not in `failed` (already
/// retried, still running, or completed) — the guard rides the UPDATE,
/// so two racing retries serialize here.
pub async fn retry_failed(store: &Store, run_id: &str) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(run_rid(run_id)?.to_string())
        .map_err(|e| map_store_err("retry", e))?
        .set("status", Value::from("pending"))
        .map_err(|e| map_store_err("retry", e))?
        .set_expr("lease_owner", raw("NONE"))
        .map_err(|e| map_store_err("retry", e))?
        .set_expr("lease_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("retry", e))?
        .set_expr("run_error", raw("NONE"))
        .map_err(|e| map_store_err("retry", e))?
        .set_expr("ended_at", raw("NONE"))
        .map_err(|e| map_store_err("retry", e))?
        .where_(eq("status", "failed"))
        .return_after();
    let rows: Vec<RunRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("retry", e))?;
    Ok(!rows.is_empty())
}

/// Test/ops support: force a terminal or stuck run back to `pending`
/// so it can be claimed again with its journal intact.
pub async fn requeue(store: &Store, run_id: &str) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(run_rid(run_id)?.to_string())
        .map_err(|e| map_store_err("requeue", e))?
        .set("status", Value::from("pending"))
        .map_err(|e| map_store_err("requeue", e))?
        .set_expr("lease_owner", raw("NONE"))
        .map_err(|e| map_store_err("requeue", e))?
        .set_expr("lease_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("requeue", e))?
        .set_expr("run_error", raw("NONE"))
        .map_err(|e| map_store_err("requeue", e))?
        .return_after();
    let rows: Vec<RunRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("requeue", e))?;
    if rows.is_empty() {
        return Err(CopalError::not_found(format!("run {run_id}")));
    }
    Ok(())
}

/// Reap expired `running` claims back to `pending` — the journal makes
/// the re-execution skip completed steps, so a crashed worker costs a
/// lease TTL, not correctness.
pub async fn reap_expired_runs(store: &Store) -> copal_core::Result<u64> {
    let query = Query::new()
        .update_set(RUN_TABLE)
        .map_err(|e| map_store_err("reap_runs", e))?
        .set("status", Value::from("pending"))
        .map_err(|e| map_store_err("reap_runs", e))?
        .set_expr("lease_owner", raw("NONE"))
        .map_err(|e| map_store_err("reap_runs", e))?
        .set_expr("lease_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("reap_runs", e))?
        .where_(eq("status", "running"))
        .where_(is_not_none("lease_expires_at"))
        .where_str(LEASE_EXPIRED)
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("reap_runs", e))?;
    Ok(rows.len() as u64)
}

/// The completed-step journal for a run, keyed by step for replay.
pub async fn completed_steps(
    store: &Store,
    run_id: &str,
) -> copal_core::Result<std::collections::BTreeMap<String, Value>> {
    let query = Query::new()
        .select(None)
        .from_table(STEP_TABLE)
        .map_err(|e| map_store_err("journal", e))?
        .where_(eq("run_key", run_id))
        .where_(eq("status", "completed"))
        .order_by("started_at", "ASC")
        .map_err(|e| map_store_err("journal", e))?;
    let rows: Vec<StepRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("journal", e))?;
    Ok(rows
        .into_iter()
        .map(|row| (row.step_key, row.output.unwrap_or(Value::Null)))
        .collect())
}

/// List every journal row for a run, oldest first.
pub async fn list_steps(store: &Store, run_id: &str) -> copal_core::Result<Vec<StepRow>> {
    let query = Query::new()
        .select(None)
        .from_table(STEP_TABLE)
        .map_err(|e| map_store_err("list_steps", e))?
        .where_(eq("run_key", run_id))
        .order_by("started_at", "ASC")
        .map_err(|e| map_store_err("list_steps", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_steps", e))
}

/// Open a step attempt: `attempt` must be one past the greatest already
/// journaled (the unique index enforces it against races). Returns the
/// step row id for completion.
pub async fn open_step(
    store: &Store,
    run_id: &str,
    step_key: &str,
    attempt: i64,
) -> copal_core::Result<String> {
    let step_id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid = RecordID::<()>::new(STEP_TABLE, step_id.as_str())
        .map_err(|e| map_store_err("open_step", e))?;
    let payload = json!({
        "run_key": run_id,
        "step_key": step_key,
        "attempt": attempt,
    });
    create_record(store.client(), &rid.to_string(), payload)
        .await
        .map_err(|e| map_store_err("open_step", e))?;
    // Arm the run link for graph-style queries; the hot path reads
    // run_key.
    let run = run_rid(run_id)?;
    let query = Query::new()
        .update_set(rid.to_string())
        .map_err(|e| map_store_err("open_step", e))?
        .set_expr("run", raw(run.to_string()))
        .map_err(|e| map_store_err("open_step", e))?
        .where_(is_none("ended_at"))
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("open_step", e))?;
    Ok(step_id)
}

/// Record a step outcome.
pub async fn close_step(
    store: &Store,
    step_id: &str,
    status: &str,
    output: Option<&Value>,
    error: Option<&str>,
) -> copal_core::Result<()> {
    let rid =
        RecordID::<()>::new(STEP_TABLE, step_id).map_err(|e| map_store_err("close_step", e))?;
    let mut query = Query::new()
        .update_set(rid.to_string())
        .map_err(|e| map_store_err("close_step", e))?
        .set("status", Value::from(status))
        .map_err(|e| map_store_err("close_step", e))?
        .set_expr("ended_at", raw("time::now()"))
        .map_err(|e| map_store_err("close_step", e))?;
    if let Some(output) = output {
        query = query
            .set("output", output.clone())
            .map_err(|e| map_store_err("close_step", e))?;
    }
    if let Some(error) = error {
        query = query
            .set("step_error", Value::from(error))
            .map_err(|e| map_store_err("close_step", e))?;
    }
    let query = query.return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("close_step", e))?;
    Ok(())
}

/// Highest journaled attempt for a step (0 when none).
pub async fn last_attempt(store: &Store, run_id: &str, step_key: &str) -> copal_core::Result<i64> {
    let query = Query::new()
        .select(None)
        .from_table(STEP_TABLE)
        .map_err(|e| map_store_err("last_attempt", e))?
        .where_(eq("run_key", run_id))
        .where_(eq("step_key", step_key))
        .order_by("attempt", "DESC")
        .map_err(|e| map_store_err("last_attempt", e))?
        .limit(1)
        .map_err(|e| map_store_err("last_attempt", e))?;
    let mut rows: Vec<StepRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("last_attempt", e))?;
    Ok(rows.pop().map(|row| row.attempt).unwrap_or(0))
}
