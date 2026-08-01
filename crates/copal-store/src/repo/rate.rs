//! The shared consumption ledger, for deployments with more than one
//! process.
//!
//! The in-memory ledger gives each replica its own budget, which
//! quietly multiplies the declared numbers by the fleet size. This
//! one keeps the count in the database every replica already shares,
//! with the same discipline as every other counter here: the check
//! and the increment are one guarded statement, so racing replicas
//! serialize instead of overspending.

use serde_json::Value;
use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::query::expressions::raw;
use surql::types::RecordID;

use copal_core::CopalError;

use crate::dto::map_store_err;
use crate::Store;

const TABLE: &str = "rate_window";

fn rid(key: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, key).map_err(|e| map_store_err("rate_window", e))
}

/// Make the window row exist. Racing creators collide on the record
/// id and the loser's conflict is swallowed: existence is the goal,
/// whoever won.
pub async fn ensure_window(store: &Store, key: &str, minute: u64) -> copal_core::Result<()> {
    let payload = serde_json::json!({ "used": 0, "minute": minute });
    match create_record(store.client(), &rid(key)?.to_string(), payload).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let text = err.to_string();
            if text.contains("already exists") {
                Ok(())
            } else {
                Err(map_store_err("ensure_window", err))
            }
        }
    }
}

/// Add `units` to the window if the budget still holds. One guarded
/// statement: an admitted charge and its check cannot interleave with
/// another replica's, and a refusal spends nothing.
pub async fn try_charge(
    store: &Store,
    key: &str,
    units: u64,
    per_minute: u64,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(rid(key)?.to_string())
        .map_err(|e| map_store_err("try_charge", e))?
        .set_expr("used", raw(format!("used + {units}")))
        .map_err(|e| map_store_err("try_charge", e))?
        .where_str(format!("used + {units} <= {per_minute}"))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("try_charge", e))?;
    Ok(!rows.is_empty())
}

/// Drop windows older than `before_minute`. The ledger only ever
/// reads the current minute, so anything older is done counting.
pub async fn cleanup_windows(store: &Store, before_minute: u64) -> copal_core::Result<u64> {
    let query = Query::new()
        .delete(TABLE)
        .map_err(|e| map_store_err("cleanup_windows", e))?
        .where_str(format!("minute < {before_minute}"))
        .return_before();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("cleanup_windows", e))?;
    Ok(rows.len() as u64)
}

/// Reject keys that would break out of the record id. The bucket is
/// server-built (class names and key ids), so this is a debug-time
/// tripwire rather than input validation.
pub fn window_key(bucket: &str, minute: u64) -> copal_core::Result<String> {
    if bucket.is_empty() {
        return Err(CopalError::validation("rate bucket must not be empty"));
    }
    Ok(format!("{bucket}|{minute}"))
}
