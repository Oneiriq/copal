//! Blob repository: content-addressed rows keyed by digest.
//!
//! The record id is the digest (`blob:<sha256>`), so deduplication is a
//! CREATE that either succeeds (first sighting) or collides (seen
//! before) — and the collision is the happy path, not a failure.
//!
//! Reference counting is DERIVED, not incremented. An increment written
//! before the file link commits drifts upward on a crash-and-retry; an
//! undercount would let garbage collection delete live data. So the
//! authoritative count is the number of inbound `file.blob` links
//! (served by `idx_file_blob`), computed at sweep time; the stored
//! `refcount` column is an advisory cache the sweep refreshes.

use serde_json::json;

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::types::operators::is_none;
use surql::types::RecordID;

use copal_core::ContentDigest;

use crate::dto::map_store_err;
use crate::store::Store;

const TABLE: &str = "blob";

fn rid(digest: &ContentDigest) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, digest.as_str()).map_err(|e| map_store_err("blob id", e))
}

/// Ensure a blob row exists for `digest`. Idempotent: replaying after a
/// crash or racing another uploader of the same content is a no-op.
pub async fn record_sighting(
    store: &Store,
    digest: &ContentDigest,
    size_bytes: u64,
    store_key: &str,
    storage_path: &str,
) -> copal_core::Result<()> {
    let payload = json!({
        "digest": digest.as_str(),
        "size_bytes": size_bytes,
        "store_key": store_key,
        "storage_path": storage_path,
        "refcount": 0,
    });
    match create_record(store.client(), &rid(digest)?.to_string(), payload).await {
        Ok(_) => Ok(()),
        Err(err) => {
            // Duplicate id: the content is already registered. Identical
            // digest implies identical size and address, so there is
            // nothing to reconcile.
            let text = err.to_string();
            if text.contains("already exists") || text.contains("already contains") {
                Ok(())
            } else {
                Err(map_store_err("record_sighting", err))
            }
        }
    }
}

/// Count the live files referencing `digest` — the authoritative
/// reference count, served by `idx_file_blob`.
pub async fn recount_inbound_links(
    store: &Store,
    digest: &ContentDigest,
) -> copal_core::Result<i64> {
    let blob_target = rid(digest)?.to_string();
    let query = Query::new()
        .select(Some(vec!["count()".to_owned()]))
        .from_table("file")
        .map_err(|e| map_store_err("recount", e))?
        // Record equality against a literal; no quoting operator can
        // express a record right-hand side, hence the fragment.
        .where_str(format!("blob = {blob_target}"))
        .where_(is_none("deleted_at"))
        .group_all();
    let rows: Vec<serde_json::Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("recount", e))?;
    Ok(rows
        .first()
        .and_then(|r| r.get("count"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0))
}

/// Fetch a blob row's storage location, if the content is known.
pub async fn get_location(
    store: &Store,
    digest: &ContentDigest,
) -> copal_core::Result<Option<(String, String)>> {
    let Some(row) = get_record(store.client(), &rid(digest)?)
        .await
        .map_err(|e| map_store_err("get_location", e))?
    else {
        return Ok(None);
    };
    let store_key = row
        .get("store_key")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    let storage_path = row
        .get("storage_path")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    Ok(Some((store_key, storage_path)))
}
