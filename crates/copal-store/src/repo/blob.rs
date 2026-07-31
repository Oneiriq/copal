//! Blob repository: content-addressed rows keyed by digest.
//!
//! The record id is the digest (`blob:<sha256>`), so deduplication is a
//! CREATE that either succeeds (first sighting, refcount 1) or collides
//! (seen before, refcount incremented atomically). No read-then-write
//! window.

use serde_json::json;

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::field;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{ContentDigest, CopalError};

use crate::dto::map_store_err;
use crate::store::Store;

const TABLE: &str = "blob";

fn rid(digest: &ContentDigest) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, digest.as_str()).map_err(|e| map_store_err("blob id", e))
}

/// Record a sighting of `digest`: create on first upload, atomic
/// refcount increment on every subsequent one.
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
        "refcount": 1,
    });
    match create_record(store.client(), &rid(digest)?.to_string(), payload).await {
        Ok(_) => Ok(()),
        Err(err) => {
            // A duplicate id means the content already exists — the happy
            // dedupe path, not a failure.
            let text = err.to_string();
            if text.contains("already exists") || text.contains("already contains") {
                increment_refcount(store, digest, 1).await
            } else {
                Err(map_store_err("record_sighting", err))
            }
        }
    }
}

/// Atomically adjust the refcount by `delta` (server-side arithmetic;
/// no read-modify-write).
pub async fn increment_refcount(
    store: &Store,
    digest: &ContentDigest,
    delta: i64,
) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(digest)?.to_string())
        .map_err(|e| map_store_err("refcount", e))?
        .set_expr("refcount", field("refcount") + delta)
        .map_err(|e| map_store_err("refcount", e))?
        .where_(eq("digest", digest.as_str()))
        .return_after();
    let rows: Vec<serde_json::Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("refcount", e))?;
    if rows.is_empty() {
        return Err(CopalError::not_found(format!("blob {digest}")));
    }
    Ok(())
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
