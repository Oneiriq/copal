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

use serde::Deserialize;
use serde_json::json;

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::{is_none, is_not_none};
use surql::types::RecordID;

use copal_core::ContentDigest;

use crate::dto::{map_store_err, strip_record_prefix};
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

/// Count everything live that references `digest` — the authoritative
/// reference count.
///
/// Two sources hold a blob alive: current file links (a live file's
/// `blob` column) and HISTORY (armed `file_version` rows whose file is
/// itself live) — a superseded version's content must survive until
/// its file dies. Version rows of deleted files traverse to a
/// tombstoned file and drop out, so deleting a file releases its whole
/// history in one recount.
pub async fn recount_inbound_links(
    store: &Store,
    digest: &ContentDigest,
) -> copal_core::Result<i64> {
    let blob_target = rid(digest)?.to_string();

    let current = Query::new()
        .select(Some(vec!["count()".to_owned()]))
        .from_table("file")
        .map_err(|e| map_store_err("recount", e))?
        // Record equality against a literal; no quoting operator can
        // express a record right-hand side, hence the fragments here
        // and below.
        .where_str(format!("blob = {blob_target}"))
        .where_(is_none("deleted_at"))
        .group_all();

    let history = Query::new()
        .select(Some(vec!["count()".to_owned()]))
        .from_table("file_version")
        .map_err(|e| map_store_err("recount", e))?
        .where_str(format!("blob = {blob_target}"))
        // Record-link traversal: the version's file must be live.
        .where_str("armed = true AND file.deleted_at IS NONE")
        .group_all();

    let mut total = 0i64;
    for query in [current, history] {
        let rows: Vec<serde_json::Value> = query_records(store.client(), &query)
            .await
            .map_err(|e| map_store_err("recount", e))?;
        total += rows
            .first()
            .and_then(|r| r.get("count"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
    }
    Ok(total)
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

/// A blob row as garbage collection sees it.
#[derive(Debug, Clone, Deserialize)]
pub struct BlobGcRow {
    pub id: String,
    pub refcount: i64,
    #[serde(default)]
    pub unreferenced_since: Option<String>,
}

impl BlobGcRow {
    /// The digest, recovered from the record id.
    pub fn digest(&self) -> copal_core::Result<ContentDigest> {
        ContentDigest::parse(strip_record_prefix(&self.id, TABLE))
    }
}

/// List one batch of blob rows for a GC pass, keyset-ordered by id
/// (blob ids ARE digests, so the order is total and stable). `after`
/// resumes past the previous batch — the sweep loops batches until a
/// short page, so every blob is visited every pass regardless of
/// population size.
pub async fn list_blobs(
    store: &Store,
    limit: i64,
    after: Option<&ContentDigest>,
) -> copal_core::Result<Vec<BlobGcRow>> {
    let mut query = Query::new()
        .select(Some(vec![
            "id".to_owned(),
            "refcount".to_owned(),
            "unreferenced_since".to_owned(),
        ]))
        .from_table(TABLE)
        .map_err(|e| map_store_err("list_blobs", e))?;
    if let Some(after) = after {
        query = query.where_str(format!("id > {}", rid(after)?));
    }
    let query = query
        .order_by("id", "ASC")
        .map_err(|e| map_store_err("list_blobs", e))?
        .limit(limit)
        .map_err(|e| map_store_err("list_blobs", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_blobs", e))
}

/// Mark a blob as unreferenced now, if it is not already marked. The
/// grace clock starts at the FIRST observation, so repeated passes do
/// not push collection out indefinitely.
pub async fn mark_unreferenced(store: &Store, digest: &ContentDigest) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(digest)?.to_string())
        .map_err(|e| map_store_err("mark_unreferenced", e))?
        .set("refcount", serde_json::Value::from(0))
        .map_err(|e| map_store_err("mark_unreferenced", e))?
        .set_expr("unreferenced_since", raw("time::now()"))
        .map_err(|e| map_store_err("mark_unreferenced", e))?
        .where_(is_none("unreferenced_since"))
        .return_after();
    query_records::<serde_json::Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("mark_unreferenced", e))?;
    Ok(())
}

/// A referenced blob: clear any stale mark and refresh the advisory
/// refcount cache with the derived truth.
pub async fn clear_unreferenced(
    store: &Store,
    digest: &ContentDigest,
    live_count: i64,
) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(digest)?.to_string())
        .map_err(|e| map_store_err("clear_unreferenced", e))?
        .set("refcount", serde_json::Value::from(live_count))
        .map_err(|e| map_store_err("clear_unreferenced", e))?
        .set_expr("unreferenced_since", raw("NONE"))
        .map_err(|e| map_store_err("clear_unreferenced", e))?
        .return_after();
    query_records::<serde_json::Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("clear_unreferenced", e))?;
    Ok(())
}

/// Collect a blob row whose mark has aged past the grace period.
///
/// Returns whether the row was deleted; the CALLER then removes the
/// object bytes. Row-before-object ordering plus the caller's fresh
/// recount narrows the resurrection race to the instant between the
/// two deletions; the remaining window (an identical-content upload
/// re-registering in that instant, after the full grace period) is a
/// known hazard queued for a collection-lock hardening.
pub async fn collect_expired(
    store: &Store,
    digest: &ContentDigest,
    grace_secs: u32,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .delete(rid(digest)?.to_string())
        .map_err(|e| map_store_err("collect", e))?
        .where_(is_not_none("unreferenced_since"))
        .where_str(format!("unreferenced_since < time::now() - {grace_secs}s"))
        .return_format(surql::query::helpers::ReturnFormat::Before);
    let rows: Vec<serde_json::Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("collect", e))?;
    Ok(!rows.is_empty())
}
