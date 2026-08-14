//! Blob repository: content-addressed rows keyed by digest.
//!
//! The record id is the digest (`blob:<sha256>`), so deduplication is a
//! CREATE that either succeeds (first sighting) or collides (seen
//! before), and the collision is the happy path.
//!
//! Reference counting is DERIVED, not incremented. An increment written
//! before the file link commits drifts upward on a crash-and-retry; an
//! undercount would let garbage collection delete live data. So the
//! authoritative count comes from the ENGINE's reference tracking:
//! every `file.blob` / `file_version.blob` write registers itself on
//! the blob row (the `REFERENCE` clause), and the recount filters
//! those inbound sets by the retention predicate at sweep time. The
//! stored `refcount` column is an advisory cache the sweep refreshes.

use serde::Deserialize;
use serde_json::json;

use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::{is_none, is_not_none};
use surql::types::RecordID;

use copal_core::ContentDigest;

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const TABLE: &str = "blob";

/// The row id for content in a residency: the bare digest for
/// `local` (compatible with every existing row), or
/// `{residency}-{digest}` otherwise. Residency names carry no hyphen
/// and digests are hex, so the id parses back unambiguously.
pub fn blob_row_id(residency: &str, digest: &ContentDigest) -> String {
    if residency == "local" {
        digest.as_str().to_owned()
    } else {
        format!("{residency}-{digest}")
    }
}

fn rid(residency: &str, digest: &ContentDigest) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, blob_row_id(residency, digest)).map_err(|e| map_store_err("blob id", e))
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
    match create_record(
        store.client(),
        &rid(store_key, digest)?.to_string(),
        payload,
    )
    .await
    {
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

/// A current link holds a blob alive while its file is undeleted.
/// One rendering, shared by the recount's inbound-set filter and the
/// aggregate oracle below, so the two can never drift apart.
pub(crate) const LIVE_FILE_PREDICATE: &str = "deleted_at IS NONE";

/// A history link holds a blob alive while its version is armed AND
/// either the owning file is live or the version itself is
/// non-erasable. A hold or an unexpired retention clock holds content
/// alive through its file's tombstone, which is the whole of
/// retention's enforcement: the GC is the only thing that erases, and
/// a retained version never lets its blob reach the mark step. The
/// `file.deleted_at` hop traverses the version's record link, which
/// the engine evaluates identically in a table WHERE and in an
/// inbound-set filter (probed on mem://).
pub(crate) const RETAINED_VERSION_PREDICATE: &str = "armed = true AND (file.deleted_at IS NONE \
     OR legal_hold = true OR (retain_until IS NOT NONE AND retain_until > time::now()))";

/// Count everything live that references `digest`: the authoritative
/// reference count.
///
/// Two sources hold a blob alive: current file links (a live file's
/// `blob` column) and HISTORY (armed `file_version` rows whose file is
/// itself live); a superseded version's content must survive until
/// its file dies. Version rows of deleted files traverse to a
/// tombstoned file and drop out, so deleting a file releases its whole
/// history in one recount.
///
/// One round trip over the blob row's engine-maintained inbound sets,
/// filtered in place: O(inbound links) per candidate where the old
/// aggregates scanned both tables. A blob row that no longer exists
/// answers with no rows, which reads as zero - the same thing its
/// absence means to the GC.
pub async fn recount_inbound_links(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<i64> {
    let blob_target = rid(residency, digest)?.to_string();
    // Raw statement: FROM targets a record id, which the query builder
    // cannot express (same reason the fragments below embed record
    // literals).
    let sql = format!(
        "SELECT count(inbound_files[WHERE {LIVE_FILE_PREDICATE}]) AS current_links, \
         count(inbound_versions[WHERE {RETAINED_VERSION_PREDICATE}]) AS history_links \
         FROM {blob_target};"
    );
    let answer = store
        .client()
        .query(&sql)
        .await
        .map_err(|e| map_store_err("recount", e))?;
    let row = answer.pointer("/0/0");
    let count = |key: &str| {
        row.and_then(|r| r.get(key))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    };
    Ok(count("current_links") + count("history_links"))
}

/// The pre-reference-tracking recount: the same two counts as
/// [`recount_inbound_links`], derived by aggregating over `file` and
/// `file_version` instead of reading the blob row's inbound sets.
///
/// Kept solely as the TEST ORACLE: the integration suite asserts the
/// two computations agree across the whole retention scenario matrix,
/// because a recount that undercounts erases live content. Hidden
/// rather than `cfg(test)` so integration tests can reach it; nothing
/// in the serving path calls it.
#[doc(hidden)]
pub async fn recount_inbound_links_by_aggregate(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<i64> {
    let blob_target = rid(residency, digest)?.to_string();

    let current = Query::new()
        .select(Some(vec!["count()".to_owned()]))
        .from_table("file")
        .map_err(|e| map_store_err("recount", e))?
        // Record equality against a literal; no quoting operator can
        // express a record right-hand side, hence the fragments here
        // and below.
        .where_str(format!("blob = {blob_target}"))
        .where_str(LIVE_FILE_PREDICATE)
        .group_all();

    let history = Query::new()
        .select(Some(vec!["count()".to_owned()]))
        .from_table("file_version")
        .map_err(|e| map_store_err("recount", e))?
        .where_str(format!("blob = {blob_target}"))
        .where_str(RETAINED_VERSION_PREDICATE)
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

/// Where a blob's bytes live: the residency's backend, the object
/// key, and which of the residency's tiers currently holds it
/// (`None` means the primary backend).
#[derive(Debug, Clone)]
pub struct BlobLocation {
    pub store_key: String,
    pub storage_path: String,
    pub tier: Option<String>,
}

/// Fetch a blob row's storage location, if the content is known.
///
/// Projects the three columns it needs rather than `SELECT *`: the
/// blob row now carries COMPUTED inbound sets, and a whole-row read of
/// a well-shared blob would resolve every inbound link just to learn a
/// path. This runs on the serving path, so it must not.
pub async fn get_location(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<Option<BlobLocation>> {
    let target = rid(residency, digest)?.to_string();
    let answer = store
        .client()
        .query(&format!(
            "SELECT store_key, storage_path, tier FROM {target};"
        ))
        .await
        .map_err(|e| map_store_err("get_location", e))?;
    let Some(row) = answer.pointer("/0/0") else {
        return Ok(None);
    };
    let text = |key: &str| {
        row.get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    Ok(Some(BlobLocation {
        store_key: text("store_key"),
        storage_path: text("storage_path"),
        tier: row.get("tier").and_then(|v| v.as_str()).map(str::to_owned),
    }))
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
    /// The bare row id (prefix and brackets stripped), the keyset
    /// cursor for [`list_blobs`].
    pub fn bare_id(&self) -> String {
        strip_record_prefix(&self.id, TABLE).to_owned()
    }

    /// The residency and digest, recovered from the record id.
    pub fn location(&self) -> copal_core::Result<(String, ContentDigest)> {
        let bare = strip_record_prefix(&self.id, TABLE);
        match bare.rsplit_once('-') {
            Some((residency, digest)) => Ok((residency.to_owned(), ContentDigest::parse(digest)?)),
            None => Ok(("local".to_owned(), ContentDigest::parse(bare)?)),
        }
    }
}

/// List one batch of blob rows for a GC pass, keyset-ordered by id
/// (blob ids ARE digests, so the order is total and stable). `after`
/// resumes past the previous batch; the sweep loops batches until a
/// short page, so every blob is visited every pass regardless of
/// population size.
pub async fn list_blobs(
    store: &Store,
    limit: i64,
    after: Option<&str>,
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
        let cursor =
            RecordID::<()>::new(TABLE, after).map_err(|e| map_store_err("list_blobs", e))?;
        query = query.where_str(format!("id > {cursor}"));
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
pub async fn mark_unreferenced(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(residency, digest)?.to_string())
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
    residency: &str,
    digest: &ContentDigest,
    live_count: i64,
) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(residency, digest)?.to_string())
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

/// Whether a row currently exists for this digest. The GC calls this
/// immediately before deleting object bytes: a row re-created between
/// the row-delete and the object-delete (an identical-content upload
/// re-registering after the full grace period) aborts the collection.
pub async fn row_exists(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<bool> {
    // `id` alone: existence must not resolve the computed inbound sets.
    let target = rid(residency, digest)?.to_string();
    let answer = store
        .client()
        .query(&format!("SELECT id FROM {target};"))
        .await
        .map_err(|e| map_store_err("row_exists", e))?;
    Ok(answer.pointer("/0/0").is_some())
}

/// Collect a blob row whose mark has aged past the grace period.
///
/// Returns whether the row was deleted; the CALLER then removes the
/// object bytes after re-checking [`row_exists`]. Row-before-object
/// ordering, the fresh recount, and the existence re-check narrow the
/// resurrection race to the instant between that check and the
/// filesystem delete.
pub async fn collect_expired(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
    grace_secs: u32,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .delete(rid(residency, digest)?.to_string())
        .map_err(|e| map_store_err("collect", e))?
        .where_(is_not_none("unreferenced_since"))
        .where_str(format!("unreferenced_since < time::now() - {grace_secs}s"))
        .return_format(surql::query::helpers::ReturnFormat::Before);
    let rows: Vec<serde_json::Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("collect", e))?;
    Ok(!rows.is_empty())
}
