//! Tiering repository: the per-tenant policy row, the per-file pin,
//! the day-coarse read-recency signal, and the classifier's batch
//! walk over blob rows.
//!
//! Nothing here moves a byte. The policy is evaluated live at
//! classification (the deliberate opposite of retention's
//! stamp-at-creation; see the schema comment on `tenant_tiering`),
//! and eligibility is DERIVED over a blob's inbound references at
//! walk time, the same way the GC refcount is: a blob shared across
//! tenants moves only when every referencing tenant's policy agrees,
//! so the most demanding reference wins.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{ContentDigest, CopalError, FileId, TenantId};

use crate::dto::map_store_err;
use crate::store::Store;

const TABLE: &str = "tenant_tiering";

/// A tenant's tiering policy: which tier cold bytes belong in, when
/// they become cold, and the floor below which moving never pays.
#[derive(Debug, Clone, Deserialize)]
pub struct TieringPolicy {
    pub tier: String,
    pub after_seconds: u64,
    pub basis: String,
    #[serde(default)]
    pub min_bytes: u64,
}

/// The policy for a tenant, when one is set.
pub async fn get_policy(
    store: &Store,
    tenant: &TenantId,
) -> copal_core::Result<Option<TieringPolicy>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("tiering_policy", e))?
        .where_(eq("tenant_id", tenant.as_str()));
    let rows: Vec<TieringPolicy> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tiering_policy", e))?;
    Ok(rows.into_iter().next())
}

/// Set the tenant's tiering policy, replacing any prior one. The
/// caller validates the tier name against configuration; this layer
/// holds only the row shape.
pub async fn set_policy(
    store: &Store,
    tenant: &TenantId,
    policy: &TieringPolicy,
) -> copal_core::Result<()> {
    if policy.basis != "created" && policy.basis != "accessed" {
        return Err(CopalError::validation("basis must be created or accessed"));
    }
    clear_policy(store, tenant).await?;
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid =
        RecordID::<()>::new(TABLE, id.as_str()).map_err(|e| map_store_err("tiering_policy", e))?;
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "tier": policy.tier,
        "after_seconds": policy.after_seconds,
        "basis": policy.basis,
        "min_bytes": policy.min_bytes,
    });
    create_record(store.client(), &rid.to_string(), payload)
        .await
        .map_err(|e| map_store_err("tiering_policy", e))?;
    Ok(())
}

/// Remove the tenant's tiering policy. The classifier stops naming
/// this tenant's content on its next pass; nothing else changes,
/// because nothing was moved.
pub async fn clear_policy(store: &Store, tenant: &TenantId) -> copal_core::Result<()> {
    let query = Query::new()
        .delete(TABLE)
        .map_err(|e| map_store_err("tiering_policy", e))?
        .where_(eq("tenant_id", tenant.as_str()));
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tiering_policy", e))?;
    Ok(())
}

/// Every tenant's policy, for the classifier: one query per pass,
/// however many blobs the walk visits.
pub async fn all_policies(store: &Store) -> copal_core::Result<Vec<(String, TieringPolicy)>> {
    #[derive(Deserialize)]
    struct Row {
        tenant_id: String,
        #[serde(flatten)]
        policy: TieringPolicy,
    }
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("tiering_policies", e))?;
    let rows: Vec<Row> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tiering_policies", e))?;
    Ok(rows.into_iter().map(|r| (r.tenant_id, r.policy)).collect())
}

/// Pin a file's content hot, or release the pin. The pin lives on the
/// file row and reaches the blob through the inbound set, so one
/// tenant's pin holds a shared blob hot for every referent. Returns
/// whether a live file matched.
pub async fn set_file_pin(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    pinned: bool,
) -> copal_core::Result<bool> {
    let value = if pinned { raw("'hot'") } else { raw("NONE") };
    let query = Query::new()
        .update_set(super::file::rid(file)?.to_string())
        .map_err(|e| map_store_err("tier_pin", e))?
        .set_expr("tier_pin", value)
        .map_err(|e| map_store_err("tier_pin", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_str("deleted_at IS NONE")
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tier_pin", e))?;
    Ok(!rows.is_empty())
}

/// Record a byte read, day-coarse: the UPDATE fires only when the
/// stored value is older than one day (or absent), so the first read
/// of a day costs one background write and every later read that day
/// costs nothing. The caller runs this fire-and-forget after the
/// response; a lost write under-records at day granularity, which
/// fails toward moving content earlier -- latency, never loss.
pub async fn note_read(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<()> {
    let target = RecordID::<()>::new("blob", super::blob::blob_row_id(residency, digest))
        .map_err(|e| map_store_err("note_read", e))?;
    let query = Query::new()
        .update_set(target.to_string())
        .map_err(|e| map_store_err("note_read", e))?
        .set_expr("last_read", raw("time::now()"))
        .map_err(|e| map_store_err("note_read", e))?
        .where_str("last_read IS NONE OR last_read < time::now() - 1d")
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("note_read", e))?;
    Ok(())
}

/// A blob's recorded read recency, as the engine's RFC3339 text.
/// `None` means no byte read since the column existed -- the
/// classifier falls back to creation age for such rows.
pub async fn last_read(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<Option<String>> {
    let target = RecordID::<()>::new("blob", super::blob::blob_row_id(residency, digest))
        .map_err(|e| map_store_err("last_read", e))?;
    // Raw statement: FROM targets a record id, which the query
    // builder cannot express (the same reason repo::blob's point
    // reads are raw).
    let answer = store
        .client()
        .query(&format!("SELECT last_read FROM {target};"))
        .await
        .map_err(|e| map_store_err("last_read", e))?;
    Ok(answer
        .pointer("/0/0")
        .and_then(|row| row.get("last_read"))
        .and_then(|v| v.as_str())
        .map(str::to_owned))
}

/// The mover's commitments, each a guarded single-statement CAS so a
/// rival mover matches nothing. The flip happens only after verified
/// bytes exist at the destination -- the caller's discipline, stated
/// here because these statements are what make it atomic.
///
/// `demoted_at` is the placement flip marker: set by either flip,
/// cleared by [`settle_erase`] once the DISPLACED copy is erased. So
/// `tier` + marker reads as "cold, hot copy pending erase"; `tier`
/// alone as "settled cold"; marker alone as "hot again, cold copy
/// pending erase"; neither as plain hot.
///
/// Flip a verified-cold row's placement to `tier`. Matches only a
/// currently-hot row.
pub async fn demote_flip(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
    tier: &str,
) -> copal_core::Result<bool> {
    let target = RecordID::<()>::new("blob", super::blob::blob_row_id(residency, digest))
        .map_err(|e| map_store_err("demote_flip", e))?;
    let query = Query::new()
        .update_set(target.to_string())
        .map_err(|e| map_store_err("demote_flip", e))?
        .set("tier", Value::from(tier))
        .map_err(|e| map_store_err("demote_flip", e))?
        .set_expr("demoted_at", raw("time::now()"))
        .map_err(|e| map_store_err("demote_flip", e))?
        .where_str("tier IS NONE")
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("demote_flip", e))?;
    Ok(!rows.is_empty())
}

/// Flip a verified-hot row's placement back to the primary backend.
/// Matches only a row currently on `tier`.
pub async fn promote_flip(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
    tier: &str,
) -> copal_core::Result<bool> {
    let target = RecordID::<()>::new("blob", super::blob::blob_row_id(residency, digest))
        .map_err(|e| map_store_err("promote_flip", e))?;
    let query = Query::new()
        .update_set(target.to_string())
        .map_err(|e| map_store_err("promote_flip", e))?
        .set_expr("tier", raw("NONE"))
        .map_err(|e| map_store_err("promote_flip", e))?
        .set_expr("demoted_at", raw("time::now()"))
        .map_err(|e| map_store_err("promote_flip", e))?
        .where_str(format!("tier = '{tier}'"))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("promote_flip", e))?;
    Ok(!rows.is_empty())
}

/// Clear the flip marker after the displaced copy is erased.
/// `settled_tier` states which placement the erase settled: `Some`
/// after a demote's hot erase, `None` after a promote's cold erase.
/// The guard means a flip that raced in between keeps its marker.
pub async fn settle_erase(
    store: &Store,
    residency: &str,
    digest: &ContentDigest,
    settled_tier: Option<&str>,
) -> copal_core::Result<()> {
    let target = RecordID::<()>::new("blob", super::blob::blob_row_id(residency, digest))
        .map_err(|e| map_store_err("settle_erase", e))?;
    let guard = match settled_tier {
        Some(tier) => format!("tier = '{tier}'"),
        None => "tier IS NONE".to_owned(),
    };
    let query = Query::new()
        .update_set(target.to_string())
        .map_err(|e| map_store_err("settle_erase", e))?
        .set_expr("demoted_at", raw("NONE"))
        .map_err(|e| map_store_err("settle_erase", e))?
        .where_str(guard)
        .where_str("demoted_at IS NOT NONE")
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("settle_erase", e))?;
    Ok(())
}

/// A blob row as the classifier sees it: placement, size, both age
/// bases (computed at the engine so no datetime parsing happens
/// here), and the referencing tenants with any pins -- the inbound
/// sets filtered by the same liveness predicates the GC recount
/// trusts.
#[derive(Debug, Clone, Deserialize)]
pub struct BlobClassifyRow {
    pub id: String,
    #[serde(default)]
    pub size_bytes: i64,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub demoted_age_secs: Option<i64>,
    #[serde(default)]
    pub created_age_secs: Option<i64>,
    #[serde(default)]
    pub read_age_secs: Option<i64>,
    #[serde(default)]
    pub file_tenants: Vec<String>,
    #[serde(default)]
    pub pinned_tenants: Vec<String>,
    #[serde(default)]
    pub version_tenants: Vec<String>,
}

impl BlobClassifyRow {
    /// The bare row id, the keyset cursor for [`list_for_classify`].
    pub fn bare_id(&self) -> String {
        crate::dto::strip_record_prefix(&self.id, "blob").to_owned()
    }

    /// The residency parsed back out of the record id.
    pub fn residency(&self) -> String {
        let bare = crate::dto::strip_record_prefix(&self.id, "blob");
        match bare.rsplit_once('-') {
            Some((residency, _)) => residency.to_owned(),
            None => "local".to_owned(),
        }
    }

    /// The residency and digest, recovered from the record id -- the
    /// same parse [`super::blob::BlobGcRow::location`] performs.
    pub fn location(&self) -> copal_core::Result<(String, ContentDigest)> {
        let bare = crate::dto::strip_record_prefix(&self.id, "blob");
        match bare.rsplit_once('-') {
            Some((residency, digest)) => Ok((residency.to_owned(), ContentDigest::parse(digest)?)),
            None => Ok(("local".to_owned(), ContentDigest::parse(bare)?)),
        }
    }

    /// Every tenant holding this blob alive, files and history both.
    pub fn referencing_tenants(&self) -> impl Iterator<Item = &str> {
        self.file_tenants
            .iter()
            .chain(self.version_tenants.iter())
            .map(String::as_str)
    }
}

/// One classifier batch, keyset-ordered by id exactly as the GC
/// walks. The age columns are engine-computed seconds; the inbound
/// projections reuse the recount's liveness predicates so the
/// classifier and the GC can never disagree about what holds a blob.
pub async fn list_for_classify(
    store: &Store,
    limit: i64,
    after: Option<&str>,
) -> copal_core::Result<Vec<BlobClassifyRow>> {
    let mut query = Query::new()
        .select(Some(vec![
            "id".to_owned(),
            "size_bytes".to_owned(),
            "tier".to_owned(),
            "IF demoted_at IS NONE THEN NONE ELSE duration::secs(time::now() - demoted_at) END \
             AS demoted_age_secs"
                .to_owned(),
            "duration::secs(time::now() - created_at) AS created_age_secs".to_owned(),
            "IF last_read IS NONE THEN NONE ELSE duration::secs(time::now() - last_read) END \
             AS read_age_secs"
                .to_owned(),
            format!(
                "inbound_files[WHERE {}].tenant_id AS file_tenants",
                super::blob::LIVE_FILE_PREDICATE,
            ),
            format!(
                "inbound_files[WHERE {} AND tier_pin = 'hot'].tenant_id AS pinned_tenants",
                super::blob::LIVE_FILE_PREDICATE,
            ),
            format!(
                "inbound_versions[WHERE {}].tenant_id AS version_tenants",
                super::blob::RETAINED_VERSION_PREDICATE,
            ),
        ]))
        .from_table("blob")
        .map_err(|e| map_store_err("list_for_classify", e))?;
    if let Some(after) = after {
        let cursor = RecordID::<()>::new("blob", after)
            .map_err(|e| map_store_err("list_for_classify", e))?;
        query = query.where_str(format!("id > {cursor}"));
    }
    let query = query
        .order_by("id", "ASC")
        .map_err(|e| map_store_err("list_for_classify", e))?
        .limit(limit)
        .map_err(|e| map_store_err("list_for_classify", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_for_classify", e))
}
