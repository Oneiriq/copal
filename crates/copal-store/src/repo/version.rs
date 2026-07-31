//! Version repository: frozen snapshots of completed uploads.
//!
//! A version row is created with its scalars (READONLY at the engine)
//! and then ARMED: one UPDATE setting the record links plus
//! `armed = true`. The freeze event admits exactly that UPDATE and
//! THROWs on everything after, so history is immutable at the engine,
//! not by convention.

use serde::Deserialize;
use serde_json::json;

use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{ContentDigest, CopalError, FileId, FileVersion, TenantId};

use crate::dto::map_store_err;
use crate::store::Store;

const TABLE: &str = "file_version";

/// Inputs for one version snapshot.
#[derive(Debug, Clone)]
pub struct VersionSnapshot {
    pub number: u64,
    pub content_type: String,
    pub size_bytes: u64,
    pub digest: ContentDigest,
    pub metadata_snapshot: serde_json::Value,
    pub created_by: String,
    /// The previous current version's raw record id, if any: the
    /// `prior` link that forms the chain.
    pub prior_version_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VersionRow {
    number: u64,
    content_type: String,
    size_bytes: u64,
    digest: String,
    created_by: String,
    created_at: String,
}

impl VersionRow {
    fn into_domain(self) -> copal_core::Result<FileVersion> {
        Ok(FileVersion {
            number: self.number,
            content_type: self.content_type,
            size_bytes: self.size_bytes,
            digest: ContentDigest::parse(self.digest)?,
            created_by: self.created_by,
            created_at: self.created_at,
        })
    }
}

/// Create and arm a version row, returning its raw record id (the
/// caller links it as the file's `current_version`).
pub async fn record_version(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    snapshot: &VersionSnapshot,
) -> copal_core::Result<String> {
    let version_id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid = RecordID::<()>::new(TABLE, version_id.as_str())
        .map_err(|e| map_store_err("version id", e))?;

    let payload = json!({
        "tenant_id": tenant.as_str(),
        "number": snapshot.number,
        "content_type": snapshot.content_type,
        "size_bytes": snapshot.size_bytes,
        "digest": snapshot.digest.as_str(),
        "metadata_snapshot": snapshot.metadata_snapshot,
        "created_by": snapshot.created_by,
    });
    create_record(store.client(), &rid.to_string(), payload)
        .await
        .map_err(|e| map_store_err("record_version", e))?;

    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("version", e))?;
    let blob_rid = RecordID::<()>::new("blob", snapshot.digest.as_str())
        .map_err(|e| map_store_err("version", e))?;
    let mut arm = Query::new()
        .update_set(rid.to_string())
        .map_err(|e| map_store_err("arm_version", e))?
        .set_expr("file", raw(file_rid.to_string()))
        .map_err(|e| map_store_err("arm_version", e))?
        .set_expr("blob", raw(blob_rid.to_string()))
        .map_err(|e| map_store_err("arm_version", e))?;
    if let Some(prior) = &snapshot.prior_version_id {
        let prior_rid = RecordID::<()>::new(TABLE, trim_table(prior))
            .map_err(|e| map_store_err("arm_version", e))?;
        arm = arm
            .set_expr("prior", raw(prior_rid.to_string()))
            .map_err(|e| map_store_err("arm_version", e))?;
    }
    let arm = arm
        .set("armed", serde_json::Value::from(true))
        .map_err(|e| map_store_err("arm_version", e))?
        .return_after();
    let rows: Vec<serde_json::Value> = query_records(store.client(), &arm)
        .await
        .map_err(|e| map_store_err("arm_version", e))?;
    if rows.is_empty() {
        return Err(CopalError::Store(
            "version row vanished between create and arm".into(),
        ));
    }
    Ok(format!("{TABLE}:{version_id}"))
}

/// Strip a leading `file_version:` and any brackets from a raw id.
fn trim_table(raw_id: &str) -> &str {
    crate::dto::strip_record_prefix(raw_id, TABLE)
}

/// List a file's versions, newest first.
pub async fn list_versions(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    limit: i64,
    before_number: Option<u64>,
) -> copal_core::Result<Vec<FileVersion>> {
    let mut query = versions_query(tenant, file)?;
    if let Some(before) = before_number {
        // Keyset on the monotone version number: newest first, resume
        // strictly below the last number of the previous page.
        query = query.where_str(format!("number < {before}"));
    }
    let query = query
        .limit(limit)
        .map_err(|e| map_store_err("list_versions", e))?;
    let rows: Vec<VersionRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_versions", e))?;
    rows.into_iter().map(VersionRow::into_domain).collect()
}

/// Fetch one version by number.
pub async fn get_version(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    number: u64,
) -> copal_core::Result<Option<FileVersion>> {
    let query = versions_query(tenant, file)?.where_(eq("number", number as i64));
    let mut rows: Vec<VersionRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("get_version", e))?;
    rows.pop().map(VersionRow::into_domain).transpose()
}

/// Test support: attempt to mutate an armed version row, so integration
/// tests can prove the engine-level freeze rather than trusting the
/// schema text. Never called by production code.
#[doc(hidden)]
pub async fn tamper_for_test(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    number: u64,
) -> copal_core::Result<()> {
    #[derive(Deserialize)]
    struct IdRow {
        id: String,
    }
    let find = versions_query(tenant, file)?.where_(eq("number", number as i64));
    let rows: Vec<IdRow> = query_records(store.client(), &find)
        .await
        .map_err(|e| map_store_err("tamper", e))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| CopalError::not_found("version"))?;
    let rid =
        RecordID::<()>::new(TABLE, trim_table(&row.id)).map_err(|e| map_store_err("tamper", e))?;
    // Target `armed` specifically: it is NOT readonly (arming needs to
    // set it once), so this exercises the freeze EVENT rather than the
    // per-field READONLY guard; the two layers refuse independently.
    let update = Query::new()
        .update_set(rid.to_string())
        .map_err(|e| map_store_err("tamper", e))?
        .set("armed", serde_json::Value::from(false))
        .map_err(|e| map_store_err("tamper", e))?
        .return_after();
    query_records::<serde_json::Value>(store.client(), &update)
        .await
        .map_err(|e| map_store_err("tamper", e))?;
    Ok(())
}

fn versions_query(tenant: &TenantId, file: &FileId) -> copal_core::Result<Query> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("versions", e))?;
    Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("versions", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        // Record equality against a literal: no quoting operator can
        // express a record right-hand side.
        .where_str(format!("file = {file_rid}"))
        .order_by("number", "DESC")
        .map_err(|e| map_store_err("versions", e))
}
