//! Version repository: frozen snapshots of completed uploads.
//!
//! Nothing here writes a version row. They are born fully armed inside
//! the completion transaction ([`crate::repo::completion`]), which is
//! also the only place that can know a version's number, because the
//! number IS the increment the completion CAS performs. What lives here
//! is reading history and moving the one part of a version that is not
//! frozen: its retention clock and its legal hold. The freeze event
//! THROWs on any UPDATE that touches the links or the armed flag of an
//! armed row, so immutability is the engine's rule rather than a
//! convention this module could forget.

use serde::Deserialize;

use surql::query::builder::Query;
use surql::query::crud::query_records;
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{ContentDigest, CopalError, FileId, FileVersion, TenantId};

use crate::dto::map_store_err;
use crate::store::Store;

pub(crate) const TABLE: &str = "file_version";

#[derive(Debug, Deserialize)]
struct VersionRow {
    number: u64,
    content_type: String,
    size_bytes: u64,
    digest: String,
    #[serde(default)]
    blob: Option<String>,
    #[serde(default)]
    metadata_snapshot: serde_json::Value,
    created_by: Option<String>,
    created_at: String,
}

impl VersionRow {
    fn into_domain(self) -> copal_core::Result<FileVersion> {
        Ok(FileVersion {
            number: self.number,
            content_type: self.content_type,
            size_bytes: self.size_bytes,
            digest: ContentDigest::parse(self.digest)?,
            blob_residency: self
                .blob
                .as_deref()
                .map(crate::dto::blob_link_residency)
                .unwrap_or_else(|| "local".to_owned()),
            metadata_snapshot: self.metadata_snapshot,
            created_by: self.created_by,
            created_at: self.created_at,
        })
    }
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

/// The clause under which a compliance clock may be touched at all:
/// never, until it has expired. Governance rows and unset rows pass.
const COMPLIANCE_ALLOWS: &str =
    "(retention_mode IS NONE OR retention_mode != 'compliance' OR retain_until < time::now())";

/// Set a version's retention clock. Returns whether the update
/// applied: `false` means the row exists and compliance mode refused,
/// which is the whole WORM property enforced in one WHERE clause
/// rather than a read-then-write the admin could race.
///
/// Compliance rows accept only extensions of themselves: a longer
/// clock in compliance mode. Everything else (shortening, clearing,
/// downgrading to governance) refuses until the clock has expired.
pub async fn set_retention(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    number: u64,
    retain_secs: u64,
    mode: &str,
) -> copal_core::Result<bool> {
    let rid = version_rid(store, tenant, file, number).await?;
    let update = retention_update(&rid, retain_secs, mode)?.return_after();
    let rows: Vec<serde_json::Value> = query_records(store.client(), &update)
        .await
        .map_err(|e| map_store_err("set_retention", e))?;
    Ok(!rows.is_empty())
}

/// Render the retention stamp for one already-known version row,
/// RETURN clause left open.
///
/// Shared with the completion transaction, which stamps the version it
/// just created and therefore already holds its record id. The WORM
/// rule must have exactly one statement of itself: a second rendering
/// that drifted would be a compliance clock an admin could shorten
/// through the completion path, which is the failure this whole guard
/// exists to prevent.
pub(crate) fn retention_update(
    rid: &str,
    retain_secs: u64,
    mode: &str,
) -> copal_core::Result<Query> {
    let allowed = if mode == "compliance" {
        // Tightening: any row may enter compliance, and a compliance
        // row may extend. `<=` because re-asserting the same clock is
        // not a shortening.
        format!(
            "(retention_mode IS NONE OR retention_mode != 'compliance' \
             OR retain_until IS NONE OR retain_until <= time::now() + {retain_secs}s)",
        )
    } else {
        // Loosening into governance: only off an expired compliance
        // clock, or a row that was never compliance.
        COMPLIANCE_ALLOWS.to_owned()
    };
    Ok(Query::new()
        .update_set(rid)
        .map_err(|e| map_store_err("set_retention", e))?
        .set_expr("retain_until", raw(format!("time::now() + {retain_secs}s")))
        .map_err(|e| map_store_err("set_retention", e))?
        .set("retention_mode", serde_json::Value::from(mode))
        .map_err(|e| map_store_err("set_retention", e))?
        .where_str(allowed))
}

/// Clear a version's retention. Refuses on an unexpired compliance
/// clock, exactly as shortening does.
pub async fn clear_retention(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    number: u64,
) -> copal_core::Result<bool> {
    let rid = version_rid(store, tenant, file, number).await?;
    let update = Query::new()
        .update_set(rid)
        .map_err(|e| map_store_err("clear_retention", e))?
        .set_expr("retain_until", raw("NONE"))
        .map_err(|e| map_store_err("clear_retention", e))?
        .set_expr("retention_mode", raw("NONE"))
        .map_err(|e| map_store_err("clear_retention", e))?
        .where_str(COMPLIANCE_ALLOWS)
        .return_after();
    let rows: Vec<serde_json::Value> = query_records(store.client(), &update)
        .await
        .map_err(|e| map_store_err("clear_retention", e))?;
    Ok(!rows.is_empty())
}

/// Apply or release a legal hold on a version.
pub async fn set_legal_hold(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    number: u64,
    held: bool,
) -> copal_core::Result<()> {
    let rid = version_rid(store, tenant, file, number).await?;
    let update = Query::new()
        .update_set(rid)
        .map_err(|e| map_store_err("set_legal_hold", e))?
        .set("legal_hold", serde_json::Value::from(held))
        .map_err(|e| map_store_err("set_legal_hold", e))?
        .return_after();
    query_records::<serde_json::Value>(store.client(), &update)
        .await
        .map_err(|e| map_store_err("set_legal_hold", e))?;
    Ok(())
}

/// The record id of one version row, by file and number.
async fn version_rid(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    number: u64,
) -> copal_core::Result<String> {
    #[derive(Deserialize)]
    struct IdRow {
        id: String,
    }
    let find = versions_query(tenant, file)?.where_(eq("number", number as i64));
    let rows: Vec<IdRow> = query_records(store.client(), &find)
        .await
        .map_err(|e| map_store_err("version_rid", e))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| CopalError::not_found("version"))?;
    let rid = RecordID::<()>::new(TABLE, trim_table(&row.id))
        .map_err(|e| map_store_err("version_rid", e))?;
    Ok(rid.to_string())
}

/// What history a `keep_last` policy is allowed to remove: never a
/// held row, never one whose clock is still running. Beside the
/// retention rules on purpose, because it is the same rule read from
/// the other side, and the completion transaction that renders the
/// pruning DELETE must not restate it.
pub(crate) const ERASABLE: &str =
    "legal_hold != true AND (retain_until IS NONE OR retain_until < time::now())";

/// Render the pruning DELETE for one file's erasable history, RETURN
/// clause left open.
///
/// `cutoff` is a SurrealQL expression rather than a number because the
/// completion transaction does not know the newest version number
/// client-side: it is whatever the CAS in the same transaction just
/// incremented `version_count` to, so the cutoff is arithmetic the
/// engine does. The current version always survives, because the
/// caller's expression subtracts a `keep` of at least one.
pub(crate) fn prune_query(
    tenant: &TenantId,
    file: &FileId,
    cutoff: &str,
) -> copal_core::Result<Query> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("prune", e))?;
    Ok(Query::new()
        .delete(TABLE)
        .map_err(|e| map_store_err("prune", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_str(format!("file = {file_rid}"))
        .where_str(format!("number <= {cutoff}"))
        .where_str(ERASABLE))
}
