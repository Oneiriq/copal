//! File repository: free functions over [`Store`], speaking domain types.
//!
//! Every state change is a guarded compare-and-swap — the WHERE clause
//! carries the expected current state plus the tenant, and an empty
//! result means the caller lost the race (or reached across a tenant
//! boundary, which is indistinguishable by design).

use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::{eq, is_none};
use surql::types::RecordID;

use copal_core::{ContentDigest, CopalError, FileId, FileRecord, FileSpec, FileState, TenantId};

use crate::dto::{map_store_err, FileRow};
use crate::store::Store;

const TABLE: &str = "file";

fn rid(id: &FileId) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, id.as_str()).map_err(|e| map_store_err("record id", e))
}

/// Create a file record in `draft`.
///
/// A duplicate live path or idempotency key surfaces as `Conflict` via
/// the unique indexes — no pre-read, no separate pending table.
pub async fn create_file(
    store: &Store,
    tenant: &TenantId,
    spec: &FileSpec,
    created_by: &str,
) -> copal_core::Result<FileRecord> {
    spec.validate()?;
    let id = FileId::generate();
    // Optional columns are OMITTED when unset, never sent as JSON null:
    // v3 distinguishes NULL from NONE, and `option<string>` accepts
    // `none | string` only — a null payload key fails coercion.
    let mut payload = serde_json::Map::new();
    payload.insert("tenant_id".into(), json!(tenant.as_str()));
    payload.insert("path".into(), json!(spec.path));
    payload.insert("access".into(), json!(spec.access.as_str()));
    payload.insert("content_type".into(), json!(spec.content_type));
    payload.insert(
        "metadata".into(),
        if spec.metadata.is_null() {
            json!({})
        } else {
            spec.metadata.clone()
        },
    );
    if let Some(key) = &spec.idempotency_key {
        payload.insert("idempotency_key".into(), json!(key));
    }
    payload.insert("created_by".into(), json!(created_by));
    let payload = Value::Object(payload);
    let created = create_record(store.client(), &format!("{TABLE}:{id}"), payload)
        .await
        .map_err(|e| map_store_err("create_file", e))?;
    let row = created
        .record
        .ok_or_else(|| CopalError::Store("create returned no record".into()))?;
    serde_json::from_value::<FileRow>(row)
        .map_err(|e| CopalError::Store(format!("create_file row shape: {e}")))?
        .into_domain()
}

/// Fetch one file, tenant-scoped, tombstones excluded.
pub async fn get_file(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
) -> copal_core::Result<Option<FileRecord>> {
    let Some(row) = get_record(store.client(), &rid(id)?)
        .await
        .map_err(|e| map_store_err("get_file", e))?
    else {
        return Ok(None);
    };
    let row: FileRow = serde_json::from_value(row)
        .map_err(|e| CopalError::Store(format!("get_file row shape: {e}")))?;
    // Tenant scoping and tombstone filtering happen before the domain
    // ever sees the record; a foreign or deleted file reads as absent.
    if row.tenant_id != tenant.as_str() || row.state == FileState::Deleted {
        return Ok(None);
    }
    row.into_domain().map(Some)
}

/// List a tenant's live files, newest first.
pub async fn list_files(
    store: &Store,
    tenant: &TenantId,
    limit: i64,
) -> copal_core::Result<Vec<FileRecord>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("list_files", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(is_none("deleted_at"))
        .order_by("created_at", "DESC")
        .map_err(|e| map_store_err("list_files", e))?
        .limit(limit)
        .map_err(|e| map_store_err("list_files", e))?;
    let rows: Vec<FileRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_files", e))?;
    rows.into_iter().map(FileRow::into_domain).collect()
}

/// Extra column writes applied atomically with a state transition.
#[derive(Debug, Default)]
pub struct TransitionSets {
    pub digest: Option<ContentDigest>,
    pub size_bytes: Option<u64>,
    /// Digest of the blob to link; rendered as a record literal.
    pub link_blob: Option<ContentDigest>,
}

/// Guarded state transition: compare-and-swap on `(tenant, id, from)`.
///
/// The transition is validated in the domain first (fast, exhaustive),
/// then enforced in the database as the WHERE guard, so concurrent
/// movers cannot double-apply. Losing the race returns `Conflict`.
pub async fn transition(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    from: FileState,
    to: FileState,
    sets: TransitionSets,
) -> copal_core::Result<FileRecord> {
    from.ensure_transition(to)?;

    let target = rid(id)?.to_string();
    let mut query = Query::new()
        .update_set(target)
        .map_err(|e| map_store_err("transition", e))?
        .set("state", Value::from(to.as_str()))
        .map_err(|e| map_store_err("transition", e))?;
    if let Some(digest) = &sets.digest {
        query = query
            .set("digest", Value::from(digest.as_str()))
            .map_err(|e| map_store_err("transition", e))?;
    }
    if let Some(size) = sets.size_bytes {
        query = query
            .set("size_bytes", Value::from(size))
            .map_err(|e| map_store_err("transition", e))?;
    }
    if let Some(blob_digest) = &sets.link_blob {
        // A record link needs a record literal on the right-hand side;
        // RecordID renders the canonical (bracketed where necessary)
        // form and set_expr injects it unquoted.
        let blob_rid = RecordID::<()>::new("blob", blob_digest.as_str())
            .map_err(|e| map_store_err("transition", e))?;
        query = query
            .set_expr("blob", raw(blob_rid.to_string()))
            .map_err(|e| map_store_err("transition", e))?;
    }
    let query = query
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(eq("state", from.as_str()))
        .return_after();

    let rows: Vec<FileRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("transition", e))?;
    let Some(row) = rows.into_iter().next() else {
        return Err(CopalError::conflict(format!(
            "file {id} is not in state {} for tenant {tenant}",
            from.as_str(),
        )));
    };
    row.into_domain()
}
