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
use surql::types::operators::{eq, is_none, is_not_none, ne};
use surql::types::RecordID;

use copal_core::{
    ContentDigest, CopalError, CreatedFile, FileId, FileRecord, FileSpec, FileState, TenantId,
};

use crate::dto::{map_store_err, FileRow};
use crate::store::Store;

const TABLE: &str = "file";

fn rid(id: &FileId) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, id.as_str()).map_err(|e| map_store_err("record id", e))
}

/// Create a file record in `draft`, or return the original on an
/// idempotency-key replay.
///
/// A duplicate live path surfaces as `Conflict` via the unique index —
/// no pre-read, no separate pending table. A duplicate idempotency key
/// is NOT a conflict: the retried request gets the original record back
/// with `created == false`, which is the contract that makes client
/// retries safe.
pub async fn create_file(
    store: &Store,
    tenant: &TenantId,
    spec: &FileSpec,
    created_by: &str,
) -> copal_core::Result<CreatedFile> {
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
    let created = match create_record(store.client(), &format!("{TABLE}:{id}"), payload).await {
        Ok(created) => created,
        Err(err) => {
            let mapped = map_store_err("create_file", err);
            // Only an idempotency-key replay converts a uniqueness
            // conflict into success; a path collision stays a conflict.
            if let (CopalError::Conflict(_), Some(key)) = (&mapped, &spec.idempotency_key) {
                if let Some(original) = find_by_idempotency_key(store, tenant, key).await? {
                    if original.state == FileState::Deleted {
                        return Err(CopalError::conflict(format!(
                            "idempotency key {key} was consumed by a deleted file",
                        )));
                    }
                    return Ok(CreatedFile {
                        record: original,
                        created: false,
                    });
                }
            }
            return Err(mapped);
        }
    };
    let row = created
        .record
        .ok_or_else(|| CopalError::Store("create returned no record".into()))?;
    let record = serde_json::from_value::<FileRow>(row)
        .map_err(|e| CopalError::Store(format!("create_file row shape: {e}")))?
        .into_domain()?;
    Ok(CreatedFile {
        record,
        created: true,
    })
}

/// Look up a file by its idempotency key, including tombstones (the
/// caller decides how a deleted holder is reported).
async fn find_by_idempotency_key(
    store: &Store,
    tenant: &TenantId,
    key: &str,
) -> copal_core::Result<Option<FileRecord>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("find_by_idempotency_key", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(eq("idempotency_key", key))
        .limit(1)
        .map_err(|e| map_store_err("find_by_idempotency_key", e))?;
    let mut rows: Vec<FileRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("find_by_idempotency_key", e))?;
    match rows.pop() {
        Some(row) => {
            // Domain mapping happens without the tombstone filter here:
            // the state is part of the answer.
            row.into_domain().map(Some)
        }
        None => Ok(None),
    }
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

/// Keyset position: strictly-after this row in (created_at DESC, id
/// DESC) order. Both values come verbatim from the last row of the
/// previous page.
#[derive(Debug, Clone)]
pub struct ListPosition {
    pub created_at: String,
    pub id: FileId,
}

/// List a tenant's live files, newest first, keyset-paginated.
///
/// Keyset rather than offset: a cursor stays correct under concurrent
/// inserts and deletes, and the predicate rides the index instead of
/// skipping rows. The tie-break on id makes the order total.
pub async fn list_files(
    store: &Store,
    tenant: &TenantId,
    limit: i64,
    after: Option<&ListPosition>,
) -> copal_core::Result<Vec<FileRecord>> {
    let mut query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("list_files", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(is_none("deleted_at"));
    if let Some(position) = after {
        let position_rid =
            rid(&position.id).map_err(|e| CopalError::validation(format!("cursor: {e}")))?;
        // Datetime and record literals on the right-hand side; the
        // created_at value is the engine's own RFC3339 rendering fed
        // back to it.
        query = query.where_str(format!(
            "(created_at < d'{ts}' OR (created_at = d'{ts}' AND id < {id}))",
            ts = position.created_at,
            id = position_rid,
        ));
    }
    let query = query
        .order_by("created_at", "DESC")
        .map_err(|e| map_store_err("list_files", e))?
        .order_by("id", "DESC")
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

/// The raw SurrealQL fragment selecting an expired lease. A fragment
/// because the right-hand side is `time::now()` — server time, which no
/// value-quoting operator can express. Kept in one place; the family's
/// condition model (`str | Operator`) sanctions raw fragments as
/// condition entries.
const LEASE_EXPIRED: &str = "upload_lease_expires_at < time::now()";

/// Stamp a fresh lease onto an UPDATE under construction. Expiry is
/// computed server-side (`time::now() + <ttl>s`), so client clock skew
/// cannot manufacture longer leases.
fn with_lease(query: Query, owner: &str, ttl_secs: u32) -> copal_core::Result<Query> {
    query
        .set("upload_lease_owner", Value::from(owner))
        .map_err(|e| map_store_err("lease", e))?
        .set_expr(
            "upload_lease_expires_at",
            raw(format!("time::now() + {ttl_secs}s")),
        )
        .map_err(|e| map_store_err("lease", e))
}

/// Claim a file for upload, as `owner`, for `ttl_secs`.
///
/// Tries, in order:
/// 1. `draft -> uploading` — the fresh-file path;
/// 2. `failed -> uploading` — the retry path;
/// 3. stealing an `uploading` claim whose lease has expired — the
///    crashed-or-disconnected-uploader path. Not a state transition (the
///    state stays `uploading`), so it deliberately bypasses
///    `ensure_transition`; the guard is the expired lease itself.
///
/// A live claim by anyone (including `owner`) loses with `Conflict`.
pub async fn claim_upload(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    owner: &str,
    ttl_secs: u32,
) -> copal_core::Result<FileRecord> {
    // Ready is claimable too: a re-upload starts the next version while
    // the previous content keeps serving (servability is digest-based).
    for from in [FileState::Draft, FileState::Failed, FileState::Ready] {
        match transition_with(store, tenant, id, from, FileState::Uploading, |q| {
            with_lease(q, owner, ttl_secs)
        })
        .await
        {
            Ok(row) => return row.into_domain(),
            Err(CopalError::Conflict(_)) => continue,
            Err(other) => return Err(other),
        }
    }

    let target = rid(id)?.to_string();
    let query = with_lease(
        Query::new()
            .update_set(target)
            .map_err(|e| map_store_err("steal_claim", e))?,
        owner,
        ttl_secs,
    )?
    .where_(eq("tenant_id", tenant.as_str()))
    .where_(eq("state", FileState::Uploading.as_str()))
    .where_(is_not_none("upload_lease_expires_at"))
    .where_str(LEASE_EXPIRED)
    .return_after();
    let rows: Vec<FileRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("steal_claim", e))?;
    match rows.into_iter().next() {
        Some(row) => row.into_domain(),
        None => Err(CopalError::conflict(format!(
            "file {id} is not claimable (missing, live-leased, or already terminal)",
        ))),
    }
}

/// Soft-delete a file: tombstone the record, free its live path.
///
/// Legal from every live state (the transition table sends all of them
/// to `deleted`), so the guard is simply `state != 'deleted'`. Setting
/// `deleted_at` recomputes the live-path sentinel, which releases the
/// unique `(tenant, path)` slot for reuse; existing grants die through
/// the tombstone filter on the read path; the blob's derived reference
/// count drops because recounting only sees live links. Repeating the
/// delete reports `NotFound`.
pub async fn soft_delete(store: &Store, tenant: &TenantId, id: &FileId) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(id)?.to_string())
        .map_err(|e| map_store_err("soft_delete", e))?
        .set("state", Value::from(FileState::Deleted.as_str()))
        .map_err(|e| map_store_err("soft_delete", e))?
        .set_expr("deleted_at", raw("time::now()"))
        .map_err(|e| map_store_err("soft_delete", e))?
        .set_expr("upload_lease_owner", raw("NONE"))
        .map_err(|e| map_store_err("soft_delete", e))?
        .set_expr("upload_lease_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("soft_delete", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(ne("state", FileState::Deleted.as_str()))
        .return_after();
    let rows: Vec<FileRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("soft_delete", e))?;
    if rows.is_empty() {
        return Err(CopalError::not_found(format!("file {id}")));
    }
    Ok(())
}

/// Sweep every expired upload claim to `failed`, clearing the lease.
///
/// System-wide by design (no tenant scope): the reaper is an operator
/// process. Returns the reaped records for logging.
pub async fn reap_expired_uploads(store: &Store) -> copal_core::Result<Vec<FileRecord>> {
    // uploading -> failed is a legal transition; asserted so a future
    // state-machine edit cannot silently break the reaper.
    FileState::Uploading.ensure_transition(FileState::Failed)?;

    let query = Query::new()
        .update_set(TABLE)
        .map_err(|e| map_store_err("reap", e))?
        .set("state", Value::from(FileState::Failed.as_str()))
        .map_err(|e| map_store_err("reap", e))?
        .set_expr("upload_lease_owner", raw("NONE"))
        .map_err(|e| map_store_err("reap", e))?
        .set_expr("upload_lease_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("reap", e))?
        .where_(eq("state", FileState::Uploading.as_str()))
        .where_(is_not_none("upload_lease_expires_at"))
        .where_str(LEASE_EXPIRED)
        .return_after();
    let rows: Vec<FileRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("reap", e))?;
    rows.into_iter().map(FileRow::into_domain).collect()
}

/// Finish an upload: one CAS moves `uploading -> ready`, writes the
/// payload columns, links the blob, and atomically increments
/// `version_count` — whose returned value IS the new version number.
/// The frozen version row is then recorded and linked as
/// `current_version`.
///
/// A crash after the CAS leaves the file correct and servable with a
/// version-history hole that self-identifies (`version_count` exceeds
/// the version rows); a reconciliation sweep is the queued hardening.
pub async fn complete_upload(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    digest: &ContentDigest,
    size_bytes: u64,
    created_by: &str,
) -> copal_core::Result<FileRecord> {
    let blob_rid =
        RecordID::<()>::new("blob", digest.as_str()).map_err(|e| map_store_err("complete", e))?;
    let row = transition_with(
        store,
        tenant,
        id,
        FileState::Uploading,
        FileState::Ready,
        |query| {
            query
                .set("digest", Value::from(digest.as_str()))
                .map_err(|e| map_store_err("complete", e))?
                .set("size_bytes", Value::from(size_bytes))
                .map_err(|e| map_store_err("complete", e))?
                .set_expr("blob", raw(blob_rid.to_string()))
                .map_err(|e| map_store_err("complete", e))?
                .set_expr("version_count", raw("version_count + 1"))
                .map_err(|e| map_store_err("complete", e))
        },
    )
    .await?;

    let snapshot = super::version::VersionSnapshot {
        number: row.version_count,
        content_type: row.content_type.clone(),
        size_bytes,
        digest: digest.clone(),
        metadata_snapshot: row.metadata.clone(),
        created_by: created_by.to_owned(),
        prior_version_id: row.current_version.clone(),
    };
    let version_id = super::version::record_version(store, tenant, id, &snapshot).await?;

    let link = Query::new()
        .update_set(rid(id)?.to_string())
        .map_err(|e| map_store_err("link_version", e))?
        .set_expr("current_version", raw(version_id))
        .map_err(|e| map_store_err("link_version", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(eq("state", FileState::Ready.as_str()))
        .return_after();
    let rows: Vec<FileRow> = query_records(store.client(), &link)
        .await
        .map_err(|e| map_store_err("link_version", e))?;
    match rows.into_iter().next() {
        Some(row) => row.into_domain(),
        // The file moved (deleted mid-completion): the version row
        // exists and is armed; report the current truth.
        None => row.into_domain(),
    }
}

/// Guarded state transition: compare-and-swap on `(tenant, id, from)`.
///
/// The transition is validated in the domain first (fast, exhaustive),
/// then enforced in the database as the WHERE guard, so concurrent
/// movers cannot double-apply. Losing the race returns `Conflict`.
/// Leaving `uploading` clears the upload lease atomically.
pub async fn transition(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    from: FileState,
    to: FileState,
    sets: TransitionSets,
) -> copal_core::Result<FileRecord> {
    transition_with(store, tenant, id, from, to, |mut query| {
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
            // A record link needs a record literal on the right-hand
            // side; RecordID renders the canonical (bracketed where
            // necessary) form and set_expr injects it unquoted.
            let blob_rid = RecordID::<()>::new("blob", blob_digest.as_str())
                .map_err(|e| map_store_err("transition", e))?;
            query = query
                .set_expr("blob", raw(blob_rid.to_string()))
                .map_err(|e| map_store_err("transition", e))?;
        }
        Ok(query)
    })
    .await?
    .into_domain()
}

/// Shared CAS core: `extra` customises the UPDATE (payload columns or a
/// fresh lease) before the guards land. Returns the raw row so
/// orchestration (completion) can read link ids the domain type does
/// not carry.
async fn transition_with<F>(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    from: FileState,
    to: FileState,
    extra: F,
) -> copal_core::Result<FileRow>
where
    F: FnOnce(Query) -> copal_core::Result<Query>,
{
    from.ensure_transition(to)?;

    let target = rid(id)?.to_string();
    let mut query = Query::new()
        .update_set(target)
        .map_err(|e| map_store_err("transition", e))?
        .set("state", Value::from(to.as_str()))
        .map_err(|e| map_store_err("transition", e))?;
    if from == FileState::Uploading {
        // The claim ends with the state, atomically.
        query = query
            .set_expr("upload_lease_owner", raw("NONE"))
            .map_err(|e| map_store_err("transition", e))?
            .set_expr("upload_lease_expires_at", raw("NONE"))
            .map_err(|e| map_store_err("transition", e))?;
    }
    let query = extra(query)?
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
    Ok(row)
}
