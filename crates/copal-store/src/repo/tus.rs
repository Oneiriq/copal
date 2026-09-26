//! Resumable-upload sessions: create, lease a PATCH, advance the
//! offset, finish or expire.
//!
//! The offset column is the protocol's truth and every advance is a
//! guarded CAS from the previous value, so a lost race is a protocol
//! 409 and never interleaved bytes. The PATCH lease uses the same
//! claim discipline as uploads: one writer at a time, expiry makes
//! crashes self-healing.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{CopalError, FileId, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const TABLE: &str = "tus_upload";

fn rid(id: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, id).map_err(|e| map_store_err("tus id", e))
}

/// A session row.
#[derive(Debug, Clone, Deserialize)]
pub struct TusRow {
    pub id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub file: Option<String>,
    pub upload_length: u64,
    pub offset: u64,
    pub staging_key: String,
    /// The declared markers, validated at creation, carried to the
    /// completing PATCH so they can land on the version row.
    #[serde(default)]
    pub markers: Option<Value>,
    pub created_at: String,
}

impl TusRow {
    /// The bare session id.
    pub fn session_id(&self) -> String {
        strip_record_prefix(&self.id, TABLE).to_owned()
    }

    /// The linked file id.
    pub fn file_id(&self) -> copal_core::Result<FileId> {
        let raw = self
            .file
            .as_deref()
            .ok_or_else(|| CopalError::Store("session has no file link".into()))?;
        FileId::parse(strip_record_prefix(raw, "file"))
    }
}

/// Create a session bound to a claimed file and a staging key.
///
/// `markers` is the declaration `Upload-Metadata` carried, already
/// validated; the session row holds it until the final PATCH,
/// because the version row it belongs on is only born at completion.
pub async fn create_session(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    upload_length: u64,
    staging_key: &str,
    markers: Option<&Value>,
) -> copal_core::Result<String> {
    let session_id = ulid::Ulid::generate().to_string().to_ascii_lowercase();
    let mut payload = json!({
        "tenant_id": tenant.as_str(),
        "upload_length": upload_length,
        "staging_key": staging_key,
    });
    if let Some(declaration) = markers {
        payload["markers"] = declaration.clone();
    }
    create_record(store.client(), &rid(&session_id)?.to_string(), payload)
        .await
        .map_err(|e| map_store_err("tus create", e))?;
    // Arm the file link (record links ride UPDATE, never CREATE).
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("tus", e))?;
    let query = Query::new()
        .update_set(rid(&session_id)?.to_string())
        .map_err(|e| map_store_err("tus", e))?
        .set_expr("file", raw(file_rid.to_string()))
        .map_err(|e| map_store_err("tus", e))?
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tus", e))?;
    Ok(session_id)
}

/// Fetch one session, tenant-scoped.
pub async fn fetch(
    store: &Store,
    tenant: &TenantId,
    session_id: &str,
) -> copal_core::Result<Option<TusRow>> {
    let Some(row) = get_record(store.client(), &rid(session_id)?)
        .await
        .map_err(|e| map_store_err("tus fetch", e))?
    else {
        return Ok(None);
    };
    let row: TusRow = serde_json::from_value(row)
        .map_err(|e| CopalError::Store(format!("tus row shape: {e}")))?;
    if row.tenant_id != tenant.as_str() {
        return Ok(None);
    }
    Ok(Some(row))
}

/// Claim the PATCH lease at the expected offset. Refuses when another
/// writer holds a live lease or the offset moved.
pub async fn claim_patch(
    store: &Store,
    session_id: &str,
    expected_offset: u64,
    owner: &str,
    lease_secs: u32,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(rid(session_id)?.to_string())
        .map_err(|e| map_store_err("tus claim", e))?
        .set("patch_owner", Value::from(owner))
        .map_err(|e| map_store_err("tus claim", e))?
        .set_expr(
            "patch_expires_at",
            raw(format!("time::now() + {lease_secs}s")),
        )
        .map_err(|e| map_store_err("tus claim", e))?
        .where_(eq("offset", expected_offset as i64))
        .where_str("(patch_owner IS NONE OR patch_expires_at < time::now())")
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tus claim", e))?;
    Ok(!rows.is_empty())
}

/// Advance the offset after a successful append, clearing the lease.
/// The CAS from the previous offset makes interleaving impossible.
pub async fn advance(
    store: &Store,
    session_id: &str,
    from_offset: u64,
    to_offset: u64,
) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(session_id)?.to_string())
        .map_err(|e| map_store_err("tus advance", e))?
        .set("offset", Value::from(to_offset as i64))
        .map_err(|e| map_store_err("tus advance", e))?
        .set_expr("patch_owner", raw("NONE"))
        .map_err(|e| map_store_err("tus advance", e))?
        .set_expr("patch_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("tus advance", e))?
        .where_(eq("offset", from_offset as i64))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tus advance", e))?;
    if rows.is_empty() {
        return Err(CopalError::conflict(
            "session offset moved underneath the append",
        ));
    }
    Ok(())
}

/// Release a lease without advancing (a failed append).
pub async fn release_patch(store: &Store, session_id: &str) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(session_id)?.to_string())
        .map_err(|e| map_store_err("tus release", e))?
        .set_expr("patch_owner", raw("NONE"))
        .map_err(|e| map_store_err("tus release", e))?
        .set_expr("patch_expires_at", raw("NONE"))
        .map_err(|e| map_store_err("tus release", e))?
        .return_after();
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tus release", e))?;
    Ok(())
}

/// Delete a session row (completion or termination).
pub async fn delete_session(store: &Store, session_id: &str) -> copal_core::Result<()> {
    let query = Query::new()
        .delete(rid(session_id)?.to_string())
        .map_err(|e| map_store_err("tus delete", e))?;
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tus delete", e))?;
    Ok(())
}

/// Sessions older than the TTL, for the sweep.
pub async fn list_expired(
    store: &Store,
    older_than_secs: u64,
    limit: i64,
) -> copal_core::Result<Vec<TusRow>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("tus expired", e))?
        .where_str(format!("created_at < time::now() - {older_than_secs}s"))
        .limit(limit)
        .map_err(|e| map_store_err("tus expired", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tus expired", e))
}
