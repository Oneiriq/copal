//! S3 multipart sessions: create, record parts, list, discard.
//!
//! A part write is an upsert on `(upload, part_number)`, because S3
//! lets a client re-send a part number and expects the last write to
//! win. Everything else is plain row work; the ordering and hashing
//! that matter happen in the server when the upload completes.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{CopalError, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const TABLE: &str = "s3_multipart";
const PART_TABLE: &str = "s3_multipart_part";

fn rid(table: &str, id: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(table, id).map_err(|e| map_store_err("multipart id", e))
}

/// A multipart session.
#[derive(Debug, Clone, Deserialize)]
pub struct MultipartRow {
    pub id: String,
    pub tenant_id: String,
    pub object_key: String,
    pub content_type: String,
    pub staging_prefix: String,
    pub created_at: String,
}

impl MultipartRow {
    /// The bare upload id, which is also the S3 `UploadId`.
    pub fn upload_id(&self) -> String {
        strip_record_prefix(&self.id, TABLE).to_owned()
    }
}

/// One recorded part.
#[derive(Debug, Clone, Deserialize)]
pub struct PartRow {
    pub part_number: i64,
    pub size_bytes: i64,
    pub digest: String,
    pub staging_key: String,
}

/// Open a session for an object key.
pub async fn create_upload(
    store: &Store,
    tenant: &TenantId,
    object_key: &str,
    content_type: &str,
) -> copal_core::Result<MultipartRow> {
    let upload_id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "object_key": object_key,
        "content_type": content_type,
        "staging_prefix": format!("mpu/{upload_id}"),
    });
    let created = create_record(
        store.client(),
        &rid(TABLE, &upload_id)?.to_string(),
        payload,
    )
    .await
    .map_err(|e| map_store_err("create_upload", e))?;
    let row = created
        .record
        .ok_or_else(|| CopalError::Store("create returned no record".into()))?;
    serde_json::from_value(row).map_err(|e| CopalError::Store(format!("multipart row: {e}")))
}

/// Fetch a session, tenant-scoped.
pub async fn fetch_upload(
    store: &Store,
    tenant: &TenantId,
    upload_id: &str,
) -> copal_core::Result<Option<MultipartRow>> {
    let Some(row) = get_record(store.client(), &rid(TABLE, upload_id)?)
        .await
        .map_err(|e| map_store_err("fetch_upload", e))?
    else {
        return Ok(None);
    };
    let row: MultipartRow = serde_json::from_value(row)
        .map_err(|e| CopalError::Store(format!("multipart row: {e}")))?;
    if row.tenant_id != tenant.as_str() {
        return Ok(None);
    }
    Ok(Some(row))
}

/// Record a finished part, replacing any previous write of the same
/// number. S3 expects last-write-wins per part number.
pub async fn put_part(
    store: &Store,
    upload_id: &str,
    part_number: i64,
    size_bytes: u64,
    digest: &str,
    staging_key: &str,
) -> copal_core::Result<()> {
    let upload_rid = rid(TABLE, upload_id)?;
    let update = Query::new()
        .update_set(PART_TABLE)
        .map_err(|e| map_store_err("put_part", e))?
        .set("size_bytes", Value::from(size_bytes as i64))
        .map_err(|e| map_store_err("put_part", e))?
        .set("digest", Value::from(digest))
        .map_err(|e| map_store_err("put_part", e))?
        .set("staging_key", Value::from(staging_key))
        .map_err(|e| map_store_err("put_part", e))?
        .where_str(format!("upload = {upload_rid}"))
        .where_(eq("part_number", part_number))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &update)
        .await
        .map_err(|e| map_store_err("put_part", e))?;
    if !rows.is_empty() {
        return Ok(());
    }

    let part_id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let payload = json!({
        "part_number": part_number,
        "size_bytes": size_bytes,
        "digest": digest,
        "staging_key": staging_key,
    });
    create_record(
        store.client(),
        &rid(PART_TABLE, &part_id)?.to_string(),
        payload,
    )
    .await
    .map_err(|e| map_store_err("put_part", e))?;
    // Record links arm via UPDATE; the unique pair lands here, so a
    // racing writer of the same number conflicts and retries into the
    // update path above.
    let arm = Query::new()
        .update_set(rid(PART_TABLE, &part_id)?.to_string())
        .map_err(|e| map_store_err("put_part", e))?
        .set_expr("upload", raw(upload_rid.to_string()))
        .map_err(|e| map_store_err("put_part", e))?
        .return_after();
    match query_records::<Value>(store.client(), &arm).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let mapped = map_store_err("put_part", err);
            let cleanup = Query::new()
                .delete(rid(PART_TABLE, &part_id)?.to_string())
                .map_err(|e| map_store_err("put_part", e))?;
            query_records::<Value>(store.client(), &cleanup)
                .await
                .map_err(|e| map_store_err("put_part", e))?;
            if matches!(mapped, CopalError::Conflict(_)) {
                Box::pin(put_part(
                    store,
                    upload_id,
                    part_number,
                    size_bytes,
                    digest,
                    staging_key,
                ))
                .await
            } else {
                Err(mapped)
            }
        }
    }
}

/// A session's parts in ascending part order.
pub async fn list_parts(store: &Store, upload_id: &str) -> copal_core::Result<Vec<PartRow>> {
    let upload_rid = rid(TABLE, upload_id)?;
    let query = Query::new()
        .select(None)
        .from_table(PART_TABLE)
        .map_err(|e| map_store_err("list_parts", e))?
        .where_str(format!("upload = {upload_rid}"))
        .order_by("part_number", "ASC")
        .map_err(|e| map_store_err("list_parts", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_parts", e))
}

/// Delete a session and its part rows.
pub async fn delete_upload(store: &Store, upload_id: &str) -> copal_core::Result<()> {
    let upload_rid = rid(TABLE, upload_id)?;
    let parts = Query::new()
        .delete(PART_TABLE)
        .map_err(|e| map_store_err("delete_upload", e))?
        .where_str(format!("upload = {upload_rid}"));
    query_records::<Value>(store.client(), &parts)
        .await
        .map_err(|e| map_store_err("delete_upload", e))?;
    let session = Query::new()
        .delete(upload_rid.to_string())
        .map_err(|e| map_store_err("delete_upload", e))?;
    query_records::<Value>(store.client(), &session)
        .await
        .map_err(|e| map_store_err("delete_upload", e))?;
    Ok(())
}

/// A tenant's open sessions, newest first.
pub async fn list_uploads(
    store: &Store,
    tenant: &TenantId,
    limit: i64,
) -> copal_core::Result<Vec<MultipartRow>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("list_uploads", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .order_by("created_at", "DESC")
        .map_err(|e| map_store_err("list_uploads", e))?
        .limit(limit)
        .map_err(|e| map_store_err("list_uploads", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_uploads", e))
}

/// Sessions older than the TTL, for the sweep.
pub async fn list_expired(
    store: &Store,
    older_than_secs: u64,
    limit: i64,
) -> copal_core::Result<Vec<MultipartRow>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("mpu expired", e))?
        .where_str(format!("created_at < time::now() - {older_than_secs}s"))
        .limit(limit)
        .map_err(|e| map_store_err("mpu expired", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("mpu expired", e))
}
