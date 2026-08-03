//! S3 gateway credentials: mint, fetch for verification, list, revoke.
//!
//! The secret column holds sealed bytes (base64 of the blob cipher's
//! output), because SigV4 verification needs the secret back. Sealing
//! and opening happen in the server; this repo moves opaque strings.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::{eq, is_none};
use surql::types::RecordID;

use copal_core::{CopalError, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const TABLE: &str = "s3_credential";

fn cred_rid(access_key_id: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, access_key_id).map_err(|e| map_store_err("s3 credential id", e))
}

/// A credential row as verification sees it.
#[derive(Debug, Clone, Deserialize)]
pub struct S3CredentialRow {
    pub id: String,
    pub tenant_id: String,
    /// The principal this credential belongs to, when one does.
    #[serde(default)]
    pub principal_id: Option<String>,
    pub secret_sealed: String,
    #[serde(default)]
    pub revoked_at: Option<String>,
    pub created_at: String,
}

impl S3CredentialRow {
    /// The bare access key id (record prefix stripped).
    pub fn access_key_id(&self) -> String {
        strip_record_prefix(&self.id, TABLE).to_owned()
    }
}

/// Create a credential row keyed by the access key id.
pub async fn create_credential(
    store: &Store,
    tenant: &TenantId,
    access_key_id: &str,
    secret_sealed: &str,
    principal_id: Option<&str>,
) -> copal_core::Result<S3CredentialRow> {
    let mut payload = json!({
        "tenant_id": tenant.as_str(),
        "secret_sealed": secret_sealed,
    });
    // Absent rather than null: option fields accept NONE, never NULL.
    if let Some(principal) = principal_id {
        payload["principal_id"] = json!(principal);
    }
    let created = create_record(
        store.client(),
        &cred_rid(access_key_id)?.to_string(),
        payload,
    )
    .await
    .map_err(|e| map_store_err("create_credential", e))?;
    let row = created
        .record
        .ok_or_else(|| CopalError::Store("create returned no record".into()))?;
    serde_json::from_value(row)
        .map_err(|e| CopalError::Store(format!("s3 credential row shape: {e}")))
}

/// Fetch one credential by access key id, for verification. Unscoped
/// on purpose: the tenant comes OUT of the row and the caller checks
/// it against the bucket.
pub async fn fetch_credential(
    store: &Store,
    access_key_id: &str,
) -> copal_core::Result<Option<S3CredentialRow>> {
    let Some(row) = get_record(store.client(), &cred_rid(access_key_id)?)
        .await
        .map_err(|e| map_store_err("fetch_credential", e))?
    else {
        return Ok(None);
    };
    serde_json::from_value(row)
        .map(Some)
        .map_err(|e| CopalError::Store(format!("s3 credential row shape: {e}")))
}

/// List a tenant's credentials; sealed secrets never leave the repo.
pub async fn list_credentials(store: &Store, tenant: &TenantId) -> copal_core::Result<Vec<Value>> {
    let query = Query::new()
        .select(Some(vec![
            "id".to_owned(),
            "created_at".to_owned(),
            "revoked_at".to_owned(),
        ]))
        .from_table(TABLE)
        .map_err(|e| map_store_err("list_credentials", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .order_by("created_at", "ASC")
        .map_err(|e| map_store_err("list_credentials", e))?;
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_credentials", e))?;
    Ok(rows
        .into_iter()
        .map(|mut row| {
            if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                let bare = strip_record_prefix(id, TABLE).to_owned();
                row["id"] = json!(bare);
            }
            row
        })
        .collect())
}

/// Revoke a credential, tenant-scoped, once. Returns whether this call
/// performed the revocation.
pub async fn revoke_credential(
    store: &Store,
    tenant: &TenantId,
    access_key_id: &str,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(cred_rid(access_key_id)?.to_string())
        .map_err(|e| map_store_err("revoke_credential", e))?
        .set_expr("revoked_at", raw("time::now()"))
        .map_err(|e| map_store_err("revoke_credential", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(is_none("revoked_at"))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("revoke_credential", e))?;
    Ok(!rows.is_empty())
}
