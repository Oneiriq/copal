//! Grant repository: issue, redeem, revoke.
//!
//! Redemption is two phases with distinct jobs:
//! 1. `fetch` returns the row so the CALLER verifies the bearer secret
//!    (constant-time, in `copal-sign`); the hash never rides a WHERE
//!    clause.
//! 2. `consume` is one atomic UPDATE whose guards carry every business
//!    rule: not revoked, armed, unexpired (server clock), and under the
//!    use limit. Two racing redeemers of a max_uses=1 grant cannot both
//!    win, for the same reason two uploaders cannot both claim a file.

use serde::Deserialize;
use serde_json::json;

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::{eq, is_none, is_not_none};
use surql::types::RecordID;

use copal_core::{CopalError, FileId, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const TABLE: &str = "access_grant";

fn rid(grant_id: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, grant_id).map_err(|e| map_store_err("grant id", e))
}

/// A grant row as redemption sees it.
#[derive(Debug, Clone, Deserialize)]
pub struct GrantRow {
    pub id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub file: Option<String>,
    pub op: String,
    pub secret_hash: String,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub max_uses: Option<i64>,
    pub uses: i64,
    #[serde(default)]
    pub revoked_at: Option<String>,
}

impl GrantRow {
    /// The granted file id, once armed.
    pub fn file_id(&self) -> copal_core::Result<FileId> {
        let raw_id = self
            .file
            .as_deref()
            .ok_or_else(|| CopalError::not_found("grant is not armed"))?;
        FileId::parse(strip_record_prefix(raw_id, "file"))
    }
}

/// Issuance parameters beyond the identities involved.
#[derive(Debug, Clone)]
pub struct GrantSpec {
    /// Seconds until the grant expires (server-computed).
    pub ttl_secs: u32,
    /// Redemption ceiling; `None` = unlimited within the TTL.
    pub max_uses: Option<u32>,
    /// Principal recorded as the issuer.
    pub created_by: String,
    /// `get` to read bytes, `put` to write them.
    pub op: String,
}

impl Default for GrantSpec {
    fn default() -> Self {
        Self {
            ttl_secs: 900,
            max_uses: None,
            created_by: "api".to_owned(),
            op: "get".to_owned(),
        }
    }
}

/// Create and arm a grant, returning its armed row.
///
/// The caller supplies the grant id (minted with the token) and the
/// secret hash; the secret itself never reaches this crate. Arming
/// sets the file link and the server-computed expiry in one UPDATE;
/// if the process dies between create and arm, the inert row fails
/// every redemption guard.
pub async fn issue(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    grant_id: &str,
    secret_hash: &str,
    spec: &GrantSpec,
) -> copal_core::Result<GrantRow> {
    let mut payload = serde_json::Map::new();
    payload.insert("tenant_id".into(), json!(tenant.as_str()));
    payload.insert("secret_hash".into(), json!(secret_hash));
    if let Some(max) = spec.max_uses {
        payload.insert("max_uses".into(), json!(max));
    }
    payload.insert("created_by".into(), json!(spec.created_by));
    payload.insert("op".into(), json!(spec.op));
    create_record(
        store.client(),
        &rid(grant_id)?.to_string(),
        serde_json::Value::Object(payload),
    )
    .await
    .map_err(|e| map_store_err("issue_grant", e))?;

    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("issue_grant", e))?;
    let query = Query::new()
        .update_set(rid(grant_id)?.to_string())
        .map_err(|e| map_store_err("arm_grant", e))?
        .set_expr("file", raw(file_rid.to_string()))
        .map_err(|e| map_store_err("arm_grant", e))?
        .set_expr(
            "expires_at",
            raw(format!("time::now() + {}s", spec.ttl_secs)),
        )
        .map_err(|e| map_store_err("arm_grant", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .return_after();
    let rows: Vec<GrantRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("arm_grant", e))?;
    rows.into_iter()
        .next()
        .ok_or_else(|| CopalError::Store("grant vanished between create and arm".into()))
}

/// Fetch a grant row by id, for secret verification. Absence and
/// malformed ids look identical to the caller by design.
pub async fn fetch(store: &Store, grant_id: &str) -> copal_core::Result<Option<GrantRow>> {
    let Some(row) = get_record(store.client(), &rid(grant_id)?)
        .await
        .map_err(|e| map_store_err("fetch_grant", e))?
    else {
        return Ok(None);
    };
    serde_json::from_value(row)
        .map(Some)
        .map_err(|e| CopalError::Store(format!("grant row shape: {e}")))
}

/// Atomically consume one use. `Ok(false)` means the grant refused:
/// revoked, unarmed, expired, or exhausted, indistinguishable on
/// purpose.
/// Whether the grant is currently redeemable, WITHOUT consuming a use
/// (the same guards as [`consume`], engine-side clock included, as a
/// read). The 304-revalidation path uses this so a revoked or expired
/// grant cannot keep refreshing a cache it no longer authorizes.
pub async fn redeemable(store: &Store, grant_id: &str) -> copal_core::Result<bool> {
    let query = Query::new()
        .select(Some(vec!["id".to_owned()]))
        .from_table(TABLE)
        .map_err(|e| map_store_err("grant_redeemable", e))?
        .where_str(format!("id = {}", rid(grant_id)?))
        .where_(is_none("revoked_at"))
        .where_(is_not_none("file"))
        .where_(is_not_none("expires_at"))
        .where_str("expires_at > time::now()")
        .where_str("(max_uses IS NONE OR uses < max_uses)")
        .limit(1)
        .map_err(|e| map_store_err("grant_redeemable", e))?;
    let rows: Vec<serde_json::Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("grant_redeemable", e))?;
    Ok(!rows.is_empty())
}

pub async fn consume(store: &Store, grant_id: &str) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(rid(grant_id)?.to_string())
        .map_err(|e| map_store_err("consume_grant", e))?
        .set_expr("uses", raw("uses + 1"))
        .map_err(|e| map_store_err("consume_grant", e))?
        .where_(is_none("revoked_at"))
        .where_(is_not_none("file"))
        .where_(is_not_none("expires_at"))
        // Server clock on both sides; the client's clock buys nothing.
        .where_str("expires_at > time::now()")
        .where_str("(max_uses IS NONE OR uses < max_uses)")
        .return_after();
    let rows: Vec<GrantRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("consume_grant", e))?;
    Ok(!rows.is_empty())
}

/// Revoke a grant. Idempotent from the caller's view: revoking an
/// already-revoked or absent grant reports `NotFound`.
pub async fn revoke(store: &Store, tenant: &TenantId, grant_id: &str) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(grant_id)?.to_string())
        .map_err(|e| map_store_err("revoke_grant", e))?
        .set_expr("revoked_at", raw("time::now()"))
        .map_err(|e| map_store_err("revoke_grant", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(is_none("revoked_at"))
        .return_after();
    let rows: Vec<GrantRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("revoke_grant", e))?;
    if rows.is_empty() {
        return Err(CopalError::not_found(format!("grant {grant_id}")));
    }
    Ok(())
}
