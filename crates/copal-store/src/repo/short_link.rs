//! Short-link repository: create, repoint, resolve.
//!
//! The link is the record key, so creating one that exists fails in
//! the engine and the caller mints another code. A link belongs to the
//! tenant that created it; only that tenant can repoint it. Resolution
//! answers with the grant id alone and leaves every guard (operation,
//! expiry, revocation, uses) to grant redemption.

use serde::Deserialize;
use serde_json::json;

use surql::query::builder::Query;
use surql::query::crud::{create_record, get_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{CopalError, TenantId};

use crate::dto::map_store_err;
use crate::store::Store;

const TABLE: &str = "short_link";

fn rid(link: &str) -> copal_core::Result<RecordID<()>> {
    RecordID::new(TABLE, link).map_err(|e| map_store_err("short link id", e))
}

/// A short-link row as resolution sees it.
#[derive(Debug, Clone, Deserialize)]
pub struct ShortLinkRow {
    pub tenant_id: String,
    pub grant_id: String,
}

/// Create a link pointing at a grant. A taken link is a
/// [`CopalError::Conflict`], which the caller answers with a new code.
pub async fn create(
    store: &Store,
    tenant: &TenantId,
    link: &str,
    grant_id: &str,
    created_by: &str,
) -> copal_core::Result<()> {
    create_record(
        store.client(),
        &rid(link)?.to_string(),
        json!({
            "tenant_id": tenant.as_str(),
            "grant_id": grant_id,
            "created_by": created_by,
        }),
    )
    .await
    .map_err(|e| map_store_err("create_short_link", e))?;
    Ok(())
}

/// Point an existing link at another grant. A link that is absent or
/// belongs to another tenant is `NotFound`, indistinguishably.
pub async fn repoint(
    store: &Store,
    tenant: &TenantId,
    link: &str,
    grant_id: &str,
) -> copal_core::Result<()> {
    let query = Query::new()
        .update_set(rid(link)?.to_string())
        .map_err(|e| map_store_err("repoint_short_link", e))?
        .set("grant_id", json!(grant_id))
        .map_err(|e| map_store_err("repoint_short_link", e))?
        .set_expr("updated_at", raw("time::now()"))
        .map_err(|e| map_store_err("repoint_short_link", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .return_after();
    let rows: Vec<ShortLinkRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("repoint_short_link", e))?;
    if rows.is_empty() {
        return Err(CopalError::not_found(format!("short link {link}")));
    }
    Ok(())
}

/// Look a link up. `None` for an unknown link.
pub async fn resolve(store: &Store, link: &str) -> copal_core::Result<Option<ShortLinkRow>> {
    let Some(row) = get_record(store.client(), &rid(link)?)
        .await
        .map_err(|e| map_store_err("resolve_short_link", e))?
    else {
        return Ok(None);
    };
    serde_json::from_value(row)
        .map(Some)
        .map_err(|e| CopalError::Store(format!("short link row shape: {e}")))
}
