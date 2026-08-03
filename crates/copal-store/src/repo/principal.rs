//! Principals: named actors under a tenant that keys belong to.
//!
//! A key without a principal is a tenant-level credential, exactly as
//! every key was before principals existed. Everything here is
//! additive on that base: creating a principal changes nothing until
//! a key is minted under it.

use serde::Deserialize;
use serde_json::{json, Value};
use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{CopalError, TenantId};

use crate::dto::map_store_err;
use crate::Store;

const TABLE: &str = "principal";

/// One named actor.
#[derive(Debug, Clone, Deserialize)]
pub struct PrincipalRow {
    pub id: String,
    pub tenant_id: String,
    pub handle: String,
    pub kind: String,
    /// Comma-joined scope ceiling; empty means every scope.
    #[serde(default)]
    pub scopes: String,
    #[serde(default)]
    pub disabled_at: Option<String>,
    pub created_at: String,
}

impl PrincipalRow {
    /// The bare row id (record prefix stripped).
    pub fn principal_id(&self) -> String {
        crate::dto::strip_record_prefix(&self.id, TABLE).to_owned()
    }

    /// The scope ceiling as a list; empty means unrestricted.
    pub fn scope_list(&self) -> Vec<String> {
        if self.scopes.is_empty() {
            Vec::new()
        } else {
            self.scopes.split(',').map(str::to_owned).collect()
        }
    }
}

/// Create a principal. The unique `(tenant, handle)` index turns a
/// duplicate into a conflict rather than a second actor with the
/// same name.
pub async fn create_principal(
    store: &Store,
    tenant: &TenantId,
    handle: &str,
    kind: &str,
    scopes: &[String],
) -> copal_core::Result<PrincipalRow> {
    if handle.trim().is_empty() {
        return Err(CopalError::validation("a principal needs a handle"));
    }
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid = RecordID::<()>::new(TABLE, id.as_str()).map_err(|e| map_store_err("principal", e))?;
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "handle": handle,
        "kind": kind,
        "scopes": scopes.join(","),
    });
    let created = create_record(store.client(), &rid.to_string(), payload)
        .await
        .map_err(|e| {
            let text = e.to_string();
            if text.contains("uniq_principal_handle") {
                CopalError::conflict(format!("principal {handle:?} already exists"))
            } else {
                map_store_err("create_principal", e)
            }
        })?;
    let row = created
        .record
        .ok_or_else(|| CopalError::Store("create returned no record".into()))?;
    serde_json::from_value(row).map_err(|e| CopalError::Store(format!("principal shape: {e}")))
}

/// Every principal under a tenant, newest first.
pub async fn list_principals(
    store: &Store,
    tenant: &TenantId,
) -> copal_core::Result<Vec<PrincipalRow>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("list_principals", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .order_by("created_at", "DESC")
        .map_err(|e| map_store_err("list_principals", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("list_principals", e))
}

/// One principal by handle.
pub async fn get_by_handle(
    store: &Store,
    tenant: &TenantId,
    handle: &str,
) -> copal_core::Result<Option<PrincipalRow>> {
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("get_principal", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(eq("handle", handle));
    let rows: Vec<PrincipalRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("get_principal", e))?;
    Ok(rows.into_iter().next())
}

/// One principal by bare row id, tenant-checked.
pub async fn get_by_id(
    store: &Store,
    tenant: &TenantId,
    principal_id: &str,
) -> copal_core::Result<Option<PrincipalRow>> {
    let rid =
        RecordID::<()>::new(TABLE, principal_id).map_err(|e| map_store_err("get_principal", e))?;
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("get_principal", e))?
        .where_str(format!("id = {rid}"))
        .where_(eq("tenant_id", tenant.as_str()));
    let rows: Vec<PrincipalRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("get_principal", e))?;
    Ok(rows.into_iter().next())
}

/// Disable a principal: every key minted under it refuses from this
/// moment, which is the operation an incident actually needs.
pub async fn disable_principal(
    store: &Store,
    tenant: &TenantId,
    handle: &str,
) -> copal_core::Result<bool> {
    let query = Query::new()
        .update_set(TABLE)
        .map_err(|e| map_store_err("disable_principal", e))?
        .set_expr("disabled_at", raw("time::now()"))
        .map_err(|e| map_store_err("disable_principal", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_(eq("handle", handle))
        .where_str("disabled_at IS NONE")
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("disable_principal", e))?;
    Ok(!rows.is_empty())
}
