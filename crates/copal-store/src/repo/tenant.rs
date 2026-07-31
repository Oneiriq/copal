//! Tenant settings: storage residency pinning.
//!
//! One row per tenant at most; absence means `local`. Assignment
//! affects new content only, because blob rows carry their residency
//! from sighting and serving resolves backends from the row.

use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{CopalError, TenantId};

use crate::dto::map_store_err;
use crate::store::Store;

const TABLE: &str = "tenant_storage";

/// Residency names must parse back out of blob row ids, so the
/// alphabet is lowercase alphanumeric only.
pub fn valid_residency_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// The residency a tenant's new content lands in.
pub async fn get_residency(store: &Store, tenant: &TenantId) -> copal_core::Result<String> {
    let query = Query::new()
        .select(Some(vec!["residency".to_owned()]))
        .from_table(TABLE)
        .map_err(|e| map_store_err("get_residency", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .limit(1)
        .map_err(|e| map_store_err("get_residency", e))?;
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("get_residency", e))?;
    Ok(rows
        .first()
        .and_then(|row| row.get("residency"))
        .and_then(|v| v.as_str())
        .unwrap_or("local")
        .to_owned())
}

/// Pin a tenant's new content to a residency. Upserts the single row.
pub async fn set_residency(
    store: &Store,
    tenant: &TenantId,
    residency: &str,
) -> copal_core::Result<()> {
    if !valid_residency_name(residency) {
        return Err(CopalError::validation(
            "residency names are 1..=32 lowercase alphanumeric characters",
        ));
    }
    // One row per tenant via the unique index: update in place when
    // present, create otherwise, retry the update on a create race.
    let update_in_place = || async {
        let update = Query::new()
            .update_set(TABLE)
            .map_err(|e| map_store_err("set_residency", e))?
            .set("residency", Value::from(residency))
            .map_err(|e| map_store_err("set_residency", e))?
            .where_(eq("tenant_id", tenant.as_str()))
            .return_after();
        let rows: Vec<Value> = query_records(store.client(), &update)
            .await
            .map_err(|e| map_store_err("set_residency", e))?;
        Ok::<bool, CopalError>(!rows.is_empty())
    };
    if update_in_place().await? {
        return Ok(());
    }
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid =
        RecordID::<()>::new(TABLE, id.as_str()).map_err(|e| map_store_err("set_residency", e))?;
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "residency": residency,
    });
    match create_record(store.client(), &rid.to_string(), payload).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let mapped = map_store_err("set_residency", err);
            if matches!(mapped, CopalError::Conflict(_)) && update_in_place().await? {
                Ok(())
            } else {
                Err(mapped)
            }
        }
    }
}
