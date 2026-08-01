//! Tenant settings: storage residency pinning.
//!
//! One row per tenant at most; absence means `local`. Assignment
//! affects new content only, because blob rows carry their residency
//! from sighting and serving resolves backends from the row.

use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::query::expressions::raw;
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

const QUOTA_TABLE: &str = "tenant_quota";

/// A tenant's logical usage: live files and the sum of their current
/// sizes. Version history and cross-file dedupe play no part; the
/// number reported is the number the quota compares against.
pub async fn usage(store: &Store, tenant: &TenantId) -> copal_core::Result<(i64, i64)> {
    let query = Query::new()
        .select(Some(vec![
            "math::sum(size_bytes ?? 0) AS total_bytes".to_owned(),
            "count() AS files".to_owned(),
        ]))
        .from_table("file")
        .map_err(|e| map_store_err("usage", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_str("deleted_at IS NONE")
        .group_all();
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("usage", e))?;
    let bytes = rows
        .first()
        .and_then(|r| r.get("total_bytes"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let files = rows
        .first()
        .and_then(|r| r.get("files"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    Ok((bytes, files))
}

/// The tenant's quota ceiling in bytes; `None` means unlimited.
pub async fn get_quota(store: &Store, tenant: &TenantId) -> copal_core::Result<Option<i64>> {
    let query = Query::new()
        .select(Some(vec!["max_bytes".to_owned()]))
        .from_table(QUOTA_TABLE)
        .map_err(|e| map_store_err("get_quota", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .limit(1)
        .map_err(|e| map_store_err("get_quota", e))?;
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("get_quota", e))?;
    Ok(rows
        .first()
        .and_then(|row| row.get("max_bytes"))
        .and_then(Value::as_i64))
}

/// Set the quota ceiling; the same update-create-retry upsert shape
/// as residency pinning.
pub async fn set_quota(store: &Store, tenant: &TenantId, max_bytes: i64) -> copal_core::Result<()> {
    if max_bytes < 0 {
        return Err(CopalError::validation("max_bytes must be non-negative"));
    }
    let update_in_place = || async {
        let update = Query::new()
            .update_set(QUOTA_TABLE)
            .map_err(|e| map_store_err("set_quota", e))?
            .set("max_bytes", Value::from(max_bytes))
            .map_err(|e| map_store_err("set_quota", e))?
            .where_(eq("tenant_id", tenant.as_str()))
            .return_after();
        let rows: Vec<Value> = query_records(store.client(), &update)
            .await
            .map_err(|e| map_store_err("set_quota", e))?;
        Ok::<bool, CopalError>(!rows.is_empty())
    };
    if update_in_place().await? {
        return Ok(());
    }
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid =
        RecordID::<()>::new(QUOTA_TABLE, id.as_str()).map_err(|e| map_store_err("set_quota", e))?;
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "max_bytes": max_bytes,
    });
    match create_record(store.client(), &rid.to_string(), payload).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let mapped = map_store_err("set_quota", err);
            if matches!(mapped, CopalError::Conflict(_)) && update_in_place().await? {
                Ok(())
            } else {
                Err(mapped)
            }
        }
    }
}

/// Remove the quota row, returning to unlimited. Returns whether a
/// row existed.
pub async fn clear_quota(store: &Store, tenant: &TenantId) -> copal_core::Result<bool> {
    let find = Query::new()
        .select(Some(vec!["id".to_owned()]))
        .from_table(QUOTA_TABLE)
        .map_err(|e| map_store_err("clear_quota", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .limit(1)
        .map_err(|e| map_store_err("clear_quota", e))?;
    let rows: Vec<Value> = query_records(store.client(), &find)
        .await
        .map_err(|e| map_store_err("clear_quota", e))?;
    let Some(raw_id) = rows
        .first()
        .and_then(|row| row.get("id"))
        .and_then(|v| v.as_str())
    else {
        return Ok(false);
    };
    let bare = crate::dto::strip_record_prefix(raw_id, QUOTA_TABLE);
    let rid =
        RecordID::<()>::new(QUOTA_TABLE, bare).map_err(|e| map_store_err("clear_quota", e))?;
    let delete = Query::new()
        .delete(rid.to_string())
        .map_err(|e| map_store_err("clear_quota", e))?;
    query_records::<Value>(store.client(), &delete)
        .await
        .map_err(|e| map_store_err("clear_quota", e))?;
    Ok(true)
}

const USAGE_TABLE: &str = "tenant_usage";

/// The cached usage counter, or `None` when no row exists yet.
pub async fn cached_usage(
    store: &Store,
    tenant: &TenantId,
) -> copal_core::Result<Option<(i64, i64)>> {
    let query = Query::new()
        .select(Some(vec!["bytes".to_owned(), "files".to_owned()]))
        .from_table(USAGE_TABLE)
        .map_err(|e| map_store_err("cached_usage", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .limit(1)
        .map_err(|e| map_store_err("cached_usage", e))?;
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("cached_usage", e))?;
    Ok(rows.first().map(|row| {
        (
            row.get("bytes").and_then(Value::as_i64).unwrap_or(0),
            row.get("files").and_then(Value::as_i64).unwrap_or(0),
        )
    }))
}

/// Write the counter to a known figure, creating the row if needed.
/// Reconciliation and lazy initialization both land here.
pub async fn set_usage(
    store: &Store,
    tenant: &TenantId,
    bytes: i64,
    files: i64,
) -> copal_core::Result<()> {
    let update = Query::new()
        .update_set(USAGE_TABLE)
        .map_err(|e| map_store_err("set_usage", e))?
        .set("bytes", Value::from(bytes))
        .map_err(|e| map_store_err("set_usage", e))?
        .set("files", Value::from(files))
        .map_err(|e| map_store_err("set_usage", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &update)
        .await
        .map_err(|e| map_store_err("set_usage", e))?;
    if !rows.is_empty() {
        return Ok(());
    }
    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid =
        RecordID::<()>::new(USAGE_TABLE, id.as_str()).map_err(|e| map_store_err("set_usage", e))?;
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "bytes": bytes,
        "files": files,
    });
    match create_record(store.client(), &rid.to_string(), payload).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let mapped = map_store_err("set_usage", err);
            // A racing initializer won; its figure is as good as ours.
            if matches!(mapped, CopalError::Conflict(_)) {
                Ok(())
            } else {
                Err(mapped)
            }
        }
    }
}

/// Reserve bytes against the ceiling, atomically.
///
/// The guard and the increment are ONE statement, so concurrent
/// uploads cannot each read the same headroom and collectively
/// overshoot: the second one's WHERE no longer holds. Returns whether
/// the reservation succeeded. A quota of `None` still counts the
/// bytes; only the refusal is skipped.
pub async fn reserve_usage(
    store: &Store,
    tenant: &TenantId,
    bytes: i64,
    quota: Option<i64>,
) -> copal_core::Result<bool> {
    let mut update = Query::new()
        .update_set(USAGE_TABLE)
        .map_err(|e| map_store_err("reserve_usage", e))?
        .set_expr("bytes", raw(format!("bytes + {bytes}")))
        .map_err(|e| map_store_err("reserve_usage", e))?
        .where_(eq("tenant_id", tenant.as_str()));
    if let Some(quota) = quota {
        update = update.where_str(format!("bytes + {bytes} <= {quota}"));
    }
    let rows: Vec<Value> = query_records(store.client(), &update.return_after())
        .await
        .map_err(|e| map_store_err("reserve_usage", e))?;
    Ok(!rows.is_empty())
}

/// Give reserved bytes back (a failed or smaller-than-declared
/// upload). Never drops below zero; the sweep's recount is the
/// backstop for any drift this cannot see.
pub async fn release_usage(store: &Store, tenant: &TenantId, bytes: i64) -> copal_core::Result<()> {
    if bytes == 0 {
        return Ok(());
    }
    let update = Query::new()
        .update_set(USAGE_TABLE)
        .map_err(|e| map_store_err("release_usage", e))?
        .set_expr("bytes", raw(format!("math::max([bytes - {bytes}, 0])")))
        .map_err(|e| map_store_err("release_usage", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .return_after();
    query_records::<Value>(store.client(), &update)
        .await
        .map_err(|e| map_store_err("release_usage", e))?;
    Ok(())
}

/// Adjust the file count alongside a completed or removed file.
pub async fn bump_files(store: &Store, tenant: &TenantId, delta: i64) -> copal_core::Result<()> {
    let update = Query::new()
        .update_set(USAGE_TABLE)
        .map_err(|e| map_store_err("bump_files", e))?
        .set_expr("files", raw(format!("math::max([files + {delta}, 0])")))
        .map_err(|e| map_store_err("bump_files", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .return_after();
    query_records::<Value>(store.client(), &update)
        .await
        .map_err(|e| map_store_err("bump_files", e))?;
    Ok(())
}

/// Recompute one tenant's counter from its files: the authoritative
/// figure, written back over whatever the cache drifted to.
pub async fn reconcile_usage(store: &Store, tenant: &TenantId) -> copal_core::Result<(i64, i64)> {
    let (bytes, files) = usage(store, tenant).await?;
    set_usage(store, tenant, bytes, files).await?;
    Ok((bytes, files))
}

/// Tenants carrying a usage row, for the reconciliation sweep.
pub async fn tenants_with_usage(store: &Store, limit: i64) -> copal_core::Result<Vec<String>> {
    let query = Query::new()
        .select(Some(vec!["tenant_id".to_owned()]))
        .from_table(USAGE_TABLE)
        .map_err(|e| map_store_err("tenants_with_usage", e))?
        .limit(limit)
        .map_err(|e| map_store_err("tenants_with_usage", e))?;
    let rows: Vec<Value> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("tenants_with_usage", e))?;
    Ok(rows
        .iter()
        .filter_map(|row| row.get("tenant_id").and_then(|v| v.as_str()))
        .map(str::to_owned)
        .collect())
}
