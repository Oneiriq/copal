//! Sealed-secret rotation: one sweep over every table holding sealed
//! bytes, so a master key change is a single operator motion.

use serde_json::Value;

use surql::query::builder::Query;
use surql::query::crud::query_records;

use crate::dto::map_store_err;
use crate::store::Store;

/// Every column holding bytes sealed under the blob master key. A new
/// sealed column joins this list or its rows survive a rotation only
/// by luck.
const SEALED_COLUMNS: &[(&str, &str)] = &[
    ("s3_credential", "secret_sealed"),
    ("webhook_endpoint", "secret_sealed"),
    ("edge_key", "secret_sealed"),
];

/// Walk every sealed column and rewrite the rows the closure
/// re-seals. The closure sees the stored string and returns the fresh
/// seal only for rows sitting under the retiring key, so a drained
/// rotation writes nothing. Returns how many rows were rewritten.
pub async fn reseal_secrets(
    store: &Store,
    reseal: impl Fn(&str) -> Option<String>,
) -> copal_core::Result<usize> {
    let mut rewritten = 0usize;
    for (table, column) in SEALED_COLUMNS {
        let query = Query::new()
            .select(Some(vec!["id".to_owned(), (*column).to_owned()]))
            .from_table(*table)
            .map_err(|e| map_store_err("reseal_secrets", e))?;
        let rows: Vec<Value> = query_records(store.client(), &query)
            .await
            .map_err(|e| map_store_err("reseal_secrets", e))?;
        for row in rows {
            let id = row.get("id").and_then(Value::as_str);
            let sealed = row.get(*column).and_then(Value::as_str);
            let (Some(id), Some(sealed)) = (id, sealed) else {
                continue;
            };
            let Some(fresh) = reseal(sealed) else {
                continue;
            };
            let update = Query::new()
                .update_set(id.to_string())
                .map_err(|e| map_store_err("reseal_secrets", e))?
                .set(*column, Value::from(fresh))
                .map_err(|e| map_store_err("reseal_secrets", e))?
                .return_after();
            let _: Vec<Value> = query_records(store.client(), &update)
                .await
                .map_err(|e| map_store_err("reseal_secrets", e))?;
            rewritten += 1;
        }
    }
    Ok(rewritten)
}
