//! Read-only observability over the shared engine: which namespaces
//! live beside this deployment's, and what they hold.
//!
//! One SurrealDB server can carry several services in sibling
//! namespaces. When the deployment's credentials have root reach,
//! this module walks `INFO FOR KV`, then each namespace's databases
//! and tables, and counts rows, all strictly read-only. Every query
//! is a self-addressing compound (`USE NS .. DB ..; ...`) on a
//! private connection, so the store's own session is never touched.
//!
//! The walk needs a remote engine (`ws://` or `http://`): an
//! embedded engine is single-process, and a second connection to it
//! would open a second engine rather than the same one.

use serde_json::Value;

use copal_core::CopalError;
use surql::connection::{ConnectionConfig, DatabaseClient};

use crate::store::StoreConfig;

/// One table with its row count; `None` when the count was refused
/// or the name is too strange to query safely.
#[derive(Debug, Clone)]
pub struct TableCount {
    pub name: String,
    pub rows: Option<i64>,
}

/// One database inside a namespace.
#[derive(Debug, Clone)]
pub struct DatabaseView {
    pub name: String,
    pub tables: Vec<TableCount>,
}

/// One namespace on the shared engine.
#[derive(Debug, Clone)]
pub struct NamespaceView {
    pub name: String,
    pub databases: Vec<DatabaseView>,
}

/// Whether the fleet walk can run at all against this configuration.
pub fn walkable(cfg: &StoreConfig) -> bool {
    let url = cfg.url.as_str();
    url.starts_with("ws://")
        || url.starts_with("wss://")
        || url.starts_with("http://")
        || url.starts_with("https://")
}

async fn private_client(cfg: &StoreConfig) -> copal_core::Result<DatabaseClient> {
    let mut builder = ConnectionConfig::builder()
        .url(cfg.url.clone())
        .namespace(cfg.namespace.clone())
        .database(cfg.database.clone());
    if let (Some(user), Some(pass)) = (cfg.username.clone(), cfg.password.clone()) {
        builder = builder.username(user).password(pass);
    }
    let config = builder
        .build()
        .map_err(|e| CopalError::Store(format!("fleet config: {e}")))?;
    let client =
        DatabaseClient::new(config).map_err(|e| CopalError::Store(format!("fleet client: {e}")))?;
    client
        .connect()
        .await
        .map_err(|e| CopalError::Store(format!("fleet connect: {e}")))?;
    Ok(client)
}

/// Keys of every object found at any of `pointers` across the
/// statement results of one answer.
pub(crate) fn keys_at(value: &Value, pointers: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Value::Array(stmts) = value {
        for stmt in stmts {
            for pointer in pointers {
                if let Some(map) = stmt.pointer(pointer).and_then(Value::as_object) {
                    out.extend(map.keys().cloned());
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// The first `rows` figure found across statement results: the
/// count query's answer, wherever the `USE` preamble left it.
pub(crate) fn rows_of(value: &Value) -> Option<i64> {
    if let Value::Array(stmts) = value {
        for stmt in stmts {
            if let Some(rows) = stmt.pointer("/0/rows").and_then(Value::as_i64) {
                return Some(rows);
            }
        }
    }
    None
}

fn plain(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Walk the engine: every namespace, its databases, their tables,
/// and row counts, tables capped at `max_tables` per database.
pub async fn overview(
    cfg: &StoreConfig,
    max_tables: usize,
) -> copal_core::Result<Vec<NamespaceView>> {
    if !walkable(cfg) {
        return Err(CopalError::Store(
            "the fleet walk needs a remote engine; an embedded engine has no siblings".into(),
        ));
    }
    let client = private_client(cfg).await?;
    let kv = client
        .query("INFO FOR KV;")
        .await
        .map_err(|e| CopalError::Store(format!("INFO FOR KV: {e}")))?;
    let mut views = Vec::new();
    for ns in keys_at(&kv, &["/namespaces", "/ns"]) {
        if !plain(&ns) {
            continue;
        }
        let ns_info = client
            .query(&format!("USE NS {ns}; INFO FOR NS;"))
            .await
            .map_err(|e| CopalError::Store(format!("INFO FOR NS {ns}: {e}")))?;
        let mut databases = Vec::new();
        for db in keys_at(&ns_info, &["/databases", "/db"]) {
            if !plain(&db) {
                continue;
            }
            let db_info = client
                .query(&format!("USE NS {ns} DB {db}; INFO FOR DB;"))
                .await
                .map_err(|e| CopalError::Store(format!("INFO FOR DB {ns}/{db}: {e}")))?;
            let mut names = keys_at(&db_info, &["/tables", "/tb"]);
            names.retain(|name| plain(name));
            names.truncate(max_tables);
            let mut tables = Vec::new();
            for name in names {
                let rows = client
                    .query(&format!(
                        "USE NS {ns} DB {db}; SELECT count() AS rows FROM {name} GROUP ALL;"
                    ))
                    .await
                    .ok()
                    .as_ref()
                    .and_then(rows_of);
                tables.push(TableCount { name, rows });
            }
            databases.push(DatabaseView { name: db, tables });
        }
        views.push(NamespaceView {
            name: ns,
            databases,
        });
    }
    Ok(views)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keys_read_both_info_shapes() {
        let modern = json!([{ "namespaces": { "alpha": "DEFINE ...", "copal": "DEFINE ..." } }]);
        assert_eq!(
            keys_at(&modern, &["/namespaces", "/ns"]),
            ["alpha", "copal"]
        );
        let terse = json!([null, { "ns": { "beta": "DEFINE ..." } }]);
        assert_eq!(keys_at(&terse, &["/namespaces", "/ns"]), ["beta"]);
        assert!(keys_at(&json!([]), &["/namespaces"]).is_empty());
    }

    #[test]
    fn counts_survive_the_use_preamble() {
        let answer = json!([null, [{ "rows": 776000 }]]);
        assert_eq!(rows_of(&answer), Some(776000));
        assert_eq!(rows_of(&json!([null, []])), None);
    }

    #[test]
    fn strange_names_never_reach_a_query() {
        assert!(plain("security_transaction"));
        assert!(!plain("evil; REMOVE TABLE x"));
        assert!(!plain(""));
    }

    #[test]
    fn embedded_engines_are_named_unwalkable() {
        let mut cfg = StoreConfig::memory();
        assert!(!walkable(&cfg));
        cfg.url = "ws://127.0.0.1:8000".into();
        assert!(walkable(&cfg));
    }
}
