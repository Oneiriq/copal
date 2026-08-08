//! The evolution mechanism end to end on a live engine.
//!
//! Boot introspects the database (`INFO FOR DB` plus `INFO FOR
//! TABLE` per table), diffs it against the code's snapshot, and
//! applies only the differences as `OVERWRITE` forms. These tests
//! hold the two invariants that make that safe: a fresh apply diffs
//! to zero (no perpetual re-application), and a database missing
//! later definitions receives them on the next apply without losing
//! a row.

use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::migration::diff::{diff_schemas, SchemaSnapshot};
use surql::schema::parser::{parse_db_info, parse_table_full};

use copal_core::{FileSpec, TenantId};
use copal_store::repo::file;
use copal_store::{Store, StoreConfig};

async fn raw_client(namespace: &str) -> DatabaseClient {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace(namespace)
        .database(namespace)
        .build()
        .unwrap();
    let client = DatabaseClient::new(cfg).unwrap();
    client.connect().await.unwrap();
    client
}

async fn db_snapshot(client: &DatabaseClient) -> SchemaSnapshot {
    let info = client.query("INFO FOR DB;").await.unwrap();
    let parsed = parse_db_info(&info[0]).expect("info parses");
    let mut tables = Vec::new();
    for (name, shallow) in &parsed.tables {
        let table_info = client
            .query(&format!("INFO FOR TABLE {name};"))
            .await
            .unwrap();
        tables.push(parse_table_full(name, &shallow.to_surql(), &table_info[0]).unwrap());
    }
    SchemaSnapshot {
        tables,
        analyzers: parsed.analyzers.into_values().collect(),
        ..Default::default()
    }
}

/// The no-perpetual-reapply invariant: what the code applies, the
/// engine echoes back structurally identical.
#[tokio::test]
async fn fresh_apply_diffs_to_zero() {
    let client = raw_client("evo_zero").await;
    let script = copal_store::schema::schema_statements().join("\n");
    client.query(&script).await.expect("schema applies");

    let code =
        copal_store::schema::code_snapshot(None, &copal_store::schema::EnginePolicy::default());
    let db = db_snapshot(&client).await;
    let diffs = diff_schemas(&code, &db);
    assert!(diffs.is_empty(), "{diffs:#?}");
}

/// The upgrade shape: a database created before `PERMISSIONS`
/// existed receives them on the next apply, keeps its rows, and then
/// diffs to zero.
#[tokio::test]
async fn apply_heals_older_databases_without_touching_rows() {
    let client = raw_client("evo_heal").await;

    // The pre-permissions release, reconstructed: today's tables with
    // every permission clause stripped.
    let mut old_tables = copal_store::schema::tables();
    for table in &mut old_tables {
        table.permissions = None;
        for field in &mut table.fields {
            field.permissions = None;
        }
    }
    let mut old_statements: Vec<String> = copal_store::schema::text::analyzers()
        .iter()
        .map(|a| a.to_surql_with_options(true))
        .collect();
    old_statements.extend(
        old_tables
            .iter()
            .flat_map(|t| surql::schema::generate_table_sql(t, true)),
    );
    client
        .query(&old_statements.join("\n"))
        .await
        .expect("old schema applies");

    let store = Store::from_connected(client);
    let tenant = TenantId::parse("acme").unwrap();
    file::create_file(
        &store,
        &tenant,
        &FileSpec {
            path: "kept.txt".to_owned(),
            content_type: "text/plain".to_owned(),
            access: copal_core::AccessLevel::Private,
            metadata: serde_json::json!({}),
            idempotency_key: None,
        },
        "tester",
    )
    .await
    .unwrap();

    store.apply_schema(None, None).await.expect("apply heals");

    let healed = db_snapshot(store.raw()).await;
    let file_table = healed
        .tables
        .iter()
        .find(|t| t.name == "file")
        .expect("file table");
    let permissions = file_table
        .permissions
        .as_ref()
        .expect("permissions applied");
    assert!(
        permissions.values().any(|rule| rule.contains("$token.tn")),
        "{permissions:?}"
    );

    let rows = file::list_files(&store, &tenant, 10, None, false, None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "rows must survive healing");
    assert_eq!(rows[0].path, "kept.txt");

    let diffs = diff_schemas(
        &copal_store::schema::code_snapshot(None, &copal_store::schema::EnginePolicy::default()),
        &healed,
    );
    assert!(diffs.is_empty(), "{diffs:#?}");
}

/// The vector index rides the same diff: applying at a width creates
/// it, re-applying at the same width changes nothing, and the code
/// snapshot with the dimension diffs to zero afterward.
#[tokio::test]
async fn vector_index_joins_the_diff() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    store.ensure_vector_index(384).await.unwrap();
    store.ensure_vector_index(384).await.unwrap();
    let db = db_snapshot(store.raw()).await;
    let diffs = diff_schemas(
        &copal_store::schema::code_snapshot(
            Some(384),
            &copal_store::schema::EnginePolicy::default(),
        ),
        &db,
    );
    assert!(diffs.is_empty(), "{diffs:#?}");
}
