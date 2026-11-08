//! Engine behavior Copal's retrieval design depends on.
//!
//! These are not tests of Copal. They pin the SurrealDB facts that
//! decided how search works here, so an engine upgrade that changes
//! one of them fails a test instead of quietly making a comment wrong.

use surql::connection::{ConnectionConfig, DatabaseClient};
use surql::query::crud::query_records;

async fn seeded_client() -> DatabaseClient {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace("assumptions")
        .database("assumptions")
        .build()
        .unwrap();
    let client = DatabaseClient::new(cfg).unwrap();
    client.connect().await.unwrap();
    for statement in [
        "DEFINE ANALYZER probe_text TOKENIZERS class FILTERS lowercase,snowball(english);",
        "DEFINE TABLE doc SCHEMALESS;",
        "DEFINE INDEX idx_body ON TABLE doc COLUMNS body FULLTEXT ANALYZER probe_text BM25;",
    ] {
        client.inner().query(statement).await.expect(statement);
    }
    // Documents of very different lengths and term frequencies, so a
    // real BM25 implementation would score them differently.
    for body in [
        "the quick brown fox jumps over the lazy dog on a long and \
         rambling afternoon full of unrelated words",
        "quick quick quick fox",
        "a treatise on the migratory patterns of waterfowl",
    ] {
        client
            .inner()
            .query(format!("CREATE doc SET body = '{body}';"))
            .await
            .unwrap();
    }
    client
}

/// The embedded engine is SurrealDB 4.0, whose `search::score` reports a
/// real per-row BM25 value: above zero for every match, higher for the
/// short term-dense document than for the long one. SurrealDB 3.x
/// returned 0 for every row, and a `ws://` deployment's server may still
/// be 3.x, which is why `SearchHit` carries no score field and hybrid
/// retrieval fuses RANKS rather than values. If this test fails, the
/// engine stopped scoring per row.
#[tokio::test]
async fn the_engine_reports_a_per_row_bm25_score() {
    let client = seeded_client().await;
    let query = surql::query::Query::new()
        .select(Some(vec![
            "body".to_owned(),
            "search::score(1) AS score".to_owned(),
        ]))
        .from_table("doc")
        .unwrap()
        .where_str("body @1@ 'quick fox'");
    let hits: Vec<serde_json::Value> = query_records(&client, &query).await.expect("rows");

    assert_eq!(hits.len(), 2, "both matching documents come back: {hits:?}");
    let score_of = |prefix: &str| {
        hits.iter()
            .find(|h| h["body"].as_str().is_some_and(|b| b.starts_with(prefix)))
            .and_then(|h| h["score"].as_f64())
            .unwrap_or_else(|| panic!("a numeric score for {prefix:?}: {hits:?}"))
    };
    let dense = score_of("quick quick quick fox");
    let rambling = score_of("the quick brown fox");
    assert!(
        rambling > 0.0 && dense > rambling,
        "SurrealDB 4 scores every match and the term-dense one higher \
         (dense {dense}, rambling {rambling})",
    );
}

/// The full-text scan ranks only when the statement asks it to.
///
/// SurrealDB 3.x yielded matching rows in insertion order whatever was
/// asked: measured across a bare scan, the score projected, ORDER BY the
/// projected alias, the `@@` operator without a reference, and an index
/// defined with HIGHLIGHTS, none ranked, and `ORDER BY search::score(1)`
/// was a parse error. SurrealDB 4.0 returns a statement's rows in no
/// particular order without an ORDER BY, so the search states one (id
/// order, which is what 3.x gave), and it ranks on the projected score
/// when asked, which this pins. Lexical ranking still happens outside
/// the engine because a deployment's server may be 3.x, where the same
/// ORDER BY ranks nothing.
#[tokio::test]
async fn the_full_text_scan_ranks_only_when_asked() {
    let client = seeded_client().await;
    let query = surql::query::Query::new()
        .select(Some(vec![
            "body".to_owned(),
            "search::score(1) AS score".to_owned(),
        ]))
        .from_table("doc")
        .unwrap()
        .where_str("body @1@ 'quick fox'")
        .order_by("score", "DESC")
        .unwrap();
    let hits: Vec<serde_json::Value> = query_records(&client, &query).await.expect("rows");

    let bodies: Vec<&str> = hits.iter().filter_map(|h| h["body"].as_str()).collect();
    assert_eq!(bodies.len(), 2, "{bodies:?}");
    // The long rambling document was inserted first, and the short
    // term-dense one is the better BM25 match by both term frequency and
    // length normalization, so it comes first only if the engine ranks.
    assert!(
        bodies[0].starts_with("quick quick quick fox"),
        "ORDER BY the projected score did not rank: {bodies:?}",
    );
}

/// SurrealDB 4.0.0 refuses a KNN statement whose WHERE holds two or more
/// conditions through a record link grouped in parentheses beside
/// another condition, with "Internal database error: IR verification
/// failed: Dominance". Handed whole to `where_str`, the file half of the
/// retrieval rule renders exactly that group, so the semantic leg states
/// it conjunct by conjunct (`text::FILE_DISCLOSABLE_CLAUSES`), the form
/// pinned here as answering. No permissions or record-access session
/// are involved: this is a root session on schemaless tables. If the
/// refusal stops, the engine is fixed and the semantic leg can take the
/// rule whole again.
#[tokio::test]
async fn a_knn_beside_a_grouped_link_conjunction_is_refused_on_4_0_0() {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace("knn_group")
        .database("knn_group")
        .build()
        .unwrap();
    let client = DatabaseClient::new(cfg).unwrap();
    client.connect().await.unwrap();
    for statement in [
        "DEFINE TABLE doc SCHEMALESS;",
        "DEFINE TABLE chunk SCHEMALESS;",
        "DEFINE INDEX idx_vec ON chunk FIELDS embedding HNSW DIMENSION 3 DIST COSINE;",
        "CREATE doc:open SET state = 'ready';",
        "CREATE doc:gone SET state = 'deleted';",
        "CREATE chunk:1 SET t = 'acme', file = doc:open, embedding = [1.0, 0.0, 0.01];",
        "CREATE chunk:2 SET t = 'acme', file = doc:gone, embedding = [0.9, 0.1, 0.01];",
    ] {
        client.inner().query(statement).await.expect(statement);
    }

    let knn = "embedding <|2,64|> [1.0, 0.0, 0.01]";
    let grouped = format!(
        "SELECT meta::id(id) AS id FROM chunk WHERE {knn} AND (t = 'acme') \
         AND (file.state != 'quarantined' AND file.state != 'deleted');"
    );
    let refused = client
        .query(&grouped)
        .await
        .expect_err("SurrealDB 4.0.0 refuses the grouped form");
    assert!(
        refused
            .to_string()
            .contains("IR verification failed: Dominance"),
        "{refused}"
    );

    let split = format!(
        "SELECT meta::id(id) AS id FROM chunk WHERE {knn} AND (t = 'acme') \
         AND (file.state != 'quarantined') AND (file.state != 'deleted');"
    );
    let rows = client.query(&split).await.expect("the split form answers");
    assert_eq!(rows[0], serde_json::json!([{ "id": 1 }]), "{rows}");
}

/// Facet counts group the linked file ids and measure the distinct
/// set, because `count()` counts ROWS and a document matching in
/// several passages would otherwise be counted several times.
///
/// `count::distinct` does not exist here, and `SELECT DISTINCT a, b`
/// is a parse error, so `array::len(array::distinct(array::group(x)))`
/// is the form that survives `GROUP BY`. If this test fails, the
/// engine grew a real distinct count and `facet_counts` can say so
/// more plainly.
#[tokio::test]
async fn distinct_files_per_group_need_array_group() {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace("facets")
        .database("facets")
        .build()
        .unwrap();
    let client = DatabaseClient::new(cfg).unwrap();
    client.connect().await.unwrap();
    for statement in [
        "DEFINE ANALYZER probe_text TOKENIZERS class FILTERS lowercase,snowball(english);",
        "DEFINE TABLE file SCHEMALESS;",
        "DEFINE TABLE chunk SCHEMALESS;",
        "DEFINE INDEX idx_chunk_body ON TABLE chunk COLUMNS body FULLTEXT ANALYZER probe_text BM25;",
    ] {
        client.inner().query(statement).await.expect(statement);
    }
    for (id, kind) in [("a", "application/pdf"), ("b", "text/plain")] {
        client
            .inner()
            .query(format!("CREATE file:{id} SET content_type = '{kind}';"))
            .await
            .unwrap();
    }
    // File a matches TWICE, file b once. Counting rows says 2 and 1;
    // counting documents says 1 and 1.
    for (file, body) in [
        ("a", "the vessel inspection went well"),
        ("a", "a second inspection of the vessel"),
        ("b", "routine inspection of the hull"),
    ] {
        client
            .inner()
            .query(format!(
                "CREATE chunk SET file = file:{file}, body = '{body}';"
            ))
            .await
            .unwrap();
    }

    let rows: Vec<serde_json::Value> = query_records_raw(
        &client,
        "SELECT file.content_type AS value, count() AS n FROM chunk          WHERE body @1@ 'inspecting' GROUP BY value;",
    )
    .await;
    let counted: Vec<i64> = rows.iter().filter_map(|r| r["n"].as_i64()).collect();
    assert!(
        counted.contains(&2),
        "count() counts passages, which is why facets cannot use it: {rows:?}",
    );

    let rows: Vec<serde_json::Value> = query_records_raw(
        &client,
        "SELECT file.content_type AS value,          array::len(array::distinct(array::group(file))) AS n FROM chunk          WHERE body @1@ 'inspecting' GROUP BY value;",
    )
    .await;
    let mut counted: Vec<(String, i64)> = rows
        .iter()
        .map(|r| {
            (
                r["value"].as_str().unwrap().to_owned(),
                r["n"].as_i64().unwrap(),
            )
        })
        .collect();
    counted.sort();
    assert_eq!(
        counted,
        vec![
            ("application/pdf".to_owned(), 1),
            ("text/plain".to_owned(), 1),
        ],
        "one document each, however many passages matched",
    );
}

/// Why `file_version.markers` and `file_text.withheld` are `TYPE any`
/// rather than `array`: through SurrealDB 3.2 a schemafull `array`
/// field refused object items outright ("no such field exists" for
/// the item's keys), the `FLEXIBLE` escape attaches only to
/// object-bearing types the schema builder cannot spell, and `any`
/// carries the array of objects verbatim, absence included.
///
/// SurrealDB 3.3 changed the first half: a schemafull `option<array>`
/// now takes object items and keeps their keys, which this pins. The
/// columns stay `any` for now because moving them to an array type is
/// a schema change with its own migration, and `any` still carries
/// everything they hold.
#[tokio::test]
async fn any_typed_fields_carry_object_arrays_and_so_do_plain_arrays_since_3_3() {
    let client = seeded_client().await;
    for statement in [
        "DEFINE TABLE spans_probe SCHEMAFULL;",
        "DEFINE FIELD strict ON TABLE spans_probe TYPE option<array>;",
        "DEFINE FIELD loose ON TABLE spans_probe TYPE any;",
    ] {
        client.inner().query(statement).await.expect(statement);
    }

    // The strict column takes the very shape markers need, keys and all.
    let rows: Vec<serde_json::Value> = query_records_raw(
        &client,
        "CREATE spans_probe:strict CONTENT \
         { strict: [{ start: 1, end: 5, access: 'grant' }] } RETURN AFTER;",
    )
    .await;
    assert_eq!(rows[0]["strict"][0]["start"], 1, "{rows:?}");
    assert_eq!(rows[0]["strict"][0]["access"], "grant", "{rows:?}");

    // The any column carries it whole, and admits absence.
    let rows: Vec<serde_json::Value> = query_records_raw(
        &client,
        "CREATE spans_probe:loose CONTENT \
         { loose: [{ start: 1, end: 5, access: 'grant' }] } RETURN AFTER;",
    )
    .await;
    assert_eq!(rows[0]["loose"][0]["start"], 1, "{rows:?}");
    assert_eq!(rows[0]["loose"][0]["access"], "grant", "{rows:?}");
    let rows: Vec<serde_json::Value> = query_records_raw(
        &client,
        "CREATE spans_probe:absent CONTENT {} RETURN AFTER;",
    )
    .await;
    assert!(rows[0].get("loose").is_none(), "{rows:?}");
}

async fn query_records_raw(client: &DatabaseClient, sql: &str) -> Vec<serde_json::Value> {
    let mut response = client.inner().query(sql).await.expect(sql);
    response.take(0).expect(sql)
}
