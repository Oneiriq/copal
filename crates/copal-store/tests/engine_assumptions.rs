//! Engine behaviour Copal's retrieval design depends on.
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

/// `search::score` returns 0 for every row, which is why `SearchHit`
/// carries no score field and why hybrid retrieval fuses RANKS rather
/// than values. If this test starts failing, the engine grew real
/// per-row scoring and the fusion can be revisited.
#[tokio::test]
async fn the_engine_reports_no_per_row_bm25_score() {
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

    assert!(
        hits.len() >= 2,
        "both matching documents come back: {hits:?}"
    );
    let scores: Vec<f64> = hits
        .iter()
        .map(|h| h["score"].as_f64().expect("score projects as a number"))
        .collect();
    assert!(
        scores.iter().all(|s| *s == 0.0),
        "search::score began reporting values ({scores:?}); hybrid retrieval \
         can stop fusing ranks and use them",
    );
}

/// The full-text scan does NOT order by relevance. It yields matching
/// rows in insertion order, which is why lexical ranking has to be
/// computed outside the engine.
///
/// Measured across every form the engine accepts: a bare scan, the
/// score projected, ORDER BY the projected alias, the `@@` operator
/// without a reference, and an index defined with HIGHLIGHTS. None
/// ranks. `ORDER BY search::score(1)` is a parse error.
#[tokio::test]
async fn the_full_text_scan_does_not_rank() {
    let client = seeded_client().await;
    let query = surql::query::Query::new()
        .select(Some(vec!["body".to_owned()]))
        .from_table("doc")
        .unwrap()
        .where_str("body @1@ 'quick fox'");
    let hits: Vec<serde_json::Value> = query_records(&client, &query).await.expect("rows");

    let bodies: Vec<&str> = hits.iter().filter_map(|h| h["body"].as_str()).collect();
    assert_eq!(bodies.len(), 2, "{bodies:?}");
    // The long rambling document was inserted first and comes back
    // first, even though the short term-dense one is the better BM25
    // match by both term frequency and length normalisation. If this
    // ever fails, the engine learned to rank and the local scorer can
    // be reconsidered.
    assert!(
        bodies[0].starts_with("the quick brown fox"),
        "the scan began ranking rather than returning insertion order: {bodies:?}",
    );
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

async fn query_records_raw(client: &DatabaseClient, sql: &str) -> Vec<serde_json::Value> {
    let mut response = client.inner().query(sql).await.expect(sql);
    response.take(0).expect(sql)
}
