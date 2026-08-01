//! Lexical search ranks by relevance, not by whichever passage was
//! written first.
//!
//! The engine returns full-text matches in insertion order (pinned in
//! `engine_assumptions.rs`), so these seed the SAME two passages in
//! BOTH orders. A search that trusted the scan would answer differently
//! each time; a ranked one answers the same.

use copal_core::{FileSpec, TenantId};
use copal_store::repo::{file, text};
use copal_store::{Store, StoreConfig};

const DENSE: &str = "pressure vessel inspection: the pressure vessel procedure";
const DILUTED: &str = "a long note about catering arrangements, parking, the weather, and \
                       various other administrative matters, which mentions a pressure \
                       vessel once in passing and then returns to catering and parking \
                       and other unrelated topics at considerable length";

fn tenant() -> TenantId {
    TenantId::parse("acme").unwrap()
}

fn spec(path: &str) -> FileSpec {
    FileSpec {
        path: path.to_owned(),
        content_type: "text/plain".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: serde_json::json!({}),
        idempotency_key: None,
    }
}

/// Seed two files whose passages are written in the given order, then
/// search. Returns the paths in ranked order.
async fn ranked_paths(first: &str, second: &str) -> Vec<String> {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let acme = tenant();

    let mut paths = Vec::new();
    for (label, body) in [("first", first), ("second", second)] {
        let created = file::create_file(&store, &acme, &spec(label), "tester")
            .await
            .unwrap();
        // Passages are written here, in this loop's order, so the
        // chunk ids ascend in exactly the order under test.
        text::put_chunks(
            &store,
            &acme,
            &created.record.id,
            "digest",
            &[body.to_owned()],
        )
        .await
        .unwrap();
        paths.push((label.to_owned(), body.to_owned()));
    }

    let hits = text::search(&store, &acme, "pressure vessel", 10)
        .await
        .unwrap();
    hits.iter()
        .map(|hit| {
            let body = &hit.body;
            if body == DENSE { "DENSE" } else { "DILUTED" }.to_owned()
        })
        .collect()
}

#[tokio::test]
async fn relevance_beats_insertion_order_in_both_directions() {
    let diluted_first = ranked_paths(DILUTED, DENSE).await;
    let dense_first = ranked_paths(DENSE, DILUTED).await;

    // The same answer regardless of write order is what proves the
    // ranking is real: the scan alone would mirror the input.
    assert_eq!(
        diluted_first, dense_first,
        "write order changed the ranking, so nothing is ranking",
    );
    assert_eq!(
        diluted_first,
        vec!["DENSE", "DILUTED"],
        "the short, term-dense passage ranks first",
    );
}

#[tokio::test]
async fn a_bounded_search_keeps_the_best_match() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let acme = tenant();

    // The weak match is written FIRST, so the scan would offer it
    // first and a limit of one would keep the wrong passage.
    for (label, body) in [("diluted", DILUTED), ("dense", DENSE)] {
        let created = file::create_file(&store, &acme, &spec(label), "tester")
            .await
            .unwrap();
        text::put_chunks(
            &store,
            &acme,
            &created.record.id,
            "digest",
            &[body.to_owned()],
        )
        .await
        .unwrap();
    }

    let hits = text::search(&store, &acme, "pressure vessel", 1)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].body, DENSE,
        "the limit kept the oldest, not the best"
    );
}

#[tokio::test]
async fn stemmed_matches_are_scored_the_way_the_index_matched_them() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let acme = tenant();

    // Neither passage contains the literal query word `inspecting`.
    // Both match through the analyzer's English stems. A scorer that
    // skipped stemming would score both zero, fall back to write
    // order, and answer with the diluted passage written first.
    for (label, body) in [
        (
            "diluted",
            "the vessel had an inspection scheduled during a long list of other              administrative activities including catering, parking, and various              further matters of no particular relevance to anything",
        ),
        ("dense", "inspection, inspection, and further inspection"),
    ] {
        let created = file::create_file(&store, &acme, &spec(label), "tester")
            .await
            .unwrap();
        text::put_chunks(
            &store,
            &acme,
            &created.record.id,
            "digest",
            &[body.to_owned()],
        )
        .await
        .unwrap();
    }

    let hits = text::search(&store, &acme, "inspecting", 10).await.unwrap();
    assert_eq!(hits.len(), 2, "both match through stemming: {hits:?}");
    assert!(
        hits[0].body.starts_with("inspection, inspection"),
        "the stemmed terms have to be scored, not just matched: {hits:?}",
    );
}
