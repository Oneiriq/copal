//! Text extraction and search end to end: upload a document, let the
//! pipeline extract it, find it by its words, and stop finding it
//! when it goes away.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::put;
use axum::Router;
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::ExtensionPolicy;
use copal_flow::FlowEngine;
use copal_server::app::Residencies;
use copal_server::pipeline::standard_registry;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

/// An extractor that returns fixed text for whatever it is sent,
/// speaking the same shape Tika does.
async fn fake_extractor(text: &'static str) -> String {
    let app = Router::new().route("/tika", put(move || async move { text }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn stack(extractor: Option<String>) -> (axum::Router, FlowEngine, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let registry = standard_registry(
        store.clone(),
        Residencies::local_only(blobs.clone()),
        ExtensionPolicy::standard(),
        false,
        None,
        extractor,
        None,
    );
    let state = AppState::new(store, blobs).with_flow(registry);
    let engine = state.flow.clone();
    (build_router(state), engine, dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn req(method: &str, uri: &str, body: Body) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-tenant", "acme");
    if method == "POST" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

async fn upload(router: &axum::Router, path: &str, content_type: &str, body: &[u8]) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": content_type }).to_string()),
    );
    let id = json_body(router.clone().oneshot(create).await.unwrap()).await["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header("content-length", body.len().to_string())
        .body(Body::from(body.to_vec()))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(put).await.unwrap().status(),
        StatusCode::OK,
    );
    id
}

async fn search(router: &axum::Router, terms: &str) -> Vec<Value> {
    let get = req("GET", &format!("/v1/search?q={terms}"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    body["items"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn uploaded_text_becomes_searchable() {
    let (router, engine, _dir) = stack(None).await;
    let id = upload(
        &router,
        "docs/handbook.txt",
        "text/plain",
        b"the escalation procedure for pressure vessel inspection",
    )
    .await;

    // Nothing is searchable until the pipeline extracts it.
    assert!(search(&router, "escalation").await.is_empty());
    assert!(engine.tick("w").await.unwrap());

    let hits = search(&router, "escalation").await;
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0]["file"], id.as_str());
    assert!(hits[0]["excerpt"]
        .as_str()
        .unwrap()
        .contains("pressure vessel"));

    // The stemmer means a searcher does not have to guess the form.
    assert_eq!(search(&router, "inspect").await.len(), 1);
    // A word nobody wrote finds nothing.
    assert!(search(&router, "helicopter").await.is_empty());

    // The whole text is retrievable for the file that owns it.
    let get = req("GET", &format!("/v1/files/{id}/text"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["extractor"], "native");
    assert!(body["text"].as_str().unwrap().contains("escalation"));
    assert_eq!(body["chars"], 55);

    // A record with no extraction says so rather than 200-ing empty.
    let other = upload(
        &router,
        "bin/blob.bin",
        "application/octet-stream",
        &[0u8, 159, 2],
    )
    .await;
    assert!(engine.tick("w").await.unwrap());
    let get = req("GET", &format!("/v1/files/{other}/text"), Body::empty());
    assert_eq!(
        router.clone().oneshot(get).await.unwrap().status(),
        StatusCode::NOT_FOUND,
    );

    // Deleting the file removes it from the index: search must not
    // answer with content nobody can fetch.
    let remove = req("DELETE", &format!("/v1/files/{id}"), Body::empty());
    assert_eq!(
        router.clone().oneshot(remove).await.unwrap().status(),
        StatusCode::NO_CONTENT,
    );
    assert!(search(&router, "escalation").await.is_empty());
}

#[tokio::test]
async fn an_extractor_handles_what_copal_declines_to_parse() {
    let addr = fake_extractor("quarterly revenue grew across every region").await;
    let (router, engine, _dir) = stack(Some(addr)).await;

    // A PDF: Copal carries no parser, so the extractor supplies text.
    let id = upload(
        &router,
        "reports/q3.pdf",
        "application/pdf",
        b"%PDF-1.7 binary body that copal will not parse itself",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());

    let hits = search(&router, "revenue").await;
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0]["file"], id.as_str());

    let get = req("GET", &format!("/v1/files/{id}/text"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["extractor"], "external");
    assert!(body["text"].as_str().unwrap().contains("quarterly"));
}

#[tokio::test]
async fn a_re_upload_replaces_what_search_finds() {
    let (router, engine, _dir) = stack(None).await;
    let id = upload(
        &router,
        "docs/notice.txt",
        "text/plain",
        b"scheduled maintenance window",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());
    assert_eq!(search(&router, "maintenance").await.len(), 1);

    // Replace the content; the old text must stop matching.
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header("content-length", "23")
        .body(Body::from("cancelled until further"))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(put).await.unwrap().status(),
        StatusCode::OK,
    );
    assert!(engine.tick("w").await.unwrap());

    assert!(
        search(&router, "maintenance").await.is_empty(),
        "superseded text stops matching",
    );
    assert_eq!(search(&router, "cancelled").await.len(), 1);
    let get = req("GET", &format!("/v1/files/{id}/text"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert!(body["text"].as_str().unwrap().contains("cancelled"));
}

#[tokio::test]
async fn search_is_tenant_scoped_and_bounded() {
    let (router, engine, _dir) = stack(None).await;
    upload(
        &router,
        "docs/ours.txt",
        "text/plain",
        b"confidential merger terms",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());

    // Another tenant asking the same question sees nothing.
    let get = Request::builder()
        .method("GET")
        .uri("/v1/search?q=merger")
        .header("x-copal-tenant", "rival")
        .body(Body::empty())
        .unwrap();
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert!(body["items"].as_array().unwrap().is_empty(), "{body}");

    // Empty terms refuse rather than returning the corpus.
    let get = req("GET", "/v1/search?q=", Body::empty());
    assert_eq!(
        router.clone().oneshot(get).await.unwrap().status(),
        StatusCode::BAD_REQUEST,
    );
}

/// An embedding service in the OpenAI shape whose vectors encode a
/// crude "topic": the returned vector leans toward whichever keyword
/// the text contains, so semantic neighbours are predictable.
async fn fake_embedder() -> String {
    use axum::routing::post;
    let app = Router::new().route(
        "/v1/embeddings",
        post(|body: axum::Json<Value>| async move {
            let input = body.0["input"].as_str().unwrap_or_default().to_lowercase();
            // Three axes: finance, machinery, and a constant so no
            // vector is all zeros (cosine is undefined at the origin).
            let finance = f64::from(
                input.contains("revenue") || input.contains("earnings") || input.contains("profit"),
            );
            let machinery = f64::from(
                input.contains("turbine") || input.contains("engine") || input.contains("motor"),
            );
            axum::Json(json!({
                "data": [ { "embedding": [finance, machinery, 0.01] } ]
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn semantic_stack() -> (axum::Router, FlowEngine, tempfile::TempDir) {
    let addr = fake_embedder().await;
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    store.ensure_vector_index(3).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let embedding = Some((addr, "test-model".to_owned()));
    let registry = standard_registry(
        store.clone(),
        Residencies::local_only(blobs.clone()),
        ExtensionPolicy::standard(),
        false,
        None,
        None,
        embedding.clone(),
    );
    let state = AppState::new(store, blobs)
        .with_flow(registry)
        .with_embedding(embedding);
    let engine = state.flow.clone();
    (build_router(state), engine, dir)
}

async fn search_mode(router: &axum::Router, terms: &str, mode: &str) -> Value {
    let get = req(
        "GET",
        &format!("/v1/search?q={terms}&mode={mode}"),
        Body::empty(),
    );
    json_body(router.clone().oneshot(get).await.unwrap()).await
}

#[tokio::test]
async fn semantic_search_finds_documents_that_share_no_words() {
    let (router, engine, _dir) = semantic_stack().await;
    let earnings = upload(
        &router,
        "docs/earnings.txt",
        "text/plain",
        b"quarterly revenue and profit summary",
    )
    .await;
    upload(
        &router,
        "docs/maintenance.txt",
        "text/plain",
        b"turbine and motor service schedule",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());
    assert!(engine.tick("w").await.unwrap());

    // "earnings" appears in no document, so lexical search finds
    // nothing; the embedding places it beside the finance document.
    let lexical = search_mode(&router, "earnings", "lexical").await;
    assert!(
        lexical["items"].as_array().unwrap().is_empty(),
        "no document contains the word: {lexical}",
    );

    let semantic = search_mode(&router, "earnings", "semantic").await;
    assert_eq!(semantic["mode"], "semantic");
    let items = semantic["items"].as_array().unwrap();
    assert!(
        !items.is_empty(),
        "meaning found what words could not: {semantic}"
    );
    assert_eq!(items[0]["file"], earnings.as_str());
}

#[tokio::test]
async fn hybrid_search_returns_what_either_retrieval_found() {
    let (router, engine, _dir) = semantic_stack().await;
    let revenue = upload(
        &router,
        "docs/revenue.txt",
        "text/plain",
        b"revenue grew in every region",
    )
    .await;
    let turbine = upload(
        &router,
        "docs/turbine.txt",
        "text/plain",
        b"turbine blade replacement",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());
    assert!(engine.tick("w").await.unwrap());

    // A word one document uses literally, in a topic the other shares.
    let hybrid = search_mode(&router, "revenue", "hybrid").await;
    assert_eq!(hybrid["mode"], "hybrid");
    let ids: Vec<&str> = hybrid["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["file"].as_str().unwrap())
        .collect();
    assert_eq!(ids.first(), Some(&revenue.as_str()), "{hybrid}");
    assert!(!ids.contains(&turbine.as_str()) || ids.len() > 1);
}

#[tokio::test]
async fn semantic_modes_degrade_to_lexical_without_a_service() {
    // No embedding service configured: asking for meaning gets words
    // and the answer says so, rather than erroring or pretending.
    let (router, engine, _dir) = stack(None).await;
    upload(
        &router,
        "docs/plain.txt",
        "text/plain",
        b"turbine inspection",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());

    let body = search_mode(&router, "turbine", "hybrid").await;
    assert_eq!(body["mode"], "lexical", "{body}");
    assert_eq!(body["items"].as_array().unwrap().len(), 1);

    let body = search_mode(&router, "turbine", "semantic").await;
    assert_eq!(body["mode"], "lexical", "{body}");

    // An unknown mode is a request error, not a silent default.
    let get = req("GET", "/v1/search?q=turbine&mode=telepathic", Body::empty());
    assert_eq!(
        router.clone().oneshot(get).await.unwrap().status(),
        StatusCode::BAD_REQUEST,
    );
}

#[tokio::test]
async fn a_long_document_matches_at_the_passage_that_says_it() {
    // The reason chunking exists: one vector for a long document
    // points at its average meaning, and one BM25 row buries a single
    // relevant sentence among thousands of irrelevant words. Passages
    // let retrieval name the part that answers the question.
    let (router, engine, _dir) = stack(None).await;

    let filler = "Routine safety notices are reviewed each quarter. ".repeat(60);
    let buried = "The evacuation muster point is the north car park. ";
    let more_filler = "Attendance records are retained for seven years. ".repeat(60);
    let document = format!("{filler}{buried}{more_filler}");
    let id = upload(
        &router,
        "docs/handbook-long.txt",
        "text/plain",
        document.as_bytes(),
    )
    .await;
    assert!(engine.tick("w").await.unwrap());

    let hits = search(&router, "evacuation").await;
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0]["file"], id.as_str());

    // The excerpt is the passage that contains the sentence, not the
    // opening of a long document that happens to mention it later.
    let excerpt = hits[0]["excerpt"].as_str().unwrap();
    assert!(
        excerpt.contains("muster point"),
        "the matching passage is returned, not the document head: {excerpt}",
    );
    // And the hit names which passage it was.
    assert!(
        hits[0]["passage"].as_i64().unwrap() > 0,
        "the match is not the first passage: {hits:?}",
    );

    // The whole document is still readable in one piece.
    let get = req("GET", &format!("/v1/files/{id}/text"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert!(body["text"].as_str().unwrap().contains("muster point"));
    assert_eq!(body["chars"], document.trim().chars().count());
}

#[tokio::test]
async fn passages_are_embedded_individually_and_replaced_together() {
    let (router, engine, _dir) = semantic_stack().await;
    let long = format!(
        "{}{}",
        "Quarterly revenue and profit commentary. ".repeat(30),
        "Turbine and motor overhaul scheduling. ".repeat(30),
    );
    let id = upload(&router, "docs/mixed.txt", "text/plain", long.as_bytes()).await;
    // One upload is one run; the run embeds every passage in it.
    assert!(engine.tick("w").await.unwrap());

    // One document, two topics: each passage embeds on its own, so a
    // query about either finds this file.
    for term in ["earnings", "engine"] {
        let body = search_mode(&router, term, "semantic").await;
        let items = body["items"].as_array().unwrap();
        assert!(!items.is_empty(), "{term} finds the document: {body}");
        assert_eq!(items[0]["file"], id.as_str());
    }

    // Replacing the content replaces every passage: the old topics
    // must stop matching, not linger as orphaned vectors.
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header("content-length", "31")
        .body(Body::from("nothing about money or machines"))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(put).await.unwrap().status(),
        StatusCode::OK,
    );
    assert!(engine.tick("w").await.unwrap());

    // Asked lexically, because nearest-neighbour search always
    // returns its k nearest however far away they are: in a corpus
    // this small every passage is somebody's neighbour, so a semantic
    // query cannot express "no longer present".
    let gone = search_mode(&router, "revenue", "lexical").await;
    assert!(
        gone["items"].as_array().unwrap().is_empty(),
        "superseded passages stop matching: {gone}",
    );
    let now = search_mode(&router, "machines", "lexical").await;
    assert_eq!(now["items"].as_array().unwrap().len(), 1, "{now}");
}

#[tokio::test]
async fn a_semantic_query_about_nothing_stored_returns_nothing() {
    // Without a relevance floor, nearest-neighbour search answers
    // every query with its nearest results however far away they
    // are, so no semantic search ever misses and "no matches" cannot
    // be expressed. The floor is what makes absence reportable.
    let (router, engine, _dir) = semantic_stack().await;
    upload(
        &router,
        "docs/finance.txt",
        "text/plain",
        b"quarterly revenue and profit",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());

    // The fake embedder puts finance on one axis and machinery on
    // another; a machinery query is orthogonal to the only document.
    let body = search_mode(&router, "turbine", "semantic").await;
    assert!(
        body["items"].as_array().unwrap().is_empty(),
        "an unrelated query matches nothing: {body}",
    );

    // A related query still finds it, so the floor did not simply
    // switch retrieval off.
    let body = search_mode(&router, "earnings", "semantic").await;
    assert_eq!(body["items"].as_array().unwrap().len(), 1, "{body}");
}

/// Stemming has to agree with the index's analyzer. The engine matches
/// `inspecting` against a document containing `inspection`; a scorer
/// that skipped stemming would score that document zero and bury a
/// real hit.
#[tokio::test]
async fn a_stemmed_match_still_scores() {
    let (router, engine, _dir) = stack(None).await;
    upload(
        &router,
        "docs/stemmed.txt",
        "text/plain",
        b"routine inspection of the vessel",
    )
    .await;
    while engine.tick("w").await.unwrap() {}

    let hits = search(&router, "inspecting").await;
    assert_eq!(hits.len(), 1, "the stemmed term matches: {hits:#?}");
    assert!(hits[0]["excerpt"].as_str().unwrap().contains("inspection"));
}

/// Filters narrow retrieval at the engine: a prefix keeps one
/// directory's documents, a content type keeps one format, and the
/// cursor walks a ranking page by page without repeating.
#[tokio::test]
async fn filters_and_cursor_narrow_retrieval() {
    let (router, engine, _dir) = stack(None).await;
    for (path, body) in [
        ("contracts/alpha.txt", "the quarterly settlement terms"),
        ("contracts/beta.txt", "the quarterly renewal terms"),
        ("reports/gamma.txt", "the quarterly revenue narrative"),
    ] {
        let id = upload(&router, path, "text/plain", body.as_bytes()).await;
        assert!(engine.tick("w").await.unwrap());
        let _ = id;
    }

    // Unfiltered: every quarterly document ranks.
    let all = search(&router, "quarterly").await;
    assert_eq!(all.len(), 3, "{all:#?}");

    // The prefix keeps the contracts directory.
    let get = req(
        "GET",
        "/v1/search?q=quarterly&prefix=contracts/",
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["items"].as_array().unwrap().len(), 2, "{body:#?}");

    // A content type nothing carries keeps nothing.
    let get = req(
        "GET",
        "/v1/search?q=quarterly&content_type=application/pdf",
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["items"].as_array().unwrap().len(), 0, "{body:#?}");

    // The cursor pages the ranking without repeating a document.
    let get = req("GET", "/v1/search?q=quarterly&limit=2", Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    let first: Vec<String> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["file"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(first.len(), 2);
    let cursor = body["next_cursor"]
        .as_str()
        .expect("more remains")
        .to_owned();
    let get = req(
        "GET",
        &format!("/v1/search?q=quarterly&limit=2&cursor={cursor}"),
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    let second: Vec<String> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["file"].as_str().unwrap().to_owned())
        .collect();
    assert!(!second.is_empty());
    assert!(
        second.iter().all(|id| !first.contains(id)),
        "pages never repeat: {second:?}",
    );
}

/// The backfill drains stale geometry: chunks embedded under an old
/// model re-embed under the current one, and a second pass finds
/// nothing left to do.
#[tokio::test]
async fn the_backfill_drains_stale_embeddings() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let registry = standard_registry(
        store.clone(),
        Residencies::local_only(blobs.clone()),
        ExtensionPolicy::standard(),
        false,
        None,
        None,
        None,
    );
    let state = AppState::new(store.clone(), blobs).with_flow(registry);
    let engine = state.flow.clone();
    let router = build_router(state);

    let (addr, served) = stub_embedder().await;
    let _id = upload(
        &router,
        "embedded.txt",
        "text/plain",
        b"a passage worth embedding",
    )
    .await;
    assert!(engine.tick("w").await.unwrap());

    // First pass embeds the vectorless chunks under the new model.
    let refreshed = copal_server::embed::backfill_pass(&store, &addr, "model-b", 16)
        .await
        .unwrap();
    assert!(refreshed >= 1, "the vectorless chunk embeds");
    assert!(served.load(std::sync::atomic::Ordering::SeqCst) >= 1);

    // A second pass finds nothing stale.
    let refreshed = copal_server::embed::backfill_pass(&store, &addr, "model-b", 16)
        .await
        .unwrap();
    assert_eq!(refreshed, 0, "nothing stale remains");
}

/// A one-route embedding stub speaking the OpenAI-compatible shape.
async fn stub_embedder() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use axum::routing::post;
    let served = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = served.clone();
    let app = axum::Router::new().route(
        "/v1/embeddings",
        post(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                axum::Json(json!({
                    "data": [ { "embedding": [0.1, 0.2, 0.3] } ]
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, served)
}
