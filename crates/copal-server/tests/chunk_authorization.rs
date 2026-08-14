//! Per-chunk authorization, adversarial by construction.
//!
//! Every test here follows the non-vacuity discipline: plant a marked
//! span, prove the leak WOULD happen without the filter (through a
//! test oracle that drops the chunk conjunct, or by reading the raw
//! row the enforcement point serves from), and prove it does not
//! happen with it. A green test whose leak could never have occurred
//! proves only that the corpus lacked a hit.
//!
//! The surfaces, in the order the design names them: ingestion (the
//! marker rides each content-bearing face to chunk levels), search
//! results, the rerank window, facet counts, the full-text read, and
//! both retrieval legs.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::ExtensionPolicy;
use copal_flow::FlowEngine;
use copal_server::app::Residencies;
use copal_server::pipeline::{standard_registry, FetchPolicy};
use copal_server::{build_router, AppState};
use copal_store::repo::text as text_repo;
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, FlowEngine, Store, tempfile::TempDir) {
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
        std::collections::HashMap::new(),
        FetchPolicy {
            allow_private_targets: true,
            max_bytes: 1 << 20,
        },
        copal_server::tiering::Topology::default(),
        Default::default(),
    );
    let mut state = AppState::new(store.clone(), blobs).with_flow(registry);
    state.limits.allow_private_fetch_targets = true;
    let engine = state.flow.clone();
    (build_router(state), engine, store, dir)
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

async fn drain(engine: &FlowEngine) {
    while engine.tick("w").await.unwrap() {}
}

/// A document long enough to split, with a marked secret in its first
/// half and open text everywhere else. The secret's terms appear
/// nowhere else, so any retrieval surface answering them is a leak.
fn marked_document() -> String {
    let open_head =
        "alpha section discusses harmless logistics and routine scheduling topics. ".repeat(9);
    let secret = "BEGIN CONFIDENTIAL the zanzibar acquisition price is nine million END. ";
    let open_tail = "omega section closes with unremarkable housekeeping notes. ".repeat(9);
    format!("{open_head}{secret}{open_tail}")
}

/// The marker every ingestion face declares over [`marked_document`].
fn marker_json() -> Value {
    json!([{ "access": "grant", "from": "BEGIN CONFIDENTIAL", "until": "END." }])
}

/// Create a file and PUT native text with a markers header.
async fn upload_marked(
    router: &axum::Router,
    engine: &FlowEngine,
    path: &str,
    body: &str,
    markers: Option<&Value>,
) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/plain" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let mut put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme");
    if let Some(markers) = markers {
        put = put.header("x-copal-markers", markers.to_string());
    }
    let put = put.body(Body::from(body.as_bytes().to_vec())).unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drain(engine).await;
    id
}

async fn chunk_rows(store: &Store, id: &str) -> Vec<text_repo::ChunkRow> {
    let file = copal_core::FileId::parse(id).unwrap();
    text_repo::chunks_without_embedding(store, &file)
        .await
        .unwrap()
}

/// The PUT header face: the declaration lands on the version row,
/// resolves at extraction, and exactly the passages the span touches
/// carry its level. This is decision one and two end to end - and the
/// baseline every leak-surface test below builds on.
#[tokio::test]
async fn a_put_header_marker_becomes_chunk_levels_and_spans() {
    let (router, engine, store, _dir) = stack().await;
    let document = marked_document();
    let id = upload_marked(
        &router,
        &engine,
        "docs/deal.txt",
        &document,
        Some(&marker_json()),
    )
    .await;

    // The declaration persisted on the version row, canonical form.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let file = copal_core::FileId::parse(&id).unwrap();
    let record = copal_store::repo::file::get_file(&store, &tenant, &file)
        .await
        .unwrap()
        .unwrap();
    let digest = record.digest.unwrap();
    let persisted =
        copal_store::repo::version::markers_for(&store, &tenant, &file, digest.as_str())
            .await
            .unwrap();
    assert_eq!(persisted, Some(marker_json()));

    // Chunk levels: every passage holding secret text inherited the
    // marker's level; at least one passage did not inherit, so the
    // file is partially available rather than all-or-nothing.
    let rows = chunk_rows(&store, &id).await;
    assert!(rows.len() > 1, "the document must split to test overlap");
    let mut restricted = 0;
    let mut open = 0;
    for row in &rows {
        if row.body.contains("zanzibar") {
            assert_eq!(
                row.access.as_deref(),
                Some("grant"),
                "a passage holding the secret must carry the marker's level",
            );
        }
        match row.access.as_deref() {
            Some(_) => restricted += 1,
            None => open += 1,
        }
    }
    assert!(restricted >= 1, "{rows:?}");
    assert!(open >= 1, "over-withholding everything is not the design");

    // The document row carries the resolved span beside the body.
    let text = text_repo::get_text(&store, &tenant, &file)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(text.withheld.len(), 1, "{:?}", text.withheld);
    assert_eq!(text.withheld[0].access, "grant");

    // The verdict is visible to the uploader: spans resolved, nothing
    // unresolved.
    let get = req("GET", &format!("/v1/files/{id}"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    let processing = &body["metadata"]["processing"];
    assert_eq!(processing["marker_spans"], json!(1), "{processing}");
    assert_eq!(processing["markers_unresolved"], json!([]), "{processing}");
    assert!(processing["passages_restricted"].as_i64().unwrap() >= 1);
}

/// Decision three: a marker looser than the file refuses before any
/// byte moves, on the header face and the fetch face alike.
#[tokio::test]
async fn widening_markers_refuse_with_400_before_bytes_move() {
    let (router, _engine, _store, _dir) = stack().await;
    // The file defaults to private; a public marker widens.
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": "docs/wide.txt", "content_type": "text/plain" }).to_string()),
    );
    let id = json_body(router.clone().oneshot(create).await.unwrap()).await["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header(
            "x-copal-markers",
            json!([{ "access": "public", "from": "x" }]).to_string(),
        )
        .body(Body::from("body that must never land"))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    // The record never left draft: the refusal preceded the claim.
    let get = req("GET", &format!("/v1/files/{id}"), Body::empty());
    let record = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(record["state"], json!("draft"), "{record}");

    // The fetch face refuses the same declaration with the same
    // vocabulary, before any record exists.
    let fetch = req(
        "POST",
        "/v1/files/fetch",
        Body::from(
            json!({
                "url": "http://127.0.0.1:9/doc",
                "path": "pulled/wide.txt",
                "markers": [{ "access": "public", "from": "x" }],
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(fetch).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // An unknown level refuses too; the vocabulary is closed.
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header(
            "x-copal-markers",
            json!([{ "access": "secret", "from": "x" }]).to_string(),
        )
        .body(Body::from("body"))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// The fail direction made concrete: an anchor that never occurs
/// restricts EVERY passage of the file, and the verdict reaches
/// `metadata.processing` so the uploader can correct and re-upload.
#[tokio::test]
async fn an_unresolvable_anchor_restricts_the_whole_file() {
    let (router, engine, store, _dir) = stack().await;
    let document = marked_document();
    let markers = json!([{ "access": "grant", "from": "PHRASE NOBODY WROTE" }]);
    let id = upload_marked(&router, &engine, "docs/typo.txt", &document, Some(&markers)).await;

    let rows = chunk_rows(&store, &id).await;
    assert!(rows.len() > 1);
    for row in &rows {
        assert_eq!(
            row.access.as_deref(),
            Some("grant"),
            "an unlocatable declaration withholds everything, never nothing",
        );
    }
    let get = req("GET", &format!("/v1/files/{id}"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    let unresolved = body["metadata"]["processing"]["markers_unresolved"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(unresolved.len(), 1, "{body}");
    assert!(
        unresolved[0].as_str().unwrap().contains("never occurs"),
        "{unresolved:?}",
    );
}

/// Markers are per-version: a re-upload without markers is unmarked
/// content, and the old declaration dies with the text it described.
#[tokio::test]
async fn a_re_upload_without_markers_returns_to_file_level() {
    let (router, engine, store, _dir) = stack().await;
    let document = marked_document();
    let id = upload_marked(
        &router,
        &engine,
        "docs/reup.txt",
        &document,
        Some(&marker_json()),
    )
    .await;
    assert!(
        chunk_rows(&store, &id)
            .await
            .iter()
            .any(|row| row.access.is_some()),
        "the first version's chunks carry levels",
    );

    // Replace the content, declaring nothing.
    let replacement =
        "entirely fresh text with no confidential passages anywhere in it. ".repeat(20);
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .body(Body::from(replacement))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(put).await.unwrap().status(),
        StatusCode::OK,
    );
    drain(&engine).await;

    let rows = chunk_rows(&store, &id).await;
    assert!(rows.len() > 1);
    assert!(
        rows.iter().all(|row| row.access.is_none()),
        "unmarked content is file-level: today's behavior exactly",
    );
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let file = copal_core::FileId::parse(&id).unwrap();
    let text = text_repo::get_text(&store, &tenant, &file)
        .await
        .unwrap()
        .unwrap();
    assert!(text.withheld.is_empty(), "{:?}", text.withheld);
}

/// The fetch face: markers ride the body field, persist on the
/// version row, and reach chunk levels through the same pipeline.
#[tokio::test]
async fn fetch_markers_ride_the_body_to_chunk_levels() {
    let (router, engine, store, _dir) = stack().await;
    let document = marked_document();
    let served = document.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = axum::Router::new().route(
        "/doc",
        axum::routing::get(move || {
            let body = served.clone();
            async move { ([("content-type", "text/plain")], body) }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let request = req(
        "POST",
        "/v1/files/fetch",
        Body::from(
            json!({
                "url": format!("http://{addr}/doc"),
                "path": "pulled/deal.txt",
                "markers": marker_json(),
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    drain(&engine).await;

    let rows = chunk_rows(&store, &id).await;
    assert!(rows.len() > 1, "{rows:?}");
    for row in &rows {
        if row.body.contains("zanzibar") {
            assert_eq!(row.access.as_deref(), Some("grant"), "{row:?}");
        }
    }
    assert!(rows.iter().any(|row| row.access.is_none()));
}

/// The tus face: markers ride Upload-Metadata at creation, survive
/// the session, and land with completion. A widening declaration
/// refuses at creation, before the session exists.
#[tokio::test]
async fn tus_markers_ride_upload_metadata_to_chunk_levels() {
    let (router, engine, store, _dir) = stack().await;
    let document = marked_document();
    let payload = document.as_bytes().to_vec();
    let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
    let metadata = format!(
        "path {},content_type {},markers {}",
        b64("tus/deal.txt"),
        b64("text/plain"),
        b64(&marker_json().to_string()),
    );
    let create = Request::builder()
        .method("POST")
        .uri("/v1/tus")
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .header("upload-length", payload.len().to_string())
        .header("upload-metadata", metadata)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let location = response.headers()["location"].to_str().unwrap().to_owned();

    let patch = Request::builder()
        .method("PATCH")
        .uri(&location)
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .header("content-type", "application/offset+octet-stream")
        .header("upload-offset", "0")
        .body(Body::from(payload))
        .unwrap();
    let response = router.clone().oneshot(patch).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    drain(&engine).await;

    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let file = copal_store::repo::file::find_by_path(&store, &tenant, "tus/deal.txt")
        .await
        .unwrap()
        .expect("the tus upload created its file");
    let rows = chunk_rows(&store, file.id.as_str()).await;
    assert!(rows.len() > 1, "{rows:?}");
    for row in &rows {
        if row.body.contains("zanzibar") {
            assert_eq!(row.access.as_deref(), Some("grant"), "{row:?}");
        }
    }
    assert!(rows.iter().any(|row| row.access.is_none()));

    // Widening refuses at session creation: no session, no record.
    let metadata = format!(
        "path {},markers {}",
        b64("tus/wide.txt"),
        b64(&json!([{ "access": "public", "from": "x" }]).to_string()),
    );
    let create = Request::builder()
        .method("POST")
        .uri("/v1/tus")
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .header("upload-length", "4")
        .header("upload-metadata", metadata)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        copal_store::repo::file::find_by_path(&store, &tenant, "tus/wide.txt")
            .await
            .unwrap()
            .is_none(),
        "the refusal preceded the file record",
    );
}

/// Leak surface one, the search results themselves: a withheld chunk
/// contributes no snippet and no rank, and a file whose ONLY matching
/// passages are withheld does not surface at all - while its open
/// passages keep answering their own questions, because partial
/// availability is the point of per-chunk granularity.
///
/// Non-vacuity: the oracle runs the same retrieval with the chunk
/// conjunct dropped and finds the withheld passage, so the empty
/// answer above is the filter working, not the corpus lacking a hit.
#[tokio::test]
async fn search_never_surfaces_a_withheld_passage() {
    let (router, engine, store, _dir) = stack().await;
    let document = marked_document();
    let id = upload_marked(
        &router,
        &engine,
        "docs/leak1.txt",
        &document,
        Some(&marker_json()),
    )
    .await;

    // The secret's term matches nothing through the API.
    let get = req("GET", "/v1/search?q=zanzibar", Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["items"], json!([]), "{body}");

    // The oracle proves the leak WOULD happen without the conjunct:
    // the withheld passage matches, names this file, and carries the
    // secret in its body - exactly what must never reach a caller.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let would_leak = text_repo::search_ignoring_chunk_levels(
        &store,
        &tenant,
        "zanzibar",
        10,
        &Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(would_leak.len(), 1, "the planted chunk must match");
    assert_eq!(would_leak[0].file_id().as_deref(), Some(id.as_str()));
    assert!(would_leak[0].body.contains("zanzibar"));

    // The file's OPEN passages still answer: withholding one passage
    // does not withhold the document.
    let get = req("GET", "/v1/search?q=housekeeping", Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{body}");
    assert_eq!(items[0]["file"], json!(id.as_str()));
    assert!(
        !items[0]["excerpt"].as_str().unwrap().contains("zanzibar"),
        "an open hit must not excerpt withheld text: {body}",
    );
}

/// Leak surface two, facet counts: computed over the caller-visible
/// match set, so a file whose only matching passages are withheld
/// contributes to no bucket. The oracle counts the raw match set and
/// finds one more file, which is the difference between a count and
/// a disclosure.
#[tokio::test]
async fn facet_counts_run_over_the_caller_visible_match_set() {
    let (router, engine, store, _dir) = stack().await;
    let marked = marked_document();
    upload_marked(
        &router,
        &engine,
        "docs/facet-marked.txt",
        &marked,
        Some(&marker_json()),
    )
    .await;
    // A second document says the term openly, so the bucket exists
    // either way and the assertion is about its NUMBER.
    let open = "an open memo mentioning zanzibar in plain sight, twice over: zanzibar. ".repeat(3);
    upload_marked(&router, &engine, "docs/facet-open.txt", &open, None).await;

    let get = req(
        "GET",
        "/v1/search?q=zanzibar&facets=content_type",
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(
        body["facets"]["content_type"],
        json!([{ "value": "text/plain", "files": 1 }]),
        "only the openly matching document counts: {body}",
    );

    // The oracle: without the chunk conjunct the marked file counts
    // too, so the enforced number above is the filter at work.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let raw = text_repo::facet_counts_ignoring_chunk_levels(
        &store,
        &tenant,
        "zanzibar",
        text_repo::FacetField::ContentType,
        &Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(
        raw[0].files, 2,
        "the raw match set holds both files; the visible one must not",
    );
}

/// Leak surface three, the rerank window: withheld chunks are
/// filtered BEFORE reranking, so the external service never receives
/// withheld text - not even to discard it, because shipping a secret
/// to a reranker is already the disclosure. The recording service
/// stands in for the real one and keeps everything it was sent.
///
/// Non-vacuity: the oracle shows the withheld passage in the raw
/// candidate set, and the window is built from the head of the
/// candidates, so without the filter it would have been read.
#[tokio::test]
async fn the_rerank_window_never_receives_withheld_text() {
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let recorder = received.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = axum::Router::new().route(
        "/rerank",
        axum::routing::post(move |body: axum::Json<Value>| {
            let recorder = recorder.clone();
            async move {
                let documents: Vec<String> = body.0["documents"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|d| d.as_str().map(|s| s.to_owned()))
                    .collect();
                let scored: Vec<Value> = (0..documents.len())
                    .map(|index| json!({ "index": index, "relevance_score": 0.5 }))
                    .collect();
                recorder.lock().unwrap().extend(documents);
                axum::Json(json!({ "results": scored }))
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

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
        std::collections::HashMap::new(),
        FetchPolicy::default(),
        copal_server::tiering::Topology::default(),
        Default::default(),
    );
    let state = AppState::new(store.clone(), blobs)
        .with_flow(registry)
        .with_reranker(Some(copal_server::rerank::Reranker {
            addr: format!("http://{addr}/rerank"),
            model: None,
            token: None,
            depth: 10,
        }));
    let engine = state.flow.clone();
    let router = build_router(state);

    // The marked document plus two open ones, so the reranker has a
    // window to read (it only runs past one candidate) and the test
    // can tell "filtered" from "never called".
    upload_marked(
        &router,
        &engine,
        "docs/rerank-marked.txt",
        &marked_document(),
        Some(&marker_json()),
    )
    .await;
    for (path, filler) in [
        (
            "docs/rerank-a.txt",
            "first open memo about zanzibar ferries and harbor schedules. ",
        ),
        (
            "docs/rerank-b.txt",
            "second open memo about zanzibar spice markets and tides. ",
        ),
    ] {
        upload_marked(&router, &engine, path, &filler.repeat(3), None).await;
    }

    let get = req("GET", "/v1/search?q=zanzibar", Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(
        body["reranked"],
        json!(2),
        "the reranker read exactly the two open documents: {body}",
    );

    let seen = received.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(
        seen.iter()
            .all(|document| !document.contains("acquisition price")),
        "withheld text reached the reranker: {seen:?}",
    );

    // The oracle: the withheld passage IS in the raw candidate set,
    // and the window is the head of the candidates, so without the
    // filter the service would have received the secret.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let raw = text_repo::search_ignoring_chunk_levels(
        &store,
        &tenant,
        "zanzibar",
        10,
        &Default::default(),
    )
    .await
    .unwrap();
    assert!(
        raw.iter().any(|hit| hit.body.contains("acquisition price")),
        "{raw:?}",
    );
}

/// Leak surface four, the full-text read: the second door beside
/// search. Marked spans are elided rather than the document refused,
/// the response carries a count of elided regions and no positions,
/// and chars counts the served text. The raw row is the oracle: it
/// still holds the secret, which is exactly why the route must not.
#[tokio::test]
async fn file_text_elides_marked_spans_and_counts_them() {
    let (router, engine, store, _dir) = stack().await;
    let document = marked_document();
    let id = upload_marked(
        &router,
        &engine,
        "docs/text-door.txt",
        &document,
        Some(&marker_json()),
    )
    .await;

    let get = req("GET", &format!("/v1/files/{id}/text"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    let served = body["text"].as_str().unwrap();
    assert!(!served.contains("zanzibar"), "the marked span served");
    assert!(!served.contains("BEGIN CONFIDENTIAL"), "{served}");
    assert!(
        served.contains("alpha section") && served.contains("omega section"),
        "elision serves the rest of the document: {served}",
    );
    assert_eq!(body["withheld"], json!(1), "{body}");
    assert_eq!(
        body["chars"],
        json!(served.chars().count()),
        "chars counts the SERVED text: {body}",
    );
    // No span positions anywhere in the answer: the length of a
    // secret is part of the secret.
    for key in ["start", "end", "spans", "positions"] {
        assert!(body.get(key).is_none(), "{key} disclosed: {body}");
    }

    // The oracle: the stored row still holds the secret, so the
    // elision above is this route's work rather than the pipeline
    // having dropped the text.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let file = copal_core::FileId::parse(&id).unwrap();
    let row = text_repo::get_text(&store, &tenant, &file)
        .await
        .unwrap()
        .unwrap();
    assert!(row.body.contains("zanzibar"), "the oracle lost its secret");

    // An unmarked document reads exactly as before, with a zero count.
    let plain = "a plain document with nothing withheld anywhere. ".repeat(4);
    let open_id = upload_marked(&router, &engine, "docs/text-open.txt", &plain, None).await;
    let get = req("GET", &format!("/v1/files/{open_id}/text"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["withheld"], json!(0));
    assert_eq!(body["text"].as_str().unwrap(), plain.trim());
}

/// The fail direction on the second door: an unresolvable declaration
/// elides EVERYTHING - the served text is empty and says one region
/// went, rather than serving a document whose protection could not be
/// located.
#[tokio::test]
async fn an_unresolvable_marker_elides_the_whole_text() {
    let (router, engine, _store, _dir) = stack().await;
    let markers = json!([{ "access": "grant", "from": "PHRASE NOBODY WROTE" }]);
    let id = upload_marked(
        &router,
        &engine,
        "docs/text-unresolved.txt",
        &marked_document(),
        Some(&markers),
    )
    .await;
    let get = req("GET", &format!("/v1/files/{id}/text"), Body::empty());
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["text"], json!(""), "{body}");
    assert_eq!(body["withheld"], json!(1), "{body}");
    assert_eq!(body["chars"], json!(0), "{body}");
}

/// Leak surface five, the semantic leg: the same conjunct rides the
/// KNN query, so a withheld passage is never a neighbor. The fake
/// embedder leans vectors toward keyword axes, making nearness
/// deterministic; the oracle proves the withheld passage IS the
/// nearest neighbor without the filter.
#[tokio::test]
async fn the_semantic_leg_withholds_the_same_passages() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = axum::Router::new().route(
        "/v1/embeddings",
        axum::routing::post(|body: axum::Json<Value>| async move {
            let input = body.0["input"].as_str().unwrap_or_default().to_lowercase();
            let finance = f64::from(input.contains("acquisition") || input.contains("price"));
            let logistics = f64::from(input.contains("logistics") || input.contains("harbor"));
            axum::Json(json!({ "data": [ { "embedding": [finance, logistics, 0.01] } ] }))
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

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
        std::collections::HashMap::new(),
        FetchPolicy::default(),
        copal_server::tiering::Topology::default(),
        Default::default(),
    );
    let state = AppState::new(store.clone(), blobs)
        .with_flow(registry)
        .with_embedding(embedding);
    let engine = state.flow.clone();
    let router = build_router(state);

    // The marked document's secret is the only finance-flavored text;
    // an open logistics document keeps the corpus from being empty.
    upload_marked(
        &router,
        &engine,
        "docs/sem-marked.txt",
        &marked_document(),
        Some(&marker_json()),
    )
    .await;
    let logistics = "open harbor logistics notes with no numbers in them at all. ".repeat(3);
    upload_marked(&router, &engine, "docs/sem-open.txt", &logistics, None).await;

    // Asking for the secret by meaning answers nothing: the withheld
    // passage is not a neighbor, and the logistics document is not
    // near a finance query.
    let get = req(
        "GET",
        "/v1/search?q=acquisition%20price&mode=semantic",
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(get).await.unwrap()).await;
    assert_eq!(body["mode"], json!("semantic"));
    assert_eq!(body["items"], json!([]), "{body}");

    // The oracle, with the query's own vector: the withheld passage
    // is the nearest neighbor the filter removed.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let raw = text_repo::semantic_search_ignoring_chunk_levels(
        &store,
        &tenant,
        &[1.0, 0.0, 0.01],
        10,
        0.5,
        &Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(raw.len(), 1, "{raw:?}");
    assert!(raw[0].body.contains("acquisition price"), "{raw:?}");
}

/// Leak surface six, the layer under all of the above: the engine's
/// compiled PERMISSIONS carry the chunk conjunct, so a caller-bound
/// session meets the refusal even when the application clause is
/// GONE. The dropped-clause bug is played by a real production query
/// (`chunks_without_embedding` carries no disclosure clause at all,
/// because the embed worker runs on the service store); run through a
/// caller session, the engine withholds the marked chunk anyway.
///
/// Non-vacuity: the service store beside it sees both chunks, so the
/// row the caller cannot see exists and the filtering is the second
/// layer's work.
#[tokio::test]
async fn the_engine_second_layer_withholds_chunks_on_its_own() {
    let mut config = StoreConfig::memory_with_engine_access("chunk-engine-key");
    config.engine_policy = copal_server::engine::engine_policy().unwrap();
    let store = Store::connect(config).await.unwrap();

    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let spec = copal_core::FileSpec {
        path: "docs/engine.txt".to_owned(),
        content_type: "text/plain".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: json!({}),
        idempotency_key: None,
    };
    let file = copal_store::repo::file::create_file(&store, &tenant, &spec, "tester")
        .await
        .unwrap()
        .record
        .id;
    text_repo::put_chunks(
        &store,
        &tenant,
        &file,
        "digest",
        &[
            text_repo::ChunkInput::plain("an open passage".to_owned()),
            text_repo::ChunkInput {
                body: "a withheld passage carrying the secret".to_owned(),
                access: Some(copal_core::AccessLevel::Grant),
            },
        ],
    )
    .await
    .unwrap();

    let access = copal_server::engine::EngineAccess {
        key: "chunk-engine-key".to_owned(),
        namespace: "copal_test".to_owned(),
        database: "copal".to_owned(),
    };
    let token = copal_server::engine::mint_caller_token(
        &access,
        &tenant,
        &ulid::Ulid::new().to_string().to_ascii_lowercase(),
        &["read".to_owned()],
        None,
    );
    let caller = store.caller(&token).await.expect("minted token binds");

    let through_caller = text_repo::chunks_without_embedding(&caller, &file)
        .await
        .unwrap();
    assert_eq!(
        through_caller.len(),
        1,
        "the engine filtered without any application clause: {through_caller:?}",
    );
    assert_eq!(through_caller[0].body, "an open passage");

    // The service store sees both rows, so the caller's view above
    // is the permission clause at work rather than a missing row.
    let through_service = text_repo::chunks_without_embedding(&store, &file)
        .await
        .unwrap();
    assert_eq!(through_service.len(), 2, "{through_service:?}");
}
