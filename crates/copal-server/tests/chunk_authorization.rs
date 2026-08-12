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
