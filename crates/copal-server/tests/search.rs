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
