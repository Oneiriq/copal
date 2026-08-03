//! Tail item four's two halves. On-the-fly rendition URLs: a GET
//! whose first request derives and whose repeats serve the existing
//! record. URL ingestion: the server pulls a tenant-supplied source
//! itself, under the outbound policy, and the fetched bytes walk the
//! standard pipeline into a ready file.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::ExtensionPolicy;
use copal_flow::FlowEngine;
use copal_server::app::Residencies;
use copal_server::pipeline::{standard_registry, FetchPolicy};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack(allow_private_fetch: bool) -> (axum::Router, FlowEngine, tempfile::TempDir) {
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
            allow_private_targets: allow_private_fetch,
            max_bytes: 1 << 20,
        },
    );
    let mut state = AppState::new(store, blobs).with_flow(registry);
    state.limits.allow_private_fetch_targets = allow_private_fetch;
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

async fn drain(engine: &FlowEngine) {
    while engine.tick("w").await.unwrap() {}
}

/// A 64x64 png generated in-process, so the test carries no fixture.
fn source_png() -> Vec<u8> {
    let img = image::RgbImage::from_fn(64, 64, |x, y| {
        image::Rgb([(x * 4) as u8, (y * 4) as u8, 128])
    });
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(img)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    bytes.into_inner()
}

/// Upload one file and drain its pipeline; returns the id.
async fn upload(
    router: &axum::Router,
    engine: &FlowEngine,
    path: &str,
    payload: Vec<u8>,
) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "image/png" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(payload),
    );
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drain(engine).await;
    id
}

#[tokio::test]
async fn a_rendition_url_derives_once_and_serves_after() {
    let (router, engine, _dir) = stack(false).await;
    let id = upload(&router, &engine, "img/photo.png", source_png()).await;

    // First request derives inline and serves the bytes.
    let first = req(
        "GET",
        &format!("/v1/files/{id}/renditions/thumb-32x32.jpeg"),
        Body::empty(),
    );
    let response = router.clone().oneshot(first).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "image/jpeg",
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let decoded = image::load_from_memory(&bytes).unwrap();
    assert_eq!(decoded.width(), 32);
    assert_eq!(decoded.height(), 32);

    // The repeat serves the existing record; exactly one derivative
    // exists.
    let second = req(
        "GET",
        &format!("/v1/files/{id}/renditions/thumb-32x32.jpeg"),
        Body::empty(),
    );
    let response = router.clone().oneshot(second).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let list = req("GET", &format!("/v1/files/{id}/renditions"), Body::empty());
    let response = router.clone().oneshot(list).await.unwrap();
    let items = json_body(response).await["items"].clone();
    assert_eq!(items.as_array().unwrap().len(), 1, "one derived record");

    // Anonymous callers cannot make the server derive.
    let anonymous = Request::builder()
        .method("GET")
        .uri(format!("/v1/files/{id}/renditions/thumb-48x48.jpeg"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(anonymous).await.unwrap();
    assert!(
        response.status().is_client_error(),
        "anonymous derive refused: {}",
        response.status(),
    );
    let list = req("GET", &format!("/v1/files/{id}/renditions"), Body::empty());
    let response = router.clone().oneshot(list).await.unwrap();
    let items = json_body(response).await["items"].clone();
    assert_eq!(items.as_array().unwrap().len(), 1, "nothing new derived");

    // A garbled spec refuses before any work.
    let garbled = req(
        "GET",
        &format!("/v1/files/{id}/renditions/nonsense"),
        Body::empty(),
    );
    let response = router.clone().oneshot(garbled).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

/// A stub origin serving a small text document.
async fn stub_origin() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = axum::Router::new().route(
        "/doc",
        axum::routing::get(|| async {
            ([("content-type", "text/plain")], "pulled from the source")
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

#[tokio::test]
async fn fetch_ingests_a_url_through_the_standard_pipeline() {
    let (router, engine, _dir) = stack(true).await;
    let addr = stub_origin().await;

    let request = req(
        "POST",
        "/v1/files/fetch",
        Body::from(
            json!({
                "url": format!("http://{addr}/doc"),
                "path": "pulled/doc.txt",
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    drain(&engine).await;

    let get = req("GET", &format!("/v1/files/{id}"), Body::empty());
    let response = router.clone().oneshot(get).await.unwrap();
    let record = json_body(response).await;
    assert_eq!(record["state"], json!("ready"));
    assert_eq!(
        record["content_type"],
        json!("text/plain"),
        "the source's served type stands when the caller declared none",
    );

    let content = req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    let response = router.clone().oneshot(content).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"pulled from the source");
}

#[tokio::test]
async fn fetch_refuses_private_targets_under_the_outbound_policy() {
    let (router, _engine, _dir) = stack(false).await;
    let request = req(
        "POST",
        "/v1/files/fetch",
        Body::from(
            json!({
                "url": "http://127.0.0.1:9/doc",
                "path": "pulled/refused.txt",
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_missing_source_fails_the_fetched_record() {
    let (router, engine, _dir) = stack(true).await;
    let addr = stub_origin().await;
    let request = req(
        "POST",
        "/v1/files/fetch",
        Body::from(
            json!({
                "url": format!("http://{addr}/absent"),
                "path": "pulled/missing.txt",
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    drain(&engine).await;

    let get = req("GET", &format!("/v1/files/{id}"), Body::empty());
    let response = router.clone().oneshot(get).await.unwrap();
    assert_eq!(json_body(response).await["state"], json!("failed"));
}
