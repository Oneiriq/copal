//! The external transformer seam end to end: a stub HTTP service
//! receives the source bytes and answers with derived bytes, and the
//! derived record finishes through the same claim and complete path a
//! rendition uses. A 4xx answer fails the derived record instead of
//! retrying; an unknown name refuses at the API.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::ExtensionPolicy;
use copal_flow::FlowEngine;
use copal_server::app::Residencies;
use copal_server::config::TransformerConfig;
use copal_server::pipeline::standard_registry;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

/// What the stub saw on its last call, for header assertions.
#[derive(Clone, Default)]
struct Seen {
    secret: Option<String>,
    params: Option<String>,
    body: Vec<u8>,
}

/// A transformer that uppercases the source bytes, plus a route that
/// refuses everything with 422.
async fn stub_transformer(seen: Arc<Mutex<Seen>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let upper = axum::routing::post(move |request: axum::extract::Request| {
        let seen = seen.clone();
        async move {
            let (parts, body) = request.into_parts();
            let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
            let params = parts
                .uri
                .query()
                .and_then(|q| q.strip_prefix("params="))
                .map(|v| v.to_owned());
            let secret = parts
                .headers
                .get("x-copal-transform-secret")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_owned());
            let mut guard = seen.lock().unwrap();
            guard.secret = secret;
            guard.params = params;
            guard.body = bytes.to_vec();
            drop(guard);
            let out = bytes.to_ascii_uppercase();
            ([("content-type", "text/plain")], out)
        }
    });
    let refuse = axum::routing::post(|| async {
        (StatusCode::UNPROCESSABLE_ENTITY, "input makes no sense")
    });
    let app = axum::Router::new()
        .route("/upper", upper)
        .route("/refuse", refuse);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn stack(seen: Arc<Mutex<Seen>>) -> (axum::Router, FlowEngine, tempfile::TempDir) {
    let addr = stub_transformer(seen).await;
    let mut transformers = std::collections::HashMap::new();
    transformers.insert(
        "upper".to_owned(),
        TransformerConfig {
            url: format!("http://{addr}/upper"),
            timeout_secs: Some(10),
            secret: Some("s3cr3t".to_owned()),
            max_source_bytes: None,
        },
    );
    transformers.insert(
        "grumpy".to_owned(),
        TransformerConfig {
            url: format!("http://{addr}/refuse"),
            timeout_secs: Some(10),
            secret: None,
            max_source_bytes: None,
        },
    );

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
        transformers.clone(),
        copal_server::pipeline::FetchPolicy::default(),
        copal_server::tiering::Topology::default(),
        Default::default(),
    );
    let state = AppState::new(store, blobs)
        .with_flow(registry)
        .with_transformers(transformers);
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

/// Upload a small text file and drain its post-upload run.
async fn upload_source(router: &axum::Router, engine: &FlowEngine) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": "notes/hello.txt" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(&b"hello transformer"[..]),
    );
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drain(engine).await;
    id
}

#[tokio::test]
async fn transform_derives_through_the_external_service() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let (router, engine, _dir) = stack(seen.clone()).await;
    let id = upload_source(&router, &engine).await;

    let request = req(
        "POST",
        &format!("/v1/files/{id}/transform"),
        Body::from(
            json!({
                "transformer": "upper",
                "params": { "mode": "loud" },
                "content_type": "text/plain",
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let derived = json_body(response).await["id"].as_str().unwrap().to_owned();
    drain(&engine).await;

    // The derived file serves the transformed bytes.
    let get = req(
        "GET",
        &format!("/v1/files/{derived}/content"),
        Body::empty(),
    );
    let response = router.clone().oneshot(get).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"HELLO TRANSFORMER");

    // The stub saw the shared secret, the params, and the source. The
    // guard lives in its own block so no lock is held at an await.
    {
        let guard = seen.lock().unwrap();
        assert_eq!(guard.secret.as_deref(), Some("s3cr3t"));
        let params = guard.params.as_deref().unwrap_or_default();
        assert!(params.contains("loud"), "params rode the query: {params}");
        assert_eq!(guard.body, b"hello transformer");
    }

    // Derived bytes came from another process, so they walk the same
    // pipeline an upload does: the text the transformer produced is
    // extracted, which is what makes it findable.
    let text = req("GET", &format!("/v1/files/{derived}/text"), Body::empty());
    let response = router.clone().oneshot(text).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "derived text extracts");
    assert_eq!(
        json_body(response).await["text"],
        json!("HELLO TRANSFORMER"),
        "what the transformer produced is what got indexed",
    );

    let found = req("GET", "/v1/search?q=TRANSFORMER&limit=5", Body::empty());
    let response = router.clone().oneshot(found).await.unwrap();
    let hits = json_body(response).await;
    assert!(
        hits["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|hit| hit["file"] == json!(derived)),
        "a transformer's output is searchable: {hits}",
    );

    // The derivation lists beside renditions, and repeating the
    // request returns the existing record.
    let list = req("GET", &format!("/v1/files/{id}/renditions"), Body::empty());
    let response = router.clone().oneshot(list).await.unwrap();
    let items = json_body(response).await["items"].clone();
    assert!(
        items
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == json!(derived)),
        "the transform lists as a derivative",
    );
    let repeat = req(
        "POST",
        &format!("/v1/files/{id}/transform"),
        Body::from(
            json!({
                "transformer": "upper",
                "params": { "mode": "loud" },
                "content_type": "text/plain",
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(repeat).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "replay returns existing");
    assert_eq!(json_body(response).await["id"], json!(derived));
}

#[tokio::test]
async fn a_refusing_transformer_fails_the_derived_record() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let (router, engine, _dir) = stack(seen).await;
    let id = upload_source(&router, &engine).await;

    let request = req(
        "POST",
        &format!("/v1/files/{id}/transform"),
        Body::from(json!({ "transformer": "grumpy" }).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let derived = json_body(response).await["id"].as_str().unwrap().to_owned();
    drain(&engine).await;

    let get = req("GET", &format!("/v1/files/{derived}"), Body::empty());
    let response = router.clone().oneshot(get).await.unwrap();
    let record = json_body(response).await;
    assert_eq!(record["state"], json!("failed"), "422 fails, no retry loop");
}

#[tokio::test]
async fn unknown_transformers_refuse_at_the_api() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let (router, engine, _dir) = stack(seen).await;
    let id = upload_source(&router, &engine).await;

    let request = req(
        "POST",
        &format!("/v1/files/{id}/transform"),
        Body::from(json!({ "transformer": "nonesuch" }).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
