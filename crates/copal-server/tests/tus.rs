//! Resumable uploads end to end: create, resume, append, complete
//! through the standard finalize path, terminate, and sweep.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use serde_json::Value;
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::sweeps::{run_pass, SweepConfig};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, Store, ObjectStore, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs.clone());
    (build_router(state), store, blobs, dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn metadata(path: &str) -> String {
    let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
    format!(
        "path {},content_type {}",
        b64(path),
        b64("application/octet-stream"),
    )
}

fn create_req(length: usize, path: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/tus")
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .header("upload-length", length.to_string())
        .header("upload-metadata", metadata(path))
        .body(Body::empty())
        .unwrap()
}

fn patch_req(location: &str, offset: usize, chunk: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PATCH")
        .uri(location)
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .header("content-type", "application/offset+octet-stream")
        .header("upload-offset", offset.to_string())
        .body(Body::from(chunk.to_vec()))
        .unwrap()
}

fn head_req(location: &str) -> Request<Body> {
    Request::builder()
        .method("HEAD")
        .uri(location)
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn resumable_upload_completes_through_the_standard_path() {
    let (router, _store, _blobs, _dir) = stack().await;
    let payload = b"resumable payload crossing two appends";
    let split = 10usize;

    // Capabilities.
    let options = Request::builder()
        .method("OPTIONS")
        .uri("/v1/tus")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(options).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers()["tus-version"], "1.0.0");
    assert!(response.headers()["tus-extension"]
        .to_str()
        .unwrap()
        .contains("creation"));

    // Create.
    let response = router
        .clone()
        .oneshot(create_req(payload.len(), "resumed.bin"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let location = response.headers()["location"].to_str().unwrap().to_owned();

    // A missing protocol version refuses.
    let mut bare = head_req(&location);
    bare.headers_mut().remove("tus-resumable");
    let response = router.clone().oneshot(bare).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // First append.
    let response = router
        .clone()
        .oneshot(patch_req(&location, 0, &payload[..split]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers()["upload-offset"],
        split.to_string().as_str()
    );

    // A stale offset refuses; the true offset survives.
    let response = router
        .clone()
        .oneshot(patch_req(&location, 0, b"replay"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = router.clone().oneshot(head_req(&location)).await.unwrap();
    assert_eq!(
        response.headers()["upload-offset"],
        split.to_string().as_str()
    );

    // Resume and complete.
    let response = router
        .clone()
        .oneshot(patch_req(&location, split, &payload[split..]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // The session is gone; the file finished through the normal path.
    let response = router.clone().oneshot(head_req(&location)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let list = Request::builder()
        .method("GET")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let body = json_body(router.clone().oneshot(list).await.unwrap()).await;
    let file = &body["items"][0];
    assert_eq!(file["state"], "ready");
    assert_eq!(file["size"], payload.len());
    let id = file["id"].as_str().unwrap();

    let download = Request::builder()
        .method("GET")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);
}

#[tokio::test]
async fn termination_discards_the_session_and_fails_the_file() {
    let (router, _store, _blobs, _dir) = stack().await;
    let response = router
        .clone()
        .oneshot(create_req(100, "doomed.bin"))
        .await
        .unwrap();
    let location = response.headers()["location"].to_str().unwrap().to_owned();
    router
        .clone()
        .oneshot(patch_req(&location, 0, b"partial"))
        .await
        .unwrap();

    let terminate = Request::builder()
        .method("DELETE")
        .uri(&location)
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(terminate).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = router.clone().oneshot(head_req(&location)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let list = Request::builder()
        .method("GET")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let body = json_body(router.clone().oneshot(list).await.unwrap()).await;
    assert_eq!(body["items"][0]["state"], "failed");
}

#[tokio::test]
async fn abandoned_sessions_sweep_with_their_bytes() {
    let (router, store, blobs, dir) = stack().await;
    let response = router
        .clone()
        .oneshot(create_req(1000, "abandoned.bin"))
        .await
        .unwrap();
    let location = response.headers()["location"].to_str().unwrap().to_owned();
    router
        .clone()
        .oneshot(patch_req(&location, 0, b"never finished"))
        .await
        .unwrap();
    assert!(dir.path().join("tus").exists(), "staged bytes exist");

    let config = SweepConfig {
        tus_session_ttl_secs: 0,
        ..SweepConfig::default()
    };
    let report = run_pass(
        &store,
        &copal_server::app::Residencies::local_only(blobs.clone()),
        &config,
    )
    .await;
    assert_eq!(report.tus_sessions_swept, 1);

    let response = router.clone().oneshot(head_req(&location)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let leftovers = std::fs::read_dir(dir.path().join("tus"))
        .map(|entries| entries.filter_map(Result::ok).count())
        .unwrap_or_default();
    assert_eq!(leftovers, 0, "staged bytes swept");
}

/// Mint a key for tenant `acme` on a key-mode router; returns the
/// bearer token.
async fn mint(router: &axum::Router, admin: &str, name: &str, scopes: &[&str]) -> String {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/keys")
        .header("x-copal-admin-token", admin)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "name": name, "scopes": scopes }).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await["token"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn bearer(mut request: Request<Body>, token: &str) -> Request<Body> {
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

/// Appending bytes is a write. A read-only key of the same tenant is
/// refused with 403 and the offset does not move.
#[tokio::test]
async fn appending_needs_the_write_scope() {
    const ADMIN: &str = "operator-token";
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
        operator_header: None,
    });
    let router = build_router(state);
    let writer = mint(&router, ADMIN, "writer", &["read", "write"]).await;
    let reader = mint(&router, ADMIN, "reader", &["read"]).await;
    let payload = b"scoped append";

    let response = router
        .clone()
        .oneshot(bearer(create_req(payload.len(), "scoped.bin"), &writer))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let location = response.headers()["location"].to_str().unwrap().to_owned();

    let response = router
        .clone()
        .oneshot(bearer(patch_req(&location, 0, payload), &reader))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = router
        .clone()
        .oneshot(bearer(head_req(&location), &writer))
        .await
        .unwrap();
    assert_eq!(response.headers()["upload-offset"], "0");

    let response = router
        .clone()
        .oneshot(bearer(patch_req(&location, 0, payload), &writer))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}
