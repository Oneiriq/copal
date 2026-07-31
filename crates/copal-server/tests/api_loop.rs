//! The vertical slice, end to end, through the real router.
//!
//! mem:// metadata plane + tempdir blob plane; no server process, no
//! container. This is the test that says "Copal is a file service":
//! create, stream bytes in, read metadata, stream bytes out — plus the
//! refusals that make it safe.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::FsBlobStore;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn test_router() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();
    (build_router(AppState { store, blobs }), dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn req(method: &str, uri: &str, tenant: Option<&str>, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        builder = builder.header("x-copal-tenant", t);
    }
    if method == "POST" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

#[tokio::test]
async fn full_file_lifecycle() {
    let (router, _dir) = test_router().await;
    let payload = b"%PDF-1.7 pretend plan document";

    // Create.
    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(
            json!({"path": "plans/a-101.pdf", "content_type": "application/pdf"}).to_string(),
        ),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let created = json_body(response).await;
    let id = created["id"].as_str().unwrap().to_owned();
    assert_eq!(created["state"], "draft");

    // Content is not servable before upload.
    let premature = req(
        "GET",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::empty(),
    );
    let response = router.clone().oneshot(premature).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Upload.
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(&payload[..]),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let ready = json_body(response).await;
    assert_eq!(ready["state"], "ready");
    assert_eq!(ready["size_bytes"], payload.len());
    let digest = ready["digest"].as_str().unwrap();
    assert_eq!(digest.len(), 64);

    // Metadata reflects completion.
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let response = router.clone().oneshot(meta).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["state"], "ready");

    // Bytes come back verbatim with the declared content type.
    let download = req(
        "GET",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::empty(),
    );
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/pdf");
    let etag = response.headers()["etag"].to_str().unwrap().to_owned();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);
    assert_eq!(etag, format!("\"{digest}\""));

    // The listing sees exactly one live file.
    let list = req("GET", "/v1/files", Some("acme"), Body::empty());
    let response = router.clone().oneshot(list).await.unwrap();
    let listed = json_body(response).await;
    assert_eq!(listed.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn tenancy_and_validation_refusals() {
    let (router, _dir) = test_router().await;

    // Missing tenant header.
    let anonymous = req("GET", "/v1/files", None, Body::empty());
    let response = router.clone().oneshot(anonymous).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Create as acme.
    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "private.txt"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    // A different tenant sees 404, not 403 — existence is not leaked.
    let foreign = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("globex"),
        Body::empty(),
    );
    let response = router.clone().oneshot(foreign).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Empty path is rejected up front.
    let invalid = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": ""}).to_string()),
    );
    let response = router.clone().oneshot(invalid).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Duplicate live path is a conflict.
    for expected in [StatusCode::CREATED, StatusCode::CONFLICT] {
        let dup = req(
            "POST",
            "/v1/files",
            Some("acme"),
            Body::from(json!({"path": "dup.txt"}).to_string()),
        );
        let response = router.clone().oneshot(dup).await.unwrap();
        assert_eq!(response.status(), expected);
    }
}

#[tokio::test]
async fn identical_uploads_share_one_blob() {
    let (router, dir) = test_router().await;
    let payload = b"identical bytes";

    for path in ["a.bin", "b.bin"] {
        let create = req(
            "POST",
            "/v1/files",
            Some("acme"),
            Body::from(json!({ "path": path }).to_string()),
        );
        let response = router.clone().oneshot(create).await.unwrap();
        let id = json_body(response).await["id"].as_str().unwrap().to_owned();
        let upload = req(
            "PUT",
            &format!("/v1/files/{id}/content"),
            Some("acme"),
            Body::from(&payload[..]),
        );
        let response = router.clone().oneshot(upload).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // One object on disk under objects/, despite two files.
    let mut objects = Vec::new();
    for entry in walkdir(dir.path().join("objects")) {
        objects.push(entry);
    }
    assert_eq!(objects.len(), 1, "expected one deduped object: {objects:?}");
}

fn walkdir(root: std::path::PathBuf) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}
