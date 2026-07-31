//! Encryption at rest, end to end through the real router: sealed on
//! disk, plaintext on the wire, digests unchanged, ranges exact across
//! frame boundaries, legacy plaintext still served.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::crypto;
use copal_blob::ObjectStore;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

async fn encrypted_stack() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open_encrypted(dir.path().to_str().unwrap(), KEY).unwrap();
    (build_router(AppState::new(store, blobs)), dir)
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

/// Three frames plus change, with a recognizable byte pattern.
fn payload() -> Vec<u8> {
    (0..2 * crypto::FRAME + 1000)
        .map(|i| (i % 251) as u8)
        .collect()
}

#[tokio::test]
async fn sealed_on_disk_plaintext_on_the_wire() {
    let (router, dir) = encrypted_stack().await;
    let payload = payload();

    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({"path": "sealed.bin"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(payload.clone()),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let record = json_body(response).await;
    let digest = record["digest"].as_str().unwrap().to_owned();

    // The digest is the PLAINTEXT digest.
    let expected = {
        use sha2::{Digest as _, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        hex::encode(hasher.finalize())
    };
    assert_eq!(digest, expected, "content addressing survives sealing");

    // On disk: the sealed magic, the sealed length, no plaintext run.
    let object_path = dir
        .path()
        .join("objects")
        .join(&digest[0..2])
        .join(&digest[2..4])
        .join(&digest);
    let on_disk = std::fs::read(&object_path).unwrap();
    assert_eq!(&on_disk[..4], crypto::MAGIC);
    assert_eq!(on_disk.len(), crypto::sealed_len(payload.len()));
    assert!(
        !on_disk
            .windows(64)
            .any(|w| w == &payload[crypto::FRAME..crypto::FRAME + 64]),
        "plaintext must not appear on disk",
    );

    // Full download round-trips.
    let download = req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-length"],
        payload.len().to_string().as_str(),
        "logical length, never the sealed length",
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], &payload[..]);

    // A range crossing a frame boundary comes back exact.
    let start = crypto::FRAME - 100;
    let end_inclusive = crypto::FRAME + 99;
    let mut ranged = req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    ranged.headers_mut().insert(
        "range",
        format!("bytes={start}-{end_inclusive}").parse().unwrap(),
    );
    let response = router.clone().oneshot(ranged).await.unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], &payload[start..=end_inclusive]);
}

#[tokio::test]
async fn legacy_plaintext_objects_keep_serving_after_enablement() {
    // Write through a PLAIN store, then reopen the same root encrypted.
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plain = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let router = build_router(AppState::new(store.clone(), plain));

    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({"path": "legacy.txt"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(&b"written before encryption"[..]),
    );
    router.clone().oneshot(upload).await.unwrap();

    let encrypted = ObjectStore::open_encrypted(dir.path().to_str().unwrap(), KEY).unwrap();
    let router = build_router(AppState::new(store, encrypted));
    let download = req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"written before encryption");
}
