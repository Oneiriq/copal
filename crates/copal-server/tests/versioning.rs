//! Versioning end to end: re-upload mints history, old content stays
//! reachable, history is frozen at the engine, and superseded blobs
//! survive GC while their file lives.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::FsBlobStore;
use copal_server::app::Limits;
use copal_server::sweeps::{run_pass, SweepConfig};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, Store, FsBlobStore, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs.clone());
    (build_router(state), store, blobs, dir)
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

async fn put_bytes(router: &axum::Router, id: &str, payload: &[u8]) -> Value {
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(payload.to_vec()),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json_body(response).await
}

async fn get_bytes(router: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = router
        .clone()
        .oneshot(req("GET", uri, Some("acme"), Body::empty()))
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn re_upload_mints_versions_and_history_stays_readable() {
    let (router, _store, _blobs, _dir) = stack().await;

    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "doc.txt", "content_type": "text/plain"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    let v1 = put_bytes(&router, &id, b"first draft").await;
    assert_eq!(v1["version_count"], 1);
    let v2 = put_bytes(&router, &id, b"second draft, improved").await;
    assert_eq!(v2["version_count"], 2);
    assert_eq!(v2["state"], "ready");

    // Current content is v2.
    let (status, bytes) = get_bytes(&router, &format!("/v1/files/{id}/content")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&bytes[..], b"second draft, improved");

    // History lists newest first with both versions.
    let versions = req(
        "GET",
        &format!("/v1/files/{id}/versions"),
        Some("acme"),
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(versions).await.unwrap()).await;
    let listed = body["items"].as_array().unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0]["number"], 2);
    assert_eq!(listed[1]["number"], 1);
    assert_ne!(listed[0]["digest"], listed[1]["digest"]);
    assert!(
        listed[0]["metadata_snapshot"].is_object(),
        "each version serves the metadata as it stood: {listed:?}",
    );
    assert!(body["next_before"].is_null(), "short page has no cursor");

    // Bounded pages walk the history without overlap.
    let page = req(
        "GET",
        &format!("/v1/files/{id}/versions?limit=1"),
        Some("acme"),
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(page).await.unwrap()).await;
    assert_eq!(body["items"][0]["number"], 2);
    assert_eq!(body["next_before"], 2);
    let page = req(
        "GET",
        &format!("/v1/files/{id}/versions?limit=1&before=2"),
        Some("acme"),
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(page).await.unwrap()).await;
    assert_eq!(body["items"][0]["number"], 1);

    // The superseded version's bytes remain reachable by number.
    let (status, bytes) = get_bytes(&router, &format!("/v1/files/{id}/versions/1/content")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&bytes[..], b"first draft");

    // An unknown version is 404.
    let (status, _) = get_bytes(&router, &format!("/v1/files/{id}/versions/9/content")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn failed_re_upload_keeps_serving_the_previous_version() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();
    let mut state = AppState::new(store.clone(), blobs.clone());
    // Tiny ceiling so the re-upload fails as oversize.
    state.limits = Limits {
        max_upload_bytes: 32,
        upload_lease_secs: 900,
        ..Limits::default()
    };
    let router = build_router(state);

    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "resilient.txt"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    put_bytes(&router, &id, b"good content").await;

    // Oversize re-upload: 413, record lands in failed...
    let oversize = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(vec![0u8; 64]),
    );
    let response = router.clone().oneshot(oversize).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    assert_eq!(
        json_body(router.clone().oneshot(meta).await.unwrap()).await["state"],
        "failed"
    );

    // ...and the previous version KEEPS SERVING: servability follows
    // the digest.
    let (status, bytes) = get_bytes(&router, &format!("/v1/files/{id}/content")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&bytes[..], b"good content");
}

#[tokio::test]
async fn version_rows_are_frozen_at_the_engine() {
    let (router, store, _blobs, _dir) = stack().await;
    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "frozen.txt"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    put_bytes(&router, &id, b"immutable").await;

    // Reach under the API and try to mutate the armed version row: the
    // freeze event must THROW.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let file_id = copal_core::FileId::parse(&id).unwrap();
    let version = copal_store::repo::version::get_version(&store, &tenant, &file_id, 1)
        .await
        .unwrap()
        .expect("version 1 exists");
    assert_eq!(version.number, 1);
    let tampered = copal_store::repo::version::tamper_for_test(&store, &tenant, &file_id, 1).await;
    let err = tampered.expect_err("armed version rows must reject updates");
    assert!(
        err.to_string().contains("immutable"),
        "engine freeze must be the refusal: {err}"
    );
}

#[tokio::test]
async fn superseded_blobs_survive_gc_while_their_file_lives() {
    let (router, store, blobs, dir) = stack().await;
    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "kept-history.txt"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    put_bytes(&router, &id, b"old content").await;
    put_bytes(&router, &id, b"new content").await;
    let old_digest = copal_core::ContentDigest::of_bytes(b"old content");

    // Aggressive GC: zero grace, two passes. The superseded blob is
    // held by the v1 history row and must survive both.
    let config = SweepConfig {
        gc_grace_secs: 0,
        ..SweepConfig::default()
    };
    run_pass(&store, &blobs, &config).await;
    let report = run_pass(&store, &blobs, &config).await;
    assert_eq!(report.blobs_collected, 0, "history must hold the blob");
    let object = dir.path().join("objects").join(old_digest.storage_key());
    assert!(object.exists(), "superseded bytes must survive: {object:?}");

    // Delete the file: history releases, and GC reclaims BOTH blobs.
    let delete = req(
        "DELETE",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    router.clone().oneshot(delete).await.unwrap();
    run_pass(&store, &blobs, &config).await; // mark
    let report = run_pass(&store, &blobs, &config).await; // collect
    assert_eq!(report.blobs_collected, 2);
    assert!(!object.exists(), "history released: bytes reclaimed");
}
