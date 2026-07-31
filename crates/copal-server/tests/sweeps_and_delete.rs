//! Maintenance and deletion, end to end on mem:// + tempdir.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::FsBlobStore;
use copal_server::sweeps::{run_pass, SweepConfig};
use copal_server::{build_router, AppState};
use copal_store::repo::blob as blob_repo;
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

async fn upload_file(router: &axum::Router, path: &str, payload: &[u8]) -> String {
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
        Body::from(payload.to_vec()),
    );
    assert_eq!(
        router.clone().oneshot(upload).await.unwrap().status(),
        StatusCode::OK
    );
    id
}

#[tokio::test]
async fn delete_tombstones_frees_the_path_and_kills_grants() {
    let (router, _store, _blobs, _dir) = stack().await;
    let id = upload_file(&router, "doomed.bin", b"bytes").await;

    // A live grant exists before deletion.
    let issue = req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Some("acme"),
        Body::from(json!({}).to_string()),
    );
    let grant = json_body(router.clone().oneshot(issue).await.unwrap()).await;
    let grant_url = grant["url"].as_str().unwrap().to_owned();

    // Delete: 204, then reads 404, then repeat-delete 404.
    let delete = req(
        "DELETE",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    assert_eq!(
        router.clone().oneshot(delete).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    assert_eq!(
        router.clone().oneshot(meta).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let again = req(
        "DELETE",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    assert_eq!(
        router.clone().oneshot(again).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    // The grant dies with the file.
    assert_eq!(
        router
            .clone()
            .oneshot(req("GET", &grant_url, None, Body::empty()))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    // The live path frees for a new file.
    let recreate = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "doomed.bin"}).to_string()),
    );
    assert_eq!(
        router.clone().oneshot(recreate).await.unwrap().status(),
        StatusCode::CREATED
    );

    // Foreign tenants cannot delete.
    let other_id = upload_file(&router, "keep.bin", b"keep").await;
    let foreign = req(
        "DELETE",
        &format!("/v1/files/{other_id}"),
        Some("globex"),
        Body::empty(),
    );
    assert_eq!(
        router.clone().oneshot(foreign).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn gc_marks_then_collects_unreferenced_content() {
    let (router, store, blobs, dir) = stack().await;
    let payload = b"collectable content";
    let digest = copal_core::ContentDigest::of_bytes(payload);

    // Two files share the blob; delete only one.
    let a = upload_file(&router, "a.bin", payload).await;
    let b = upload_file(&router, "b.bin", payload).await;
    let delete = req(
        "DELETE",
        &format!("/v1/files/{a}"),
        Some("acme"),
        Body::empty(),
    );
    router.clone().oneshot(delete).await.unwrap();

    let config = SweepConfig {
        gc_grace_secs: 0,
        ..SweepConfig::default()
    };

    // Still referenced by b: the pass refreshes the cache, collects
    // nothing, and the object stays. Two references — b's current link
    // plus b's version-1 history row; a's links dropped with its
    // tombstone.
    let report = run_pass(&store, &blobs, &config).await;
    assert_eq!(report.blobs_collected, 0);
    assert_eq!(
        blob_repo::recount_inbound_links(&store, &digest)
            .await
            .unwrap(),
        2
    );

    // Delete b too: first pass MARKS (grace clock starts)...
    let delete = req(
        "DELETE",
        &format!("/v1/files/{b}"),
        Some("acme"),
        Body::empty(),
    );
    router.clone().oneshot(delete).await.unwrap();
    let report = run_pass(&store, &blobs, &config).await;
    assert_eq!(report.blobs_marked, 1);
    assert_eq!(report.blobs_collected, 0);

    // ...second pass (grace already elapsed at zero) collects row AND
    // bytes.
    let report = run_pass(&store, &blobs, &config).await;
    assert_eq!(report.blobs_collected, 1);
    assert!(blob_repo::get_location(&store, &digest)
        .await
        .unwrap()
        .is_none());
    let object = dir.path().join("objects").join(digest.storage_key());
    assert!(!object.exists(), "object bytes must be gone: {object:?}");
}

#[tokio::test]
async fn relink_during_grace_cancels_collection() {
    let (router, store, blobs, _dir) = stack().await;
    let payload = b"resurrected content";

    let a = upload_file(&router, "gone.bin", payload).await;
    let delete = req(
        "DELETE",
        &format!("/v1/files/{a}"),
        Some("acme"),
        Body::empty(),
    );
    router.clone().oneshot(delete).await.unwrap();

    // Long grace: this pass marks and must never collect in-test.
    let config = SweepConfig {
        gc_grace_secs: 3_600,
        ..SweepConfig::default()
    };
    let report = run_pass(&store, &blobs, &config).await;
    assert_eq!(report.blobs_marked, 1);

    // Same content uploaded again during grace: the next pass must
    // CLEAR the mark, not collect.
    upload_file(&router, "back.bin", payload).await;
    let report = run_pass(&store, &blobs, &config).await;
    assert_eq!(report.blobs_collected, 0);
    assert_eq!(report.blobs_refreshed, 1);

    // A further pass with zero grace still refuses: the mark is gone.
    let zero = SweepConfig {
        gc_grace_secs: 0,
        ..SweepConfig::default()
    };
    let report = run_pass(&store, &blobs, &zero).await;
    assert_eq!(report.blobs_collected, 0);
}

#[tokio::test]
async fn staging_sweep_removes_only_aged_entries() {
    let (_router, store, blobs, dir) = stack().await;

    let staging = dir.path().join("staging");
    std::fs::create_dir_all(&staging).unwrap();
    // Age lives in the ULID key itself: mint one two hours in the past
    // and one fresh.
    let old = ulid::Ulid::from_datetime(
        std::time::SystemTime::now() - std::time::Duration::from_secs(7_200),
    )
    .to_string()
    .to_lowercase();
    let fresh = ulid::Ulid::new().to_string().to_lowercase();
    std::fs::write(staging.join(&old), b"orphaned partial upload").unwrap();
    std::fs::write(staging.join(&fresh), b"in-flight upload").unwrap();

    let config = SweepConfig {
        staging_ttl_secs: 3_600,
        ..SweepConfig::default()
    };
    let report = run_pass(&store, &blobs, &config).await;
    assert_eq!(report.staging_removed, 1);
    assert!(!staging.join(&old).exists());
    assert!(staging.join(&fresh).exists(), "fresh staging must survive");
}
