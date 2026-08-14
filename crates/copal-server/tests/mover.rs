//! The mover, proven at the disk: demotion copies verified bytes
//! cold and reads follow, the grace-delayed erase reclaims the hot
//! copy, a pin brings bytes home, a corrupt copy never flips the
//! row, a half-done move converges by replay, and collection erases
//! every tier.

use std::collections::HashMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::tier::TierClass;
use copal_blob::{BlobStore as _, ObjectStore};
use copal_core::ContentDigest;
use copal_server::app::Residencies;
use copal_server::auth::AuthConfig;
use copal_server::mover::move_pass;
use copal_server::sweeps::{run_pass, SweepConfig};
use copal_server::tiering::Topology;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-token";

struct Stack {
    router: axum::Router,
    store: Store,
    residencies: Residencies<ObjectStore>,
    topology: Topology,
    hot_dir: tempfile::TempDir,
    cold_dir: tempfile::TempDir,
}

fn topology() -> Topology {
    let mut t = Topology::default();
    t.insert(
        "local",
        HashMap::from([("cold".to_owned(), TierClass::Online)]),
    );
    t
}

/// One residency (`local`) with one online tier (`cold`), each on its
/// own directory, shared by the router and the mover exactly as the
/// server shares them.
async fn stack() -> Stack {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let hot_dir = tempfile::tempdir().unwrap();
    let cold_dir = tempfile::tempdir().unwrap();
    let hot = ObjectStore::open(hot_dir.path().to_str().unwrap()).unwrap();
    let cold = ObjectStore::open(cold_dir.path().to_str().unwrap()).unwrap();
    let mut residencies = Residencies::local_only(hot);
    residencies.tiers.insert(
        "local".to_owned(),
        HashMap::from([("cold".to_owned(), cold)]),
    );
    let mut state = AppState::new(store.clone(), residencies.local.clone())
        .with_auth(AuthConfig {
            admin_token: Some(ADMIN.into()),
            ..AuthConfig::default()
        })
        .with_tiering(topology());
    state.residencies = residencies.clone();
    Stack {
        router: build_router(state),
        store,
        residencies,
        topology: topology(),
        hot_dir,
        cold_dir,
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn admin(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-admin-token", ADMIN);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    builder
        .body(body.map_or(Body::empty(), |value| Body::from(value.to_string())))
        .unwrap()
}

async fn upload(router: &axum::Router, path: &str, payload: &[u8]) -> String {
    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "path": path }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .body(Body::from(payload.to_vec()))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

async fn set_immediate_policy(router: &axum::Router) {
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/tiering",
            Some(json!({ "tier": "cold", "after_seconds": 0, "basis": "created" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn get_content(router: &axum::Router, id: &str) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method("GET")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, bytes.to_vec())
}

fn object_path(dir: &tempfile::TempDir, digest: &ContentDigest) -> std::path::PathBuf {
    dir.path().join("objects").join(digest.storage_key())
}

async fn tier_of(store: &Store, digest: &ContentDigest) -> Option<String> {
    copal_store::repo::blob::get_location(store, "local", digest)
        .await
        .unwrap()
        .unwrap()
        .tier
}

#[tokio::test]
async fn demotion_moves_verified_bytes_and_reads_follow() {
    let stack = stack().await;
    let payload = b"cold-bound bytes";
    let digest = ContentDigest::of_bytes(payload);
    let id = upload(&stack.router, "corpus.txt", payload).await;
    set_immediate_policy(&stack.router).await;

    let report = move_pass(
        &stack.store,
        &stack.residencies,
        &stack.topology,
        100,
        86_400,
    )
    .await;
    assert_eq!(report.demoted, 1);
    assert_eq!(report.verify_failures, 0);
    assert_eq!(
        tier_of(&stack.store, &digest).await.as_deref(),
        Some("cold")
    );
    assert!(
        object_path(&stack.cold_dir, &digest).exists(),
        "the cold backend holds the object",
    );
    assert!(
        object_path(&stack.hot_dir, &digest).exists(),
        "the hot copy survives until the grace has aged",
    );

    // Reads resolve residency-then-tier: the response is unchanged.
    let (status, body) = get_content(&stack.router, &id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);

    // A later pass, grace aged: the displaced hot copy goes, and
    // reads still answer -- from the only copy left.
    let report = move_pass(&stack.store, &stack.residencies, &stack.topology, 100, 0).await;
    assert_eq!(report.hot_erased, 1);
    assert_eq!(report.demoted, 0, "a settled row does not move again");
    assert!(!object_path(&stack.hot_dir, &digest).exists());
    let (status, body) = get_content(&stack.router, &id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);
}

#[tokio::test]
async fn a_pin_promotes_and_the_cold_copy_erases_after_grace() {
    let stack = stack().await;
    let payload = b"pinned back home";
    let digest = ContentDigest::of_bytes(payload);
    let id = upload(&stack.router, "pinned.txt", payload).await;
    set_immediate_policy(&stack.router).await;

    // Demote and settle: the blob lives cold alone.
    move_pass(
        &stack.store,
        &stack.residencies,
        &stack.topology,
        100,
        86_400,
    )
    .await;
    let report = move_pass(&stack.store, &stack.residencies, &stack.topology, 100, 0).await;
    assert_eq!(report.hot_erased, 1);

    // The operator pins the file hot: the symmetric motions run.
    let response = stack
        .router
        .clone()
        .oneshot(admin(
            "PUT",
            &format!("/v1/admin/tenants/acme/files/{id}/tier"),
            Some(json!({ "pin": "hot" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let report = move_pass(
        &stack.store,
        &stack.residencies,
        &stack.topology,
        100,
        86_400,
    )
    .await;
    assert_eq!(report.promoted, 1);
    assert_eq!(tier_of(&stack.store, &digest).await, None);
    assert!(object_path(&stack.hot_dir, &digest).exists());
    let (status, body) = get_content(&stack.router, &id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);

    // Grace aged: the displaced cold copy goes.
    let report = move_pass(&stack.store, &stack.residencies, &stack.topology, 100, 0).await;
    assert_eq!(report.cold_erased, 1);
    assert!(!object_path(&stack.cold_dir, &digest).exists());
    let (status, body) = get_content(&stack.router, &id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);
}

#[tokio::test]
async fn a_corrupt_copy_never_flips_the_row() {
    let stack = stack().await;
    let payload = b"bytes that will rot";
    let digest = ContentDigest::of_bytes(payload);
    upload(&stack.router, "rotten.txt", payload).await;
    set_immediate_policy(&stack.router).await;

    // Rot the HOT object on disk: the copy lands rotten, the
    // verify's digest disagrees with the address, and the flip never
    // happens -- the row keeps serving whatever the hot backend has,
    // rather than committing to a copy nothing checked.
    std::fs::write(object_path(&stack.hot_dir, &digest), b"not those bytes").unwrap();
    let report = move_pass(
        &stack.store,
        &stack.residencies,
        &stack.topology,
        100,
        86_400,
    )
    .await;
    assert_eq!(report.verify_failures, 1);
    assert_eq!(report.demoted, 0);
    assert_eq!(tier_of(&stack.store, &digest).await, None, "no flip");
    assert!(
        !object_path(&stack.cold_dir, &digest).exists(),
        "the failed copy is removed, not left at a lying address",
    );
}

#[tokio::test]
async fn a_half_done_move_converges_by_replay() {
    let stack = stack().await;
    let payload = b"interrupted mid-motion";
    let digest = ContentDigest::of_bytes(payload);
    upload(&stack.router, "interrupted.txt", payload).await;
    set_immediate_policy(&stack.router).await;

    // Simulate the copy-before-flip crash window: the cold object
    // exists, the row still says hot.
    let hot = stack.residencies.local.clone();
    let cold = stack.residencies.tiers["local"]["cold"].clone();
    let (_, raw) = hot.open_raw(&digest).await.unwrap();
    cold.put_raw(&digest, raw).await.unwrap();
    assert_eq!(tier_of(&stack.store, &digest).await, None);

    // The next pass re-copies (byte-identical), verifies, flips.
    let report = move_pass(
        &stack.store,
        &stack.residencies,
        &stack.topology,
        100,
        86_400,
    )
    .await;
    assert_eq!(report.demoted, 1);
    assert_eq!(
        tier_of(&stack.store, &digest).await.as_deref(),
        Some("cold")
    );
}

#[tokio::test]
async fn collection_erases_every_tier() {
    let stack = stack().await;
    let payload = b"deleted and collected";
    let digest = ContentDigest::of_bytes(payload);
    let id = upload(&stack.router, "collected.txt", payload).await;
    set_immediate_policy(&stack.router).await;

    move_pass(
        &stack.store,
        &stack.residencies,
        &stack.topology,
        100,
        86_400,
    )
    .await;
    assert!(object_path(&stack.cold_dir, &digest).exists());

    // Delete the file; both copies exist (the grace has not aged).
    let delete = Request::builder()
        .method("DELETE")
        .uri(format!("/v1/files/{id}"))
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let response = stack.router.clone().oneshot(delete).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Mark, then collect with zero grace: the GC erases the object
    // from the primary AND the tier, replay-safe on whichever copy
    // is absent.
    let config = SweepConfig {
        gc_grace_secs: 0,
        ..SweepConfig::default()
    };
    run_pass(&stack.store, &stack.residencies, &config).await;
    let report = run_pass(&stack.store, &stack.residencies, &config).await;
    assert_eq!(report.blobs_collected, 1);
    assert!(!object_path(&stack.hot_dir, &digest).exists());
    assert!(!object_path(&stack.cold_dir, &digest).exists());
}
