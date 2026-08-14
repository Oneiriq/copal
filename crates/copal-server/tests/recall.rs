//! Recall, proven end to end on a filesystem archive tier: readable
//! archive objects read through, unreadable ones answer 202 with an
//! idempotent journaled run, the run promotes the bytes home once
//! the backend answers, and a counted grant survives the wait.
//!
//! The unreadable-archived state is simulated by replacing the cold
//! object file with a directory: `read_probe` refuses anything that
//! is not a readable file, which is also what a real Glacier GET
//! refusal resolves to.

use std::collections::HashMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::tier::TierClass;
use copal_blob::ObjectStore;
use copal_core::ContentDigest;
use copal_server::app::Residencies;
use copal_server::auth::AuthConfig;
use copal_server::mover::move_pass;
use copal_server::recall::RestoreDriver;
use copal_server::tiering::Topology;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-token";

struct Stack {
    router: axum::Router,
    store: Store,
    engine: copal_flow::FlowEngine,
    residencies: Residencies<ObjectStore>,
    topology: Topology,
    cold_dir: tempfile::TempDir,
    _hot_dir: tempfile::TempDir,
}

fn topology() -> Topology {
    let mut t = Topology::default();
    t.insert(
        "local",
        HashMap::from([("frozen".to_owned(), TierClass::Archive)]),
    );
    t
}

async fn stack() -> Stack {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let hot_dir = tempfile::tempdir().unwrap();
    let cold_dir = tempfile::tempdir().unwrap();
    let hot = ObjectStore::open(hot_dir.path().to_str().unwrap()).unwrap();
    let frozen = ObjectStore::open(cold_dir.path().to_str().unwrap()).unwrap();
    let mut residencies = Residencies::local_only(hot);
    residencies.tiers.insert(
        "local".to_owned(),
        HashMap::from([("frozen".to_owned(), frozen)]),
    );
    let drivers: copal_server::recall::RestoreDrivers = HashMap::from([(
        "local".to_owned(),
        HashMap::from([("frozen".to_owned(), RestoreDriver::Instant)]),
    )]);
    let registry = copal_server::pipeline::standard_registry(
        store.clone(),
        residencies.clone(),
        copal_core::ExtensionPolicy::standard(),
        false,
        None,
        None,
        None,
        std::collections::HashMap::new(),
        copal_server::pipeline::FetchPolicy::default(),
        topology(),
        drivers,
    );
    let mut state = AppState::new(store.clone(), residencies.local.clone())
        .with_flow(registry)
        .with_auth(AuthConfig {
            admin_token: Some(ADMIN.into()),
            ..AuthConfig::default()
        })
        .with_tiering(topology());
    state.residencies = residencies.clone();
    let engine = state.flow.clone();
    Stack {
        router: build_router(state),
        store,
        engine,
        residencies,
        topology: topology(),
        cold_dir,
        _hot_dir: hot_dir,
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

fn tenant_req(method: &str, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

async fn upload(router: &axum::Router, path: &str, payload: &[u8]) -> String {
    let response = router
        .clone()
        .oneshot(tenant_req(
            "POST",
            "/v1/files",
            Body::from(json!({ "path": path }).to_string()),
        ))
        .await
        .unwrap();
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

async fn get_content(router: &axum::Router, id: &str) -> axum::response::Response {
    router
        .clone()
        .oneshot(tenant_req(
            "GET",
            &format!("/v1/files/{id}/content"),
            Body::empty(),
        ))
        .await
        .unwrap()
}

fn object_path(dir: &tempfile::TempDir, digest: &ContentDigest) -> std::path::PathBuf {
    dir.path().join("objects").join(digest.storage_key())
}

/// Demote the freshly uploaded blob to the archive tier and erase
/// the hot copy, so the archive holds the only bytes.
async fn archive_settled(stack: &Stack, payload: &[u8]) -> ContentDigest {
    let digest = ContentDigest::of_bytes(payload);
    let response = stack
        .router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/tiering",
            Some(json!({ "tier": "frozen", "after_seconds": 0, "basis": "created" })),
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
    assert_eq!(report.demoted, 1, "archive tiers accept demotion");
    let report = move_pass(&stack.store, &stack.residencies, &stack.topology, 100, 0).await;
    assert_eq!(report.hot_erased, 1);
    digest
}

/// Simulate the bucket archiving the object: the address stops
/// answering reads. The bytes come back when the test "restores".
fn freeze(stack: &Stack, digest: &ContentDigest) -> Vec<u8> {
    let path = object_path(&stack.cold_dir, digest);
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    bytes
}

fn thaw(stack: &Stack, digest: &ContentDigest, bytes: &[u8]) {
    let path = object_path(&stack.cold_dir, digest);
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, bytes).unwrap();
}

#[tokio::test]
async fn readable_archive_objects_read_through() {
    let stack = stack().await;
    let payload = b"archived but still warm";
    let id = upload(&stack.router, "warm.txt", payload).await;
    archive_settled(&stack, payload).await;

    // Written before the bucket would have archived it: the GET
    // serves directly, no recall machinery involved -- exactly how
    // S3 treats a not-yet-transitioned or restored object.
    let response = get_content(&stack.router, &id).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], payload);
}

#[tokio::test]
async fn archive_cold_bytes_answer_202_and_the_run_brings_them_home() {
    let stack = stack().await;
    let payload = b"deep frozen bytes";
    let id = upload(&stack.router, "frozen.txt", payload).await;
    let digest = archive_settled(&stack, payload).await;
    let bytes = freeze(&stack, &digest);

    // The GET answers 202 with the run to poll, and Retry-After.
    let response = get_content(&stack.router, &id).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers().get("retry-after").unwrap(), "60");
    let first = json_body(response).await["run"]
        .as_str()
        .unwrap()
        .to_owned();

    // A second GET shares the same durable run: idempotent enqueue.
    let response = get_content(&stack.router, &id).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(json_body(response).await["run"].as_str().unwrap(), first);

    // The run is pollable, tenant-scoped, pending.
    let response = stack
        .router
        .clone()
        .oneshot(tenant_req(
            "GET",
            &format!("/v1/runs/{first}"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["status"], "pending");

    // The backend's restore lands; the worker drains the queue
    // (upload pipeline runs included) and carries the recall through
    // restore-request, the readability poll, and the promote motions.
    thaw(&stack, &digest, &bytes);
    while stack.engine.tick("recall-worker").await.unwrap() {}

    let location = copal_store::repo::blob::get_location(&stack.store, "local", &digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(location.tier, None, "recall promoted the bytes home");

    // The retried GET serves; the recall is invisible after the fact.
    let response = get_content(&stack.router, &id).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], payload);

    // The displaced cold copy erases on the mover's grace, exactly
    // like any promotion.
    let report = move_pass(&stack.store, &stack.residencies, &stack.topology, 100, 0).await;
    assert_eq!(report.cold_erased, 1);
}

#[tokio::test]
async fn a_counted_grant_survives_the_recall() {
    let stack = stack().await;
    let payload = b"granted frozen bytes";
    let id = upload(&stack.router, "granted.txt", payload).await;
    let digest = archive_settled(&stack, payload).await;

    // A single-use grant, issued while the content is archive-cold:
    // issuance never touches bytes, so it succeeds regardless.
    let response = stack
        .router
        .clone()
        .oneshot(tenant_req(
            "POST",
            &format!("/v1/files/{id}/url"),
            Body::from(json!({ "max_uses": 1 }).to_string()),
        ))
        .await
        .unwrap();
    let grant_path = json_body(response).await["url"]
        .as_str()
        .unwrap()
        .to_owned();

    let bytes = freeze(&stack, &digest);

    // Redemption answers 202 WITHOUT consuming the use: no byte was
    // read, and the grant must survive until the recall lands.
    let response = stack
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&grant_path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    thaw(&stack, &digest, &bytes);
    while stack.engine.tick("recall-worker").await.unwrap() {}

    // The same grant now redeems its one use for real.
    let response = stack
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&grant_path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], payload);
}
