//! The standard post-upload pipeline, end to end: upload lands in
//! scanning, the worker runs sniff -> policy -> finalize over the
//! journal, and the file comes out ready (annotated) or quarantined
//! (refusing content and grants).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::ExtensionPolicy;
use copal_flow::FlowEngine;
use copal_server::pipeline::standard_registry;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn pipelined_stack_with(
    enforce_type_match: bool,
) -> (axum::Router, FlowEngine, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let registry = standard_registry(
        store.clone(),
        copal_server::app::Residencies::local_only(blobs.clone()),
        ExtensionPolicy::standard(),
        enforce_type_match,
        None,
        None,
        None,
        std::collections::HashMap::new(),
    );
    let state = AppState::new(store, blobs).with_flow(registry);
    let engine = state.flow.clone();
    (build_router(state), engine, dir)
}

async fn pipelined_stack() -> (axum::Router, FlowEngine, tempfile::TempDir) {
    pipelined_stack_with(false).await
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

async fn create_and_upload(
    router: &axum::Router,
    path: &str,
    declared: &str,
    payload: &[u8],
) -> (String, Value) {
    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": path, "content_type": declared}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(payload.to_vec()),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let record = json_body(response).await;
    (id, record)
}

#[tokio::test]
async fn clean_upload_scans_then_serves_with_annotations() {
    let (router, engine, _dir) = pipelined_stack().await;

    let (id, record) =
        create_and_upload(&router, "plans/a.pdf", "application/pdf", b"%PDF-1.7 body").await;
    // With a pipeline configured, completion lands in scanning...
    assert_eq!(record["state"], "scanning");

    // ...and content refuses until the pipeline finishes (no digest
    // yet? no -- digest exists; scanning is not quarantined, so content
    // SERVES during scan per the digest-based rule).
    let during = req(
        "GET",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::empty(),
    );
    assert_eq!(
        router.clone().oneshot(during).await.unwrap().status(),
        StatusCode::OK
    );

    // Worker tick runs sniff -> policy -> finalize.
    assert!(engine.tick("test-worker").await.unwrap());

    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "ready");
    let processing = &record["metadata"]["processing"];
    assert_eq!(processing["sniffed_type"], "application/pdf");
    assert_eq!(processing["type_matches"], true);
    assert_eq!(processing["verdict"], "clean");
}

#[tokio::test]
async fn blocked_extension_quarantines_and_refuses_everything() {
    let (router, engine, _dir) = pipelined_stack().await;

    let (id, record) = create_and_upload(
        &router,
        "tools/installer.exe",
        "application/octet-stream",
        b"MZ\x90\x00fake binary",
    )
    .await;
    assert_eq!(record["state"], "scanning");
    assert!(engine.tick("test-worker").await.unwrap());

    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "quarantined");
    assert_eq!(
        record["metadata"]["processing"]["verdict_reason"],
        "blocked extension .exe"
    );
    // The sniffer also saw through the declared type.
    assert_eq!(
        record["metadata"]["processing"]["sniffed_type"],
        "application/x-msdownload"
    );

    // Quarantine refuses content...
    let content = req(
        "GET",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::empty(),
    );
    assert_eq!(
        router.clone().oneshot(content).await.unwrap().status(),
        StatusCode::CONFLICT
    );
    // ...and grant issuance.
    let issue = req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Some("acme"),
        Body::from(json!({}).to_string()),
    );
    assert_eq!(
        router.clone().oneshot(issue).await.unwrap().status(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn duplicate_content_reprocessing_dedupes_by_run_key() {
    let (router, engine, _dir) = pipelined_stack().await;

    // Upload, process to ready.
    let (id, _) = create_and_upload(&router, "again.txt", "text/plain", b"hello").await;
    assert!(engine.tick("w").await.unwrap());

    // Re-upload the SAME bytes: same digest, same deterministic run
    // key -- the enqueue dedupes, upload still succeeds, and one more
    // tick finalizes the (already-journaled) run without a second
    // processing pass.
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(&b"hello"[..]),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let record = json_body(response).await;
    // The dedupe hit resolves IN the upload: the completed run's
    // verdict stands (same file, same path, same bytes), so the fresh
    // scan finalizes to ready immediately: no worker involved, no
    // stranded scanning state.
    assert_eq!(record["state"], "ready");
    assert_eq!(record["metadata"]["processing"]["verdict"], "clean");
    assert!(!engine.tick("w").await.unwrap(), "no second run enqueued");
}

#[tokio::test]
async fn enforcement_quarantines_declared_type_lies() {
    // Annotate-only by default: the lie is recorded, the file serves.
    let (router, engine, _dir) = pipelined_stack().await;
    let (id, _) = create_and_upload(&router, "notes.txt", "text/plain", b"%PDF-1.7 not text").await;
    assert!(engine.tick("w").await.unwrap());
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "ready");
    assert_eq!(record["metadata"]["processing"]["type_matches"], false);

    // Enforcing: the same lie becomes a blocking verdict.
    let (router, engine, _dir) = pipelined_stack_with(true).await;
    let (id, _) = create_and_upload(&router, "notes.txt", "text/plain", b"%PDF-1.7 not text").await;
    assert!(engine.tick("w").await.unwrap());
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "quarantined");
    assert_eq!(
        record["metadata"]["processing"]["verdict_reason"],
        "declared text/plain but content is application/pdf",
    );
}

/// The stranded-scanning slice, fully real: a pipeline that fails
/// because the bytes are unreadable propagates failure to the file,
/// the failure is visible in the runs listing, and the retry route
/// recovers everything once the bytes are back.
#[tokio::test]
async fn failed_pipeline_propagates_and_retry_recovers() {
    let (router, engine, dir) = pipelined_stack().await;

    let (id, record) = create_and_upload(&router, "fragile.bin", "text/plain", b"payload").await;
    assert_eq!(record["state"], "scanning");
    let digest = record["digest"].as_str().unwrap().to_owned();

    // Sabotage: remove the blob object, so sniff_type cannot read it.
    let object = dir
        .path()
        .join("objects")
        .join(&digest[0..2])
        .join(&digest[2..4])
        .join(&digest);
    std::fs::remove_file(&object).expect("blob object exists");

    // The worker claims, every sniff attempt fails, the run fails,
    // and the failure PROPAGATES: the file leaves scanning for failed.
    assert!(engine.tick("w").await.unwrap());
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "failed", "propagation moved the file");

    // The failure is visible in the runs listing.
    let list = req("GET", "/v1/runs?status=failed", Some("acme"), Body::empty());
    let body = json_body(router.clone().oneshot(list).await.unwrap()).await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["workflow"], "post_upload");
    assert!(items[0]["error"].as_str().unwrap().contains("sniff_type"));
    let run_id = items[0]["id"].as_str().unwrap().to_owned();

    // Restore the bytes (content-addressed: same content, same path).
    std::fs::write(&object, b"payload").unwrap();

    // Retry: 202, the file returns to scanning, the run to pending.
    let retry = req(
        "POST",
        &format!("/v1/runs/{run_id}/retry"),
        Some("acme"),
        Body::from(""),
    );
    let response = router.clone().oneshot(retry).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "scanning");

    // A second retry while pending refuses; only failed runs retry.
    let retry = req(
        "POST",
        &format!("/v1/runs/{run_id}/retry"),
        Some("acme"),
        Body::from(""),
    );
    assert_eq!(
        router.clone().oneshot(retry).await.unwrap().status(),
        StatusCode::CONFLICT
    );

    // The worker finishes the job this time.
    assert!(engine.tick("w").await.unwrap());
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "ready");
    assert_eq!(record["metadata"]["processing"]["verdict"], "clean");
    let run = req(
        "GET",
        &format!("/v1/runs/{run_id}"),
        Some("acme"),
        Body::empty(),
    );
    let body = json_body(router.clone().oneshot(run).await.unwrap()).await;
    assert_eq!(body["status"], "completed");
}
