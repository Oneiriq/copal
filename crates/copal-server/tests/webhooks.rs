//! Eventing end to end: the engine outbox, webhook registration
//! custody, signed delivery, and backoff retry against a live local
//! receiver.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tower::ServiceExt as _;

use copal_blob::crypto::BlobCipher;
use copal_blob::ObjectStore;
use copal_server::app::{build_router, AppState};
use copal_server::webhooks::{run_pass, sign_body, webhook_router};
use copal_store::repo::eventing;
use copal_store::{Store, StoreConfig};

const MASTER_KEY: &str = "9c1f2e3d4c5b6a7980f1e2d3c4b5a6971827364554637281910203040506070a";

async fn stack() -> (
    axum::Router,
    axum::Router,
    Store,
    BlobCipher,
    tempfile::TempDir,
) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let mut state = AppState::new(store.clone(), blobs)
        .with_cipher(Some(BlobCipher::from_hex(MASTER_KEY).unwrap()));
    // The fixture's receiver is a loopback listener, which the
    // outbound guard refuses by default; this is the same opt-in a
    // deployment with internal receivers uses.
    state.limits.allow_private_webhook_targets = true;
    let cipher = BlobCipher::from_hex(MASTER_KEY).unwrap();
    let api = build_router(state.clone());
    let hooks = webhook_router(state);
    (api, hooks, store, cipher, dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// Create a file and upload content through the REST face; the ready
/// transition is what writes the outbox row.
async fn upload(api: &axum::Router, path: &str, content: &[u8]) -> String {
    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "path": path, "content_type": "text/plain" }).to_string(),
        ))
        .unwrap();
    let response = api.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .body(Body::from(content.to_vec()))
        .unwrap();
    let response = api.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

/// A live local receiver: captures every request, answers with the
/// switchable status.
async fn receiver(
    status: Arc<AtomicU16>,
) -> (String, mpsc::UnboundedReceiver<(HeaderMap, Vec<u8>)>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/hook",
        post(move |headers: HeaderMap, body: Bytes| {
            let tx = tx.clone();
            let status = status.clone();
            async move {
                tx.send((headers, body.to_vec())).ok();
                StatusCode::from_u16(status.load(Ordering::Relaxed)).unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}/hook"), rx)
}

fn register_req(url: &str, events: &[&str]) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/webhooks")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "url": url, "events": events }).to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn private_targets_refuse_without_the_opt_in() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    // Default limits: the guard is armed.
    let cipher = BlobCipher::from_hex(MASTER_KEY).unwrap();
    let state = AppState::new(store, blobs).with_cipher(Some(cipher));
    let hooks = webhook_router(state);

    for url in [
        "http://127.0.0.1:9000/hook",
        "http://localhost/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://10.0.0.5/hook",
        "ftp://example.com/hook",
    ] {
        let response = hooks.clone().oneshot(register_req(url, &[])).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{url} must refuse",
        );
    }
}

#[tokio::test]
async fn engine_outbox_records_terminal_transitions() {
    let (api, hooks, _store, _cipher, _dir) = stack().await;
    let id = upload(&api, "outbox.txt", b"observable bytes").await;

    let remove = Request::builder()
        .method("DELETE")
        .uri(format!("/v1/files/{id}"))
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let response = api.clone().oneshot(remove).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let list = Request::builder()
        .method("GET")
        .uri("/v1/events")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let body = json_body(hooks.clone().oneshot(list).await.unwrap()).await;
    let items = body["items"].as_array().unwrap();
    let actions: Vec<&str> = items
        .iter()
        .map(|item| item["event"].as_str().unwrap())
        .collect();
    assert_eq!(actions, ["file.deleted", "file.ready"], "newest first");
    let ready = &items[1];
    assert_eq!(ready["file"].as_str().unwrap(), id);
    assert_eq!(ready["payload"]["path"], "outbox.txt");
    assert!(ready["payload"]["digest"].is_string());
}

#[tokio::test]
async fn deliveries_sign_and_settle() {
    let (api, hooks, store, cipher, _dir) = stack().await;
    let status = Arc::new(AtomicU16::new(200));
    let (url, mut inbox) = receiver(status.clone()).await;

    let response = hooks
        .clone()
        .oneshot(register_req(&url, &[]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let registered = json_body(response).await;
    let secret = registered["secret"].as_str().unwrap().to_owned();

    // The listing shows the endpoint without the secret.
    let list = Request::builder()
        .method("GET")
        .uri("/v1/webhooks")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let body = json_body(hooks.clone().oneshot(list).await.unwrap()).await;
    assert!(body["items"][0].get("secret").is_none());
    assert!(body["items"][0].get("secret_sealed").is_none());

    upload(&api, "delivered.txt", b"signed payload").await;

    let http = reqwest::Client::new();
    let report = run_pass(&store, &cipher, &http, "test-instance", true).await;
    assert_eq!(report.events_dispatched, 1);
    assert_eq!(report.delivered, 1);

    let (headers, body) = inbox.recv().await.expect("delivery arrived");
    assert_eq!(headers["x-copal-event"], "file.ready");
    assert_eq!(
        headers["x-copal-signature"].to_str().unwrap(),
        sign_body(&secret, &body),
    );
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["event"], "file.ready");
    assert_eq!(payload["tenant"], "acme");
    assert_eq!(payload["payload"]["path"], "delivered.txt");

    // Settled: the delivery reads delivered, the pass goes idle.
    let deliveries = eventing::list_deliveries(
        &store,
        &copal_core::TenantId::parse("acme").unwrap(),
        None,
        None,
        10,
    )
    .await
    .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].state, "delivered");
    assert_eq!(deliveries[0].last_status, Some(200));
    assert!(run_pass(&store, &cipher, &http, "test-instance", true)
        .await
        .idle());
}

#[tokio::test]
async fn failures_back_off_and_filters_hold() {
    let (api, hooks, store, cipher, _dir) = stack().await;
    let status = Arc::new(AtomicU16::new(500));
    let (url, mut inbox) = receiver(status.clone()).await;

    // One endpoint takes everything; one wants only deletions.
    let response = hooks
        .clone()
        .oneshot(register_req(&url, &[]))
        .await
        .unwrap();
    let secret = json_body(response).await["secret"]
        .as_str()
        .unwrap()
        .to_owned();
    let response = hooks
        .clone()
        .oneshot(register_req(&url, &["file.deleted"]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    upload(&api, "retry.txt", b"eventually delivered").await;

    let http = reqwest::Client::new();
    let report = run_pass(&store, &cipher, &http, "test-instance", true).await;
    // The filtered endpoint got no delivery; the other failed with 500.
    assert_eq!(report.retried, 1);
    assert_eq!(report.delivered, 0);
    inbox.recv().await.expect("first attempt arrived");

    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let deliveries = eventing::list_deliveries(&store, &tenant, None, None, 10)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1, "filter held");
    assert_eq!(deliveries[0].state, "pending");
    assert_eq!(deliveries[0].attempts, 1);
    assert!(deliveries[0].next_attempt_at.is_some(), "backoff scheduled");

    // Not due yet: the pass finds nothing.
    assert!(run_pass(&store, &cipher, &http, "test-instance", true)
        .await
        .idle());

    // Past the backoff, a healthy receiver settles it.
    status.store(200, Ordering::Relaxed);
    eventing::force_due_for_test(&store, &deliveries[0].delivery_id())
        .await
        .unwrap();
    let report = run_pass(&store, &cipher, &http, "test-instance", true).await;
    assert_eq!(report.delivered, 1);
    let (headers, body) = inbox.recv().await.expect("retry arrived");
    assert_eq!(
        headers["x-copal-signature"].to_str().unwrap(),
        sign_body(&secret, &body),
    );
    let deliveries = eventing::list_deliveries(&store, &tenant, None, None, 10)
        .await
        .unwrap();
    assert_eq!(deliveries[0].state, "delivered");

    // Deactivation is once; the second delete reads 404.
    let list = Request::builder()
        .method("GET")
        .uri("/v1/webhooks")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let body = json_body(hooks.clone().oneshot(list).await.unwrap()).await;
    let first_id = body["items"][0]["id"].as_str().unwrap().to_owned();
    for expected in [StatusCode::NO_CONTENT, StatusCode::NOT_FOUND] {
        let remove = Request::builder()
            .method("DELETE")
            .uri(format!("/v1/webhooks/{first_id}"))
            .header("x-copal-tenant", "acme")
            .body(Body::empty())
            .unwrap();
        let response = hooks.clone().oneshot(remove).await.unwrap();
        assert_eq!(response.status(), expected);
    }
}
