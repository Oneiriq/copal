//! The scan gate on every face that serves bytes: with a scanner
//! configured, content no scan has cleared is withheld on the S3
//! gateway, on edge and grant redemption, and on version downloads,
//! exactly as it is on `/content`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tower::ServiceExt as _;

use copal_blob::crypto::BlobCipher;
use copal_blob::ObjectStore;
use copal_core::{ExtensionPolicy, FileId, FileState, TenantId};
use copal_flow::FlowEngine;
use copal_server::app::{build_router, AppState, Residencies};
use copal_server::auth::AuthConfig;
use copal_server::edge::{edge_admin_router, edge_router};
use copal_server::pipeline::standard_registry;
use copal_server::s3::{s3_admin_router, s3_router};
use copal_store::{Store, StoreConfig};

const MASTER_KEY: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f00112233445566778899aabbccddeeff0";

/// A clamd that clears every stream it reads.
async fn clean_clamd() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut command = [0u8; 10];
                if socket.read_exact(&mut command).await.is_err() {
                    return;
                }
                loop {
                    let mut len = [0u8; 4];
                    if socket.read_exact(&mut len).await.is_err() {
                        return;
                    }
                    let len = u32::from_be_bytes(len) as usize;
                    if len == 0 {
                        break;
                    }
                    let mut chunk = vec![0u8; len];
                    if socket.read_exact(&mut chunk).await.is_err() {
                        return;
                    }
                }
                let _ = socket.write_all(b"stream: OK\0").await;
                let _ = socket.shutdown().await;
            });
        }
    });
    addr
}

struct Stack {
    router: axum::Router,
    gateway: axum::Router,
    s3_admin: axum::Router,
    engine: FlowEngine,
    store: Store,
    _dir: tempfile::TempDir,
}

async fn stack() -> Stack {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let registry = standard_registry(
        store.clone(),
        Residencies::local_only(blobs.clone()),
        ExtensionPolicy::standard(),
        false,
        Some(clean_clamd().await),
        None,
        None,
        std::collections::HashMap::new(),
        copal_server::pipeline::FetchPolicy::default(),
        copal_server::tiering::Topology::default(),
        Default::default(),
    );
    let state = AppState::new(store.clone(), blobs)
        .with_flow(registry)
        .with_scan_gate(true)
        .with_auth(AuthConfig {
            admin_token: Some("root".to_owned()),
            ..AuthConfig::default()
        })
        .with_cipher(Some(BlobCipher::from_hex(MASTER_KEY).unwrap()));
    let engine = state.flow.clone();
    let router = build_router(state.clone())
        .merge(edge_router(state.clone()))
        .merge(edge_admin_router(state.clone()));
    Stack {
        router,
        gateway: s3_router(state.clone()),
        s3_admin: s3_admin_router(state),
        engine,
        store,
        _dir: dir,
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn bytes_of(response: axum::response::Response) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
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

fn anonymous(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

async fn create(router: &axum::Router, path: &str) -> String {
    let request = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/plain" }).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await["id"].as_str().unwrap().to_owned()
}

async fn put_content(router: &axum::Router, id: &str, content: &[u8]) {
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header("content-length", content.len().to_string())
        .body(Body::from(content.to_vec()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Run every queued scan to completion.
async fn drain(engine: &FlowEngine) {
    while engine.tick("w").await.unwrap() {}
}

#[tokio::test]
async fn grant_redemption_waits_for_the_scan_of_newer_content() {
    let stack = stack().await;
    let id = create(&stack.router, "gate/granted.txt").await;
    put_content(&stack.router, &id, b"first cleared bytes").await;
    drain(&stack.engine).await;

    let issue = req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Body::from(json!({ "ttl_secs": 300 }).to_string()),
    );
    let response = stack.router.clone().oneshot(issue).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let url = json_body(response).await["url"]
        .as_str()
        .unwrap()
        .to_owned();
    let response = stack.router.clone().oneshot(anonymous(&url)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // New bytes replace the cleared ones. The grant outlives them, but
    // it must not serve them before their own scan.
    put_content(&stack.router, &id, b"second unscanned bytes").await;
    let response = stack.router.clone().oneshot(anonymous(&url)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    drain(&stack.engine).await;
    let response = stack.router.clone().oneshot(anonymous(&url)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(bytes_of(response).await, b"second unscanned bytes");
}

#[tokio::test]
async fn edge_tokens_wait_for_the_scan_and_are_not_issued_before_it() {
    let stack = stack().await;
    let mint = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/edge-keys")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = stack.router.clone().oneshot(mint).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let id = create(&stack.router, "gate/edge.txt").await;
    put_content(&stack.router, &id, b"edge bytes, not yet scanned").await;
    let issue = || {
        req(
            "POST",
            &format!("/v1/files/{id}/edge-url"),
            Body::from(json!({ "ttl_secs": 60 }).to_string()),
        )
    };
    let response = stack.router.clone().oneshot(issue()).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "no token for content still awaiting its scan",
    );

    drain(&stack.engine).await;
    let response = stack.router.clone().oneshot(issue()).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let url = json_body(response).await["url"]
        .as_str()
        .unwrap()
        .to_owned();
    let response = stack.router.clone().oneshot(anonymous(&url)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    put_content(&stack.router, &id, b"replacement awaiting its scan").await;
    let response = stack.router.clone().oneshot(anonymous(&url)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    drain(&stack.engine).await;
    let response = stack.router.clone().oneshot(anonymous(&url)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(bytes_of(response).await, b"replacement awaiting its scan");
}

#[tokio::test]
async fn version_downloads_serve_only_cleared_versions() {
    let stack = stack().await;
    let id = create(&stack.router, "gate/versions.txt").await;
    let version = |n: u32| {
        req(
            "GET",
            &format!("/v1/files/{id}/versions/{n}/content"),
            Body::empty(),
        )
    };

    put_content(&stack.router, &id, b"version one").await;
    let response = stack.router.clone().oneshot(version(1)).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    drain(&stack.engine).await;
    let response = stack.router.clone().oneshot(version(1)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Version two lands unscanned. Version one stays servable, since
    // its own scan cleared it.
    put_content(&stack.router, &id, b"version two").await;
    let response = stack.router.clone().oneshot(version(2)).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = stack.router.clone().oneshot(version(1)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(bytes_of(response).await, b"version one");

    // Version two's processing fails before any scan, and version
    // three replaces it. Version two is history now, and still
    // unscanned, so it stays withheld.
    let tenant = TenantId::parse("acme").unwrap();
    let file = FileId::parse(&id).unwrap();
    copal_store::repo::file::transition(
        &stack.store,
        &tenant,
        &file,
        FileState::Scanning,
        FileState::Failed,
        Default::default(),
    )
    .await
    .unwrap();
    put_content(&stack.router, &id, b"version three").await;
    let response = stack.router.clone().oneshot(version(2)).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = stack.router.clone().oneshot(version(1)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compact UTC timestamp (YYYYMMDDTHHMMSSZ) for `x-amz-date`.
fn amz_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year_base = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year_base + 1 } else { year_base };
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        tod / 3_600,
        tod % 3_600 / 60,
        tod % 60,
    )
}

/// A SigV4-signed GET with an unsigned payload.
fn signed_get(path: &str, access_key_id: &str, secret: &str) -> Request<Body> {
    let amz_date = amz_now();
    let date = &amz_date[..8];
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let payload_hash = "UNSIGNED-PAYLOAD";
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "GET\n{path}\n\nhost:localhost\nx-amz-content-sha256:{payload_hash}\n\
         x-amz-date:{amz_date}\n\n{signed_headers}\n{payload_hash}",
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    );
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, b"us-east-1");
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    Request::builder()
        .method("GET")
        .uri(path)
        .header("host", "localhost")
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header(
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, \
                 SignedHeaders={signed_headers}, Signature={signature}",
            ),
        )
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn the_s3_gateway_withholds_unscanned_objects() {
    let stack = stack().await;
    let mint = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/s3-credentials")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = stack.s3_admin.clone().oneshot(mint).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let credential = json_body(response).await;
    let access_key = credential["access_key_id"].as_str().unwrap().to_owned();
    let secret = credential["secret_access_key"].as_str().unwrap().to_owned();

    let id = create(&stack.router, "gate/object.txt").await;
    put_content(&stack.router, &id, b"object bytes").await;
    let get = || signed_get("/acme/gate/object.txt", &access_key, &secret);
    let response = stack.gateway.clone().oneshot(get()).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    drain(&stack.engine).await;
    let response = stack.gateway.clone().oneshot(get()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(bytes_of(response).await, b"object bytes");
}
