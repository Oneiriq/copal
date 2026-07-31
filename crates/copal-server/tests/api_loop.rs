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
use copal_server::app::Limits;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn test_router_with(limits: Limits) -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();
    let mut state = AppState::new(store, blobs);
    state.limits = limits;
    (build_router(state), dir)
}

async fn test_router() -> (axum::Router, tempfile::TempDir) {
    test_router_with(Limits::default()).await
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
    // Streaming body still advertises the exact length up front.
    assert_eq!(
        response.headers()["content-length"],
        payload.len().to_string().as_str(),
    );
    let etag = response.headers()["etag"].to_str().unwrap().to_owned();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);
    assert_eq!(etag, format!("\"{digest}\""));

    // The listing sees exactly one live file, enveloped for pagination.
    let list = req("GET", "/v1/files", Some("acme"), Body::empty());
    let response = router.clone().oneshot(list).await.unwrap();
    let listed = json_body(response).await;
    assert_eq!(listed["items"].as_array().unwrap().len(), 1);
    assert!(listed["next_cursor"].is_null());
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

#[tokio::test]
async fn idempotent_create_replays_with_200_and_the_original() {
    let (router, _dir) = test_router().await;

    let first = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "orig.txt", "idempotency_key": "req-77"}).to_string()),
    );
    let response = router.clone().oneshot(first).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let original = json_body(response).await;

    // The retried request -- even with a different path -- returns the
    // original record with 200, never a 409.
    let replay = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "changed.txt", "idempotency_key": "req-77"}).to_string()),
    );
    let response = router.clone().oneshot(replay).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let replayed = json_body(response).await;
    assert_eq!(replayed["id"], original["id"]);
    assert_eq!(replayed["path"], "orig.txt");
}

#[tokio::test]
async fn oversized_uploads_are_413_and_retryable() {
    let (router, _dir) = test_router_with(Limits {
        max_upload_bytes: 16,
        upload_lease_secs: 900,
    })
    .await;

    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "big.bin"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(vec![0u8; 64]),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // The record landed in failed -- retryable, and a within-limit
    // retry succeeds end to end.
    let meta = req(
        "GET",
        &format!("/v1/files/{id}"),
        Some("acme"),
        Body::empty(),
    );
    let response = router.clone().oneshot(meta).await.unwrap();
    assert_eq!(json_body(response).await["state"], "failed");

    let retry = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(&b"small"[..]),
    );
    let response = router.clone().oneshot(retry).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["state"], "ready");
}

/// Full grant lifecycle: issue, redeem without any tenant header,
/// enforce the use limit, and refuse after revocation. Every refusal
/// is the same 404 — a signed URL is not an oracle.
#[tokio::test]
async fn grant_urls_serve_share_limit_and_revoke() {
    let (router, _dir) = test_router().await;
    let payload = b"shared via link";

    // Create + upload a servable file.
    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "share.bin"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(&payload[..]),
    );
    assert_eq!(
        router.clone().oneshot(upload).await.unwrap().status(),
        StatusCode::OK
    );

    // A draft file refuses issuance; a ready one issues.
    let issue = req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Some("acme"),
        Body::from(json!({"ttl_secs": 900, "max_uses": 2}).to_string()),
    );
    let response = router.clone().oneshot(issue).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let grant = json_body(response).await;
    let url = grant["url"].as_str().unwrap().to_owned();
    let grant_id = grant["grant_id"].as_str().unwrap().to_owned();
    assert!(grant["expires_at"].as_str().is_some());

    // Redemption needs NO tenant header and streams the bytes.
    let redeem = req("GET", &url, None, Body::empty());
    let response = router.clone().oneshot(redeem).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);

    // Tampered secret: same grant id, flipped final hex digit -> 404.
    let mut tampered = url.clone();
    let last = tampered.pop().unwrap();
    tampered.push(if last == '0' { '1' } else { '0' });
    let response = router
        .clone()
        .oneshot(req("GET", &tampered, None, Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Second legitimate use consumes the limit...
    let response = router
        .clone()
        .oneshot(req("GET", &url, None, Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // ...and the third is refused: max_uses = 2.
    let response = router
        .clone()
        .oneshot(req("GET", &url, None, Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // A fresh unlimited grant works until revoked, then 404s.
    let issue = req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Some("acme"),
        Body::from(json!({}).to_string()),
    );
    let fresh = json_body(router.clone().oneshot(issue).await.unwrap()).await;
    let fresh_url = fresh["url"].as_str().unwrap().to_owned();
    let fresh_id = fresh["grant_id"].as_str().unwrap().to_owned();
    assert_eq!(
        router
            .clone()
            .oneshot(req("GET", &fresh_url, None, Body::empty()))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let revoke = req(
        "DELETE",
        &format!("/v1/grants/{fresh_id}"),
        Some("acme"),
        Body::empty(),
    );
    assert_eq!(
        router.clone().oneshot(revoke).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        router
            .clone()
            .oneshot(req("GET", &fresh_url, None, Body::empty()))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    // Revocation is tenant-guarded: a foreign tenant cannot revoke.
    let foreign_revoke = req(
        "DELETE",
        &format!("/v1/grants/{grant_id}"),
        Some("globex"),
        Body::empty(),
    );
    assert_eq!(
        router
            .clone()
            .oneshot(foreign_revoke)
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

/// Expiry and issuance validation edges.
#[tokio::test]
async fn grant_expiry_and_validation() {
    let (router, _dir) = test_router().await;

    let create = req(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": "exp.bin"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    // No URL for a non-servable file.
    let premature = req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Some("acme"),
        Body::from(json!({}).to_string()),
    );
    assert_eq!(
        router.clone().oneshot(premature).await.unwrap().status(),
        StatusCode::CONFLICT
    );

    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(&b"x"[..]),
    );
    router.clone().oneshot(upload).await.unwrap();

    // ttl_secs = 0 is rejected at issuance (a link that can never be
    // redeemed is a caller bug, not a product feature).
    let zero = req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Some("acme"),
        Body::from(json!({"ttl_secs": 0}).to_string()),
    );
    assert_eq!(
        router.clone().oneshot(zero).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );

    // Garbage tokens 404 without touching the store.
    let response = router
        .clone()
        .oneshot(req("GET", "/v1/grants/not-a-token", None, Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Keyset pagination walks the full set without overlap or misses.
#[tokio::test]
async fn pagination_pages_are_disjoint_and_complete() {
    let (router, _dir) = test_router().await;
    for n in 0..5 {
        let create = req(
            "POST",
            "/v1/files",
            Some("acme"),
            Body::from(json!({ "path": format!("p/{n}.txt") }).to_string()),
        );
        assert_eq!(
            router.clone().oneshot(create).await.unwrap().status(),
            StatusCode::CREATED
        );
    }

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let uri = match &cursor {
            Some(c) => format!("/v1/files?limit=2&cursor={c}"),
            None => "/v1/files?limit=2".to_owned(),
        };
        let page = json_body(
            router
                .clone()
                .oneshot(req("GET", &uri, Some("acme"), Body::empty()))
                .await
                .unwrap(),
        )
        .await;
        let items = page["items"].as_array().unwrap().clone();
        assert!(items.len() <= 2);
        for item in &items {
            seen.push(item["id"].as_str().unwrap().to_owned());
        }
        match page["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    // All five, exactly once, newest first.
    assert_eq!(seen.len(), 5, "no misses: {seen:?}");
    let unique: std::collections::BTreeSet<_> = seen.iter().collect();
    assert_eq!(unique.len(), 5, "no overlaps: {seen:?}");

    // A garbage cursor is a 400, not a scan.
    let response = router
        .clone()
        .oneshot(req(
            "GET",
            "/v1/files?cursor=zzzz",
            Some("acme"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
