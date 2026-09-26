//! cg2 edge tokens end to end: key custody, issuance, anonymous
//! redemption, and the uniform refusals.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::crypto::BlobCipher;
use copal_blob::ObjectStore;
use copal_server::app::{build_router, AppState};
use copal_server::auth::AuthConfig;
use copal_server::edge::{edge_admin_router, edge_router};
use copal_store::{Store, StoreConfig};

const MASTER_KEY: &str = "77661f2e3d4c5b6a980f1e2d3c4b5a69788796a5b4c3d2e1f001122334455667";

async fn stack() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let cipher = BlobCipher::from_hex(MASTER_KEY).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            admin_token: Some("root".to_owned()),
            ..AuthConfig::default()
        })
        .with_cipher(Some(cipher));
    let router = build_router(state.clone())
        .merge(edge_router(state.clone()))
        .merge(edge_admin_router(state));
    (router, dir)
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

async fn upload(router: &axum::Router, path: &str, content: &[u8]) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/plain" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let mut put = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(content.to_vec()),
    );
    put.headers_mut().remove("content-type");
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

async fn mint_key(router: &axum::Router) -> (String, String) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/edge-keys")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = json_body(response).await;
    (
        body["key_id"].as_str().unwrap().to_owned(),
        body["secret"].as_str().unwrap().to_owned(),
    )
}

#[tokio::test]
async fn edge_tokens_issue_redeem_and_refuse_uniformly() {
    let (router, _dir) = stack().await;
    let payload = b"edge cached bytes";
    let id = upload(&router, "cdn/hero.txt", payload).await;

    // Issuance without a key names the fix.
    let issue = req(
        "POST",
        &format!("/v1/files/{id}/edge-url"),
        Body::from(json!({}).to_string()),
    );
    let response = router.clone().oneshot(issue).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let (key_id, secret) = mint_key(&router).await;

    // The listing shows the key without its secret.
    let list = Request::builder()
        .method("GET")
        .uri("/v1/admin/tenants/acme/edge-keys")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let body = json_body(router.clone().oneshot(list).await.unwrap()).await;
    assert!(body["items"][0].get("secret").is_none());
    assert!(body["items"][0].get("secret_sealed").is_none());

    // A ttl past the ceiling refuses.
    let issue = req(
        "POST",
        &format!("/v1/files/{id}/edge-url"),
        Body::from(json!({ "ttl_secs": 999_999 }).to_string()),
    );
    let response = router.clone().oneshot(issue).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Issue and redeem anonymously: no tenant header on the way back.
    let issue = req(
        "POST",
        &format!("/v1/files/{id}/edge-url"),
        Body::from(json!({ "ttl_secs": 60 }).to_string()),
    );
    let response = router.clone().oneshot(issue).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let issued = json_body(response).await;
    let url = issued["url"].as_str().unwrap().to_owned();
    let token = issued["token"].as_str().unwrap().to_owned();
    assert!(token.starts_with("cg2."));

    let redeem = Request::builder()
        .method("GET")
        .uri(&url)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(redeem).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);

    // A tampered signature refuses with the uniform 404.
    let mut forged = token.clone();
    forged.pop();
    let redeem = Request::builder()
        .method("GET")
        .uri(format!("/v1/edge/{forged}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(redeem).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // An expired token refuses: signed with the REAL secret, expiry in
    // the past, so only the clock gate can be the refusal.
    let expired = copal_sign::EdgeToken::sign(
        &copal_sign::EdgeClaims {
            key: key_id.clone(),
            tenant: "acme".to_owned(),
            file: id.clone(),
            exp: 1,
        },
        &secret,
    )
    .unwrap();
    let redeem = Request::builder()
        .method("GET")
        .uri(format!("/v1/edge/{expired}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(redeem).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Revoking the key ends the live token too.
    let revoke = Request::builder()
        .method("DELETE")
        .uri(format!("/v1/admin/tenants/acme/edge-keys/{key_id}"))
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(revoke).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let redeem = Request::builder()
        .method("GET")
        .uri(&url)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(redeem).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// A request that names no TTL gets the one the contract declares, on
/// the REST face and through the contract dispatcher alike.
#[tokio::test]
async fn every_face_defaults_to_the_declared_ttl() {
    let (router, _dir) = stack().await;
    let id = upload(&router, "cdn/default-ttl.txt", b"default ttl").await;
    mint_key(&router).await;
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    };

    let issue = req(
        "POST",
        &format!("/v1/files/{id}/edge-url"),
        Body::from(json!({}).to_string()),
    );
    let response = router.clone().oneshot(issue).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let rest_ttl = json_body(response).await["expires_at"].as_i64().unwrap() - now();

    let call = req(
        "POST",
        "/mcp",
        Body::from(
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "file_issue_edge_url", "arguments": { "id": id } },
            })
            .to_string(),
        ),
    );
    let body = json_body(router.clone().oneshot(call).await.unwrap()).await;
    let text = body["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("{body:#?}"));
    let issued: Value = serde_json::from_str(text).unwrap();
    let mcp_ttl = issued["expires_at"].as_i64().unwrap() - now();

    for ttl in [rest_ttl, mcp_ttl] {
        assert!(
            (895..=900).contains(&ttl),
            "declared default is 900, got {ttl}"
        );
    }
}
