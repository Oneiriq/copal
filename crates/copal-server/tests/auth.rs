//! API-key authentication, end to end over both faces.
//!
//! Admin mints a key (guarded by the operator token), the bearer works
//! on REST and GraphQL alike, revocation kills it, and every failure
//! (absent, malformed, wrong-secret, revoked) is the same uniform 401.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-secret";

async fn keyed_router() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
    });
    (build_router(state), dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn request(
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    admin: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(token) = admin {
        builder = builder.header("x-copal-admin-token", token);
    }
    match body {
        Some(value) => builder
            .header("content-type", "application/json")
            .body(Body::from(value.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

async fn mint(router: &axum::Router, tenant: &str, name: &str) -> (String, String) {
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/admin/tenants/{tenant}/keys"),
            None,
            Some(ADMIN),
            Some(json!({ "name": name })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = json_body(response).await;
    (
        body["key_id"].as_str().unwrap().to_owned(),
        body["token"].as_str().unwrap().to_owned(),
    )
}

#[tokio::test]
async fn keys_gate_both_faces_and_the_tenant_comes_from_the_key() {
    let (router, _dir) = keyed_router().await;

    // Unauthenticated REST refuses uniformly.
    let response = router
        .clone()
        .oneshot(request("GET", "/v1/files", None, None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Mint and use: the key IS the tenant identity.
    let (_, token) = mint(&router, "acme", "ci").await;
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/files",
            Some(&token),
            None,
            Some(json!({ "path": "a.txt", "content_type": "text/plain" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // The x-copal-tenant header is IGNORED in keys mode: another
    // tenant's key sees nothing, whatever headers claim.
    let (_, rival) = mint(&router, "rival", "spy").await;
    let mut spoofed = request("GET", "/v1/files", Some(&rival), None, None);
    spoofed
        .headers_mut()
        .insert("x-copal-tenant", "acme".parse().unwrap());
    let response = router.clone().oneshot(spoofed).await.unwrap();
    let body = json_body(response).await;
    assert_eq!(body["items"].as_array().unwrap().len(), 0);

    // GraphQL authenticates through the same path.
    let gql = |token: Option<&str>| {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/graphql")
            .header("content-type", "application/json");
        if let Some(t) = token {
            builder = builder.header("authorization", format!("Bearer {t}"));
        }
        builder
            .body(Body::from(
                json!({ "query": "{ files { items { path } } }" }).to_string(),
            ))
            .unwrap()
    };
    let body = json_body(router.clone().oneshot(gql(Some(&token))).await.unwrap()).await;
    assert!(body["errors"].is_null(), "{body}");
    assert_eq!(body["data"]["files"]["items"][0]["path"], "a.txt");
    let body = json_body(router.clone().oneshot(gql(None)).await.unwrap()).await;
    assert_eq!(body["errors"][0]["extensions"]["code"], "unauthorized");
}

#[tokio::test]
async fn revocation_and_garbage_are_the_same_refusal() {
    let (router, _dir) = keyed_router().await;
    let (key_id, token) = mint(&router, "acme", "doomed").await;

    // Wrong secret, right id: flip a hex digit.
    let mut wrong = token.clone();
    let flipped = if wrong.ends_with('0') { '1' } else { '0' };
    wrong.pop();
    wrong.push(flipped);
    for bad in [wrong.as_str(), "garbage", "ck1.nope.short"] {
        let response = router
            .clone()
            .oneshot(request("GET", "/v1/files", Some(bad), None, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{bad}");
    }

    // Revoke, then the real token refuses identically.
    let response = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("/v1/admin/tenants/acme/keys/{key_id}"),
            None,
            Some(ADMIN),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = router
        .clone()
        .oneshot(request("GET", "/v1/files", Some(&token), None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Second revocation reads as 404 (not an oracle for key state).
    let response = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("/v1/admin/tenants/acme/keys/{key_id}"),
            None,
            Some(ADMIN),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The listing shows the revoked key without any hash material.
    let response = router
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/tenants/acme/keys",
            None,
            Some(ADMIN),
            None,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "doomed");
    assert!(items[0]["revoked_at"].is_string());
    assert!(items[0].get("key_hash").is_none(), "{body}");
}

#[tokio::test]
async fn the_admin_surface_is_gated_and_disableable() {
    let (router, _dir) = keyed_router().await;

    // Wrong or missing operator token refuses.
    for admin in [None, Some("wrong")] {
        let response = router
            .clone()
            .oneshot(request(
                "POST",
                "/v1/admin/tenants/acme/keys",
                None,
                admin,
                Some(json!({ "name": "x" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // Duplicate names per tenant conflict via the unique index.
    mint(&router, "acme", "dup").await;
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/admin/tenants/acme/keys",
            None,
            Some(ADMIN),
            Some(json!({ "name": "dup" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // No admin token configured = the admin surface does not exist.
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: None,
        admin_token_previous: None,
    });
    let disabled = build_router(state);
    let response = disabled
        .oneshot(request(
            "POST",
            "/v1/admin/tenants/acme/keys",
            None,
            Some(ADMIN),
            Some(json!({ "name": "x" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_token_rotation_window_accepts_both() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some("new-token".into()),
        admin_token_previous: Some("old-token".into()),
    });
    let router = build_router(state);

    for (token, expected) in [
        ("new-token", StatusCode::CREATED),
        ("old-token", StatusCode::CREATED),
        ("neither", StatusCode::UNAUTHORIZED),
    ] {
        let response = router
            .clone()
            .oneshot(request(
                "POST",
                "/v1/admin/tenants/acme/keys",
                None,
                Some(token),
                Some(json!({ "name": format!("k-{token}") })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{token}");
    }
}
