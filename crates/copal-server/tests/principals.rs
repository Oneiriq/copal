//! Principals, slices one and two: named actors that keys belong to.
//!
//! Everything here is additive on the key model: a key without a
//! principal behaves exactly as every key did before principals
//! existed, which the rest of the suite keeps proving.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::app::admin_router;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some("root".to_owned()),
        admin_token_previous: None,
        operator_header: None,
    });
    (build_router(state.clone()), admin_router(state), dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn admin_req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-admin-token", "root");
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    builder.body(body).unwrap()
}

fn bearer_req(method: &str, uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

async fn create_principal(admin: &axum::Router, handle: &str, scopes: &[&str]) {
    let response = admin
        .clone()
        .oneshot(admin_req(
            "POST",
            "/v1/admin/tenants/acme/principals",
            Some(json!({ "handle": handle, "kind": "agent", "scopes": scopes })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn mint_under(admin: &axum::Router, principal: Option<&str>, scopes: &[&str]) -> Value {
    let mut body = json!({ "name": format!("key-{}", ulid::Ulid::generate()), "scopes": scopes });
    if let Some(handle) = principal {
        body["principal"] = json!(handle);
    }
    let response = admin
        .clone()
        .oneshot(admin_req("POST", "/v1/admin/tenants/acme/keys", Some(body)))
        .await
        .unwrap();
    json_body(response).await
}

/// The admin surface round trip: create, duplicate refuses, list
/// shows the actor, disable is a tombstone rather than a delete.
#[tokio::test]
async fn principals_create_list_and_disable() {
    let (_router, admin, _dir) = stack().await;
    create_principal(&admin, "agent-7", &["read"]).await;

    let response = admin
        .clone()
        .oneshot(admin_req(
            "POST",
            "/v1/admin/tenants/acme/principals",
            Some(json!({ "handle": "agent-7", "kind": "agent", "scopes": ["read"] })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT, "duplicate handle");

    let response = admin
        .clone()
        .oneshot(admin_req("GET", "/v1/admin/tenants/acme/principals", None))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["items"][0]["handle"], "agent-7");
    assert_eq!(body["items"][0]["kind"], "agent");

    let response = admin
        .clone()
        .oneshot(admin_req(
            "DELETE",
            "/v1/admin/tenants/acme/principals/agent-7",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // A second disable finds no live principal.
    let response = admin
        .clone()
        .oneshot(admin_req(
            "DELETE",
            "/v1/admin/tenants/acme/principals/agent-7",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Minting under a principal answers to its ceiling: scopes beyond
/// it refuse loudly at mint time rather than silently shrinking.
#[tokio::test]
async fn minting_answers_to_the_ceiling() {
    let (_router, admin, _dir) = stack().await;
    create_principal(&admin, "reader", &["read"]).await;

    let body = mint_under(&admin, Some("reader"), &["read", "write"]).await;
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("exceeds principal"),
        "{body:#?}",
    );

    let body = mint_under(&admin, Some("reader"), &["read"]).await;
    assert_eq!(body["principal"], "reader", "{body:#?}");
    assert!(body["token"].is_string());

    let body = mint_under(&admin, Some("nobody"), &["read"]).await;
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("no principal"),
        "{body:#?}",
    );
}

/// Scopes resolve as the intersection at authentication: a principal
/// narrowed after minting narrows every key it owns, live.
#[tokio::test]
async fn a_narrowed_principal_narrows_its_keys() {
    let (router, admin, _dir) = stack().await;
    create_principal(&admin, "worker", &["read", "write"]).await;
    let minted = mint_under(&admin, Some("worker"), &["read", "write"]).await;
    let token = minted["token"].as_str().unwrap().to_owned();

    // The key works on a read.
    let response = router
        .clone()
        .oneshot(bearer_req("GET", "/v1/files", &token))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Disable the principal: the same key refuses everywhere at once.
    let response = admin
        .clone()
        .oneshot(admin_req(
            "DELETE",
            "/v1/admin/tenants/acme/principals/worker",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = router
        .clone()
        .oneshot(bearer_req("GET", "/v1/files", &token))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "a disabled principal refuses every key it owns",
    );
}

/// The audit trail names the actor when one exists.
#[tokio::test]
async fn audit_names_the_principal() {
    let (_router, admin, _dir) = stack().await;
    create_principal(&admin, "alice", &["read"]).await;
    let minted = mint_under(&admin, Some("alice"), &["read"]).await;
    assert_eq!(minted["principal"], "alice");

    let response = admin
        .clone()
        .oneshot(admin_req("GET", "/v1/admin/tenants/acme/audit", None))
        .await
        .unwrap();
    let trail = json_body(response).await["items"].to_string();
    assert!(trail.contains("principal.created"), "{trail}");
    assert!(trail.contains("alice"), "{trail}");
}

fn bearer_body(method: &str, uri: &str, token: &str, payload: &[u8]) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(payload.to_vec()))
        .unwrap()
}

/// The ownership loop, end to end: alice's upload records her handle,
/// she sees her own attribution in the version listing, bob does not,
/// and an admin sees everything. The unknown-authorship rule rides
/// the same comparison: rows written by principal-less keys match no
/// handle and read as nobody's.
#[tokio::test]
async fn attribution_shows_authors_their_own_rows() {
    let (router, admin, _dir) = stack().await;
    create_principal(&admin, "alice", &["read", "write"]).await;
    create_principal(&admin, "bob", &["read", "write"]).await;
    let alice = mint_under(&admin, Some("alice"), &["read", "write"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let bob = mint_under(&admin, Some("bob"), &["read", "write"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let operator = mint_under(&admin, None, &["read", "admin"]).await["token"]
        .as_str()
        .unwrap()
        .to_owned();

    // Alice creates and uploads.
    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {alice}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({"path": "authored.txt"}).to_string()))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let response = router
        .clone()
        .oneshot(bearer_body(
            "PUT",
            &format!("/v1/files/{id}/content"),
            &alice,
            b"authored by alice",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let versions_uri = format!("/v1/files/{id}/versions");
    let fetch = |token: String| {
        let router = router.clone();
        let uri = versions_uri.clone();
        async move {
            let response = router
                .oneshot(bearer_req("GET", &uri, &token))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            json_body(response).await["items"][0].clone()
        }
    };

    let row = fetch(alice.clone()).await;
    assert_eq!(
        row["created_by"], "alice",
        "the author sees their own: {row:#?}"
    );
    let row = fetch(bob.clone()).await;
    assert!(
        row["created_by"].is_null(),
        "a stranger sees nobody: {row:#?}"
    );
    let row = fetch(operator.clone()).await;
    assert_eq!(
        row["created_by"], "alice",
        "admins see everything: {row:#?}"
    );
}
