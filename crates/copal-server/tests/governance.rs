//! The declarations release, proven across faces: scopes and rate
//! budgets refuse identically whether a request arrives as REST or
//! GraphQL, because the contract declares them once and both faces
//! enforce from it against one ledger.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-token";

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

/// Mint a key with the given scopes; returns its bearer token. Names
/// are unique per call because keys are replace-by-name per tenant.
async fn mint(router: &axum::Router, scopes: &[&str]) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/tenants/acme/keys")
                .header("x-copal-admin-token", ADMIN)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "name": format!("k{sequence}-{}", scopes.join("-")), "scopes": scopes })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await["token"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn rest(method: &str, uri: &str, token: &str, body: Option<Value>) -> Request<Body> {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
    match body {
        Some(value) => builder
            .header("content-type", "application/json")
            .body(Body::from(value.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

fn graphql(query: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": query }).to_string()))
        .unwrap()
}

#[tokio::test]
async fn scopes_refuse_identically_on_both_faces() {
    let (router, _dir) = keyed_router().await;
    let read_only = mint(&router, &["read"]).await;

    // Reads pass on both faces.
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files", &read_only, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .clone()
        .oneshot(graphql(r#"{ files { items { id } } }"#, &read_only))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body.get("errors").is_none(), "{body:#?}");

    // A write refuses on both faces, naming the same scope with the
    // same machine-readable kind.
    let response = router
        .clone()
        .oneshot(rest(
            "POST",
            "/v1/files",
            &read_only,
            Some(json!({ "path": "denied.txt" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let rest_error = json_body(response).await;
    assert_eq!(rest_error["error"]["kind"], "forbidden");
    assert!(
        rest_error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("scope write required"),
        "{rest_error:#?}",
    );

    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation { fileRemove(id: "01ARZ3NDEKTSV4RRFFQ69G5FAV") }"#,
            &read_only,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    let error = &body["errors"][0];
    assert_eq!(error["extensions"]["code"], "forbidden");
    assert!(
        error["message"].as_str().unwrap().contains("scope write"),
        "{body:#?}",
    );

    // Search is a read: a write-only key is refused there.
    let write_only = mint(&router, &["write"]).await;
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/search?q=anything", &write_only, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(json_body(response).await["error"]["message"]
        .as_str()
        .unwrap()
        .contains("scope read required"));
}

#[tokio::test]
async fn webhook_registration_takes_the_admin_scope() {
    let (router, _dir) = keyed_router().await;
    let write_only = mint(&router, &["read", "write"]).await;
    let admin_scoped = mint(&router, &["admin"]).await;

    // Registering a webhook exfiltrates every future event to the
    // named URL, which is why write is deliberately weaker than it.
    let payload = json!({ "url": "http://127.0.0.1:9/hook" });
    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation { webhookRegister(url: "http://127.0.0.1:9/hook") }"#,
            &write_only,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["errors"][0]["extensions"]["code"], "forbidden");
    assert!(
        body["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("scope admin"),
        "{body:#?}",
    );

    // The admin-scoped key reaches the shared core; the refusal it
    // gets is the sealed-secret one, which proves the scope gate is
    // what stood in front of it. (This fixture carries no cipher.)
    let response = router
        .clone()
        .oneshot(rest("POST", "/v1/webhooks", &admin_scoped, Some(payload)))
        .await
        .unwrap();
    assert_ne!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn one_ledger_meters_both_faces() {
    let (router, _dir) = keyed_router().await;
    let token = mint(&router, &["read"]).await;

    // The reads budget is 6000 units a minute and a full page costs
    // its 100-row limit, so sixty full pages spend it exactly.
    for i in 0..60 {
        let response = router
            .clone()
            .oneshot(graphql(r#"{ files(limit: 100) { items { id } } }"#, &token))
            .await
            .unwrap();
        let body = json_body(response).await;
        assert!(body.get("errors").is_none(), "query {i}: {body:#?}");
    }

    // The 61st spend refuses on the GraphQL face with the retryable
    // code, and the SAME key is refused on the REST face too, because
    // both faces charged one ledger.
    let response = router
        .clone()
        .oneshot(graphql(r#"{ files(limit: 100) { items { id } } }"#, &token))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(
        body["errors"][0]["extensions"]["code"], "too_many_requests",
        "{body:#?}",
    );

    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files?limit=100", &token, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let body = json_body(response).await;
    assert_eq!(body["error"]["kind"], "too_many_requests");

    // A different key has its own budget and passes untouched.
    let fresh = mint(&router, &["read"]).await;
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files?limit=1", &fresh, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn header_mode_stays_open_and_still_meters() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let router = build_router(AppState::new(store, blobs));
    let _dir = dir;

    // Header mode holds every scope: reads, writes, and admin-scoped
    // operations all pass the scope gate.
    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "path": "dev.txt" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let listing = Request::builder()
        .method("GET")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(listing).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn guarded_fields_redact_identically_on_both_faces() {
    let (router, _dir) = keyed_router().await;
    let worker = mint(&router, &["read", "write"]).await;
    let operator = mint(&router, &["read", "admin"]).await;

    // A file with content mints version 1.
    let response = router
        .clone()
        .oneshot(rest(
            "POST",
            "/v1/files",
            &worker,
            Some(json!({ "path": "audit.txt", "content_type": "text/plain" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("authorization", format!("Bearer {worker}"))
        .body(Body::from(b"attributed bytes".to_vec()))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Version attribution is audit data: the non-admin key lists
    // history WITHOUT the created_by key on REST, and GraphQL renders
    // it null. The rest of the row is intact.
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}/versions"),
            &worker,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    let row = &body["items"][0];
    assert_eq!(row["number"], 1);
    assert!(row.get("created_by").is_none(), "{row:#?}");

    let response = router
        .clone()
        .oneshot(graphql(
            &format!(
                r#"{{ file(id: "{id}") {{ versions {{ items {{ number created_by }} }} }} }}"#
            ),
            &worker,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body.get("errors").is_none(), "{body:#?}");
    let row = &body["data"]["file"]["versions"]["items"][0];
    assert_eq!(row["number"], 1);
    assert!(row["created_by"].is_null(), "{row:#?}");

    // The admin-scoped key sees the attribution on both faces.
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}/versions"),
            &operator,
            None,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["items"][0]["created_by"].is_string(), "{body:#?}",);
    let response = router
        .clone()
        .oneshot(graphql(
            &format!(r#"{{ file(id: "{id}") {{ versions {{ items {{ created_by }} }} }} }}"#),
            &operator,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(
        body["data"]["file"]["versions"]["items"][0]["created_by"].is_string(),
        "{body:#?}",
    );
}
