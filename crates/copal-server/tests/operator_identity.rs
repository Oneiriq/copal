//! Operator identity: who an audited action names.
//!
//! A shared token says a deployment acted. With an authenticating
//! proxy in front and its header named, the trail says which person
//! did, and a request that did not come through that proxy is
//! refused rather than attributed to nobody.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-secret";
const HEADER: &str = "x-forwarded-email";

async fn stack(operator_header: Option<&str>) -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
        operator_header: operator_header.map(str::to_owned),
    });
    (build_router(state), dir)
}

async fn body_of(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Set a tenant quota, which is an audited operator action.
fn quota_request(operator: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("PUT")
        .uri("/v1/admin/tenants/acme/quota")
        .header("x-copal-admin-token", ADMIN)
        .header("content-type", "application/json");
    if let Some(operator) = operator {
        builder = builder.header(HEADER, operator);
    }
    builder
        .body(Body::from(json!({ "max_bytes": 1024 }).to_string()))
        .unwrap()
}

async fn audit_actors(router: &axum::Router) -> Vec<String> {
    // Reading the trail is itself an operator action, so it carries an
    // identity too when the seam is configured.
    let request = Request::builder()
        .uri("/v1/admin/audit/export?limit=20")
        .header("x-copal-admin-token", ADMIN)
        .header(HEADER, "reader@oneiriq.test")
        .body(Body::empty())
        .unwrap();
    let text = body_of(router.clone().oneshot(request).await.unwrap()).await;
    text.lines()
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|row| row["actor"].as_str().map(str::to_owned))
        .collect()
}

#[tokio::test]
async fn without_the_seam_the_deployment_is_the_actor() {
    let (router, _dir) = stack(None).await;
    let response = router.clone().oneshot(quota_request(None)).await.unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    assert_eq!(audit_actors(&router).await, vec!["admin".to_owned()]);
}

#[tokio::test]
async fn with_the_seam_the_trail_names_the_person() {
    let (router, _dir) = stack(Some(HEADER)).await;
    let response = router
        .clone()
        .oneshot(quota_request(Some("dana@oneiriq.test")))
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    assert_eq!(
        audit_actors(&router).await,
        vec!["dana@oneiriq.test".to_owned()],
        "the audit trail names who acted, rather than what",
    );
}

/// The seam is a control, not a label: a request holding the token but
/// carrying no identity did not pass the proxy, and is refused.
#[tokio::test]
async fn a_request_that_skipped_the_proxy_is_refused() {
    let (router, _dir) = stack(Some(HEADER)).await;
    let response = router.clone().oneshot(quota_request(None)).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        body_of(response).await.contains("authenticating proxy"),
        "the refusal says what is missing",
    );

    let blank = router
        .clone()
        .oneshot(quota_request(Some("   ")))
        .await
        .unwrap();
    assert_eq!(blank.status(), StatusCode::UNAUTHORIZED, "empty is absent");

    // Nothing was written under either attempt.
    assert!(audit_actors(&router).await.is_empty());
}

/// An identity lands in a trail nothing can rewrite, so it arrives
/// bounded. A control character cannot be made into a header value at
/// all, which the request builder proves here and the sanitizer
/// guards anyway.
#[tokio::test]
async fn a_hostile_identity_is_bounded() {
    let (router, _dir) = stack(Some(HEADER)).await;
    let long = format!("dana{}", "x".repeat(400));
    let response = router
        .clone()
        .oneshot(quota_request(Some(&long)))
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());

    let actors = audit_actors(&router).await;
    let actor = actors.first().expect("one audited action");
    assert!(actor.len() <= 128, "bounded, got {}", actor.len());
    assert!(actor.starts_with("danaxxx"));

    assert!(
        Request::builder()
            .header(HEADER, "dana\u{7}injected")
            .body(Body::empty())
            .is_err(),
        "a control character never becomes a header value",
    );
}

/// The console reports who is signed in, and its refusal explains the
/// username nobody has to guess.
#[tokio::test]
async fn the_console_shows_the_operator() {
    let (router, _dir) = stack(Some(HEADER)).await;
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("ignored:{ADMIN}"));
    let request = Request::builder()
        .uri("/admin/console")
        .header("authorization", format!("Basic {basic}"))
        .header(HEADER, "dana@oneiriq.test")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(body_of(response)
        .await
        .contains("signed in as dana@oneiriq.test"));

    let bare = Request::builder()
        .uri("/admin/console")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(bare).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let realm = response
        .headers()
        .get("www-authenticate")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    assert!(
        realm.contains("any username"),
        "the browser dialog says the username is ignored: {realm}",
    );
}
