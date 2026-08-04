//! The operator console on the admin surface: Basic auth against
//! the admin token, a deployment home only copal can render, and
//! the janus contract pages per tenant, dispatched through the same
//! chain every API face uses.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use serde_json::json;
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-secret";

async fn stack() -> (axum::Router, tempfile::TempDir) {
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

fn basic() -> String {
    let pair = base64::engine::general_purpose::STANDARD.encode(format!("operator:{ADMIN}"));
    format!("Basic {pair}")
}

async fn text(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Mint a key, create a file, upload bytes: one tenant with content,
/// through the API the console will mirror.
async fn seed(router: &axum::Router) -> String {
    let mint = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/keys")
        .header("x-copal-admin-token", ADMIN)
        .header("content-type", "application/json")
        .body(Body::from(json!({ "name": "console-test" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(mint).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let token = serde_json::from_str::<serde_json::Value>(&text(response).await).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_owned();

    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({ "path": "ops/manual.txt" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = serde_json::from_str::<serde_json::Value>(&text(response).await).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(&b"console proof"[..]))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn the_console_gates_on_the_admin_token() {
    let (router, _dir) = stack().await;

    let bare = Request::builder()
        .uri("/admin/console")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(bare).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        response.headers().contains_key("www-authenticate"),
        "the browser gets a Basic challenge",
    );

    let wrong = Request::builder()
        .uri("/admin/console")
        .header(
            "authorization",
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("operator:guess"),
            ),
        )
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(wrong).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_deployment_home_names_tenants_and_tails_the_audit() {
    let (router, _dir) = stack().await;
    seed(&router).await;

    let home = Request::builder()
        .uri("/admin/console")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(home).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = text(response).await;
    assert!(html.contains("/admin/console/t/acme"), "the tenant links");
    assert!(html.contains("key.minted"), "the audit tail shows custody");
}

#[tokio::test]
async fn the_tenant_console_serves_the_contract_pages() {
    let (router, _dir) = stack().await;
    let id = seed(&router).await;

    let overview = Request::builder()
        .uri("/admin/console/t/acme")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(overview).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(text(response)
        .await
        .contains("/admin/console/t/acme/r/files"));

    let listing = Request::builder()
        .uri("/admin/console/t/acme/r/files")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(listing).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = text(response).await;
    assert!(html.contains(&id), "the uploaded file lists");
    assert!(html.contains("ops/manual.txt"));

    let detail = Request::builder()
        .uri(format!("/admin/console/t/acme/r/files/{id}"))
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(detail).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = text(response).await;
    assert!(html.contains("versions"), "sub-collections render");
    assert!(html.contains("/a/issue_url"), "actions become forms");
}

#[tokio::test]
async fn a_console_form_dispatches_and_redirects() {
    let (router, _dir) = stack().await;
    let id = seed(&router).await;

    let submit = Request::builder()
        .method("POST")
        .uri(format!("/admin/console/t/acme/r/files/{id}/a/issue_url"))
        .header("authorization", basic())
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("ttl_secs=60"))
        .unwrap();
    let response = router.clone().oneshot(submit).await.unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let target = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    assert!(
        target.contains(&format!(
            "/admin/console/t/acme/r/files/{id}?done=issue_url"
        )),
        "the redirect returns to the instance: {target}",
    );
}

#[tokio::test]
async fn the_fleet_view_gates_on_configuration_and_names_its_limits() {
    // Off by default: no fleet section at all.
    let (router, _dir) = stack().await;
    let home = Request::builder()
        .uri("/admin/console")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(home).await.unwrap();
    let html = text(response).await;
    assert!(
        !html.contains(">fleet<"),
        "no fleet section unless configured"
    );

    // Configured against an embedded engine, the walk refuses with
    // its reason instead of pretending.
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            mode: AuthMode::ApiKeys,
            admin_token: Some(ADMIN.into()),
            admin_token_previous: None,
        })
        .with_fleet(Some(StoreConfig::memory()));
    let router = build_router(state);
    let home = Request::builder()
        .uri("/admin/console")
        .header("authorization", basic())
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(home).await.unwrap();
    let html = text(response).await;
    assert!(
        html.contains(">fleet<"),
        "the section renders when configured"
    );
    assert!(
        html.contains("needs a remote engine"),
        "an unwalkable engine is named, never faked",
    );
}
