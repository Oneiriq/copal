//! The root: a person who types the host into a browser and a program
//! that probes `/` both get the surface list, each in the shape it
//! asked for.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack(admin_token: Option<&str>) -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let mut state = AppState::new(store, blobs);
    if let Some(token) = admin_token {
        state = state.with_auth(AuthConfig {
            mode: AuthMode::ApiKeys,
            admin_token: Some(token.to_owned()),
            admin_token_previous: None,
        });
    }
    (build_router(state), dir)
}

async fn body_of(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn a_browser_gets_a_page_and_a_program_gets_a_document() {
    let (router, _dir) = stack(Some("operator-secret")).await;

    let browser = Request::builder()
        .uri("/")
        .header("accept", "text/html,application/xhtml+xml")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(browser).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .starts_with("text/html"));
    let html = body_of(response).await;
    assert!(html.contains("/admin/console"), "the console is linked");
    assert!(html.contains("/graphql"));

    let program = Request::builder().uri("/").body(Body::empty()).unwrap();
    let response = router.clone().oneshot(program).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let answer: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();
    assert_eq!(answer["service"], serde_json::json!("copal"));
    assert_eq!(
        answer["surfaces"]["mcp"],
        serde_json::json!("/mcp"),
        "the agent face is named",
    );
}

#[tokio::test]
async fn the_console_is_named_only_when_it_exists() {
    // Without an admin token the console 404s, so the root must not
    // point at it.
    let (router, _dir) = stack(None).await;
    let response = router
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let answer: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();
    assert!(
        answer["surfaces"].get("console").is_none(),
        "an unguarded deployment has no console to advertise",
    );
    assert_eq!(
        answer["surfaces"]["readiness"],
        serde_json::json!("/readyz")
    );
}
