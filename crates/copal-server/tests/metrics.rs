//! The scrape endpoint: guarded like the rest of the admin surface,
//! and reporting what the process actually served.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::json;
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::app::AppState;
use copal_server::auth::AuthConfig;
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        admin_token: Some("root".to_owned()),
        ..AuthConfig::default()
    });
    (copal_server::build_router(state), dir)
}

async fn text_body(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[tokio::test]
async fn metrics_report_served_traffic_behind_the_admin_token() {
    let (router, _dir) = stack().await;

    // Anonymous scrapes refuse: request volumes are operator data.
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Serve some traffic: one success and one refusal.
    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "path": "metrics/a.txt", "content_type": "text/plain" }).to_string(),
        ))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(create).await.unwrap().status(),
        StatusCode::CREATED,
    );
    let missing = Request::builder()
        .uri("/v1/files/01jnope00000000000000000000")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(missing).await.unwrap().status(),
        StatusCode::NOT_FOUND,
    );

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("x-copal-admin-token", "root")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; version=0.0.4",
    );
    let body = text_body(response).await;

    // The exposition names its families and carries both classes.
    assert!(
        body.contains("# TYPE copal_http_responses_total counter"),
        "{body}"
    );
    assert!(
        body.contains("copal_http_responses_total{class=\"2xx\"}"),
        "{body}"
    );
    assert!(
        body.contains("copal_http_responses_total{class=\"4xx\"}"),
        "{body}"
    );
    assert!(
        body.contains("copal_http_request_duration_seconds_count"),
        "{body}",
    );
}
