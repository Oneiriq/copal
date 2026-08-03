//! The request-span middleware in the serving path: a caller's W3C
//! trace context parses without disturbing the request, and requests
//! without one serve identically. Span EXPORT needs a collector and
//! stays out of CI; this proves the middleware is inert to callers.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

#[tokio::test]
async fn traceparent_headers_pass_through_the_middleware() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs);
    let router =
        build_router(state).layer(axum::middleware::from_fn(copal_server::trace::middleware));

    // A well-formed traceparent, a garbled one, and none at all: the
    // middleware must treat all three the same way the bare router
    // would.
    for traceparent in [
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        Some("garbage-in"),
        None,
    ] {
        let mut request = Request::builder().method("GET").uri("/healthz");
        if let Some(value) = traceparent {
            request = request.header("traceparent", value);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "case {traceparent:?}");
    }
}
