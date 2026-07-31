//! Signed upload URLs: a browser holding only the URL puts bytes,
//! once, and every other use of that token refuses.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::app::AppState;
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    (copal_server::build_router(AppState::new(store, blobs)), dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn tenant_req(method: &str, uri: &str, body: Body) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-tenant", "acme");
    if method == "POST" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

/// The browser's request: no tenant header, no key, just the URL.
fn anonymous_put(url: &str, content: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(url)
        .header("content-length", content.len().to_string())
        .body(Body::from(content.to_vec()))
        .unwrap()
}

async fn create(router: &axum::Router, path: &str) -> String {
    let create = tenant_req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/plain" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await["id"].as_str().unwrap().to_owned()
}

async fn upload_url(router: &axum::Router, id: &str, ttl: Option<u32>) -> (String, StatusCode) {
    let body = match ttl {
        Some(ttl) => json!({ "ttl_secs": ttl }),
        None => json!({}),
    };
    let request = tenant_req(
        "POST",
        &format!("/v1/files/{id}/upload-url"),
        Body::from(body.to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    if status != StatusCode::CREATED {
        return (String::new(), status);
    }
    let body = json_body(response).await;
    (body["url"].as_str().unwrap().to_owned(), status)
}

#[tokio::test]
async fn upload_urls_accept_one_anonymous_write() {
    let (router, _dir) = stack().await;
    let id = create(&router, "browser/photo.txt").await;
    let payload = b"uploaded straight from the browser";

    let (url, _) = upload_url(&router, &id, Some(300)).await;
    assert!(url.starts_with("/v1/grants/cg1."));

    // The anonymous PUT lands and finishes the record.
    let response = router
        .clone()
        .oneshot(anonymous_put(&url, payload))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let record = json_body(response).await;
    assert_eq!(record["state"], "ready");
    assert_eq!(record["size"], payload.len());

    // The bytes are the file's bytes.
    let download = tenant_req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);

    // The capability is spent: a replay refuses uniformly.
    let response = router
        .clone()
        .oneshot(anonymous_put(&url, b"second write"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // And it never authorized reading.
    let read = Request::builder()
        .method("GET")
        .uri(&url)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(read).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn download_tokens_cannot_write_and_bad_ttls_refuse() {
    let (router, _dir) = stack().await;
    let id = create(&router, "browser/existing.txt").await;

    // Put content the ordinary way so a download URL can exist.
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header("content-length", "7")
        .body(Body::from("initial"))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(put).await.unwrap().status(),
        StatusCode::OK,
    );

    // A read capability refuses to write.
    let issue = tenant_req(
        "POST",
        &format!("/v1/files/{id}/url"),
        Body::from(json!({ "ttl_secs": 300 }).to_string()),
    );
    let body = json_body(router.clone().oneshot(issue).await.unwrap()).await;
    let read_url = body["url"].as_str().unwrap().to_owned();
    let response = router
        .clone()
        .oneshot(anonymous_put(&read_url, b"hostile overwrite"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The original content is untouched.
    let download = tenant_req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    let bytes = router
        .clone()
        .oneshot(download)
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&bytes[..], b"initial");

    // TTLs outside the write window refuse.
    assert_eq!(
        upload_url(&router, &id, Some(0)).await.1,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        upload_url(&router, &id, Some(90_000)).await.1,
        StatusCode::BAD_REQUEST,
    );

    // A deleted file cannot be handed a write capability.
    let remove = tenant_req("DELETE", &format!("/v1/files/{id}"), Body::empty());
    assert_eq!(
        router.clone().oneshot(remove).await.unwrap().status(),
        StatusCode::NO_CONTENT,
    );
    assert_eq!(
        upload_url(&router, &id, None).await.1,
        StatusCode::NOT_FOUND
    );
}
