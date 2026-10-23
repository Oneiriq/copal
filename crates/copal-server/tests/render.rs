//! Render links end to end: issuance against the render origin,
//! sandboxed display on the render listener, and the refusals that keep
//! script-capable content off the service's own origin.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::app::{build_router, AppState};
use copal_server::render::render_router;
use copal_store::{Store, StoreConfig};

const RENDER_ORIGIN: &str = "https://view.example.test";
const PAGE: &[u8] = b"<!doctype html><title>Bounded</title><script>document.title='ran'</script>";

/// The service router and the render listener's router over one state,
/// as `main` wires them: two listeners, never merged.
async fn stack(render_origin: Option<&str>) -> (axum::Router, axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state =
        AppState::new(store, blobs).with_render_origin(render_origin.map(|o| format!("{o}/")));
    (build_router(state.clone()), render_router(state), dir)
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

fn anonymous(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

async fn upload_html(router: &axum::Router) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(
            json!({ "path": "pages/bounded.html", "content_type": "text/html" }).to_string(),
        ),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let mut put = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(PAGE.to_vec()),
    );
    put.headers_mut().remove("content-type");
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

async fn issue(router: &axum::Router, id: &str, action: &str, body: Value) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(req(
            "POST",
            &format!("/v1/files/{id}/{action}"),
            Body::from(body.to_string()),
        ))
        .await
        .unwrap();
    let status = response.status();
    (status, json_body(response).await)
}

/// The path half of an absolute render link.
fn path_of(url: &str) -> &str {
    url.strip_prefix(RENDER_ORIGIN)
        .expect("render links name the render origin")
}

#[tokio::test]
async fn render_links_display_inside_the_sandbox_on_their_own_listener() {
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let id = upload_html(&service).await;

    let (status, issued) = issue(&service, &id, "render-url", json!({ "ttl_secs": 600 })).await;
    assert_eq!(status, StatusCode::CREATED);
    let url = issued["url"].as_str().unwrap();
    // Absolute, on the render origin, with the configured trailing
    // slash dropped rather than doubled.
    assert!(
        url.starts_with(&format!("{RENDER_ORIGIN}/v1/render/")),
        "{url}"
    );

    let response = render
        .clone()
        .oneshot(anonymous(path_of(url)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    let get = |name: &str| headers.get(name).unwrap().to_str().unwrap().to_owned();
    assert_eq!(get("content-type"), "text/html");
    assert_eq!(
        get("content-disposition"),
        "inline; filename=\"bounded.html\""
    );
    let csp = get("content-security-policy");
    assert!(csp.starts_with("sandbox allow-scripts"), "{csp}");
    assert!(!csp.contains("allow-same-origin"), "{csp}");
    assert_eq!(get("cache-control"), "no-store");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], PAGE);

    // The same HTML on the service's own origin still downloads.
    let (_, download) = issue(&service, &id, "url", json!({})).await;
    let response = service
        .clone()
        .oneshot(anonymous(download["url"].as_str().unwrap()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let disposition = response.headers()["content-disposition"].to_str().unwrap();
    assert!(disposition.starts_with("attachment"), "{disposition}");
    assert!(response.headers().get("content-security-policy").is_none());
}

#[tokio::test]
async fn each_link_works_only_where_it_belongs() {
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let id = upload_html(&service).await;
    let (_, rendered) = issue(&service, &id, "render-url", json!({})).await;
    let (_, download) = issue(&service, &id, "url", json!({})).await;
    let render_token = rendered["token"].as_str().unwrap();
    let download_token = download["token"].as_str().unwrap();

    // A render link cannot download under the service origin.
    let response = service
        .clone()
        .oneshot(anonymous(&format!("/v1/grants/{render_token}")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // A download link cannot render.
    let response = render
        .clone()
        .oneshot(anonymous(&format!("/v1/render/{download_token}")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The service origin has no render route, and the render listener
    // has nothing but its one route.
    let response = service
        .clone()
        .oneshot(anonymous(&format!("/v1/render/{render_token}")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    for path in [
        "/",
        "/v1/files",
        "/healthz",
        &format!("/v1/grants/{download_token}"),
    ] {
        let response = render
            .clone()
            .oneshot(req("GET", path, Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn render_links_count_views_and_revoke() {
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let id = upload_html(&service).await;

    let (_, once) = issue(&service, &id, "render-url", json!({ "max_uses": 1 })).await;
    let path = path_of(once["url"].as_str().unwrap()).to_owned();
    let first = render.clone().oneshot(anonymous(&path)).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let second = render.clone().oneshot(anonymous(&path)).await.unwrap();
    assert_eq!(second.status(), StatusCode::NOT_FOUND);

    let (_, shared) = issue(&service, &id, "render-url", json!({})).await;
    let path = path_of(shared["url"].as_str().unwrap()).to_owned();
    let grant_id = shared["grant_id"].as_str().unwrap();
    let revoke = service
        .clone()
        .oneshot(req(
            "DELETE",
            &format!("/v1/grants/{grant_id}"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert!(revoke.status().is_success(), "{}", revoke.status());
    let after = render.clone().oneshot(anonymous(&path)).await.unwrap();
    assert_eq!(after.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn without_a_render_listener_issuance_names_the_fix() {
    let (service, _render, _dir) = stack(None).await;
    let id = upload_html(&service).await;
    let (status, body) = issue(&service, &id, "render-url", json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        body.to_string().contains("COPAL_RENDER_BIND"),
        "the refusal names the setting: {body}",
    );
}
