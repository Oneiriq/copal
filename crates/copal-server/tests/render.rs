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
    req_as("acme", method, uri, body)
}

fn req_as(tenant: &str, method: &str, uri: &str, body: Body) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-tenant", tenant);
    if method == "POST" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

fn anonymous(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

async fn upload_html(router: &axum::Router) -> String {
    upload_page(router, "pages/bounded.html", PAGE).await
}

async fn upload_page(router: &axum::Router, path: &str, page: &[u8]) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/html" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let mut put = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(page.to_vec()),
    );
    put.headers_mut().remove("content-type");
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

async fn issue(router: &axum::Router, id: &str, action: &str, body: Value) -> (StatusCode, Value) {
    issue_as(router, "acme", id, action, body).await
}

async fn issue_as(
    router: &axum::Router,
    tenant: &str,
    id: &str,
    action: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(req_as(
            tenant,
            "POST",
            &format!("/v1/files/{id}/{action}"),
            Body::from(body.to_string()),
        ))
        .await
        .unwrap();
    let status = response.status();
    (status, json_body(response).await)
}

/// Status and body of an anonymous GET on the render listener.
async fn view(render: &axum::Router, path: &str) -> (StatusCode, Vec<u8>) {
    let response = render.clone().oneshot(anonymous(path)).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, body.to_vec())
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
async fn short_links_open_the_page_and_follow_new_versions() {
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let first = upload_html(&service).await;

    let (status, issued) = issue(
        &service,
        &first,
        "render-url",
        json!({ "ttl_secs": 600, "short_name": "Ceiling" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{issued}");
    let link = issued["short_link"].as_str().unwrap().to_owned();
    let (name, code) = link.rsplit_once('-').unwrap();
    assert_eq!(name, "ceiling");
    assert_eq!(code.len(), 4, "{link}");
    assert_eq!(
        issued["short_url"],
        json!(format!("{RENDER_ORIGIN}/a/{link}"))
    );
    // The long link still works beside it.
    assert!(issued["url"].as_str().unwrap().contains("/v1/render/cg1."));

    // Displayed exactly as the render link is: sandboxed, never cached.
    let response = render
        .clone()
        .oneshot(anonymous(&format!("/a/{link}")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let csp = response.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(csp.starts_with("sandbox allow-scripts"), "{csp}");
    assert!(!csp.contains("allow-same-origin"), "{csp}");
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], PAGE);

    // Phone keyboards capitalize; the link still opens.
    let (status, _) = view(&render, &format!("/a/{}", link.to_ascii_uppercase())).await;
    assert_eq!(status, StatusCode::OK);

    // A new version takes over the same address.
    let next: &[u8] = b"<!doctype html><title>Bounded v2</title>";
    let second = upload_page(&service, "pages/bounded-v2.html", next).await;
    let (status, moved) = issue(
        &service,
        &second,
        "render-url",
        json!({ "short_link": link.to_ascii_uppercase() }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{moved}");
    assert_eq!(moved["short_link"], json!(link));
    let (status, body) = view(&render, &format!("/a/{link}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, next);
}

#[tokio::test]
async fn short_links_close_with_their_grant_and_refuse_strangers() {
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let id = upload_html(&service).await;
    let (_, issued) = issue(
        &service,
        &id,
        "render-url",
        json!({ "short_name": "bounded" }),
    )
    .await;
    let link = issued["short_link"].as_str().unwrap().to_owned();

    // Another tenant can neither move the link nor learn that it exists.
    let theirs = {
        let create = req_as(
            "globex",
            "POST",
            "/v1/files",
            Body::from(json!({ "path": "x.html", "content_type": "text/html" }).to_string()),
        );
        let response = service.clone().oneshot(create).await.unwrap();
        json_body(response).await["id"].as_str().unwrap().to_owned()
    };
    let (status, _) = issue_as(
        &service,
        "globex",
        &theirs,
        "render-url",
        json!({ "short_link": link }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Unknown, malformed and wrong-listener requests are all a plain 404.
    for path in ["/a/bounded-2222", "/a/bounded", "/a/", "/a/bounded-k7q0"] {
        let (status, _) = view(&render, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
    let (status, _) = view(&service, &format!("/a/{link}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Requests that cannot be honored say why, before minting anything.
    for body in [
        json!({ "short_name": "two words" }),
        json!({ "short_name": "-edge" }),
        json!({ "short_name": "a", "short_link": link }),
    ] {
        let (status, refused) = issue(&service, &id, "render-url", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} -> {refused}");
    }
    let (status, _) = issue(
        &service,
        &id,
        "render-url",
        json!({ "short_link": "nope-2222" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Revoking the grant closes the short link with it.
    let (status, _) = view(&render, &format!("/a/{link}")).await;
    assert_eq!(status, StatusCode::OK);
    let grant_id = issued["grant_id"].as_str().unwrap();
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
    let (status, _) = view(&render, &format!("/a/{link}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// POST an access change as `tenant`, returning status and body.
async fn set_access_as(
    router: &axum::Router,
    tenant: &str,
    id: &str,
    access: &str,
) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(req_as(
            tenant,
            "POST",
            &format!("/v1/files/{id}/access"),
            Body::from(json!({ "access": access }).to_string()),
        ))
        .await
        .unwrap();
    let status = response.status();
    (status, json_body(response).await)
}

#[tokio::test]
async fn public_files_render_with_no_grant_until_made_private() {
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let id = upload_html(&service).await;
    let public_path = format!("/v1/public/{id}");

    // Private by default: the public route knows nothing about it.
    let (status, _) = view(&render, &public_path).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, record) = set_access_as(&service, "acme", &id, "public").await;
    assert_eq!(status, StatusCode::OK, "{record}");
    assert_eq!(record["access"], json!("public"));

    let response = render
        .clone()
        .oneshot(anonymous(&public_path))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    let csp = headers["content-security-policy"].to_str().unwrap();
    assert!(csp.starts_with("sandbox allow-scripts"), "{csp}");
    assert!(!csp.contains("allow-same-origin"), "{csp}");
    // Never cached, so taking a page private again is immediate.
    assert_eq!(headers["cache-control"], "no-store");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], PAGE);

    // The service origin still downloads public HTML instead of running it.
    let response = service
        .clone()
        .oneshot(anonymous(&format!("/v1/files/{id}/content")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let disposition = response.headers()["content-disposition"].to_str().unwrap();
    assert!(disposition.starts_with("attachment"), "{disposition}");

    // Back to private: the permanent address closes on the next request.
    let (status, record) = set_access_as(&service, "acme", &id, "private").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["access"], json!("private"));
    let (status, _) = view(&render, &public_path).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn access_changes_belong_to_the_owner_and_name_a_real_level() {
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let id = upload_html(&service).await;

    // Another tenant cannot publish this file, nor learn it exists.
    let (status, _) = set_access_as(&service, "globex", &id, "public").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = view(&render, &format!("/v1/public/{id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, refused) = set_access_as(&service, "acme", &id, "everyone").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");

    // Unknown and malformed ids on the public route are a plain 404.
    for path in [
        "/v1/public/01m5pbtfswyy7b3etqjaeqbvqe",
        "/v1/public/not-an-id",
    ] {
        let (status, _) = view(&render, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }

    // A deleted file cannot be made public.
    let removed = service
        .clone()
        .oneshot(req("DELETE", &format!("/v1/files/{id}"), Body::empty()))
        .await
        .unwrap();
    assert!(removed.status().is_success(), "{}", removed.status());
    let (status, _) = set_access_as(&service, "acme", &id, "public").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_configured_gallery_may_frame_rendered_pages() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs)
        .with_render_origin(Some(RENDER_ORIGIN.to_owned()))
        .with_render_frame_ancestors(vec!["http://gallery.example.test".to_owned()]);
    let (service, render) = (build_router(state.clone()), render_router(state));
    let id = upload_html(&service).await;

    let (_, issued) = issue(&service, &id, "render-url", json!({})).await;
    set_access_as(&service, "acme", &id, "public").await;
    for path in [
        path_of(issued["url"].as_str().unwrap()).to_owned(),
        format!("/v1/public/{id}"),
    ] {
        let response = render.clone().oneshot(anonymous(&path)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let csp = response.headers()["content-security-policy"]
            .to_str()
            .unwrap();
        assert!(
            csp.ends_with("frame-ancestors http://gallery.example.test"),
            "{path}: {csp}"
        );
        assert!(!csp.contains("allow-same-origin"), "{csp}");
        assert!(
            response.headers().get("x-frame-options").is_none(),
            "{path}"
        );
    }

    // Without the setting, nothing may frame a page.
    let (service, render, _dir) = stack(Some(RENDER_ORIGIN)).await;
    let id = upload_html(&service).await;
    let (_, issued) = issue(&service, &id, "render-url", json!({})).await;
    let response = render
        .clone()
        .oneshot(anonymous(path_of(issued["url"].as_str().unwrap())))
        .await
        .unwrap();
    assert_eq!(response.headers()["x-frame-options"], "DENY");
    let csp = response.headers()["content-security-policy"]
        .to_str()
        .unwrap();
    assert!(csp.ends_with("frame-ancestors 'none'"), "{csp}");
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
