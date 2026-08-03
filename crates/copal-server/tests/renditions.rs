//! Derivatives end to end: request, render through the flow engine,
//! serve, repeat idempotently, and refuse non-image sources.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::ExtensionPolicy;
use copal_flow::FlowEngine;
use copal_server::pipeline::standard_registry;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, FlowEngine, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let registry = standard_registry(
        store.clone(),
        copal_server::app::Residencies::local_only(blobs.clone()),
        ExtensionPolicy::standard(),
        false,
        None,
        None,
        None,
        std::collections::HashMap::new(),
    );
    let state = AppState::new(store, blobs).with_flow(registry);
    let engine = state.flow.clone();
    (build_router(state), engine, dir)
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

/// A 64x64 png generated in-process, so the test carries no fixture.
fn source_png() -> Vec<u8> {
    let img = image::RgbImage::from_fn(64, 64, |x, y| {
        image::Rgb([(x * 4) as u8, (y * 4) as u8, 128])
    });
    let mut out = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(img)
        .write_to(&mut out, image::ImageFormat::Png)
        .unwrap();
    out.into_inner()
}

async fn upload(router: &axum::Router, path: &str, content_type: &str, bytes: Vec<u8>) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": content_type }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = req("PUT", &format!("/v1/files/{id}/content"), Body::from(bytes));
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn renditions_render_serve_and_repeat_idempotently() {
    let (router, engine, _dir) = stack().await;
    let id = upload(&router, "photos/sunset.png", "image/png", source_png()).await;
    // The post-upload pipeline finishes the source first.
    assert!(engine.tick("w").await.unwrap());

    let request = req(
        "POST",
        &format!("/v1/files/{id}/renditions"),
        Body::from(
            json!({ "kind": "thumb", "width": 16, "height": 16, "format": "png" }).to_string(),
        ),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let accepted = json_body(response).await;
    let derived_id = accepted["id"].as_str().unwrap().to_owned();
    assert_eq!(accepted["state"], "draft");
    assert!(accepted["run"].is_string());
    assert_eq!(accepted["path"], "photos/sunset.png@thumb-16x16.png");

    // The render runs on the flow engine like any other work.
    assert!(engine.tick("w").await.unwrap());

    let listing = req("GET", &format!("/v1/files/{id}/renditions"), Body::empty());
    let body = json_body(router.clone().oneshot(listing).await.unwrap()).await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], derived_id.as_str());
    assert_eq!(items[0]["state"], "ready");

    // The rendition serves as a real file, and it decodes at the
    // requested bound.
    let download = req(
        "GET",
        &format!("/v1/files/{derived_id}/content"),
        Body::empty(),
    );
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "image/png");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let rendered = image::load_from_memory(&bytes).unwrap();
    assert_eq!(rendered.width(), 16);
    assert_eq!(rendered.height(), 16);

    // The same request again returns the existing record, no new run.
    let repeat = req(
        "POST",
        &format!("/v1/files/{id}/renditions"),
        Body::from(
            json!({ "kind": "thumb", "width": 16, "height": 16, "format": "png" }).to_string(),
        ),
    );
    let response = router.clone().oneshot(repeat).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let repeated = json_body(response).await;
    assert_eq!(repeated["id"], derived_id.as_str());
    assert!(!engine.tick("w").await.unwrap(), "no second render queued");
}

#[tokio::test]
async fn decode_bombs_refuse_instead_of_exhausting_the_host() {
    // A small compressed file describing an enormous canvas: the
    // source passes the byte ceiling, and only the decoder's
    // allocation limit stops it.
    let (router, engine, _dir) = stack().await;
    let bomb = {
        let img = image::GrayImage::new(20_000, 20_000);
        let mut out = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageLuma8(img)
            .write_to(&mut out, image::ImageFormat::Png)
            .unwrap();
        out.into_inner()
    };
    assert!(
        bomb.len() < 32 * 1024 * 1024,
        "the bomb is small on disk ({} bytes) and huge decoded",
        bomb.len(),
    );

    let id = upload(&router, "bombs/huge.png", "image/png", bomb).await;
    assert!(engine.tick("w").await.unwrap());
    let request = req(
        "POST",
        &format!("/v1/files/{id}/renditions"),
        Body::from(json!({}).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let derived_id = json_body(response).await["id"].as_str().unwrap().to_owned();
    assert!(engine.tick("w").await.unwrap());
    let meta = req("GET", &format!("/v1/files/{derived_id}"), Body::empty());
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "failed", "the render refused the bomb");
}

#[tokio::test]
async fn non_image_sources_refuse_and_bad_params_reject() {
    let (router, engine, _dir) = stack().await;

    // A declared image whose bytes are not an image: the request is
    // accepted (declared type passes the gate) and the render itself
    // refuses, failing the derived record with the run completed.
    let id = upload(
        &router,
        "docs/report.png",
        "image/png",
        b"clearly not a png".to_vec(),
    )
    .await;
    assert!(engine.tick("w").await.unwrap());
    let request = req(
        "POST",
        &format!("/v1/files/{id}/renditions"),
        Body::from(json!({}).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let derived_id = json_body(response).await["id"].as_str().unwrap().to_owned();
    assert!(engine.tick("w").await.unwrap());
    let meta = req("GET", &format!("/v1/files/{derived_id}"), Body::empty());
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "failed");

    // A plain non-image declared type rejects at the door.
    let id = upload(
        &router,
        "docs/notes.txt",
        "text/plain",
        b"plain text".to_vec(),
    )
    .await;
    assert!(engine.tick("w").await.unwrap());
    let request = req(
        "POST",
        &format!("/v1/files/{id}/renditions"),
        Body::from(json!({}).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Out-of-range dimensions reject before any record exists.
    let request = req(
        "POST",
        &format!("/v1/files/{id}/renditions"),
        Body::from(json!({ "width": 9999 }).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
