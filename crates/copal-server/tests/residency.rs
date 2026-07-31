//! Storage residencies end to end: pin a tenant, land bytes in the
//! pinned backend, serve from the linked row, reassign safely, and
//! collect garbage in the right backend.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::app::{build_router, AppState, Residencies};
use copal_server::auth::AuthConfig;
use copal_server::sweeps::{run_pass, SweepConfig};
use copal_store::{Store, StoreConfig};

struct Stack {
    router: axum::Router,
    store: Store,
    residencies: Residencies<ObjectStore>,
    local_dir: tempfile::TempDir,
    eu_dir: tempfile::TempDir,
}

async fn stack() -> Stack {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let local_dir = tempfile::tempdir().unwrap();
    let eu_dir = tempfile::tempdir().unwrap();
    let local = ObjectStore::open(local_dir.path().to_str().unwrap()).unwrap();
    let eu = ObjectStore::open(eu_dir.path().to_str().unwrap()).unwrap();
    let named = std::collections::HashMap::from([("eu".to_owned(), eu)]);
    let state = AppState::new(store.clone(), local)
        .with_auth(AuthConfig {
            admin_token: Some("root".to_owned()),
            ..AuthConfig::default()
        })
        .with_residencies(named);
    let residencies = state.residencies.clone();
    Stack {
        router: build_router(state),
        store,
        residencies,
        local_dir,
        eu_dir,
    }
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
    if method == "POST" || method == "PUT" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

/// Object files under a root's `objects/` tree.
fn object_count(dir: &tempfile::TempDir) -> usize {
    fn walk(path: &std::path::Path, found: &mut usize) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, found);
                } else {
                    *found += 1;
                }
            }
        }
    }
    let mut found = 0;
    walk(&dir.path().join("objects"), &mut found);
    found
}

async fn upload(router: &axum::Router, path: &str, content: &[u8]) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/plain" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let mut put = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(content.to_vec()),
    );
    put.headers_mut().remove("content-type");
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn pinned_tenants_land_serve_and_collect_in_their_residency() {
    let stack = stack().await;
    let router = &stack.router;

    // An unknown residency refuses; a configured one assigns.
    let assign = Request::builder()
        .method("PUT")
        .uri("/v1/admin/tenants/acme/storage")
        .header("x-copal-admin-token", "root")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "residency": "mars" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(assign).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let assign = Request::builder()
        .method("PUT")
        .uri("/v1/admin/tenants/acme/storage")
        .header("x-copal-admin-token", "root")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "residency": "eu" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(assign).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Upload: bytes land in the eu root and nowhere else.
    let payload = b"resident bytes stay put";
    let id = upload(router, "docs/resident.txt", payload).await;
    assert_eq!(object_count(&stack.eu_dir), 1, "eu holds the object");
    assert_eq!(object_count(&stack.local_dir), 0, "local holds nothing");

    // The record names its residency and content serves from it.
    let meta = req("GET", &format!("/v1/files/{id}"), Body::empty());
    let record = json_body(router.clone().oneshot(meta).await.unwrap()).await;
    assert_eq!(record["state"], "ready");
    let download = req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    let response = router.clone().oneshot(download).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "download body: {} record: {record}",
        String::from_utf8_lossy(&bytes),
    );
    assert_eq!(&bytes[..], payload);

    // Reassignment to local: the OLD file keeps serving from eu, the
    // NEXT upload lands locally.
    let assign = Request::builder()
        .method("PUT")
        .uri("/v1/admin/tenants/acme/storage")
        .header("x-copal-admin-token", "root")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "residency": "local" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(assign).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let second = upload(router, "docs/second.txt", b"now local").await;
    assert_eq!(
        object_count(&stack.local_dir),
        1,
        "local holds the new object"
    );
    let download = req("GET", &format!("/v1/files/{id}/content"), Body::empty());
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "old file still serves from eu"
    );
    let download = req("GET", &format!("/v1/files/{second}/content"), Body::empty());
    let response = router.clone().oneshot(download).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Deleting the eu file and sweeping with zero grace removes the
    // bytes from the eu root, through the row's residency.
    let remove = req("DELETE", &format!("/v1/files/{id}"), Body::empty());
    let response = router.clone().oneshot(remove).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let zero = SweepConfig {
        gc_grace_secs: 0,
        ..SweepConfig::default()
    };
    // Two passes: the first marks unreferenced, the second collects.
    run_pass(&stack.store, &stack.residencies, &zero).await;
    let report = run_pass(&stack.store, &stack.residencies, &zero).await;
    assert_eq!(report.blobs_collected, 1);
    assert_eq!(object_count(&stack.eu_dir), 0, "eu bytes collected");
    assert_eq!(object_count(&stack.local_dir), 1, "local object untouched");
}
