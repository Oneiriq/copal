//! The change feed: a cursor over a tenant's events, so a down
//! indexer resumes from where it stopped instead of re-listing the
//! world. Event ids are ULIDs, so the id order is the time order and
//! a bare id is a stable cursor.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, Store, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs);
    (build_router(state), store, dir)
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

/// Upload one file, which mints its lifecycle events.
async fn upload(router: &axum::Router, path: &str, payload: &[u8]) {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(payload.to_vec()),
    );
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn page(router: &axum::Router, query: &str) -> (Vec<String>, Option<String>) {
    let response = router
        .clone()
        .oneshot(req("GET", &format!("/v1/events?{query}"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    let ids = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap().to_owned())
        .collect();
    let cursor = body["next_cursor"].as_str().map(str::to_owned);
    (ids, cursor)
}

/// The cursor of a full drain: page with limit 1 until the feed
/// dries, keeping the last cursor a full page handed out.
async fn cursor_of_last_page(router: &axum::Router) -> String {
    let mut cursor: Option<String> = None;
    loop {
        let query = match &cursor {
            Some(c) => format!("order=asc&limit=1&cursor={c}"),
            None => "order=asc&limit=1".to_owned(),
        };
        let (_ids, next) = page(router, &query).await;
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    cursor.expect("at least one full page exists")
}

/// Replay walks forward exactly once: every event appears once
/// across pages, no repeats, no gaps, and the feed picks up events
/// written after a drain from the saved cursor.
#[tokio::test]
async fn the_feed_replays_forward_and_resumes() {
    let (router, _store, _dir) = stack().await;
    for i in 0..3 {
        upload(&router, &format!("doc-{i}.txt"), b"content").await;
    }

    // Drain forward in small pages.
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let query = match &cursor {
            Some(c) => format!("order=asc&limit=2&cursor={c}"),
            None => "order=asc&limit=2".to_owned(),
        };
        let (ids, next) = page(&router, &query).await;
        seen.extend(ids);
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert!(seen.len() >= 3, "three uploads mint at least three events");
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), seen.len(), "no repeats across pages");

    // New activity lands after the drain; the saved cursor sees
    // exactly the new events.
    let resume_from = cursor_of_last_page(&router).await;
    upload(&router, "late.txt", b"late content").await;
    let (fresh, _) = page(&router, &format!("order=asc&limit=50&cursor={resume_from}")).await;
    assert!(!fresh.is_empty(), "the resume sees the late events");
    assert!(
        fresh.iter().all(|id| !seen.contains(id)),
        "only events after the cursor: {fresh:?}",
    );
}

/// The default order stays newest-first, and a cursor pages backward
/// through history.
#[tokio::test]
async fn the_default_pages_backward() {
    let (router, _store, _dir) = stack().await;
    for i in 0..3 {
        upload(&router, &format!("back-{i}.txt"), b"content").await;
    }
    let (first_page, cursor) = page(&router, "limit=2").await;
    assert_eq!(first_page.len(), 2);

    let cursor = cursor.expect("more history remains");
    let (second_page, _) = page(&router, &format!("limit=2&cursor={cursor}")).await;
    assert!(!second_page.is_empty());
    assert!(
        second_page.iter().all(|id| !first_page.contains(id)),
        "backward pages never repeat: {second_page:?}",
    );
}

/// The action filter composes with the cursor, and the two faces
/// answer identically through the shared core.
#[tokio::test]
async fn filters_compose_and_faces_agree() {
    let (router, _store, _dir) = stack().await;
    upload(&router, "filtered.txt", b"content").await;

    let (ready_only, _) = page(&router, "order=asc&limit=50&action=file.ready").await;
    assert!(!ready_only.is_empty());

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("x-copal-tenant", "acme")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "query": "{ events(limit: 50, action: \"file.ready\", sort: CREATED_AT_ASC) { items { id action } nextCursor } }"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["errors"].is_null(), "{body:#?}");
    let graphql_ids: Vec<String> = body["data"]["events"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(graphql_ids, ready_only, "one core, two faces");
}
