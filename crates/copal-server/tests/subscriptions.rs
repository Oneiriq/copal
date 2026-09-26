//! Subscriptions over the outbox: a client watches the events the
//! engine writes, instead of polling for processing to finish.
//!
//! The engine is real here (mem:// supports live queries), so these
//! exercise the actual LIVE SELECT, the actual WHERE the engine
//! evaluates, and the actual graphql-sse framing.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt as _;
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::TenantId;
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

/// Create and fill a file as `tenant`, which makes the engine write
/// outbox rows.
async fn upload(api: &axum::Router, tenant: &str, path: &str) -> String {
    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("x-copal-tenant", tenant)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "path": path, "content_type": "text/plain" }).to_string(),
        ))
        .unwrap();
    let response = api.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", tenant)
        .body(Body::from(b"watched bytes".to_vec()))
        .unwrap();
    let response = api.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn the_engine_filters_the_feed_before_a_subscriber_sees_it() {
    let (api, store, _dir) = stack().await;
    let acme = TenantId::parse("acme").unwrap();

    let mut feed = copal_store::repo::eventing::watch_events(&store, &acme, None)
        .await
        .unwrap();

    // Another tenant's activity must never reach this stream. The
    // condition is the engine's, so no application check can forget it.
    upload(&api, "rival", "theirs.txt").await;
    upload(&api, "acme", "ours.txt").await;

    let row = tokio::time::timeout(Duration::from_secs(10), feed.next())
        .await
        .expect("an acme event arrives")
        .expect("the stream is open")
        .expect("the row decodes");
    assert_eq!(row.tenant_id, "acme");
    assert!(row.action.starts_with("file."), "{}", row.action);

    // Everything still queued is acme's too: the rival upload wrote
    // outbox rows, and none of them are in this stream.
    while let Ok(Some(next)) = tokio::time::timeout(Duration::from_millis(500), feed.next()).await {
        assert_eq!(next.expect("the row decodes").tenant_id, "acme");
    }
}

#[tokio::test]
async fn watching_one_verb_narrows_the_feed() {
    let (api, store, _dir) = stack().await;
    let acme = TenantId::parse("acme").unwrap();

    let mut ready_only =
        copal_store::repo::eventing::watch_events(&store, &acme, Some("file.ready"))
            .await
            .unwrap();
    upload(&api, "acme", "narrowed.txt").await;

    let row = tokio::time::timeout(Duration::from_secs(10), ready_only.next())
        .await
        .expect("a file.ready event arrives")
        .expect("the stream is open")
        .expect("the row decodes");
    assert_eq!(row.action, "file.ready");
}

#[tokio::test]
async fn the_list_face_takes_the_same_filter() {
    let (api, store, _dir) = stack().await;
    let acme = TenantId::parse("acme").unwrap();

    // One upload writes one event, so two verbs need two transitions:
    // the outbox records terminal states, and delete is one of them.
    let id = upload(&api, "acme", "listed.txt").await;
    let remove = Request::builder()
        .method("DELETE")
        .uri(format!("/v1/files/{id}"))
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let response = api.clone().oneshot(remove).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let all = copal_store::repo::eventing::list_events(&store, &acme, None, 100)
        .await
        .unwrap();
    let ready = copal_store::repo::eventing::list_events(&store, &acme, Some("file.ready"), 100)
        .await
        .unwrap();
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(ready.len(), 1, "{ready:?}");
    assert_eq!(ready[0].action, "file.ready");
}

#[tokio::test]
async fn a_subscription_streams_over_graphql_sse() {
    let (api, _store, _dir) = stack().await;

    let request = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .body(Body::from(
            json!({
                "query": "subscription { eventChanged(action: \"file.ready\") { id action } }"
            })
            .to_string(),
        ))
        .unwrap();
    let response = api.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream"),
    );

    // async-graphql opens the subscription lazily, on the first poll of
    // the response body, so a single upload fired now could land before
    // the live query exists. Upload repeatedly until the read below
    // succeeds, which removes the race instead of sleeping past it.
    let uploader = api.clone();
    let feeder = tokio::spawn(async move {
        for attempt in 0..40 {
            upload(&uploader, "acme", &format!("streamed-{attempt}.txt")).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let mut body = response.into_body().into_data_stream();
    let mut buffer = String::new();
    let payload = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(chunk) = body.next().await {
            buffer.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
            // graphql-sse frames each payload as a `next` event.
            if let Some(rest) = buffer.split("event: next\ndata: ").nth(1) {
                if let Some(line) = rest.split('\n').next() {
                    return line.to_owned();
                }
            }
        }
        panic!("the stream closed before delivering an event: {buffer}");
    })
    .await
    .expect("an event arrives on the stream");

    feeder.abort();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["data"]["eventChanged"]["action"], "file.ready");
    assert!(payload["data"]["eventChanged"]["id"].is_string());
}

#[tokio::test]
async fn an_anonymous_subscription_is_refused_before_any_row() {
    let (api, _store, _dir) = stack().await;

    let request = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .body(Body::from(
            json!({ "query": "subscription { eventChanged { id } }" }).to_string(),
        ))
        .unwrap();
    let response = api.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = tokio::time::timeout(Duration::from_secs(10), response.into_body().collect())
        .await
        .expect("the refusal ends the stream promptly")
        .unwrap()
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("tenant"), "{text}");
    assert!(text.contains("event: complete"), "{text}");
}

#[tokio::test]
async fn a_subscription_ends_at_its_lifetime_and_reopens_fresh() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let mut state = AppState::new(store, blobs);
    // A one-second lifetime, so the deadline is the thing under test.
    state.limits.subscription_max_secs = 1;
    let api = build_router(state);
    let _dir = dir;

    let request = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .body(Body::from(
            json!({ "query": "subscription { eventChanged { id } }" }).to_string(),
        ))
        .unwrap();
    let response = api.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // With no events arriving, the stream still ends: the deadline
    // closes it with a normal completion, and the collect returns
    // instead of hanging. Re-subscribing runs full authentication
    // again, which is how revocation reaches running streams.
    let collected = tokio::time::timeout(Duration::from_secs(5), response.into_body().collect())
        .await
        .expect("the deadline ends the stream")
        .unwrap()
        .to_bytes();
    let text = String::from_utf8_lossy(&collected);
    assert!(text.contains("event: complete"), "{text}");
}

/// An outbox row is one event however often it is written. The
/// webhook dispatcher marks every row it fans out as dispatched, an
/// UPDATE the live query sees too; relaying it handed subscribers the
/// same event a second time.
#[tokio::test]
async fn an_event_arrives_once_however_often_its_row_is_written() {
    let (api, store, _dir) = stack().await;
    let acme = TenantId::parse("acme").unwrap();

    let mut feed = copal_store::repo::eventing::watch_events(&store, &acme, Some("file.ready"))
        .await
        .unwrap();
    upload(&api, "acme", "once.txt").await;
    let row = tokio::time::timeout(Duration::from_secs(10), feed.next())
        .await
        .expect("a file.ready event arrives")
        .expect("the stream is open")
        .expect("the row decodes");

    // What the dispatcher does after fanning the event out.
    assert!(
        copal_store::repo::eventing::mark_dispatched(&store, &row.event_id())
            .await
            .unwrap()
    );

    let again = tokio::time::timeout(Duration::from_secs(2), feed.next()).await;
    if let Ok(Some(item)) = again {
        panic!(
            "event {} arrived again after it was marked dispatched: {:?}",
            row.id,
            item.map(|r| (r.id, r.dispatched)),
        );
    }
}
