//! Collections that hang off one instance: a file's versions and an
//! endpoint's delivery attempts.
//!
//! Both are declared once in the contract, so the point of these tests
//! is that REST and GraphQL render the same rows. A divergence here is
//! the failure the contract exists to prevent.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::crypto::BlobCipher;
use copal_blob::ObjectStore;
use copal_server::webhooks::{run_pass, webhook_router};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const MASTER_KEY: &str = "5f4e3d2c1b0a99887766554433221100ffeeddccbbaa99887766554433221100";

struct Stack {
    router: axum::Router,
    store: Store,
    cipher: BlobCipher,
    _dir: tempfile::TempDir,
}

async fn stack() -> Stack {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let cipher = BlobCipher::from_hex(MASTER_KEY).unwrap();
    let mut state = AppState::new(store.clone(), blobs).with_cipher(Some(cipher.clone()));
    // A loopback receiver is a private address; an internal deployment
    // opts in the same way.
    state.limits.allow_private_webhook_targets = true;
    let router = build_router(state.clone()).merge(webhook_router(state));
    Stack {
        router,
        store,
        cipher,
        _dir: dir,
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn rest(method: &str, uri: &str, body: Body) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-tenant", "acme");
    if method == "POST" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

fn graphql(query: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": query }).to_string()))
        .unwrap()
}

/// Create a file and fill it, returning the id.
async fn upload(router: &axum::Router, path: &str, content: &[u8]) -> String {
    let create = rest(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/plain" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    put(router, &id, content).await;
    id
}

async fn put(router: &axum::Router, id: &str, content: &[u8]) {
    let request = rest(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(content.to_vec()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_files_versions_read_the_same_on_both_faces() {
    let router = stack().await.router;
    let id = upload(&router, "history.txt", b"first draft").await;
    put(&router, &id, b"second draft").await;

    let listing = rest("GET", &format!("/v1/files/{id}/versions"), Body::empty());
    let over_rest = json_body(router.clone().oneshot(listing).await.unwrap()).await;

    let over_graphql = json_body(
        router
            .clone()
            .oneshot(graphql(&format!(
                r#"{{ file(id: "{id}") {{ versions {{ items {{ id number content_type size
                   digest created_by }} nextCursor }} }} }}"#
            )))
            .await
            .unwrap(),
    )
    .await;
    assert!(over_graphql.get("errors").is_none(), "{over_graphql:#?}",);
    let gql_items = over_graphql["data"]["file"]["versions"]["items"]
        .as_array()
        .unwrap();
    let rest_items = over_rest["items"].as_array().unwrap();

    assert_eq!(rest_items.len(), 2, "{over_rest:#?}");
    assert_eq!(gql_items.len(), rest_items.len());
    // Newest first on both, with the same values in the same order.
    for (a, b) in gql_items.iter().zip(rest_items) {
        for key in [
            "id",
            "number",
            "content_type",
            "size",
            "digest",
            "created_by",
        ] {
            assert_eq!(a[key], b[key], "{key} differs: {a:?} vs {b:?}");
        }
    }
    assert_eq!(gql_items[0]["number"], 2);
}

#[tokio::test]
async fn a_versions_page_resumes_on_the_contract_cursor() {
    let router = stack().await.router;
    let id = upload(&router, "paged.txt", b"one").await;
    put(&router, &id, b"two").await;
    put(&router, &id, b"three").await;

    let first = json_body(
        router
            .clone()
            .oneshot(graphql(&format!(
                r#"{{ file(id: "{id}") {{ versions(limit: 1) {{
                   items {{ number }} nextCursor }} }} }}"#
            )))
            .await
            .unwrap(),
    )
    .await;
    let page = &first["data"]["file"]["versions"];
    assert_eq!(page["items"][0]["number"], 3);
    let cursor = page["nextCursor"].as_str().expect("a full page resumes");

    let second = json_body(
        router
            .clone()
            .oneshot(graphql(&format!(
                r#"{{ file(id: "{id}") {{ versions(limit: 1, cursor: "{cursor}") {{
                   items {{ number }} }} }} }}"#
            )))
            .await
            .unwrap(),
    )
    .await;
    // The cursor resumes strictly below, so no row repeats.
    assert_eq!(second["data"]["file"]["versions"]["items"][0]["number"], 2);
}

#[tokio::test]
async fn an_endpoints_deliveries_read_the_same_on_both_faces() {
    let stack = stack().await;
    let router = stack.router.clone();

    let register = rest(
        "POST",
        "/v1/webhooks",
        Body::from(json!({ "url": "http://127.0.0.1:9/hook" }).to_string()),
    );
    let response = router.clone().oneshot(register).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let endpoint = json_body(response).await["id"].as_str().unwrap().to_owned();

    // An upload writes an outbox row; one dispatch pass fans it out
    // into a delivery. The receiver is a closed port, so the delivery
    // lands in a retrying state, which is a row either way.
    upload(&router, "attempted.txt", b"payload").await;
    let http = reqwest::Client::new();
    run_pass(&stack.store, &stack.cipher, &http, "test-instance", true).await;

    let listing = rest(
        "GET",
        &format!("/v1/webhooks/{endpoint}/deliveries"),
        Body::empty(),
    );
    let over_rest = json_body(router.clone().oneshot(listing).await.unwrap()).await;
    let rest_items = over_rest["items"].as_array().unwrap();
    assert!(!rest_items.is_empty(), "{over_rest:#?}");

    let over_graphql = json_body(
        router
            .clone()
            .oneshot(graphql(&format!(
                r#"{{ webhook(id: "{endpoint}") {{ deliveries {{
                   items {{ id state attempts }} }} }} }}"#
            )))
            .await
            .unwrap(),
    )
    .await;
    assert!(over_graphql.get("errors").is_none(), "{over_graphql:#?}");
    let gql_items = over_graphql["data"]["webhook"]["deliveries"]["items"]
        .as_array()
        .unwrap();
    assert_eq!(gql_items.len(), rest_items.len());
    for (a, b) in gql_items.iter().zip(rest_items) {
        for key in ["id", "state", "attempts"] {
            assert_eq!(a[key], b[key], "{key} differs");
        }
    }

    // The declared filter narrows on both faces. It is checked
    // against the state these rows are actually in, so a filter that
    // silently matched everything would fail the second half.
    let observed = rest_items[0]["state"].as_str().unwrap().to_owned();
    let matching = rest(
        "GET",
        &format!("/v1/webhooks/{endpoint}/deliveries?state={observed}"),
        Body::empty(),
    );
    let matching = json_body(router.clone().oneshot(matching).await.unwrap()).await;
    assert_eq!(
        matching["items"].as_array().unwrap().len(),
        rest_items.len(),
    );

    let other = if observed == "delivered" {
        "failed"
    } else {
        "delivered"
    };
    let empty = json_body(
        router
            .clone()
            .oneshot(graphql(&format!(
                r#"{{ webhook(id: "{endpoint}") {{
                   deliveries(state: "{other}") {{ items {{ state }} }} }} }}"#
            )))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        empty["data"]["webhook"]["deliveries"]["items"]
            .as_array()
            .unwrap()
            .is_empty(),
        "a state nothing is in must return nothing: {empty:#?}",
    );
}

#[tokio::test]
async fn a_sub_collection_is_tenant_scoped() {
    let router = stack().await.router;
    let id = upload(&router, "mine.txt", b"mine").await;

    // Another tenant asking for this file's history gets a refusal,
    // because the parent fetch carries the tenancy.
    let listing = Request::builder()
        .method("GET")
        .uri(format!("/v1/files/{id}/versions"))
        .header("x-copal-tenant", "rival")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(listing).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let over_graphql = json_body(
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("x-copal-tenant", "rival")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "query": format!(
                                r#"{{ file(id: "{id}") {{ versions {{ items {{ number }} }} }} }}"#
                            )
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    // The parent itself is already invisible to the other tenant.
    assert!(over_graphql["data"]["file"].is_null(), "{over_graphql:#?}");
}
