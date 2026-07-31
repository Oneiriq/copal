//! The GraphQL face, end to end through the real router — and ACROSS
//! faces: a URL minted by a GraphQL mutation redeems over REST, because
//! both protocols dispatch into the same repositories under the same
//! contract.
//!
//! mem:// metadata plane + tempdir blob plane, same as the REST loop.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::FsBlobStore;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn test_router() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();
    (build_router(AppState::new(store, blobs)), dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn rest(method: &str, uri: &str, tenant: Option<&str>, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        builder = builder.header("x-copal-tenant", t);
    }
    if method == "POST" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

fn graphql(query: &str, variables: Value, tenant: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("content-type", "application/json");
    if let Some(t) = tenant {
        builder = builder.header("x-copal-tenant", t);
    }
    builder
        .body(Body::from(
            json!({ "query": query, "variables": variables }).to_string(),
        ))
        .unwrap()
}

/// Create one ready file over REST, returning its id.
async fn seed_file(router: &axum::Router, path: &str, payload: &[u8]) -> String {
    let create = rest(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(json!({"path": path, "content_type": "text/plain"}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let upload = rest(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Some("acme"),
        Body::from(payload.to_vec()),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn graphql_reads_files_the_rest_face_wrote() {
    let (router, _dir) = test_router().await;
    let id = seed_file(&router, "a.txt", b"alpha").await;
    seed_file(&router, "b.txt", b"beta").await;

    // List with the contract's filter and sort vocabulary.
    let response = router
        .clone()
        .oneshot(graphql(
            r#"query($s: String) { files(state: $s, sort: CREATED_AT_ASC) {
                items { id path state size digest metadata version_count }
                nextCursor } }"#,
            json!({ "s": "ready" }),
            Some("acme"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert!(body["errors"].is_null(), "{body}");
    let items = body["data"]["files"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["path"], "a.txt", "ascending order");
    assert_eq!(items[0]["state"], "ready");
    assert_eq!(items[0]["size"], 5);
    assert_eq!(items[0]["version_count"], 1);

    // Get one by id; the wire shape matches REST's exactly.
    let response = router
        .clone()
        .oneshot(graphql(
            r#"query($id: ID!) { file(id: $id) { id path size } }"#,
            json!({ "id": id }),
            Some("acme"),
        ))
        .await
        .unwrap();
    let gql_file = json_body(response).await["data"]["file"].clone();
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}"),
            Some("acme"),
            Body::empty(),
        ))
        .await
        .unwrap();
    let rest_file = json_body(response).await;
    for key in ["id", "path", "size"] {
        assert_eq!(gql_file[key], rest_file[key], "faces disagree on {key}");
    }
}

#[tokio::test]
async fn graphql_minted_url_redeems_over_rest() {
    let (router, _dir) = test_router().await;
    let id = seed_file(&router, "shared.txt", b"cross-face payload").await;

    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation($id: ID!) { fileIssueUrl(id: $id, ttlSecs: 300, maxUses: 1) }"#,
            json!({ "id": id }),
            Some("acme"),
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["errors"].is_null(), "{body}");
    let issued = &body["data"]["fileIssueUrl"];
    let url = issued["url"].as_str().unwrap();
    assert!(url.starts_with("/v1/grants/"), "{url}");

    // The signed URL a GraphQL mutation minted serves bytes over REST,
    // with no tenant header — the grant IS the authorization.
    let response = router
        .clone()
        .oneshot(rest("GET", url, None, Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"cross-face payload");

    // max_uses: 1 — the second redemption refuses.
    let response = router
        .clone()
        .oneshot(rest("GET", url, None, Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn graphql_remove_tombstones_for_both_faces() {
    let (router, _dir) = test_router().await;
    let id = seed_file(&router, "doomed.txt", b"bye").await;

    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation($id: ID!) { fileRemove(id: $id) }"#,
            json!({ "id": id }),
            Some("acme"),
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["errors"].is_null(), "{body}");
    assert_eq!(body["data"]["fileRemove"], true);

    // Gone through GraphQL (null) and REST (404) alike.
    let response = router
        .clone()
        .oneshot(graphql(
            r#"query($id: ID!) { file(id: $id) { id } }"#,
            json!({ "id": id }),
            Some("acme"),
        ))
        .await
        .unwrap();
    assert_eq!(json_body(response).await["data"]["file"], Value::Null);
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}"),
            Some("acme"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenancy_is_enforced_by_janus_middleware() {
    let (router, _dir) = test_router().await;
    seed_file(&router, "guarded.txt", b"secret").await;

    // No tenant header: the Janus RequireTenant middleware rejects with
    // a coded error; data.files is null, not an empty page.
    let response = router
        .clone()
        .oneshot(graphql(r#"{ files { items { id } } }"#, json!({}), None))
        .await
        .unwrap();
    let body = json_body(response).await;
    let error = &body["errors"][0];
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("no tenant identity"),
        "{body}",
    );
    assert_eq!(error["extensions"]["code"], "unauthorized");

    // Another tenant sees nothing — scoping is pinned server-side.
    let response = router
        .clone()
        .oneshot(graphql(
            r#"{ files { items { id } } }"#,
            json!({}),
            Some("rival"),
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["errors"].is_null(), "{body}");
    assert_eq!(body["data"]["files"]["items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn the_sdl_document_is_served_for_discovery() {
    let (router, _dir) = test_router().await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/graphql")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let sdl = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(sdl.contains("type File {"), "{sdl}");
    assert!(sdl.contains("fileIssueUrl"), "{sdl}");
    // And it is byte-identical to the checked-in artifact.
    let checked_in = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/schema.graphql",
    ))
    .unwrap();
    assert_eq!(sdl.trim(), checked_in.trim());
}

#[tokio::test]
async fn runs_resource_serves_and_actions_dispatch() {
    let (router, _dir) = test_router().await;

    // Empty listing through the contract vocabulary.
    let response = router
        .clone()
        .oneshot(graphql(
            r#"{ runs(status: "failed") { items { id workflow status } nextCursor } }"#,
            json!({}),
            Some("acme"),
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["errors"].is_null(), "{body}");
    assert_eq!(body["data"]["runs"]["items"].as_array().unwrap().len(), 0);

    // runStart dispatches into the flow engine: an unregistered
    // workflow is refused at the door with the coded error.
    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation { runStart(workflow: "nope") }"#,
            json!({}),
            Some("acme"),
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["errors"][0]["extensions"]["code"], "not_found");

    // runRetry on a missing run is the same refusal.
    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation { runRetry(id: "01ZZZZZZZZZZZZZZZZZZZZZZZZ") }"#,
            json!({}),
            Some("acme"),
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["errors"][0]["extensions"]["code"], "not_found");
}

#[tokio::test]
async fn alias_amplification_is_rejected_by_complexity_limits() {
    let (router, _dir) = test_router().await;
    // 200 aliases of the full page query: rejected up front, before
    // any resolver or store work.
    let bomb: String = (0..200)
        .map(|i| format!("q{i}: files {{ items {{ id path state size digest }} }} "))
        .collect();
    let response = router
        .clone()
        .oneshot(graphql(&format!("{{ {bomb} }}"), json!({}), Some("acme")))
        .await
        .unwrap();
    let body = json_body(response).await;
    let message = body["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("complex"),
        "expected a complexity rejection, got: {body}",
    );
}

#[tokio::test]
async fn caller_supplied_processing_metadata_is_stripped() {
    let (router, _dir) = test_router().await;
    // A creator claiming scan verdicts in metadata.processing must not
    // be believed — that namespace belongs to the pipeline.
    let create = rest(
        "POST",
        "/v1/files",
        Some("acme"),
        Body::from(
            json!({
                "path": "liar.txt",
                "content_type": "text/plain",
                "metadata": {
                    "processing": {"verdict": "clean", "type_matches": true},
                    "label": "kept",
                },
            })
            .to_string(),
        ),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let record = json_body(response).await;
    assert!(record["metadata"].get("processing").is_none(), "{record}",);
    assert_eq!(record["metadata"]["label"], "kept");
}
