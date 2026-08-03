//! The contract-first REST face under `/v1c`: janus's RestRouter
//! answering through the same dispatcher as GraphQL and MCP. The
//! hand-written `/v1` routes stay canonical; these tests hold the
//! generated face to the same answers.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs);
    (build_router(state), dir)
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

async fn upload(router: &axum::Router, path: &str) -> String {
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
        Body::from(&b"generated face"[..]),
    );
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn the_generated_face_answers_what_the_hand_face_answers() {
    let (router, _dir) = stack().await;
    let id = upload(&router, "twin/proof.txt").await;

    // Listing parity: the same record appears on both faces.
    let hand = router
        .clone()
        .oneshot(req("GET", "/v1/files", Body::empty()))
        .await
        .unwrap();
    let generated = router
        .clone()
        .oneshot(req("GET", "/v1c/files", Body::empty()))
        .await
        .unwrap();
    assert_eq!(generated.status(), StatusCode::OK);
    let hand_items = json_body(hand).await["items"].clone();
    let generated_items = json_body(generated).await["items"].clone();
    let find = |items: &Value| {
        items
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == json!(id))
            .cloned()
            .expect("the uploaded record lists")
    };
    let hand_row = find(&hand_items);
    let generated_row = find(&generated_items);
    for field in ["id", "path", "state", "content_type"] {
        assert_eq!(
            hand_row[field], generated_row[field],
            "listing field {field} agrees across faces",
        );
    }

    // Get parity on the core fields.
    let generated = router
        .clone()
        .oneshot(req("GET", &format!("/v1c/files/{id}"), Body::empty()))
        .await
        .unwrap();
    assert_eq!(generated.status(), StatusCode::OK);
    let row = json_body(generated).await;
    assert_eq!(row["id"], json!(id));
    assert_eq!(row["path"], json!("twin/proof.txt"));

    // An action through the generated face: a grant issues and its
    // URL redeems on the canonical byte route.
    let issued = router
        .clone()
        .oneshot(req(
            "POST",
            &format!("/v1c/files/{id}/url"),
            Body::from(json!({ "ttl_secs": 60 }).to_string()),
        ))
        .await
        .unwrap();
    assert_eq!(issued.status(), StatusCode::OK);
    let grant = json_body(issued).await;
    let url = grant["url"].as_str().expect("grant carries a url");
    let redeemed = router
        .clone()
        .oneshot(Request::builder().uri(url).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(redeemed.status(), StatusCode::OK);
    let bytes = redeemed.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"generated face");
}

#[tokio::test]
async fn the_generated_face_serves_contract_queries() {
    let (router, _dir) = stack().await;
    upload(&router, "search/target.txt").await;
    let response = router
        .clone()
        .oneshot(req("GET", "/v1c/search?q=anything&limit=3", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "the query face answers");
    assert!(json_body(response).await["items"].is_array());
}

#[tokio::test]
async fn the_generated_face_refuses_like_a_rest_face() {
    let (router, _dir) = stack().await;

    let unknown = router
        .clone()
        .oneshot(req("GET", "/v1c/nothing", Body::empty()))
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

    let anonymous = Request::builder()
        .method("GET")
        .uri("/v1c/files")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(anonymous).await.unwrap();
    assert!(
        response.status().is_client_error(),
        "anonymous refused: {}",
        response.status(),
    );
}
