//! The MCP face: the generated manifest served at tools/list, and
//! tools/call enforcing exactly what every other face enforces.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::app::admin_router;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some("root".to_owned()),
        admin_token_previous: None,
        operator_header: None,
    });
    (build_router(state.clone()), admin_router(state), dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn mint(admin: &axum::Router, scopes: &[&str]) -> String {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/keys")
        .header("x-copal-admin-token", "root")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "name": format!("k-{}", ulid::Ulid::generate()), "scopes": scopes })
                .to_string(),
        ))
        .unwrap();
    json_body(admin.clone().oneshot(request).await.unwrap()).await["token"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn rpc(router: &axum::Router, token: &str, body: Value) -> Value {
    let request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json_body(response).await
}

/// The handshake and the manifest: initialize speaks the protocol,
/// and tools/list serves exactly what the generator derives from the
/// contract.
#[tokio::test]
async fn initialize_and_list_serve_the_generated_manifest() {
    let (router, admin, _dir) = stack().await;
    let token = mint(&admin, &["read"]).await;

    let body = rpc(
        &router,
        &token,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }),
    )
    .await;
    assert_eq!(body["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(body["result"]["serverInfo"]["name"], "copal");

    let body = rpc(
        &router,
        &token,
        json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
    )
    .await;
    let generated = kayak::generate_mcp_tools(&copal_server::contract::contract())
        .expect("the contract validates; the drift gate enforces it");
    assert_eq!(body["result"], generated, "the manifest IS the artifact");
}

/// A tool call runs through the dispatcher: files uploaded over REST
/// appear through files_list, and search answers as a query tool.
#[tokio::test]
async fn tool_calls_dispatch_through_the_contract() {
    let (router, admin, _dir) = stack().await;
    let token = mint(&admin, &["read", "write"]).await;

    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "path": "agent-visible.txt" }).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = rpc(
        &router,
        &token,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "files_list", "arguments": { "limit": 10 } },
        }),
    )
    .await;
    assert_eq!(body["result"]["isError"], false, "{body:#?}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("agent-visible.txt"), "{text}");

    let body = rpc(
        &router,
        &token,
        json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": { "name": "search", "arguments": { "q": "anything" } },
        }),
    )
    .await;
    assert_eq!(body["result"]["isError"], false, "{body:#?}");
    let text = body["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("lexical"),
        "no embedder means lexical: {text}"
    );
}

/// The contract's scopes bind agents exactly as they bind everyone:
/// a read-only key calling a write tool is refused by the
/// dispatcher, and the refusal names the scope.
#[tokio::test]
async fn scopes_bind_agents_too() {
    let (router, admin, _dir) = stack().await;
    let writer = mint(&admin, &["read", "write"]).await;
    let reader = mint(&admin, &["read"]).await;

    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("authorization", format!("Bearer {writer}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "path": "guarded-from-agents.txt" }).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    let body = rpc(
        &router,
        &reader,
        json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": { "name": "file_remove", "arguments": { "id": id } },
        }),
    )
    .await;
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("write"),
        "the refusal names the scope: {body:#?}"
    );

    // An unknown tool is a caller error, named as one.
    let body = rpc(
        &router,
        &reader,
        json!({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": { "name": "nonexistent_tool", "arguments": {} },
        }),
    )
    .await;
    assert_eq!(body["error"]["code"], -32602, "{body:#?}");
}

/// The loop the audit found missing: an agent creates a file,
/// obtains an upload grant, delivers bytes, and reads its own work
/// back, all through tools plus one grant URL.
#[tokio::test]
async fn an_agent_ingests_end_to_end() {
    let (router, admin, _dir) = stack().await;
    let token = mint(&admin, &["read", "write"]).await;

    let body = rpc(
        &router,
        &token,
        json!({
            "jsonrpc": "2.0", "id": 10, "method": "tools/call",
            "params": { "name": "file_create", "arguments": {
                "path": "agent-authored.txt",
                "content_type": "text/plain",
            } },
        }),
    )
    .await;
    assert_eq!(body["result"]["isError"], false, "{body:#?}");
    let record: Value =
        serde_json::from_str(body["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    let id = record["id"].as_str().unwrap().to_owned();

    let body = rpc(
        &router,
        &token,
        json!({
            "jsonrpc": "2.0", "id": 11, "method": "tools/call",
            "params": { "name": "file_issue_upload_url", "arguments": { "id": id } },
        }),
    )
    .await;
    assert_eq!(body["result"]["isError"], false, "{body:#?}");
    let grant: Value =
        serde_json::from_str(body["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    let url = grant["url"].as_str().unwrap().to_owned();

    // The grant URL is the byte path; the agent PUTs to it directly.
    let put = Request::builder()
        .method("PUT")
        .uri(url)
        .body(Body::from(b"authored by an agent".to_vec()))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "the grant accepts bytes");

    let body = rpc(
        &router,
        &token,
        json!({
            "jsonrpc": "2.0", "id": 12, "method": "tools/call",
            "params": { "name": "file_get", "arguments": { "id": id } },
        }),
    )
    .await;
    let fetched: Value =
        serde_json::from_str(body["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(fetched["state"], "ready", "{fetched:#?}");
    assert_eq!(fetched["path"], "agent-authored.txt");
}

/// A get for a row that does not exist fails the call, as REST
/// answers 404, instead of succeeding with a null body.
#[tokio::test]
async fn a_missing_row_is_an_error_not_a_null_success() {
    let (router, admin, _dir) = stack().await;
    let token = mint(&admin, &["read"]).await;
    let absent = ulid::Ulid::generate().to_string().to_ascii_lowercase();

    let body = rpc(
        &router,
        &token,
        json!({
            "jsonrpc": "2.0", "id": 9, "method": "tools/call",
            "params": { "name": "file_get", "arguments": { "id": absent } },
        }),
    )
    .await;
    assert!(body.get("result").is_none(), "{body:#?}");
    let message = body["error"]["message"].as_str().unwrap();
    assert!(message.contains("not found"), "{message}");
    assert!(message.contains(&absent), "{message}");
}
