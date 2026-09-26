//! `facets` is declared as a list of field names. Every face takes it
//! in that shape: REST as the key repeated, MCP as a JSON array. The
//! comma-separated string the GraphQL schema and generated clients
//! send keeps working beside it.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::ExtensionPolicy;
use copal_server::app::Residencies;
use copal_server::pipeline::standard_registry;
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn searchable() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let registry = standard_registry(
        store.clone(),
        Residencies::local_only(blobs.clone()),
        ExtensionPolicy::standard(),
        false,
        None,
        None,
        None,
        std::collections::HashMap::new(),
        copal_server::pipeline::FetchPolicy::default(),
        copal_server::tiering::Topology::default(),
        Default::default(),
    );
    let state = AppState::new(store, blobs).with_flow(registry);
    let engine = state.flow.clone();
    let router = build_router(state);
    for (path, content_type, body) in [
        ("a.txt", "text/plain", "a routine inspection log"),
        ("b.md", "text/markdown", "inspection procedures"),
    ] {
        let create = req(
            "POST",
            "/v1/files",
            Body::from(json!({ "path": path, "content_type": content_type }).to_string()),
        );
        let id = json_body(router.clone().oneshot(create).await.unwrap()).await["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let put = Request::builder()
            .method("PUT")
            .uri(format!("/v1/files/{id}/content"))
            .header("x-copal-tenant", "acme")
            .header("content-length", body.len().to_string())
            .body(Body::from(body.as_bytes().to_vec()))
            .unwrap();
        assert_eq!(
            router.clone().oneshot(put).await.unwrap().status(),
            StatusCode::OK,
        );
    }
    while engine.tick("w").await.unwrap() {}
    (router, dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
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

/// Call the MCP search tool and return the JSON-RPC answer.
async fn mcp_search(router: &axum::Router, facets: Value) -> Value {
    let call = req(
        "POST",
        "/mcp",
        Body::from(
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": { "q": "inspection", "facets": facets },
                },
            })
            .to_string(),
        ),
    );
    json_body(router.clone().oneshot(call).await.unwrap()).await
}

fn counted(body: &Value) -> Vec<&str> {
    let mut fields: Vec<&str> = body["facets"]
        .as_object()
        .map(|facets| facets.keys().map(String::as_str).collect())
        .unwrap_or_default();
    fields.sort_unstable();
    fields
}

#[tokio::test]
async fn rest_reads_the_facets_key_repeated() {
    let (router, _dir) = searchable().await;
    for query in [
        "q=inspection&facets=content_type&facets=access",
        "q=inspection&facets=content_type,access",
    ] {
        let response = router
            .clone()
            .oneshot(req("GET", &format!("/v1/search?{query}"), Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{query}");
        let body = json_body(response).await;
        assert_eq!(counted(&body), vec!["access", "content_type"], "{query}");
    }
}

#[tokio::test]
async fn mcp_reads_facets_as_the_list_it_declares() {
    let (router, _dir) = searchable().await;

    let answer = mcp_search(&router, json!(["content_type", "access"])).await;
    let text = answer["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("{answer:#?}"));
    let body: Value = serde_json::from_str(text).unwrap();
    assert_eq!(counted(&body), vec!["access", "content_type"]);
    let plain = body["facets"]["content_type"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["value"] == "text/plain")
        .unwrap_or_else(|| panic!("{body:#?}"));
    assert_eq!(plain["files"], 1);

    // The string spelling still counts, and a list of anything but
    // field names refuses instead of dropping the counts.
    let answer = mcp_search(&router, json!("access")).await;
    let text = answer["result"]["content"][0]["text"].as_str().unwrap();
    let body: Value = serde_json::from_str(text).unwrap();
    assert_eq!(counted(&body), vec!["access"]);
    let answer = mcp_search(&router, json!([7])).await;
    assert!(answer.get("result").is_none(), "{answer:#?}");
    assert!(answer["error"]["message"]
        .as_str()
        .unwrap()
        .contains("facets"));
}
