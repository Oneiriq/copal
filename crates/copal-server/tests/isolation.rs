//! Every door, asked for another tenant's data.
//!
//! Per-feature tests already check their own seam: `auth.rs` that the
//! tenant comes from the key, `search.rs` that a rival's question
//! finds nothing, `sub_resources.rs` that a child list refuses,
//! `graphql.rs` and `subscriptions.rs` the same for their faces. What
//! none of them gives is one place where every route is asked the
//! same question with the same pair of tenants, so a door added later
//! has somewhere obvious to be checked and a door nobody thought
//! about is visible by its absence.
//!
//! Tenant crossing is the worst thing this service can do, so the
//! sweep is written to fail loudly and name the route that let it
//! through.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-secret";

async fn keyed_router() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
        operator_header: None,
    });
    (build_router(state), dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn text_body(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn signed(method: &str, uri: &str, bearer: &str, body: Option<Value>) -> Request<Body> {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {bearer}"));
    match body {
        Some(value) => builder
            .header("content-type", "application/json")
            .body(Body::from(value.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

async fn mint(router: &axum::Router, tenant: &str) -> String {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/v1/admin/tenants/{tenant}/keys"))
        .header("x-copal-admin-token", ADMIN)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "name": "sweep", "scopes": ["read", "write", "admin"] }).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "minting for {tenant}"
    );
    json_body(response).await["token"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn create_file(router: &axum::Router, token: &str, path: &str) -> String {
    let response = router
        .clone()
        .oneshot(signed(
            "POST",
            "/v1/files",
            token,
            Some(json!({ "path": path, "content_type": "text/plain" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED, "creating {path}");
    json_body(response).await["id"].as_str().unwrap().to_owned()
}

/// Nothing a rival asks for reaches the owner's file.
///
/// Every route that takes a file id is asked for one belonging to
/// somebody else. An id is not a secret, so the answer has to be the
/// same as for an id that never existed.
#[tokio::test]
async fn no_route_hands_one_tenant_another_tenant_s_file() {
    let (router, _dir) = keyed_router().await;
    let owner = mint(&router, "owner").await;
    let rival = mint(&router, "rival").await;

    let secret = create_file(&router, &owner, "owner-secret.txt").await;
    // The rival has a file of their own, so the sweep cannot pass by
    // the tenant simply being empty.
    let theirs = create_file(&router, &rival, "rival-own.txt").await;

    let by_id: Vec<(&str, String, Option<Value>)> = vec![
        ("GET", format!("/v1/files/{secret}"), None),
        ("GET", format!("/v1/files/{secret}/content"), None),
        ("GET", format!("/v1/files/{secret}/text"), None),
        ("GET", format!("/v1/files/{secret}/versions"), None),
        (
            "GET",
            format!("/v1/files/{secret}/versions/1/content"),
            None,
        ),
        ("GET", format!("/v1/files/{secret}/renditions"), None),
        ("DELETE", format!("/v1/files/{secret}"), None),
        (
            "PATCH",
            format!("/v1/files/{secret}"),
            Some(json!({ "access": "public" })),
        ),
        (
            "POST",
            format!("/v1/files/{secret}/url"),
            Some(json!({ "ttl_secs": 300 })),
        ),
        (
            "POST",
            format!("/v1/files/{secret}/upload-url"),
            Some(json!({})),
        ),
        (
            "POST",
            format!("/v1/files/{secret}/transform"),
            Some(json!({ "spec": "thumb" })),
        ),
        (
            "PUT",
            format!("/v1/files/{secret}/content"),
            Some(json!({})),
        ),
    ];

    let mut reached = Vec::new();
    for (method, uri, body) in by_id {
        let response = router
            .clone()
            .oneshot(signed(method, &uri, &rival, body))
            .await
            .unwrap();
        let status = response.status();
        let said = text_body(response).await;
        // Anything but a refusal is the rival having touched it. A
        // 404 is the honest answer: as far as this tenant is
        // concerned the id does not exist.
        if status.is_success() || status.is_redirection() {
            reached.push(format!("{method} {uri} -> {status}"));
        }
        // And no refusal may describe what it is refusing.
        if said.contains("owner-secret") {
            reached.push(format!("{method} {uri} named the path in its refusal"));
        }
    }
    assert!(
        reached.is_empty(),
        "a rival reached the owner's file:\n  {}",
        reached.join("\n  "),
    );

    // The rival's own file still answers, so the sweep is testing
    // isolation rather than a router that refuses everything.
    let response = router
        .clone()
        .oneshot(signed("GET", &format!("/v1/files/{theirs}"), &rival, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "the rival's own file");
}

/// Nothing a rival lists, searches or subscribes to carries the
/// owner's rows.
#[tokio::test]
async fn no_collection_carries_another_tenant_s_rows() {
    let (router, _dir) = keyed_router().await;
    let owner = mint(&router, "owner").await;
    let rival = mint(&router, "rival").await;

    create_file(&router, &owner, "owner-secret.txt").await;
    create_file(&router, &rival, "rival-own.txt").await;

    let collections = [
        "/v1/files",
        "/v1/files?limit=100",
        "/v1/events",
        "/v1/runs",
        "/v1/search?q=owner",
        "/v1/search?q=secret",
        "/v1/search?q=txt",
    ];
    let mut leaked = Vec::new();
    for uri in collections {
        let response = router
            .clone()
            .oneshot(signed("GET", uri, &rival, None))
            .await
            .unwrap();
        let said = text_body(response).await;
        if said.contains("owner-secret") || said.contains("\"owner\"") {
            leaked.push(uri.to_owned());
        }
    }
    assert!(
        leaked.is_empty(),
        "a rival's collection carried the owner's rows: {leaked:?}",
    );
}

/// The GraphQL face answers the same questions and has to give the
/// same answers.
#[tokio::test]
async fn graphql_does_not_reach_across_either() {
    let (router, _dir) = keyed_router().await;
    let owner = mint(&router, "owner").await;
    let rival = mint(&router, "rival").await;
    let secret = create_file(&router, &owner, "owner-secret.txt").await;
    create_file(&router, &rival, "rival-own.txt").await;

    let documents = [
        "{ files { items { id path } } }".to_owned(),
        format!("{{ file(id: \"{secret}\") {{ id path }} }}"),
        format!("{{ file(id: \"{secret}\") {{ versions {{ items {{ id }} }} }} }}"),
        "{ events { items { id } } }".to_owned(),
    ];
    let mut leaked = Vec::new();
    for document in documents {
        let response = router
            .clone()
            .oneshot(signed(
                "POST",
                "/graphql",
                &rival,
                Some(json!({ "query": document })),
            ))
            .await
            .unwrap();
        let said = text_body(response).await;
        if said.contains("owner-secret") {
            leaked.push(document.clone());
        }
    }
    assert!(
        leaked.is_empty(),
        "graphql carried the owner's rows: {leaked:?}",
    );
}

/// A key that is not one, or none at all, reaches nothing.
///
/// The uniform refusal matters as much as the isolation: a 404 for a
/// revoked key and a 401 for an absent one would tell a caller which
/// of their guesses was closer.
#[tokio::test]
async fn credentials_that_are_not_credentials_reach_nothing() {
    let (router, _dir) = keyed_router().await;
    let owner = mint(&router, "owner").await;
    let secret = create_file(&router, &owner, "owner-secret.txt").await;

    let attempts: Vec<Option<&str>> = vec![
        None,
        Some(""),
        Some("not-a-key"),
        Some("ck1.nonsense.nonsense"),
        // The right shape with the wrong secret.
        Some("ck1.01kzea136qxdja8mj30b7d407t.0000000000000000000000000000000000000000000000000000000000000000"),
    ];
    for attempt in attempts {
        for uri in ["/v1/files", &format!("/v1/files/{secret}")] {
            let mut builder = Request::builder().method("GET").uri(uri);
            if let Some(token) = attempt {
                builder = builder.header("authorization", format!("Bearer {token}"));
            }
            let response = router
                .clone()
                .oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{uri} with {attempt:?}",
            );
        }
    }
}

/// The tenant header cannot widen what a key reaches.
///
/// In key mode the tenant comes from the key. A caller who believes
/// otherwise, or who sends two headers hoping one is read, gets their
/// own tenant either way.
#[tokio::test]
async fn a_tenant_header_cannot_widen_a_key() {
    let (router, _dir) = keyed_router().await;
    let owner = mint(&router, "owner").await;
    let rival = mint(&router, "rival").await;
    create_file(&router, &owner, "owner-secret.txt").await;
    create_file(&router, &rival, "rival-own.txt").await;

    for headers in [
        vec![("x-copal-tenant", "owner")],
        vec![("x-copal-tenant", "owner"), ("x-copal-tenant", "rival")],
        vec![("x-copal-tenant", "")],
    ] {
        let mut builder = Request::builder()
            .method("GET")
            .uri("/v1/files")
            .header("authorization", format!("Bearer {rival}"));
        for (name, value) in &headers {
            builder = builder.header(*name, *value);
        }
        let response = router
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let said = text_body(response).await;
        assert!(
            !said.contains("owner-secret"),
            "{headers:?} widened the key: {said}",
        );
    }
}

/// The keyed stack with the built-in workflows registered, the way a
/// deployment runs. Private fetch targets are allowed so the stub
/// origin is reachable: the wall under test is the tenant, not the
/// outbound policy.
async fn keyed_router_with_pipeline() -> (axum::Router, copal_flow::FlowEngine, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let registry = copal_server::pipeline::standard_registry(
        store.clone(),
        copal_server::app::Residencies::local_only(blobs.clone()),
        copal_core::ExtensionPolicy::standard(),
        false,
        None,
        None,
        None,
        std::collections::HashMap::new(),
        copal_server::pipeline::FetchPolicy {
            allow_private_targets: true,
            max_bytes: 1 << 20,
        },
        copal_server::tiering::Topology::default(),
        Default::default(),
    );
    let mut state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            mode: AuthMode::ApiKeys,
            admin_token: Some(ADMIN.into()),
            admin_token_previous: None,
            operator_header: None,
        })
        .with_flow(registry);
    state.limits.allow_private_fetch_targets = true;
    let engine = state.flow.clone();
    (build_router(state), engine, dir)
}

/// A stub origin whose body would land in whichever file a fetch run
/// names.
async fn stub_origin() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = axum::Router::new().route(
        "/doc",
        axum::routing::get(|| async { ([("content-type", "text/plain")], "planted") }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// No run a rival starts writes into the owner's file.
///
/// The built-in workflows act on the tenant, file, and digest their
/// input names, because the endpoints that enqueue them checked those
/// first. The runs API hands a caller the whole input, so it must not
/// start them at all, on any face and in either mode.
#[tokio::test]
async fn no_caller_started_run_reaches_another_tenant_s_file() {
    let (router, engine, _dir) = keyed_router_with_pipeline().await;
    let owner = mint(&router, "owner").await;
    let rival = mint(&router, "rival").await;
    let draft = create_file(&router, &owner, "owner-draft.txt").await;
    let origin = stub_origin().await;
    let input = json!({
        "tenant": "owner",
        "file": draft,
        "url": format!("http://{origin}/doc"),
    });

    let built_in = ["fetch", "post_upload", "derive", "transform", "tier.recall"];
    for workflow in built_in {
        assert!(engine.has_workflow(workflow), "{workflow} is registered");
        for mode in ["sync", "async"] {
            let response = router
                .clone()
                .oneshot(signed(
                    "POST",
                    "/v1/runs",
                    &rival,
                    Some(json!({ "workflow": workflow, "mode": mode, "input": input })),
                ))
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::FORBIDDEN,
                "POST /v1/runs started {workflow} ({mode}) for a caller",
            );
        }

        let response = router
            .clone()
            .oneshot(signed(
                "POST",
                "/graphql",
                &rival,
                Some(json!({
                    "query": "mutation($w: String!, $i: JSON) \
                              { runStart(workflow: $w, input: $i, mode: \"sync\") }",
                    "variables": { "w": workflow, "i": input },
                })),
            ))
            .await
            .unwrap();
        let body = json_body(response).await;
        assert_eq!(
            body["errors"][0]["extensions"]["code"], "forbidden",
            "runStart started {workflow} for a caller: {body}",
        );
    }

    // The refusals queued nothing, and the owner's draft is still an
    // empty draft.
    while engine.tick("sweep").await.unwrap() {}
    for token in [&owner, &rival] {
        let response = router
            .clone()
            .oneshot(signed("GET", "/v1/runs", token, None))
            .await
            .unwrap();
        assert_eq!(json_body(response).await["items"], json!([]));
    }
    let response = router
        .clone()
        .oneshot(signed("GET", &format!("/v1/files/{draft}"), &owner, None))
        .await
        .unwrap();
    let record = json_body(response).await;
    assert_eq!(record["state"], "draft", "{record}");
}
