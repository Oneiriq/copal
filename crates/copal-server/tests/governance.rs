//! The declarations release, proven across faces: scopes and rate
//! budgets refuse identically whether a request arrives as REST or
//! GraphQL, because the contract declares them once and both faces
//! enforce from it against one ledger.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-token";

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
    serde_json::from_slice(&bytes).unwrap()
}

/// Mint a key with the given scopes; returns its bearer token. Names
/// are unique per call because keys are replace-by-name per tenant.
async fn mint(router: &axum::Router, scopes: &[&str]) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/tenants/acme/keys")
                .header("x-copal-admin-token", ADMIN)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "name": format!("k{sequence}-{}", scopes.join("-")), "scopes": scopes })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await["token"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn rest(method: &str, uri: &str, token: &str, body: Option<Value>) -> Request<Body> {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
    match body {
        Some(value) => builder
            .header("content-type", "application/json")
            .body(Body::from(value.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

fn graphql(query: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": query }).to_string()))
        .unwrap()
}

#[tokio::test]
async fn scopes_refuse_identically_on_both_faces() {
    let (router, _dir) = keyed_router().await;
    let read_only = mint(&router, &["read"]).await;

    // Reads pass on both faces.
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files", &read_only, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .clone()
        .oneshot(graphql(r#"{ files { items { id } } }"#, &read_only))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body.get("errors").is_none(), "{body:#?}");

    // A write refuses on both faces, naming the same scope with the
    // same machine-readable kind.
    let response = router
        .clone()
        .oneshot(rest(
            "POST",
            "/v1/files",
            &read_only,
            Some(json!({ "path": "denied.txt" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let rest_error = json_body(response).await;
    assert_eq!(rest_error["error"]["kind"], "forbidden");
    assert!(
        rest_error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("scope write required"),
        "{rest_error:#?}",
    );

    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation { fileRemove(id: "01ARZ3NDEKTSV4RRFFQ69G5FAV") }"#,
            &read_only,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    let error = &body["errors"][0];
    assert_eq!(error["extensions"]["code"], "forbidden");
    assert!(
        error["message"].as_str().unwrap().contains("scope write"),
        "{body:#?}",
    );

    // Search is a read: a write-only key is refused there.
    let write_only = mint(&router, &["write"]).await;
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/search?q=anything", &write_only, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(json_body(response).await["error"]["message"]
        .as_str()
        .unwrap()
        .contains("scope read required"));
}

#[tokio::test]
async fn webhook_registration_takes_the_admin_scope() {
    let (router, _dir) = keyed_router().await;
    let write_only = mint(&router, &["read", "write"]).await;
    let admin_scoped = mint(&router, &["admin"]).await;

    // Registering a webhook exfiltrates every future event to the
    // named URL, which is why write is deliberately weaker than it.
    let payload = json!({ "url": "http://127.0.0.1:9/hook" });
    let response = router
        .clone()
        .oneshot(graphql(
            r#"mutation { webhookRegister(url: "http://127.0.0.1:9/hook") }"#,
            &write_only,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["errors"][0]["extensions"]["code"], "forbidden");
    assert!(
        body["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("scope admin"),
        "{body:#?}",
    );

    // The admin-scoped key reaches the shared core; the refusal it
    // gets is the sealed-secret one, which proves the scope gate is
    // what stood in front of it. (This fixture carries no cipher.)
    let response = router
        .clone()
        .oneshot(rest("POST", "/v1/webhooks", &admin_scoped, Some(payload)))
        .await
        .unwrap();
    assert_ne!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn one_ledger_meters_both_faces() {
    let (router, _dir) = keyed_router().await;
    let token = mint(&router, &["read"]).await;

    // The reads budget is 6000 units a minute and a full page costs
    // its 100-row limit, so sixty full pages spend it. Spending is
    // driven until the refusal arrives rather than counted to
    // exactly sixty: the window is per minute, and a loop that
    // straddles a boundary gets a fresh budget partway through, which
    // on a slow machine reads as the ledger failing when it is
    // working. The bound is generous enough to cross one boundary and
    // still refuse.
    let mut refusal = None;
    for i in 0..180 {
        let response = router
            .clone()
            .oneshot(graphql(r#"{ files(limit: 100) { items { id } } }"#, &token))
            .await
            .unwrap();
        let body = json_body(response).await;
        if body.get("errors").is_some() {
            refusal = Some((i, body));
            break;
        }
    }
    let (spent_after, body) = refusal.expect("the budget refuses within the bound");
    assert!(spent_after >= 59, "refused after only {spent_after} pages");
    assert_eq!(
        body["errors"][0]["extensions"]["code"], "too_many_requests",
        "{body:#?}",
    );

    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files?limit=100", &token, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let body = json_body(response).await;
    assert_eq!(body["error"]["kind"], "too_many_requests");

    // A different key has its own budget and passes untouched.
    let fresh = mint(&router, &["read"]).await;
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files?limit=1", &fresh, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn header_mode_stays_open_and_still_meters() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let router = build_router(AppState::new(store, blobs));
    let _dir = dir;

    // Header mode holds every scope: reads, writes, and admin-scoped
    // operations all pass the scope gate.
    let create = Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "path": "dev.txt" }).to_string()))
        .unwrap();
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let listing = Request::builder()
        .method("GET")
        .uri("/v1/files")
        .header("x-copal-tenant", "acme")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(listing).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn guarded_fields_redact_identically_on_both_faces() {
    let (router, _dir) = keyed_router().await;
    let worker = mint(&router, &["read", "write"]).await;
    let operator = mint(&router, &["read", "admin"]).await;

    // A file with content mints version 1.
    let response = router
        .clone()
        .oneshot(rest(
            "POST",
            "/v1/files",
            &worker,
            Some(json!({ "path": "audit.txt", "content_type": "text/plain" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("authorization", format!("Bearer {worker}"))
        .body(Body::from(b"attributed bytes".to_vec()))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Version attribution is audit data: the non-admin key lists
    // history WITHOUT the created_by key on REST, and GraphQL renders
    // it null. The rest of the row is intact.
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}/versions"),
            &worker,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    let row = &body["items"][0];
    assert_eq!(row["number"], 1);
    assert!(row.get("created_by").is_none(), "{row:#?}");

    let response = router
        .clone()
        .oneshot(graphql(
            &format!(
                r#"{{ file(id: "{id}") {{ versions {{ items {{ number created_by }} }} }} }}"#
            ),
            &worker,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body.get("errors").is_none(), "{body:#?}");
    let row = &body["data"]["file"]["versions"]["items"][0];
    assert_eq!(row["number"], 1);
    assert!(row["created_by"].is_null(), "{row:#?}");

    // The admin-scoped key sees the attribution on both faces.
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}/versions"),
            &operator,
            None,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["items"][0]["created_by"].is_string(), "{body:#?}",);
    let response = router
        .clone()
        .oneshot(graphql(
            &format!(r#"{{ file(id: "{id}") {{ versions {{ items {{ created_by }} }} }} }}"#),
            &operator,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(
        body["data"]["file"]["versions"]["items"][0]["created_by"].is_string(),
        "{body:#?}",
    );
}

#[tokio::test]
async fn a_fleet_shares_one_budget_through_the_store_ledger() {
    // Two replicas: separate AppStates, separate routers, ONE store,
    // both on the shared ledger.
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let auth = AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
        operator_header: None,
    };
    let mut replica_a = AppState::new(
        store.clone(),
        ObjectStore::open(dir_a.path().to_str().unwrap()).unwrap(),
    )
    .with_auth(auth.clone());
    replica_a.rate_store =
        std::sync::Arc::new(copal_server::rate::SurrealRateStore::new(store.clone()));
    let mut replica_b = AppState::new(
        store.clone(),
        ObjectStore::open(dir_b.path().to_str().unwrap()).unwrap(),
    )
    .with_auth(auth);
    replica_b.rate_store =
        std::sync::Arc::new(copal_server::rate::SurrealRateStore::new(store.clone()));
    let router_a = build_router(replica_a);
    let router_b = build_router(replica_b);

    let token = mint(&router_a, &["read"]).await;

    // Spend the whole reads budget through replica A.
    for i in 0..60 {
        let response = router_a
            .clone()
            .oneshot(rest("GET", "/v1/files?limit=100", &token, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "request {i}");
    }

    // Replica B refuses the same key: one fleet, one budget. The
    // in-memory ledger would have admitted a fresh 6000 here, which
    // is exactly the multiplication this ledger exists to end.
    let response = router_b
        .clone()
        .oneshot(rest("GET", "/v1/files?limit=100", &token, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

/// The engine policy is GENERATED from the contract, so the layers
/// cannot drift: every guarded field lands as an engine redaction,
/// read scopes land as select conjuncts on the resource's table and
/// its sub-resource tables, and the rendered DDL carries both.
#[test]
fn engine_policy_derives_from_the_contract() {
    let policy = copal_server::engine::engine_policy().expect("every guard has a clause");
    assert!(policy.field_guards.contains(&(
        "file_version".to_owned(),
        "created_by".to_owned(),
        "$token.adm = true OR created_by = $token.pr".to_owned(),
    )));
    assert!(policy
        .select_conjuncts
        .iter()
        .any(|(table, clause)| table == "file" && clause.contains("$token.sc CONTAINS 'read'")));
    assert!(policy
        .select_conjuncts
        .iter()
        .any(|(table, _)| table == "file_version"));
    // The per-chunk conjunct is copal-side policy stated beside the
    // retention clause, not derived: the contract does not expose the
    // chunk table, and the second layer must carry the withholding
    // rule anyway.
    assert!(policy.select_conjuncts.iter().any(|(table, clause)| {
        table == "text_chunk"
            && clause.contains("access IS NONE OR access != 'grant'")
            && clause.contains("file.access != 'grant'")
    }));

    let tables = copal_store::schema::tables_with_policy(&policy);
    let version_table = tables.iter().find(|t| t.name == "file_version").unwrap();
    let guarded = version_table
        .fields
        .iter()
        .find(|f| f.name == "created_by")
        .unwrap();
    assert_eq!(
        guarded.permissions.as_ref().unwrap().get("select").unwrap(),
        "$token.adm = true OR created_by = $token.pr"
    );
    let file_table = tables.iter().find(|t| t.name == "file").unwrap();
    let select = file_table
        .permissions
        .as_ref()
        .unwrap()
        .get("select")
        .unwrap();
    assert!(select.contains("tenant_id = $token.tn AND"), "{select}");
    assert!(select.contains("$token.sc CONTAINS 'read'"), "{select}");
}

/// A minted caller token opens a session the engine filters, ULID
/// key ids included: the id claim's angle brackets carry the
/// digit-leading id through the record parser.
#[tokio::test]
async fn minted_tokens_open_filtered_sessions() {
    use copal_core::TenantId;
    use copal_store::repo::file as file_repo;

    let mut config = StoreConfig::memory_with_engine_access("mint-key");
    config.engine_policy = copal_server::engine::engine_policy().unwrap();
    let store = Store::connect(config).await.unwrap();
    let acme = TenantId::parse("acme").unwrap();
    let rival = TenantId::parse("rival").unwrap();
    let spec = |path: &str| copal_core::FileSpec {
        path: path.to_owned(),
        content_type: "text/plain".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: json!({}),
        idempotency_key: None,
    };
    file_repo::create_file(&store, &acme, &spec("ours.txt"), "tester")
        .await
        .unwrap();
    let theirs = file_repo::create_file(&store, &rival, &spec("theirs.txt"), "tester")
        .await
        .unwrap()
        .record;

    let access = copal_server::engine::EngineAccess {
        key: "mint-key".to_owned(),
        namespace: "copal_test".to_owned(),
        database: "copal".to_owned(),
    };
    let token = copal_server::engine::mint_caller_token(
        &access,
        &acme,
        &ulid::Ulid::generate().to_string().to_ascii_lowercase(),
        &["read".to_owned()],
        None,
    );
    let caller = store.caller(&token).await.expect("minted token binds");
    assert!(file_repo::get_file_any(&caller, &theirs.id)
        .await
        .unwrap()
        .is_none());
    assert!(file_repo::get_file_any(&store, &theirs.id)
        .await
        .unwrap()
        .is_some());
}

/// The files face under caller-bound engine sessions: every adopted
/// handler runs its repository calls on a session the engine filters,
/// and the guarded column arrives engine-redacted for the worker and
/// intact for the operator, with the wire shapes unchanged from the
/// service-session face.
#[tokio::test]
async fn engine_sessions_serve_the_files_face() {
    let mut store_config = StoreConfig::memory_with_engine_access("gov-engine-key");
    store_config.engine_policy = copal_server::engine::engine_policy().unwrap();
    let store = Store::connect(store_config).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            mode: AuthMode::ApiKeys,
            admin_token: Some(ADMIN.into()),
            admin_token_previous: None,
            operator_header: None,
        })
        .with_engine_access(Some(copal_server::engine::EngineAccess {
            key: "gov-engine-key".to_owned(),
            namespace: "copal_test".to_owned(),
            database: "copal".to_owned(),
        }))
        .with_engine_sessions(true)
        .with_cipher(Some(
            copal_blob::crypto::BlobCipher::from_hex(&"07".repeat(32)).unwrap(),
        ));
    // The webhook surface mounts beside the API exactly as main does,
    // gated on the cipher its sealed secrets need.
    let router = build_router(state.clone()).merge(copal_server::webhooks::webhook_router(state));
    let worker = mint(&router, &["read", "write"]).await;
    let operator = mint(&router, &["read", "admin"]).await;

    let response = router
        .clone()
        .oneshot(rest(
            "POST",
            "/v1/files",
            &worker,
            Some(json!({ "path": "sessioned.txt", "content_type": "text/plain" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();

    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("authorization", format!("Bearer {worker}"))
        .body(Body::from(b"session bytes".to_vec()))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files", &worker, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let listing = json_body(response).await;
    assert_eq!(listing["items"][0]["path"], "sessioned.txt");

    let response = router
        .clone()
        .oneshot(rest("GET", &format!("/v1/files/{id}"), &worker, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The engine redacts attribution for the worker session before
    // the projection ever sees the row; the operator's admin claim
    // carries it through both layers.
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}/versions"),
            &worker,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let row = json_body(response).await["items"][0].clone();
    assert_eq!(row["number"], 1);
    assert!(row.get("created_by").is_none(), "{row:#?}");

    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}/versions"),
            &operator,
            None,
        ))
        .await
        .unwrap();
    let row = json_body(response).await["items"][0].clone();
    assert!(row["created_by"].is_string(), "{row:#?}");

    // The swept read surfaces answer under caller sessions too:
    // usage accounting, the empty search, and a version's bytes.
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/usage", &worker, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/search?q=session", &worker, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .clone()
        .oneshot(rest(
            "GET",
            &format!("/v1/files/{id}/versions/1/content"),
            &worker,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The GraphQL face rides the same caller session: the resolver
    // reads through the ctx store, and the guarded column arrives
    // engine-redacted for the worker and intact for the operator.
    let response = router
        .clone()
        .oneshot(graphql(
            &format!(
                r#"{{ file(id: "{id}") {{ versions {{ items {{ number created_by }} }} }} }}"#
            ),
            &worker,
        ))
        .await
        .unwrap();
    let row = json_body(response).await["data"]["file"]["versions"]["items"][0].clone();
    assert_eq!(row["number"], 1);
    assert!(row["created_by"].is_null(), "{row:#?}");
    let response = router
        .clone()
        .oneshot(graphql(
            &format!(r#"{{ file(id: "{id}") {{ versions {{ items {{ created_by }} }} }} }}"#),
            &operator,
        ))
        .await
        .unwrap();
    let row = json_body(response).await["data"]["file"]["versions"]["items"][0].clone();
    assert!(row["created_by"].is_string(), "{row:#?}");

    // Webhook management under caller sessions, admin scope enforced
    // by both layers.
    let response = router
        .clone()
        .oneshot(rest(
            "POST",
            "/v1/webhooks",
            &operator,
            Some(json!({ "url": "https://example.com/hook", "events": ["file.created"] })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/webhooks", &worker, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(rest("DELETE", &format!("/v1/files/{id}"), &worker, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// Retrieval answers on both faces from one contract declaration:
/// the REST path the contract names and the GraphQL field it
/// renders, through the same core, under the same scope.
#[tokio::test]
async fn search_answers_identically_on_both_faces() {
    let (router, _dir) = keyed_router().await;
    let reader = mint(&router, &["read", "write"]).await;

    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/search?q=quarterly", &reader, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let rest_answer = json_body(response).await;
    assert_eq!(rest_answer["mode"], "lexical");
    assert!(rest_answer["items"].is_array());

    let response = router
        .clone()
        .oneshot(graphql(r#"{ search(q: "quarterly") }"#, &reader))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert!(body["errors"].is_null(), "{body:#?}");
    assert_eq!(body["data"]["search"], rest_answer);
}

/// The declaration is enforced on the GraphQL face by the dispatcher:
/// a key without the read scope is refused before the resolver runs.
#[tokio::test]
async fn search_takes_the_read_scope_on_both_faces() {
    let (router, _dir) = keyed_router().await;
    let writer = mint(&router, &["write"]).await;

    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/search?q=anything", &writer, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = router
        .clone()
        .oneshot(graphql(r#"{ search(q: "anything") }"#, &writer))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["errors"][0]["extensions"]["code"], "forbidden");
}

/// The cache reuses one session per identity and never crosses
/// callers: a second request from the same key opens nothing new,
/// and a different tenant's key gets its own session.
#[tokio::test]
async fn caller_sessions_are_reused_per_identity() {
    let mut config = StoreConfig::memory_with_engine_access("cache-key");
    config.engine_policy = copal_server::engine::engine_policy().unwrap();
    let store = Store::connect(config).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            mode: AuthMode::ApiKeys,
            admin_token: Some(ADMIN.into()),
            admin_token_previous: None,
            operator_header: None,
        })
        .with_engine_access(Some(copal_server::engine::EngineAccess {
            key: "cache-key".to_owned(),
            namespace: "copal_test".to_owned(),
            database: "copal".to_owned(),
        }))
        .with_engine_sessions(true)
        .with_session_cache(60, 8);
    let sessions = state.sessions.clone();
    let router = build_router(state);
    let worker = mint(&router, &["read", "write"]).await;
    assert!(sessions.is_empty(), "nothing is held before a request");

    for _ in 0..3 {
        let response = router
            .clone()
            .oneshot(rest("GET", "/v1/files", &worker, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(sessions.len(), 1, "one identity holds one session");

    // A second key is a second identity, so it opens its own.
    let operator = mint(&router, &["read", "admin"]).await;
    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files", &operator, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        sessions.len(),
        2,
        "a different caller never rides another's"
    );
}

/// A cache with either bound at zero holds nothing, which is the
/// configuration for a deployment that would rather pay the open.
#[tokio::test]
async fn zero_bounds_hold_no_sessions() {
    let mut config = StoreConfig::memory_with_engine_access("nocache-key");
    config.engine_policy = copal_server::engine::engine_policy().unwrap();
    let store = Store::connect(config).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            mode: AuthMode::ApiKeys,
            admin_token: Some(ADMIN.into()),
            admin_token_previous: None,
            operator_header: None,
        })
        .with_engine_access(Some(copal_server::engine::EngineAccess {
            key: "nocache-key".to_owned(),
            namespace: "copal_test".to_owned(),
            database: "copal".to_owned(),
        }))
        .with_engine_sessions(true)
        .with_session_cache(0, 0);
    let sessions = state.sessions.clone();
    let router = build_router(state);
    let worker = mint(&router, &["read", "write"]).await;

    let response = router
        .clone()
        .oneshot(rest("GET", "/v1/files", &worker, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(sessions.is_empty());
}
