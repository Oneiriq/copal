//! The SIEM face of the audit trail: one NDJSON stream over every
//! tenant, ascending, keyset-cursored through the
//! `x-copal-next-cursor` header. An exporter checkpoints the header
//! value and replays forward; an empty body with no header means the
//! checkpoint is current.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::Value;
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_core::TenantId;
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-secret";

async fn stack() -> (axum::Router, Store, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
        operator_header: None,
    });
    (build_router(state), store, dir)
}

async fn seed(store: &Store) {
    let acme = TenantId::parse("acme").unwrap();
    let beta = TenantId::parse("beta").unwrap();
    for (tenant, action, subject) in [
        (&acme, "key.minted", "ck1_one"),
        (&beta, "key.minted", "ck1_two"),
        (&acme, "grant.issued", "grant_a"),
        (&acme, "key.revoked", "ck1_one"),
        (&beta, "webhook.registered", "hook_b"),
    ] {
        copal_store::repo::auth::record_audit(store, tenant, "admin", action, subject, None, None)
            .await
            .unwrap();
    }
}

/// One export request; returns the parsed NDJSON lines and the next
/// cursor header.
async fn page(router: &axum::Router, query: &str) -> (Vec<Value>, Option<String>) {
    let request = Request::builder()
        .method("GET")
        .uri(format!("/v1/admin/audit/export{query}"))
        .header("x-copal-admin-token", ADMIN)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-ndjson",
    );
    let cursor = response
        .headers()
        .get("x-copal-next-cursor")
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    let rows = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (rows, cursor)
}

#[tokio::test]
async fn export_tails_the_whole_deployment_in_order() {
    let (router, store, _dir) = stack().await;
    seed(&store).await;

    // Page through with a small limit; every event arrives exactly
    // once, ascending, across both tenants.
    let mut seen: Vec<Value> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let query = match &cursor {
            Some(cursor) => format!("?limit=2&cursor={cursor}"),
            None => "?limit=2".to_owned(),
        };
        let (rows, next) = page(&router, &query).await;
        if rows.is_empty() {
            assert!(next.is_none(), "an empty page carries no cursor");
            break;
        }
        assert!(next.is_some(), "a page with rows advances the checkpoint");
        seen.extend(rows);
        cursor = next;
    }
    assert_eq!(seen.len(), 5);
    let times: Vec<&str> = seen
        .iter()
        .map(|row| row["created_at"].as_str().unwrap())
        .collect();
    let mut sorted = times.clone();
    sorted.sort();
    assert_eq!(times, sorted, "the stream is ascending");
    let ids: std::collections::BTreeSet<&str> =
        seen.iter().map(|row| row["id"].as_str().unwrap()).collect();
    assert_eq!(ids.len(), 5, "no duplicates across pages");
    assert!(ids.iter().all(|id| !id.contains(':')), "ids come back bare",);
    let tenants: std::collections::BTreeSet<&str> = seen
        .iter()
        .map(|row| row["tenant_id"].as_str().unwrap())
        .collect();
    assert_eq!(tenants.len(), 2, "the stream crosses tenants");
}

#[tokio::test]
async fn export_narrows_to_one_tenant() {
    let (router, store, _dir) = stack().await;
    seed(&store).await;
    let (rows, _) = page(&router, "?tenant=beta").await;
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| row["tenant_id"].as_str().unwrap() == "beta"));
}

#[tokio::test]
async fn export_is_admin_only_and_rejects_bad_cursors() {
    let (router, store, _dir) = stack().await;
    seed(&store).await;

    let bare = Request::builder()
        .method("GET")
        .uri("/v1/admin/audit/export")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(bare).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let wrong = Request::builder()
        .method("GET")
        .uri("/v1/admin/audit/export")
        .header("x-copal-admin-token", "guess")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(wrong).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let mangled = Request::builder()
        .method("GET")
        .uri("/v1/admin/audit/export?cursor=nonsense")
        .header("x-copal-admin-token", ADMIN)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(mangled).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
