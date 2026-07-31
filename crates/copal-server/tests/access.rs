//! The access model, enforced: public serves anonymously and caches
//! hard, grant-only refuses every direct byte path, tenants stay
//! invisible to each other, all under keys-mode auth, so anonymity
//! is real. Plus the audit trail: recorded, listable, and
//! engine-immutable.

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

async fn keyed_stack() -> (axum::Router, Store, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
    });
    (build_router(state), store, dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn request(
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    admin: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(token) = admin {
        builder = builder.header("x-copal-admin-token", token);
    }
    match body {
        Some(value) => builder
            .header("content-type", "application/json")
            .body(Body::from(value.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

async fn mint(router: &axum::Router, tenant: &str, name: &str) -> String {
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/admin/tenants/{tenant}/keys"),
            None,
            Some(ADMIN),
            Some(json!({ "name": name })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await["token"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn seed(router: &axum::Router, token: &str, path: &str, access: &str) -> String {
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/files",
            Some(token),
            None,
            Some(json!({ "path": path, "content_type": "text/plain", "access": access })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let mut upload = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(format!("bytes of {path}")))
        .unwrap();
    upload.headers_mut().remove("content-type");
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn public_serves_anonymously_and_caches_hard() {
    let (router, _store, _dir) = keyed_stack().await;
    let token = mint(&router, "acme", "ci").await;
    let id = seed(&router, &token, "open.txt", "public").await;

    // No credentials at all, and it serves, cacheable forever.
    let response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/files/{id}/content"),
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["cache-control"],
        "public, max-age=31536000, immutable",
    );
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");

    // The management surface stays authenticated even for public files.
    let response = router
        .clone()
        .oneshot(request("GET", &format!("/v1/files/{id}"), None, None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn grant_only_refuses_direct_bytes_but_grants_flow() {
    let (router, _store, _dir) = keyed_stack().await;
    let token = mint(&router, "acme", "ci").await;
    let id = seed(&router, &token, "sealed.txt", "grant").await;

    // Even the OWNER cannot pull bytes directly: grant-only means
    // every access is an issued, revocable capability.
    let response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/files/{id}/content"),
            Some(&token),
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(json_body(response).await["error"]["kind"], "forbidden");

    // Anonymous is unauthorized before it is forbidden.
    let response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/files/{id}/content"),
            None,
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // The issued URL still serves; that is the point of the level.
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/files/{id}/url"),
            Some(&token),
            None,
            Some(json!({})),
        ))
        .await
        .unwrap();
    let url = json_body(response).await["url"]
        .as_str()
        .unwrap()
        .to_owned();
    let response = router
        .clone()
        .oneshot(request("GET", &url, None, None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"bytes of sealed.txt");
}

#[tokio::test]
async fn private_files_stay_invisible_across_tenants() {
    let (router, _store, _dir) = keyed_stack().await;
    let owner = mint(&router, "acme", "ci").await;
    let rival = mint(&router, "rival", "spy").await;
    let id = seed(&router, &owner, "secret.txt", "private").await;

    // The rival sees absence, not refusal.
    let response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/files/{id}/content"),
            Some(&rival),
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The owner reads normally, un-cacheably.
    let response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/files/{id}/content"),
            Some(&owner),
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
}

#[tokio::test]
async fn the_audit_trail_records_custody_and_refuses_rewrites() {
    let (router, store, _dir) = keyed_stack().await;
    let token = mint(&router, "acme", "ci").await;
    let id = seed(&router, &token, "tracked.txt", "private").await;

    // Issue a URL, then delete the file: two more auditable actions.
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/files/{id}/url"),
            Some(&token),
            None,
            Some(json!({})),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("/v1/files/{id}"),
            Some(&token),
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // The admin surface lists the trail, newest first.
    let response = router
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/tenants/acme/audit",
            None,
            Some(ADMIN),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    let actions: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    for expected in ["key.minted", "grant.issued", "file.removed"] {
        assert!(actions.contains(&expected), "{actions:?}");
    }
    let minted = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["action"] == "key.minted")
        .unwrap();
    assert_eq!(minted["actor"], "admin");

    // Rewriting history THROWs inside the engine, not in app code.
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let error = copal_store::repo::auth::tamper_audit_for_test(&store, &tenant)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("immutable"),
        "expected the engine THROW, got: {error}",
    );
}

#[tokio::test]
async fn the_admin_surface_splits_off_the_tenant_router() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
    });

    // The tenant-facing router alone has NO admin routes at all.
    let api_only = copal_server::app::api_router(state.clone());
    let response = api_only
        .oneshot(request(
            "POST",
            "/v1/admin/tenants/acme/keys",
            None,
            Some(ADMIN),
            Some(json!({ "name": "x" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // The admin router alone serves them.
    let admin_only = copal_server::app::admin_router(state);
    let response = admin_only
        .oneshot(request(
            "POST",
            "/v1/admin/tenants/acme/keys",
            None,
            Some(ADMIN),
            Some(json!({ "name": "x" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let _ = dir;
}

#[tokio::test]
async fn grant_uses_burn_only_on_actual_reads() {
    let (router, store, _dir) = keyed_stack().await;
    let token = mint(&router, "acme", "ci").await;
    let id = seed(&router, &token, "counted.txt", "grant").await;

    // A two-use grant.
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/files/{id}/url"),
            Some(&token),
            None,
            Some(json!({ "max_uses": 2 })),
        ))
        .await
        .unwrap();
    let issued = json_body(response).await;
    let url = issued["url"].as_str().unwrap().to_owned();
    let grant_id = issued["grant_id"].as_str().unwrap().to_owned();
    let uses = |store: Store, grant_id: String| async move {
        copal_store::repo::grant::fetch(&store, &grant_id)
            .await
            .unwrap()
            .unwrap()
            .uses
    };

    // First real read consumes one use.
    let response = router
        .clone()
        .oneshot(request("GET", &url, None, None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response.headers()["etag"].to_str().unwrap().to_owned();
    assert_eq!(uses(store.clone(), grant_id.clone()).await, 1);

    // Revalidations are NOT reads: three 304s, zero consumed, and a
    // weak validator (W/ prefix) matches per RFC 9110.
    for validator in [etag.clone(), etag.clone(), format!("W/{etag}")] {
        let mut revalidate = request("GET", &url, None, None, None);
        revalidate
            .headers_mut()
            .insert("if-none-match", validator.parse().unwrap());
        let response = router.clone().oneshot(revalidate).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    }
    assert_eq!(uses(store.clone(), grant_id.clone()).await, 1);

    // The second real read exhausts the grant; a third refuses.
    let response = router
        .clone()
        .oneshot(request("GET", &url, None, None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(uses(store.clone(), grant_id.clone()).await, 2);
    let response = router
        .clone()
        .oneshot(request("GET", &url, None, None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // And an exhausted grant cannot keep revalidating a cache either.
    let mut revalidate = request("GET", &url, None, None, None);
    revalidate
        .headers_mut()
        .insert("if-none-match", etag.parse().unwrap());
    let response = router.clone().oneshot(revalidate).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn refused_serves_do_not_burn_grant_uses() {
    let (router, store, _dir) = keyed_stack().await;
    let token = mint(&router, "acme", "ci").await;
    let id = seed(&router, &token, "doomed.txt", "private").await;

    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/files/{id}/url"),
            Some(&token),
            None,
            Some(json!({ "max_uses": 1 })),
        ))
        .await
        .unwrap();
    let issued = json_body(response).await;
    let url = issued["url"].as_str().unwrap().to_owned();
    let grant_id = issued["grant_id"].as_str().unwrap().to_owned();

    // Delete the file out from under the grant, then redeem: refused,
    // and the single use SURVIVES the refusal.
    let response = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("/v1/files/{id}"),
            Some(&token),
            None,
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = router
        .clone()
        .oneshot(request("GET", &url, None, None, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let row = copal_store::repo::grant::fetch(&store, &grant_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.uses, 0, "a refused serve must not burn a use");
}

#[tokio::test]
async fn audit_rows_record_the_forwarded_origin() {
    let (router, _store, _dir) = keyed_stack().await;
    let token = mint(&router, "acme", "ci").await;
    let id = seed(&router, &token, "traced.txt", "private").await;

    // Delete with a proxy-forwarded origin on the request.
    let mut remove = request(
        "DELETE",
        &format!("/v1/files/{id}"),
        Some(&token),
        None,
        None,
    );
    remove
        .headers_mut()
        .insert("x-forwarded-for", "203.0.113.7, 10.0.0.1".parse().unwrap());
    let response = router.clone().oneshot(remove).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = router
        .clone()
        .oneshot(request(
            "GET",
            "/v1/admin/tenants/acme/audit",
            None,
            Some(ADMIN),
            None,
        ))
        .await
        .unwrap();
    let body = json_body(response).await;
    let removed = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["action"] == "file.removed")
        .expect("file.removed audited");
    assert_eq!(
        removed["origin"], "203.0.113.7",
        "first forwarded hop only: {removed}",
    );
    // Rows without a forwarded header carry no origin at all.
    let minted = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["action"] == "key.minted")
        .unwrap();
    assert!(minted["origin"].is_null(), "{minted}");
}
