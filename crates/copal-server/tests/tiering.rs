//! Observe-only tiering, proven at the surfaces: the policy row
//! validates against configured tiers and audits, pins hold, byte
//! reads record day-coarse recency, and the observe-only classifier
//! reports what would move without touching anything.

use std::collections::HashMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::tier::TierClass;
use copal_blob::ObjectStore;
use copal_server::auth::AuthConfig;
use copal_server::tiering::{classify_pass, Topology};
use copal_server::{build_router, AppState};
use copal_store::repo::tier as tier_repo;
use copal_store::{Store, StoreConfig};

const ADMIN: &str = "operator-token";

fn topology() -> Topology {
    let mut t = Topology::default();
    t.insert(
        "local",
        HashMap::from([
            ("cold".to_owned(), TierClass::Online),
            ("chill".to_owned(), TierClass::Online),
        ]),
    );
    t
}

async fn stack() -> (axum::Router, Store, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs)
        .with_auth(AuthConfig {
            admin_token: Some(ADMIN.into()),
            ..AuthConfig::default()
        })
        .with_tiering(topology());
    (build_router(state), store, dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn admin(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-admin-token", ADMIN);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    builder
        .body(body.map_or(Body::empty(), |value| Body::from(value.to_string())))
        .unwrap()
}

fn tenant_req(tenant: &str, method: &str, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-tenant", tenant)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

/// Create a file and upload content for a tenant; returns the file id.
async fn upload(router: &axum::Router, tenant: &str, path: &str, payload: &[u8]) -> String {
    let create = tenant_req(
        tenant,
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", tenant)
        .body(Body::from(payload.to_vec()))
        .unwrap();
    let response = router.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    id
}

#[tokio::test]
async fn the_policy_surface_validates_against_configured_tiers_and_audits() {
    let (router, _store, _dir) = stack().await;

    // A tier no residency configures refuses at validation.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/tiering",
            Some(json!({ "tier": "glacial", "after_seconds": 0 })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // A configured tier lands, with the accessed default spelled out.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/tiering",
            Some(json!({ "tier": "cold", "after_seconds": 7_776_000, "min_bytes": 131_072 })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["basis"], "accessed");

    let response = router
        .clone()
        .oneshot(admin("GET", "/v1/admin/tenants/acme/tiering", None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["tier"], "cold");
    assert_eq!(body["after_seconds"], 7_776_000);
    assert_eq!(body["min_bytes"], 131_072);

    // An unknown basis refuses.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/tiering",
            Some(json!({ "tier": "cold", "after_seconds": 0, "basis": "guessed" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Clearing removes the row; reading afterward is a 404.
    let response = router
        .clone()
        .oneshot(admin("DELETE", "/v1/admin/tenants/acme/tiering", None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = router
        .clone()
        .oneshot(admin("GET", "/v1/admin/tenants/acme/tiering", None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Both operator actions audited.
    let response = router
        .clone()
        .oneshot(admin("GET", "/v1/admin/tenants/acme/audit", None))
        .await
        .unwrap();
    let audit = json_body(response).await;
    let actions: Vec<&str> = audit["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row["action"].as_str())
        .collect();
    assert!(
        actions.contains(&"tenant.tiering_policy_set"),
        "{actions:?}"
    );
    assert!(
        actions.contains(&"tenant.tiering_policy_cleared"),
        "{actions:?}"
    );
}

#[tokio::test]
async fn pins_apply_to_live_files_and_audit() {
    let (router, _store, _dir) = stack().await;
    let id = upload(&router, "acme", "pinned.txt", b"pin me").await;

    // Only "hot" is a pin.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            &format!("/v1/admin/tenants/acme/files/{id}/tier"),
            Some(json!({ "pin": "cold" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            &format!("/v1/admin/tenants/acme/files/{id}/tier"),
            Some(json!({ "pin": "hot" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(admin(
            "DELETE",
            &format!("/v1/admin/tenants/acme/files/{id}/tier"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // A file that does not exist answers 404, not a silent no-op.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/files/01jzzzzzzzzzzzzzzzzzzzzzzz/tier",
            Some(json!({ "pin": "hot" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = router
        .clone()
        .oneshot(admin("GET", "/v1/admin/tenants/acme/audit", None))
        .await
        .unwrap();
    let audit = json_body(response).await;
    let actions: Vec<&str> = audit["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row["action"].as_str())
        .collect();
    assert!(actions.contains(&"file.tier_pinned"), "{actions:?}");
    assert!(actions.contains(&"file.tier_pin_released"), "{actions:?}");
}

/// Poll for the fire-and-forget recency write; a bounded wait, not a
/// bare sleep, so the test fails fast with the actual state.
async fn wait_for_last_read(store: &Store, digest: &copal_core::ContentDigest) -> Option<String> {
    for _ in 0..50 {
        if let Some(value) = tier_repo::last_read(store, "local", digest).await.unwrap() {
            return Some(value);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test]
async fn byte_reads_record_recency_day_coarse_and_metadata_reads_do_not() {
    let (router, store, _dir) = stack().await;
    let payload = b"read recency";
    let digest = copal_core::ContentDigest::of_bytes(payload);
    let id = upload(&router, "acme", "recency.txt", payload).await;

    // Uploading and reading metadata are not byte reads.
    let response = router
        .clone()
        .oneshot(tenant_req(
            "acme",
            "GET",
            &format!("/v1/files/{id}"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        tier_repo::last_read(&store, "local", &digest)
            .await
            .unwrap(),
        None,
        "metadata reads must not keep a corpus hot",
    );

    // A content GET records, once.
    let response = router
        .clone()
        .oneshot(tenant_req(
            "acme",
            "GET",
            &format!("/v1/files/{id}/content"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let first = wait_for_last_read(&store, &digest)
        .await
        .expect("a byte read records last_read");

    // A second read the same day rides the coarse bucket: no write.
    let response = router
        .clone()
        .oneshot(tenant_req(
            "acme",
            "GET",
            &format!("/v1/files/{id}/content"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        tier_repo::last_read(&store, "local", &digest)
            .await
            .unwrap(),
        Some(first),
        "the same day's later reads cost nothing",
    );
}

#[tokio::test]
async fn the_observer_reports_and_touches_nothing() {
    let (router, store, _dir) = stack().await;
    let payload = b"cold candidate bytes";
    let digest = copal_core::ContentDigest::of_bytes(payload);

    // With no policy anywhere, the classifier does not even walk.
    upload(&router, "acme", "corpus.txt", payload).await;
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.total_candidate_blobs, 0);
    assert!(report.per_tenant.is_empty());

    // An immediate created-basis policy makes the blob a candidate.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/tiering",
            Some(json!({ "tier": "cold", "after_seconds": 0, "basis": "created" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.total_candidate_blobs, 1);
    assert_eq!(report.total_candidate_bytes, payload.len() as u64);
    assert_eq!(
        report.per_tenant["acme"].candidate_bytes,
        payload.len() as u64
    );

    // Observing moved nothing: the blob still reads as primary-tier.
    let location = copal_store::repo::blob::get_location(&store, "local", &digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(location.tier, None, "observe-only must not place bytes");

    // The admin listing carries the same figures.
    let response = router
        .clone()
        .oneshot(admin("GET", "/v1/admin/tiering/report", None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["totals"]["candidate_blobs"], 1);
    assert_eq!(body["tenants"][0]["tenant"], "acme");

    // A pin holds the blob and the report says why.
    let id = upload(&router, "acme", "pinned-copy.txt", payload).await;
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            &format!("/v1/admin/tenants/acme/files/{id}/tier"),
            Some(json!({ "pin": "hot" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.total_candidate_blobs, 0);
    assert_eq!(report.held.get("pinned"), Some(&1));
}

#[tokio::test]
async fn shared_blobs_move_only_when_every_referent_agrees() {
    let (router, store, _dir) = stack().await;
    let payload = b"shared across tenants";

    // Two tenants store identical content: one blob row, two
    // referents (dedupe is the point).
    upload(&router, "acme", "shared.txt", payload).await;
    upload(&router, "beta", "shared.txt", payload).await;

    // acme says cold immediately; beta has no policy, and a tenant
    // without a policy demands hot.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/tiering",
            Some(json!({ "tier": "cold", "after_seconds": 0, "basis": "created" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.total_candidate_blobs, 0);
    assert_eq!(report.held.get("referenced_without_policy"), Some(&1));

    // beta naming a different tier cannot agree on a placement.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/beta/tiering",
            Some(json!({ "tier": "chill", "after_seconds": 0, "basis": "created" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.held.get("policies_name_different_tiers"), Some(&1));

    // beta agreeing on the tier but demanding a longer age holds the
    // blob: the most demanding reference wins.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/beta/tiering",
            Some(json!({ "tier": "cold", "after_seconds": 31_536_000, "basis": "created" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.total_candidate_blobs, 0);
    assert_eq!(report.held.get("not_yet_cold"), Some(&1));

    // beta agreeing but flooring min_bytes above the object holds it.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/beta/tiering",
            Some(
                json!({ "tier": "cold", "after_seconds": 0, "basis": "created",
                          "min_bytes": 1_000_000 }),
            ),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.held.get("below_min_bytes"), Some(&1));

    // Full agreement: one physical candidate, credited to both
    // tenants in their own views.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/beta/tiering",
            Some(json!({ "tier": "cold", "after_seconds": 0, "basis": "created" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = classify_pass(&store, &topology()).await.unwrap();
    assert_eq!(report.total_candidate_blobs, 1);
    assert_eq!(report.total_candidate_bytes, payload.len() as u64);
    assert_eq!(report.per_tenant["acme"].candidate_blobs, 1);
    assert_eq!(report.per_tenant["beta"].candidate_blobs, 1);
}
