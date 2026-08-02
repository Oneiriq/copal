//! Retention slice one, proven at the eraser: a legal hold or an
//! unexpired retention clock holds a version's bytes through its
//! file's deletion, because the GC is the only thing that erases and
//! a non-erasable version never lets its blob reach the mark step.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::sweeps::{run_pass, SweepConfig};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, Store, ObjectStore, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs.clone());
    (build_router(state), store, blobs, dir)
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

/// Create a file, upload one version, soft-delete the file, and
/// return what the sweep needs to decide the blob's fate.
async fn deleted_file_with_version(
    router: &axum::Router,
    payload: &[u8],
) -> (
    copal_core::TenantId,
    copal_core::FileId,
    copal_core::ContentDigest,
) {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({"path": format!("held-{}.txt", payload.len())}).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    let id = json_body(response).await["id"].as_str().unwrap().to_owned();
    let upload = req(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from(payload.to_vec()),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let tenant = copal_core::TenantId::parse("acme").unwrap();
    let file = copal_core::FileId::parse(&id).unwrap();
    (tenant, file, copal_core::ContentDigest::of_bytes(payload))
}

async fn delete_file(router: &axum::Router, file: &copal_core::FileId) {
    let delete = req("DELETE", &format!("/v1/files/{file}"), Body::empty());
    let response = router.clone().oneshot(delete).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

async fn sweep_twice(store: &Store, blobs: &ObjectStore) -> u64 {
    let config = SweepConfig {
        gc_grace_secs: 0,
        ..SweepConfig::default()
    };
    run_pass(
        store,
        &copal_server::app::Residencies::local_only(blobs.clone()),
        &config,
    )
    .await;
    let report = run_pass(
        store,
        &copal_server::app::Residencies::local_only(blobs.clone()),
        &config,
    )
    .await;
    report.blobs_collected
}

/// A legal hold outlives the file's tombstone: the bytes survive an
/// aggressive GC until the hold is released, and are collected on the
/// first sweep after.
#[tokio::test]
async fn a_legal_hold_carries_bytes_through_deletion() {
    let (router, store, blobs, dir) = stack().await;
    let payload = b"held evidence";
    let (tenant, file, digest) = deleted_file_with_version(&router, payload).await;

    copal_store::repo::version::set_legal_hold(&store, &tenant, &file, 1, true)
        .await
        .expect("holds apply to armed versions");
    delete_file(&router, &file).await;

    assert_eq!(
        sweep_twice(&store, &blobs).await,
        0,
        "held bytes must survive"
    );
    let object = dir.path().join("objects").join(digest.storage_key());
    assert!(object.exists(), "the object must still be on disk");

    copal_store::repo::version::set_legal_hold(&store, &tenant, &file, 1, false)
        .await
        .expect("holds release");
    assert_eq!(
        sweep_twice(&store, &blobs).await,
        1,
        "released bytes collect"
    );
    assert!(!object.exists(), "the object is gone after release");
}

/// An unexpired retention clock refuses collection; an expired one
/// permits it. The clock alone decides, which is what makes
/// compliance mode one column rather than a subsystem.
#[tokio::test]
async fn the_retention_clock_gates_collection() {
    let (router, store, blobs, dir) = stack().await;
    let payload = b"retained quarterly report";
    let (tenant, file, digest) = deleted_file_with_version(&router, payload).await;

    assert!(copal_store::repo::version::set_retention(
        &store,
        &tenant,
        &file,
        1,
        3600,
        "governance",
    )
    .await
    .expect("retention applies to armed versions"));
    delete_file(&router, &file).await;

    assert_eq!(
        sweep_twice(&store, &blobs).await,
        0,
        "retained bytes survive"
    );
    let object = dir.path().join("objects").join(digest.storage_key());
    assert!(object.exists());

    // The clock runs out (simulated by clearing, which slice two will
    // gate behind mode and authority; the GC's contract is the
    // predicate, and an absent clock is an expired one).
    assert!(
        copal_store::repo::version::clear_retention(&store, &tenant, &file, 1)
            .await
            .expect("governance clears")
    );
    assert_eq!(sweep_twice(&store, &blobs).await, 1);
    assert!(!object.exists());
}

/// Retention state moves on an armed row while the artifact stays
/// frozen: the narrowed freeze event still throws on link tampering.
#[tokio::test]
async fn retention_moves_while_the_artifact_stays_frozen() {
    let (router, store, _blobs, _dir) = stack().await;
    let (tenant, file, _digest) = deleted_file_with_version(&router, b"frozen artifact").await;

    assert!(
        copal_store::repo::version::set_retention(&store, &tenant, &file, 1, 60, "governance")
            .await
            .expect("the clock sets on an armed row")
    );
    copal_store::repo::version::set_legal_hold(&store, &tenant, &file, 1, true)
        .await
        .expect("the hold sets on an armed row");

    let err = copal_store::repo::version::tamper_for_test(&store, &tenant, &file, 1)
        .await
        .expect_err("disarming must still throw");
    assert!(err.to_string().contains("immutable"), "{err}");
}

use copal_server::app::admin_router;
use copal_server::auth::AuthConfig;

async fn admin_stack() -> (
    axum::Router,
    axum::Router,
    Store,
    ObjectStore,
    tempfile::TempDir,
) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs.clone()).with_auth(AuthConfig {
        admin_token: Some("root".to_owned()),
        ..AuthConfig::default()
    });
    (
        build_router(state.clone()),
        admin_router(state),
        store,
        blobs,
        dir,
    )
}

fn admin_req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-admin-token", "root");
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    builder.body(body).unwrap()
}

/// Compliance mode is an authority line: the clock extends and
/// nothing else, admin or no admin. Governance shortens and clears
/// freely, and an expired compliance clock is an ordinary row again.
#[tokio::test]
async fn compliance_only_extends() {
    let (router, admin, _store, _blobs, _dir) = admin_stack().await;
    let (_tenant, file, _digest) = deleted_file_with_version(&router, b"worm subject").await;
    let retention = format!("/v1/admin/tenants/acme/files/{file}/versions/1/retention");

    // Into compliance, then extend: both apply.
    for seconds in [3600, 7200] {
        let response = admin
            .clone()
            .oneshot(admin_req(
                "PUT",
                &retention,
                Some(json!({ "seconds": seconds, "mode": "compliance" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "seconds {seconds}");
    }

    // Shortening, downgrading, and clearing all refuse with 409.
    for body in [
        json!({ "seconds": 60, "mode": "compliance" }),
        json!({ "seconds": 999_999, "mode": "governance" }),
    ] {
        let response = admin
            .clone()
            .oneshot(admin_req("PUT", &retention, Some(body.clone())))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT, "{body}");
    }
    let response = admin
        .clone()
        .oneshot(admin_req("DELETE", &retention, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

/// An expired compliance clock no longer binds: clearing succeeds.
#[tokio::test]
async fn an_expired_compliance_clock_releases_its_grip() {
    let (router, admin, _store, _blobs, _dir) = admin_stack().await;
    let (_tenant, file, _digest) = deleted_file_with_version(&router, b"expired worm").await;
    let retention = format!("/v1/admin/tenants/acme/files/{file}/versions/1/retention");

    let response = admin
        .clone()
        .oneshot(admin_req(
            "PUT",
            &retention,
            Some(json!({ "seconds": 0, "mode": "compliance" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = admin
        .clone()
        .oneshot(admin_req("DELETE", &retention, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// Governance is the operator's own leash: shorten and clear freely,
/// with every change audited.
#[tokio::test]
async fn governance_shortens_and_clears() {
    let (router, admin, _store, _blobs, _dir) = admin_stack().await;
    let (_tenant, file, _digest) = deleted_file_with_version(&router, b"governed subject").await;
    let retention = format!("/v1/admin/tenants/acme/files/{file}/versions/1/retention");

    for seconds in [3600, 60] {
        let response = admin
            .clone()
            .oneshot(admin_req(
                "PUT",
                &retention,
                Some(json!({ "seconds": seconds })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let response = admin
        .clone()
        .oneshot(admin_req("DELETE", &retention, None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// Holds require a stated reason, and both directions land in the
/// audit trail carrying it: the trail of who was granted what is the
/// reason the surface exists.
#[tokio::test]
async fn holds_carry_their_reasons_into_audit() {
    let (router, admin, _store, _blobs, _dir) = admin_stack().await;
    let (_tenant, file, _digest) = deleted_file_with_version(&router, b"litigation subject").await;
    let hold = format!("/v1/admin/tenants/acme/files/{file}/versions/1/hold");

    let response = admin
        .clone()
        .oneshot(admin_req("PUT", &hold, Some(json!({ "reason": "  " }))))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = admin
        .clone()
        .oneshot(admin_req(
            "PUT",
            &hold,
            Some(json!({ "reason": "case 2026-cv-1138" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = admin
        .clone()
        .oneshot(admin_req(
            "DELETE",
            &hold,
            Some(json!({ "reason": "case closed" })),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = admin
        .clone()
        .oneshot(admin_req("GET", "/v1/admin/tenants/acme/audit", None))
        .await
        .unwrap();
    let body = json_body(response).await;
    let trail = body["items"].to_string();
    assert!(trail.contains("version.hold_applied"), "{trail}");
    assert!(trail.contains("version.hold_released"), "{trail}");
    assert!(trail.contains("case 2026-cv-1138"), "{trail}");
}
