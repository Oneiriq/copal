//! Tenant quotas end to end: usage accounting, admin assignment, and
//! enforcement across the upload faces.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use serde_json::{json, Value};
use tower::ServiceExt as _;

use copal_blob::ObjectStore;
use copal_server::app::{build_router, AppState};
use copal_server::auth::AuthConfig;
use copal_store::{Store, StoreConfig};

async fn stack() -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        admin_token: Some("root".to_owned()),
        ..AuthConfig::default()
    });
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

fn admin(method: &str, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-admin-token", "root")
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

async fn create(router: &axum::Router, path: &str) -> String {
    let create = req(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path, "content_type": "text/plain" }).to_string()),
    );
    let response = router.clone().oneshot(create).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await["id"].as_str().unwrap().to_owned()
}

async fn put_content(router: &axum::Router, id: &str, content: &[u8]) -> StatusCode {
    // In production hyper sets content-length from the client request;
    // oneshot tests must declare it for the pre-check path.
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/files/{id}/content"))
        .header("x-copal-tenant", "acme")
        .header("content-length", content.len().to_string())
        .body(Body::from(content.to_vec()))
        .unwrap();
    router.clone().oneshot(put).await.unwrap().status()
}

#[tokio::test]
async fn quotas_meter_enforce_and_release() {
    let (router, _dir) = stack().await;

    // Unlimited by default; usage starts empty.
    let body = json_body(
        router
            .clone()
            .oneshot(req("GET", "/v1/usage", Body::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["bytes"], 0);
    assert_eq!(body["files"], 0);
    assert!(body["quota_bytes"].is_null());

    // A 100-byte ceiling.
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/quota",
            Body::from(json!({ "max_bytes": 100 }).to_string()),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 60 bytes fit.
    let first = create(&router, "a.txt").await;
    assert_eq!(
        put_content(&router, &first, &[b'x'; 60]).await,
        StatusCode::OK,
    );
    let body = json_body(
        router
            .clone()
            .oneshot(req("GET", "/v1/usage", Body::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["bytes"], 60);
    assert_eq!(body["files"], 1);
    assert_eq!(body["quota_bytes"], 100);

    // A declared 60 more refuses up front (content-length is known).
    let second = create(&router, "b.txt").await;
    assert_eq!(
        put_content(&router, &second, &[b'y'; 60]).await,
        StatusCode::CONFLICT,
    );

    // 30 more fit under the ceiling.
    assert_eq!(
        put_content(&router, &second, &[b'y'; 30]).await,
        StatusCode::OK,
    );

    // A resumable session past the remaining headroom refuses at
    // creation, before any byte moves.
    let tus = Request::builder()
        .method("POST")
        .uri("/v1/tus")
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .header("upload-length", "50")
        .header(
            "upload-metadata",
            format!(
                "path {}",
                base64::engine::general_purpose::STANDARD.encode("c.bin"),
            ),
        )
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(tus).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Deleting a file releases its usage.
    let remove = req("DELETE", &format!("/v1/files/{first}"), Body::empty());
    assert_eq!(
        router.clone().oneshot(remove).await.unwrap().status(),
        StatusCode::NO_CONTENT,
    );
    let body = json_body(
        router
            .clone()
            .oneshot(req("GET", "/v1/usage", Body::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["bytes"], 30);
    assert_eq!(body["files"], 1);

    // The admin view agrees; clearing returns to unlimited and the
    // second clear reads 404.
    let body = json_body(
        router
            .clone()
            .oneshot(admin("GET", "/v1/admin/tenants/acme/quota", Body::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["max_bytes"], 100);
    assert_eq!(body["bytes"], 30);
    for expected in [StatusCode::NO_CONTENT, StatusCode::NOT_FOUND] {
        let response = router
            .clone()
            .oneshot(admin(
                "DELETE",
                "/v1/admin/tenants/acme/quota",
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let big = create(&router, "big.txt").await;
    assert_eq!(
        put_content(&router, &big, &[b'z'; 500]).await,
        StatusCode::OK,
        "unlimited again",
    );
}

#[tokio::test]
async fn concurrent_uploads_cannot_overshoot_the_ceiling() {
    // The reservation is the point of this design: without it, every
    // in-flight upload reads the same headroom and they collectively
    // exceed the quota.
    let (router, _dir) = stack().await;
    let response = router
        .clone()
        .oneshot(admin(
            "PUT",
            "/v1/admin/tenants/acme/quota",
            Body::from(json!({ "max_bytes": 100 }).to_string()),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Five records, each wanting 40 bytes against a 100-byte ceiling:
    // at most two can fit.
    let mut ids = Vec::new();
    for n in 0..5 {
        ids.push(create(&router, &format!("racer-{n}.bin")).await);
    }
    let mut handles = Vec::new();
    for id in ids {
        let router = router.clone();
        handles.push(tokio::spawn(async move {
            let put = Request::builder()
                .method("PUT")
                .uri(format!("/v1/files/{id}/content"))
                .header("x-copal-tenant", "acme")
                .header("content-length", "40")
                .body(Body::from(vec![b'x'; 40]))
                .unwrap();
            router.oneshot(put).await.unwrap().status()
        }));
    }
    let mut accepted = 0;
    for handle in handles {
        if handle.await.unwrap() == StatusCode::OK {
            accepted += 1;
        }
    }
    assert!(
        (1..=2).contains(&accepted),
        "at most two 40-byte uploads fit under 100 bytes, {accepted} were accepted",
    );

    let body = json_body(
        router
            .clone()
            .oneshot(req("GET", "/v1/usage", Body::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert!(
        body["bytes"].as_i64().unwrap() <= 100,
        "usage never exceeds the ceiling: {body}",
    );
}

#[tokio::test]
async fn the_sweep_recomputes_a_drifted_counter() {
    use copal_server::app::Residencies;
    use copal_server::sweeps::{run_pass, SweepConfig};
    use copal_store::repo::tenant as tenant_repo;

    // The counter is a cache, so it can drift (a crash between the
    // reservation and the release, say). The sweep recomputes it from
    // the file rows, which are the truth.
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs.clone());
    let router = build_router(state);
    let tenant = copal_core::TenantId::parse("acme").unwrap();

    let id = create(&router, "counted.txt").await;
    assert_eq!(put_content(&router, &id, &[b'z'; 42]).await, StatusCode::OK);
    assert_eq!(
        tenant_repo::cached_usage(&store, &tenant).await.unwrap(),
        Some((42, 1)),
    );

    // Drift it far from the truth in both directions.
    tenant_repo::set_usage(&store, &tenant, 9_999, 7)
        .await
        .unwrap();
    let body = json_body(
        router
            .clone()
            .oneshot(req("GET", "/v1/usage", Body::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body["bytes"], 9_999, "the cache is believed until swept");

    let report = run_pass(
        &store,
        &Residencies::local_only(blobs),
        &SweepConfig::default(),
    )
    .await;
    assert_eq!(report.usage_reconciled, 1);
    assert_eq!(
        tenant_repo::cached_usage(&store, &tenant).await.unwrap(),
        Some((42, 1)),
        "the recount restores the truth",
    );
}

#[tokio::test]
async fn abandoned_resumable_sessions_give_their_reservation_back() {
    use copal_blob::ObjectStore as Store2;
    use copal_server::app::Residencies;
    use copal_server::sweeps::{run_pass, SweepConfig};
    use copal_store::repo::tenant as tenant_repo;

    // A resumable session reserves its declared length up front, so a
    // client that starts sessions and walks away could deny its own
    // tenant headroom until the next reconciliation. Terminating and
    // sweeping both release it.
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Store2::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store.clone(), blobs.clone()).with_auth(AuthConfig {
        admin_token: Some("root".to_owned()),
        ..AuthConfig::default()
    });
    let router = build_router(state);
    let tenant = copal_core::TenantId::parse("acme").unwrap();

    let start = |path: &'static str, length: usize| {
        let router = router.clone();
        async move {
            let request = Request::builder()
                .method("POST")
                .uri("/v1/tus")
                .header("x-copal-tenant", "acme")
                .header("tus-resumable", "1.0.0")
                .header("upload-length", length.to_string())
                .header(
                    "upload-metadata",
                    format!(
                        "path {}",
                        base64::engine::general_purpose::STANDARD.encode(path),
                    ),
                )
                .body(Body::empty())
                .unwrap();
            let response = router.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            response.headers()["location"].to_str().unwrap().to_owned()
        }
    };

    let terminated = start("abandoned/one.bin", 5_000).await;
    let swept = start("abandoned/two.bin", 7_000).await;
    assert_eq!(
        tenant_repo::cached_usage(&store, &tenant).await.unwrap(),
        Some((12_000, 2)),
        "both sessions hold their declared length",
    );

    // Terminating returns that session's bytes at once.
    let request = Request::builder()
        .method("DELETE")
        .uri(&terminated)
        .header("x-copal-tenant", "acme")
        .header("tus-resumable", "1.0.0")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(request).await.unwrap().status(),
        StatusCode::NO_CONTENT,
    );
    assert_eq!(
        tenant_repo::cached_usage(&store, &tenant)
            .await
            .unwrap()
            .unwrap()
            .0,
        7_000,
        "termination releases immediately",
    );

    // Sweeping the abandoned one releases the rest.
    let _ = swept;
    let config = SweepConfig {
        tus_session_ttl_secs: 0,
        ..SweepConfig::default()
    };
    let report = run_pass(&store, &Residencies::local_only(blobs), &config).await;
    assert_eq!(report.tus_sessions_swept, 1);
    assert_eq!(
        tenant_repo::cached_usage(&store, &tenant)
            .await
            .unwrap()
            .unwrap()
            .0,
        0,
        "the sweep releases what the client abandoned",
    );
}
