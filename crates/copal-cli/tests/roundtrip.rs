//! copalctl's layer against a live in-process server: the same
//! [`Api`] the binary uses, over a real TCP listener, so what these
//! tests prove is what a terminal gets.

use copal_blob::ObjectStore;
use copal_cli::{Api, Auth};
use copal_server::auth::{AuthConfig, AuthMode};
use copal_server::{build_router, AppState};
use copal_store::{Store, StoreConfig};
use serde_json::json;

const ADMIN: &str = "operator-secret";

async fn serve() -> (String, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs).with_auth(AuthConfig {
        mode: AuthMode::ApiKeys,
        admin_token: Some(ADMIN.into()),
        admin_token_previous: None,
        operator_header: None,
    });
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (base, dir)
}

#[tokio::test]
async fn the_terminal_reaches_every_surface_it_wraps() {
    let (base, _dir) = serve().await;

    // Admin: mint a key for a tenant, which also proves the admin
    // helpers and creates the identity the file plane uses.
    let admin = Api::new(base.clone(), Auth::Anonymous, Some(ADMIN.into()));
    let minted = admin
        .admin_post("/v1/admin/tenants/acme/keys", json!({ "name": "cli" }))
        .await
        .unwrap();
    let token = minted["token"].as_str().unwrap().to_owned();

    // Files: create, upload, list, get, download, through the same
    // calls the subcommands make.
    let api = Api::new(base.clone(), Auth::Bearer(token), Some(ADMIN.into()));
    let created = api
        .post("/v1/files", json!({ "path": "cli/proof.txt" }))
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_owned();
    api.put_bytes(
        &format!("/v1/files/{id}/content"),
        b"terminal proof".to_vec(),
    )
    .await
    .unwrap();
    let listed = api
        .get("/v1/files", &[("state", "ready".to_owned())])
        .await
        .unwrap();
    assert!(
        listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == json!(id)),
        "the uploaded file lists as ready",
    );
    let record = api.get(&format!("/v1/files/{id}"), &[]).await.unwrap();
    assert_eq!(record["path"], json!("cli/proof.txt"));
    let bytes = api
        .get_bytes(&format!("/v1/files/{id}/content"))
        .await
        .unwrap();
    assert_eq!(&bytes, b"terminal proof");

    // Admin reads: the tenant population and the audit tail.
    let tenants = admin.admin_get("/v1/admin/tenants", &[]).await.unwrap();
    assert!(
        tenants["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["tenant_id"] == json!("acme")),
        "the tenant appears in the population",
    );
    let (audit, _cursor) = admin
        .admin_text("/v1/admin/audit/export", &[("limit", "50".to_owned())])
        .await
        .unwrap();
    assert!(audit.contains("key.minted"), "custody landed in the trail");

    // Refusals stay refusals: admin surfaces need the token.
    let bare = Api::new(base, Auth::Anonymous, None);
    assert!(bare.admin_get("/v1/admin/tenants", &[]).await.is_err());
}
