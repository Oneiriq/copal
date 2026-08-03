//! The embedded tier's distinguishing claim: `surrealkv://<path>`
//! runs the engine inside the process and the data survives a
//! reconnect. Everything else about the store is proven by the
//! mem:// suites; this file exists for durability.
//!
//! Gated on the `embedded` feature. Workspace builds enable it
//! through copal-server's default features, so CI runs this without
//! any extra flags.
#![cfg(feature = "embedded")]

use serde_json::json;

use copal_core::{FileSpec, TenantId};
use copal_store::repo::file;
use copal_store::{Store, StoreConfig};

fn config(path: &str) -> StoreConfig {
    StoreConfig {
        url: format!("surrealkv://{path}"),
        ..StoreConfig::memory()
    }
}

#[tokio::test]
async fn surrealkv_persists_across_reconnects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db").to_str().unwrap().replace('\\', "/");
    let tenant = TenantId::parse("acme").unwrap();
    let spec = FileSpec {
        path: "durable/notes.txt".to_owned(),
        content_type: "text/plain".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: json!({"kept": true}),
        idempotency_key: None,
    };

    let id = {
        let store = Store::connect(config(&path))
            .await
            .expect("surrealkv store connects and applies schema");
        let created = file::create_file(&store, &tenant, &spec, "tester")
            .await
            .expect("create");
        assert!(created.created);
        created.record.id
    };

    // A second connection to the same directory sees the record and
    // reconciles the schema without complaint.
    let store = Store::connect(config(&path))
        .await
        .expect("reconnect to the same directory");
    let record = file::get_file(&store, &tenant, &id)
        .await
        .expect("get")
        .expect("the record survived the reconnect");
    assert_eq!(record.path, "durable/notes.txt");
    assert_eq!(record.metadata["kept"], json!(true));
}
