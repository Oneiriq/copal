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

/// A surrealkv directory SurrealDB 3.3.5 wrote, the kind every Copal
/// release before the SurrealDB 4 engine left behind. Namespace and
/// database `fixture`; the same bytes surql-rs pins its upgrade tests on.
const SURREALDB_3_DIRECTORY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/surrealkv-3.3.5"
);

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Every file under `dir` with its bytes, the engine's lock aside.
fn snapshot(dir: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name != "LOCK") {
                let relative = path.strip_prefix(dir).unwrap().to_path_buf();
                files.push((relative, std::fs::read(&path).unwrap()));
            }
        }
    }
    files.sort();
    files
}

/// The embedded engine is SurrealDB 4, which refuses a directory 3.x
/// wrote unless the connection opts in to an experimental, one-way
/// upgrade. Copal does not opt in: boot stops, the error says what an
/// operator can do (the variable SurrealDB's own message names reaches
/// only a server), and the directory is left byte for byte as it was,
/// so the release that wrote it can still open it.
#[tokio::test]
async fn a_directory_surrealdb_3_wrote_is_refused_and_left_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join("db");
    copy_dir(std::path::Path::new(SURREALDB_3_DIRECTORY), &store_dir);
    let before = snapshot(&store_dir);
    let path = store_dir.to_str().unwrap().replace('\\', "/");

    let refused = Store::connect(config(&path))
        .await
        .err()
        .expect("a 3.x directory is refused");
    let message = refused.to_string();
    assert!(message.contains("SurrealDB 3"), "{message}");
    assert!(
        message.contains("does not upgrade a 3.x datastore itself"),
        "{message}"
    );
    assert!(message.contains("docs/operations.md"), "{message}");

    assert_eq!(
        snapshot(&store_dir),
        before,
        "the refusal changed the directory"
    );
}
