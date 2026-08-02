//! Engine facts schema evolution stands on, pinned like the session
//! and retrieval facts beside them.
//!
//! The mechanism re-applies changed definitions with `OVERWRITE`, so
//! three behaviors are load-bearing: `OVERWRITE` creates when the
//! object is absent (one form serves fresh and existing databases),
//! it replaces the definition when present (an upgraded deployment
//! receives the `PERMISSIONS` it was defined without), and it leaves
//! stored rows untouched.

use surql::connection::{ConnectionConfig, DatabaseClient};

async fn client(namespace: &str) -> DatabaseClient {
    let cfg = ConnectionConfig::builder()
        .url("mem://")
        .namespace(namespace)
        .database(namespace)
        .build()
        .unwrap();
    let client = DatabaseClient::new(cfg).unwrap();
    client.connect().await.unwrap();
    client
}

#[tokio::test]
async fn overwrite_creates_when_absent() {
    let client = client("ow_creates").await;
    let result = client
        .query("DEFINE TABLE OVERWRITE doc SCHEMALESS; CREATE doc SET body = 'first';")
        .await
        .expect("overwrite on an absent table creates it");
    let rows = result.as_array().unwrap();
    assert_eq!(rows.len(), 2, "{result:?}");
}

#[tokio::test]
async fn overwrite_replaces_definitions_and_keeps_rows() {
    let client = client("ow_replaces").await;
    client
        .query(
            "DEFINE TABLE doc SCHEMALESS; \
             CREATE doc SET tenant_id = 'acme', body = 'kept';",
        )
        .await
        .unwrap();

    // The upgrade shape: the same table redefined WITH permissions.
    client
        .query(
            "DEFINE TABLE OVERWRITE doc SCHEMALESS \
             PERMISSIONS FOR select, create, update, delete WHERE tenant_id = $token.tn;",
        )
        .await
        .expect("overwrite replaces an existing definition");

    let info = client.query("INFO FOR DB;").await.unwrap();
    let echo = info[0]["tables"]["doc"].as_str().unwrap().to_owned();
    assert!(
        echo.contains("PERMISSIONS"),
        "definition not replaced: {echo}"
    );

    let rows = client.query("SELECT * FROM doc;").await.unwrap();
    let rows = rows[0].as_array().unwrap();
    assert_eq!(rows.len(), 1, "rows lost by overwrite: {rows:?}");
    assert_eq!(rows[0]["body"], "kept");
}

#[tokio::test]
async fn access_overwrite_creates_and_replaces() {
    let client = client("ow_access").await;
    for key in ["first-key", "second-key"] {
        client
            .query(&format!(
                "DEFINE ACCESS OVERWRITE caller ON DATABASE TYPE RECORD \
                 WITH JWT ALGORITHM HS256 KEY '{key}' DURATION FOR SESSION NONE;",
            ))
            .await
            .unwrap_or_else(|e| panic!("access overwrite with {key}: {e}"));
    }
    let info = client.query("INFO FOR DB;").await.unwrap();
    let access = info[0]["accesses"]["caller"].as_str().unwrap().to_owned();
    assert!(access.contains("TYPE RECORD"), "{access}");
}
