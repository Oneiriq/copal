//! The GC recount over engine reference tracking.
//!
//! The recount is GC-load-bearing: an undercount ERASES LIVE CONTENT.
//! So the old aggregate recount stays in the tree as a test oracle,
//! and every retention scenario asserts the two computations agree
//! and equal the count the scenario was built to have. Alongside the
//! matrix sit the two properties that make the adoption safe to ship:
//! the boot loop applies zero statements against a database that
//! matches, and a database whose rows PREDATE the `REFERENCE` clause
//! is backfilled on its first boot under this code (references do not
//! backfill on their own - probed - and without the migration those
//! rows would recount to zero).

use copal_core::ContentDigest;
use copal_store::repo::blob;
use copal_store::{Store, StoreConfig};

fn digest(fill: char) -> ContentDigest {
    ContentDigest::parse(fill.to_string().repeat(64)).expect("64 hex chars parse")
}

async fn raw(store: &Store, sql: &str) {
    store
        .raw()
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn seeded_store() -> Store {
    Store::connect(StoreConfig::memory())
        .await
        .expect("connect")
}

/// Register a blob row and return the record-id literal for links.
async fn seed_blob(store: &Store, d: &ContentDigest) -> String {
    blob::record_sighting(store, d, 3, "local", "objects/x")
        .await
        .expect("sighting");
    format!("blob:{}", d.as_str())
}

/// Both recounts, asserted equal before either is returned: the whole
/// point of keeping the oracle.
async fn agreed_recount(store: &Store, d: &ContentDigest) -> i64 {
    let new = blob::recount_inbound_links(store, "local", d)
        .await
        .unwrap();
    let old = blob::recount_inbound_links_by_aggregate(store, "local", d)
        .await
        .unwrap();
    assert_eq!(
        new, old,
        "the reference-tracking recount and the aggregate oracle disagree",
    );
    new
}

async fn create_file(store: &Store, id: &str, blob_target: &str, deleted: bool) {
    let deleted_at = if deleted { "time::now()" } else { "NONE" };
    raw(
        store,
        &format!(
            "CREATE file:{id} SET tenant_id = 't1', path = '{id}.txt', created_by = 'tester', \
             metadata = {{}}, blob = {blob_target}, deleted_at = {deleted_at};"
        ),
    )
    .await;
}

/// The retention posture of a scenario version: what varies across
/// the matrix, separated from the plumbing arguments.
struct VersionSpec {
    number: u32,
    armed: bool,
    legal_hold: bool,
    /// A SurrealQL datetime expression, or None for no clock.
    retain_until: Option<&'static str>,
}

impl VersionSpec {
    fn armed(number: u32) -> Self {
        Self {
            number,
            armed: true,
            legal_hold: false,
            retain_until: None,
        }
    }
}

async fn create_version(
    store: &Store,
    id: &str,
    file_id: &str,
    blob_target: &str,
    spec: &VersionSpec,
) {
    let retain = spec.retain_until.unwrap_or("NONE");
    raw(
        store,
        &format!(
            "CREATE file_version:{id} SET tenant_id = 't1', number = {number}, \
             content_type = 'text/plain', size_bytes = 3, digest = 'd', \
             metadata_snapshot = {{}}, created_by = 'tester', file = file:{file_id}, \
             blob = {blob_target}, armed = {armed}, legal_hold = {legal_hold}, \
             retain_until = {retain};",
            number = spec.number,
            armed = spec.armed,
            legal_hold = spec.legal_hold,
        ),
    )
    .await;
}

#[tokio::test]
async fn a_live_file_counts_once() {
    let store = seeded_store().await;
    let d = digest('a');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "alpha", &b, false).await;
    assert_eq!(agreed_recount(&store, &d).await, 1);
}

#[tokio::test]
async fn a_soft_deleted_file_does_not_count() {
    let store = seeded_store().await;
    let d = digest('b');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "gone", &b, true).await;
    assert_eq!(agreed_recount(&store, &d).await, 0);
}

#[tokio::test]
async fn a_legal_hold_survives_its_files_tombstone() {
    let store = seeded_store().await;
    let d = digest('c');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "gone", &b, true).await;
    let held = VersionSpec {
        legal_hold: true,
        ..VersionSpec::armed(1)
    };
    create_version(&store, "held", "gone", &b, &held).await;
    assert_eq!(agreed_recount(&store, &d).await, 1);
}

#[tokio::test]
async fn an_unexpired_retention_clock_survives_the_tombstone() {
    let store = seeded_store().await;
    let d = digest('d');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "gone", &b, true).await;
    let kept = VersionSpec {
        retain_until: Some("time::now() + 1h"),
        ..VersionSpec::armed(1)
    };
    create_version(&store, "kept", "gone", &b, &kept).await;
    assert_eq!(agreed_recount(&store, &d).await, 1);
}

#[tokio::test]
async fn an_expired_clock_releases_the_blob() {
    let store = seeded_store().await;
    let d = digest('e');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "gone", &b, true).await;
    let lapsed = VersionSpec {
        retain_until: Some("time::now() - 1h"),
        ..VersionSpec::armed(1)
    };
    create_version(&store, "lapsed", "gone", &b, &lapsed).await;
    assert_eq!(agreed_recount(&store, &d).await, 0);
}

#[tokio::test]
async fn an_unarmed_version_never_counts() {
    let store = seeded_store().await;
    let d = digest('f');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "live", &b, false).await;
    let unarmed = VersionSpec {
        armed: false,
        ..VersionSpec::armed(1)
    };
    create_version(&store, "draft", "live", &b, &unarmed).await;
    assert_eq!(agreed_recount(&store, &d).await, 1);
}

/// Everything at once on one blob: two live files, a deleted one, a
/// version armed under the live file, a held version and an expired
/// one under the deleted file. Expected: 2 files + v1 + held = 4.
#[tokio::test]
async fn mixed_multiples_on_one_blob() {
    let store = seeded_store().await;
    let d = digest('1');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "one", &b, false).await;
    create_file(&store, "two", &b, false).await;
    create_file(&store, "dead", &b, true).await;
    create_version(&store, "v1", "one", &b, &VersionSpec::armed(1)).await;
    let held = VersionSpec {
        legal_hold: true,
        ..VersionSpec::armed(1)
    };
    create_version(&store, "held", "dead", &b, &held).await;
    let lapsed = VersionSpec {
        retain_until: Some("time::now() - 1h"),
        ..VersionSpec::armed(2)
    };
    create_version(&store, "lapsed", "dead", &b, &lapsed).await;
    assert_eq!(agreed_recount(&store, &d).await, 4);
}

/// The dedupe case: two files share one blob, one is deleted. The
/// survivor holds the content; the tombstone does not.
#[tokio::test]
async fn dedupe_counts_only_the_surviving_file() {
    let store = seeded_store().await;
    let d = digest('2');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "keeper", &b, false).await;
    create_file(&store, "dropped", &b, true).await;
    assert_eq!(agreed_recount(&store, &d).await, 1);
}

/// Hard-deleting the referencing rows (what version pruning does)
/// deregisters them at the engine, and the recount follows.
#[tokio::test]
async fn hard_deletes_deregister() {
    let store = seeded_store().await;
    let d = digest('3');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "brief", &b, false).await;
    assert_eq!(agreed_recount(&store, &d).await, 1);
    raw(&store, "DELETE file:brief;").await;
    assert_eq!(agreed_recount(&store, &d).await, 0);
}

/// The boot-loop property: a second apply against the same database
/// renders ZERO statements, computed fields and REFERENCE clauses
/// echoing back structurally identical.
#[tokio::test]
async fn a_second_apply_renders_zero_statements() {
    let store = seeded_store().await;
    let statements = store.apply_schema(None, None).await.expect("second apply");
    assert_eq!(statements, 0);
}

/// Same property with the vector index folded in: the CONCURRENTLY
/// build directive is excluded from diffs upstream (and echoed back
/// by INFO without it), so a backgrounded build does not re-apply on
/// every boot.
#[tokio::test]
async fn the_backgrounded_vector_index_does_not_flap() {
    let store = seeded_store().await;
    store.ensure_vector_index(384).await.expect("first ensure");
    let statements = store.apply_schema(None, Some(384)).await.expect("re-apply");
    assert_eq!(statements, 0);
}

/// The in-place upgrade: a database created BEFORE the REFERENCE
/// clause, carrying rows, must recount correctly after its first boot
/// under this code. Without the backfill the inbound sets would be
/// empty and the GC would erase live content.
#[tokio::test]
async fn upgrading_in_place_backfills_pre_existing_links() {
    // The pre-adoption release, reconstructed: today's tables with
    // reference tracking stripped - no REFERENCE on the blob links,
    // no computed inbound sets on the blob table.
    let mut old_tables = copal_store::schema::tables();
    for table in &mut old_tables {
        table.fields.retain(|field| field.computed.is_none());
        for field in &mut table.fields {
            field.reference = None;
        }
    }
    let mut old_statements: Vec<String> = copal_store::schema::text::analyzers()
        .iter()
        .map(|a| a.to_surql_with_options(true))
        .collect();
    old_statements.extend(
        old_tables
            .iter()
            .flat_map(|t| surql::schema::generate_table_sql(t, true)),
    );

    let cfg = surql::connection::ConnectionConfig::builder()
        .url("mem://")
        .namespace("upgrade")
        .database("upgrade")
        .build()
        .unwrap();
    let client = surql::connection::DatabaseClient::new(cfg).unwrap();
    client.connect().await.unwrap();
    client
        .query(&old_statements.join("\n"))
        .await
        .expect("old schema applies");

    // Rows written under the old schema: a live file, a tombstoned
    // one, an armed version under each (the dead file's version held).
    let store = Store::from_connected(client);
    let d = digest('4');
    let b = seed_blob(&store, &d).await;
    create_file(&store, "old_live", &b, false).await;
    create_file(&store, "old_dead", &b, true).await;
    create_version(&store, "old_v1", "old_live", &b, &VersionSpec::armed(1)).await;
    let held = VersionSpec {
        legal_hold: true,
        ..VersionSpec::armed(1)
    };
    create_version(&store, "old_held", "old_dead", &b, &held).await;

    // First boot under the new code: the diff adds REFERENCE and the
    // computed fields, and the backfill registers every old link.
    let applied = store.apply_schema(None, None).await.expect("upgrade apply");
    assert!(applied > 0, "the upgrade boot has definitions to apply");
    assert_eq!(
        agreed_recount(&store, &d).await,
        3,
        "old_live + old_v1 + old_held survive the upgrade"
    );

    // The next boot is quiet, and the recount holds.
    let second = store.apply_schema(None, None).await.expect("quiet boot");
    assert_eq!(second, 0);
    assert_eq!(agreed_recount(&store, &d).await, 3);

    // The freeze event was restored after the backfill's dance: an
    // armed version's links are immutable again.
    let frozen = store
        .raw()
        .query("UPDATE file_version:old_v1 SET blob = NONE;")
        .await;
    assert!(
        frozen.is_err(),
        "the freeze event guards after the backfill"
    );
}
