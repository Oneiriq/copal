//! End-to-end store tests on the embedded mem:// engine.
//!
//! These run without any server or container: the dev-dependency on
//! `surrealdb` with `kv-mem` wires the in-memory engine into test builds
//! via feature unification. Every test gets a fresh engine, so there is
//! no cross-test state.

use serde_json::json;

use copal_core::{CopalError, FileSpec, FileState, TenantId};
use copal_store::repo::{blob, file};
use copal_store::{Store, StoreConfig};

async fn fresh_store() -> Store {
    Store::connect(StoreConfig::memory())
        .await
        .expect("mem:// store connects and applies schema")
}

fn tenant() -> TenantId {
    TenantId::parse("acme").unwrap()
}

fn spec(path: &str) -> FileSpec {
    FileSpec {
        path: path.to_owned(),
        content_type: "application/pdf".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: json!({"source": "test"}),
        idempotency_key: None,
    }
}

#[tokio::test]
async fn create_then_get_then_list() {
    let store = fresh_store().await;
    let t = tenant();

    let created = file::create_file(&store, &t, &spec("docs/plan.pdf"), "tester")
        .await
        .expect("create");
    assert_eq!(created.state, FileState::Draft);
    assert_eq!(created.path, "docs/plan.pdf");

    let fetched = file::get_file(&store, &t, &created.id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.metadata["source"], "test");

    // The optimistic-create contract: create-then-list must see the file.
    let listed = file::list_files(&store, &t, 10).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, created.id);
}

#[tokio::test]
async fn tenancy_is_a_hard_wall() {
    let store = fresh_store().await;
    let created = file::create_file(&store, &tenant(), &spec("a.txt"), "tester")
        .await
        .unwrap();

    let other = TenantId::parse("globex").unwrap();
    // A foreign tenant reads absence, not denial.
    assert!(file::get_file(&store, &other, &created.id)
        .await
        .unwrap()
        .is_none());
    assert!(file::list_files(&store, &other, 10)
        .await
        .unwrap()
        .is_empty());
    // And cannot move the state machine.
    let err = file::transition(
        &store,
        &other,
        &created.id,
        FileState::Draft,
        FileState::Uploading,
        Default::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CopalError::Conflict(_)), "{err}");
}

#[tokio::test]
async fn duplicate_live_path_conflicts_and_idempotency_key_dedupes() {
    let store = fresh_store().await;
    let t = tenant();
    file::create_file(&store, &t, &spec("same/path.txt"), "tester")
        .await
        .unwrap();

    // Same live path -> unique index violation -> Conflict.
    let err = file::create_file(&store, &t, &spec("same/path.txt"), "tester")
        .await
        .unwrap_err();
    assert!(matches!(err, CopalError::Conflict(_)), "{err}");

    // Same idempotency key -> Conflict as well (probed: absent keys never
    // collide, equal keys do).
    let mut keyed = spec("k1.txt");
    keyed.idempotency_key = Some("retry-abc".into());
    file::create_file(&store, &t, &keyed, "tester")
        .await
        .unwrap();
    let mut keyed2 = spec("k2.txt");
    keyed2.idempotency_key = Some("retry-abc".into());
    let err = file::create_file(&store, &t, &keyed2, "tester")
        .await
        .unwrap_err();
    assert!(matches!(err, CopalError::Conflict(_)), "{err}");

    // Distinct absent keys stay independent: a third keyless create works.
    file::create_file(&store, &t, &spec("k3.txt"), "tester")
        .await
        .unwrap();
}

#[tokio::test]
async fn state_machine_cas_enforces_transitions() {
    let store = fresh_store().await;
    let t = tenant();
    let f = file::create_file(&store, &t, &spec("cas.bin"), "tester")
        .await
        .unwrap();

    // Legal: draft -> uploading.
    let up = file::transition(
        &store,
        &t,
        &f.id,
        FileState::Draft,
        FileState::Uploading,
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(up.state, FileState::Uploading);

    // Replaying the same CAS loses: the record is no longer draft.
    let err = file::transition(
        &store,
        &t,
        &f.id,
        FileState::Draft,
        FileState::Uploading,
        Default::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CopalError::Conflict(_)), "{err}");

    // Illegal transitions are rejected in the domain before any IO.
    let err = file::transition(
        &store,
        &t,
        &f.id,
        FileState::Uploading,
        FileState::Draft,
        Default::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CopalError::Conflict(_)), "{err}");
}

#[tokio::test]
async fn completion_links_blob_and_dedupes_content() {
    let store = fresh_store().await;
    let t = tenant();
    let digest = copal_core::ContentDigest::of_bytes(b"hello world");

    // First sighting creates the row; second increments refcount.
    blob::record_sighting(&store, &digest, 11, "local", &digest.storage_key())
        .await
        .unwrap();
    blob::record_sighting(&store, &digest, 11, "local", &digest.storage_key())
        .await
        .unwrap();
    let loc = blob::get_location(&store, &digest).await.unwrap().unwrap();
    assert_eq!(loc.0, "local");
    assert_eq!(loc.1, digest.storage_key());

    // Full loop: create -> uploading -> ready with digest+size+link.
    let f = file::create_file(&store, &t, &spec("hello.txt"), "tester")
        .await
        .unwrap();
    file::transition(
        &store,
        &t,
        &f.id,
        FileState::Draft,
        FileState::Uploading,
        Default::default(),
    )
    .await
    .unwrap();
    let done = file::transition(
        &store,
        &t,
        &f.id,
        FileState::Uploading,
        FileState::Ready,
        file::TransitionSets {
            digest: Some(digest.clone()),
            size_bytes: Some(11),
            link_blob: Some(digest.clone()),
        },
    )
    .await
    .unwrap();
    assert_eq!(done.state, FileState::Ready);
    assert_eq!(done.digest.as_ref().unwrap(), &digest);
    assert_eq!(done.size_bytes, Some(11));
}
