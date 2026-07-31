//! End-to-end store tests on the embedded mem:// engine.
//!
//! These run without any server or container: the dev-dependency on
//! `surrealdb` with `kv-mem` wires the in-memory engine into test builds
//! via feature unification. Every test gets a fresh engine, so there is
//! no cross-test state.

use serde_json::json;

use copal_core::{CopalError, FileRecord, FileSpec, FileState, TenantId};
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

async fn create(store: &Store, t: &TenantId, path: &str) -> FileRecord {
    let created = file::create_file(store, t, &spec(path), "tester")
        .await
        .expect("create");
    assert!(created.created);
    created.record
}

#[tokio::test]
async fn create_then_get_then_list() {
    let store = fresh_store().await;
    let t = tenant();

    let created = create(&store, &t, "docs/plan.pdf").await;
    assert_eq!(created.state, FileState::Draft);
    assert_eq!(created.path, "docs/plan.pdf");

    let fetched = file::get_file(&store, &t, &created.id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.metadata["source"], "test");

    // The optimistic-create contract: create-then-list must see the file.
    let listed = file::list_files(&store, &t, 10, None, false, None)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, created.id);
}

#[tokio::test]
async fn tenancy_is_a_hard_wall() {
    let store = fresh_store().await;
    let created = create(&store, &tenant(), "a.txt").await;

    let other = TenantId::parse("globex").unwrap();
    // A foreign tenant reads absence, not denial.
    assert!(file::get_file(&store, &other, &created.id)
        .await
        .unwrap()
        .is_none());
    assert!(file::list_files(&store, &other, 10, None, false, None)
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
async fn duplicate_live_path_conflicts() {
    let store = fresh_store().await;
    let t = tenant();
    create(&store, &t, "same/path.txt").await;

    // Same live path -> unique index violation -> Conflict.
    let err = file::create_file(&store, &t, &spec("same/path.txt"), "tester")
        .await
        .unwrap_err();
    assert!(matches!(err, CopalError::Conflict(_)), "{err}");
}

#[tokio::test]
async fn idempotency_key_replay_returns_the_original() {
    let store = fresh_store().await;
    let t = tenant();

    let mut keyed = spec("k1.txt");
    keyed.idempotency_key = Some("retry-abc".into());
    let first = file::create_file(&store, &t, &keyed, "tester")
        .await
        .unwrap();
    assert!(first.created);

    // Replay with the same key (even under a different path) is
    // success returning the ORIGINAL record instead of a conflict. The key
    // identifies the request; retries must be safe.
    let mut replay = spec("entirely/different.txt");
    replay.idempotency_key = Some("retry-abc".into());
    let second = file::create_file(&store, &t, &replay, "tester")
        .await
        .unwrap();
    assert!(!second.created);
    assert_eq!(second.record.id, first.record.id);
    assert_eq!(second.record.path, "k1.txt");

    // Keys are tenant-scoped: another tenant reuses the key freely.
    let other = TenantId::parse("globex").unwrap();
    let mut foreign = spec("k1.txt");
    foreign.idempotency_key = Some("retry-abc".into());
    assert!(
        file::create_file(&store, &other, &foreign, "tester")
            .await
            .unwrap()
            .created
    );

    // Distinct absent keys stay independent.
    create(&store, &t, "k3.txt").await;
}

#[tokio::test]
async fn state_machine_cas_enforces_transitions() {
    let store = fresh_store().await;
    let t = tenant();
    let f = create(&store, &t, "cas.bin").await;

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
async fn claim_sets_a_lease_and_completion_clears_it() {
    let store = fresh_store().await;
    let t = tenant();
    let f = create(&store, &t, "leased.bin").await;

    let claimed = file::claim_upload(&store, &t, &f.id, "worker-1", 900)
        .await
        .unwrap();
    assert_eq!(claimed.state, FileState::Uploading);
    assert_eq!(claimed.upload_lease_owner.as_deref(), Some("worker-1"));
    assert!(claimed.upload_lease_expires_at.is_some());

    // A live lease refuses a second claimant.
    let err = file::claim_upload(&store, &t, &f.id, "worker-2", 900)
        .await
        .unwrap_err();
    assert!(matches!(err, CopalError::Conflict(_)), "{err}");

    // Completion clears the lease atomically with the state.
    let done = file::transition(
        &store,
        &t,
        &f.id,
        FileState::Uploading,
        FileState::Ready,
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(done.state, FileState::Ready);
    assert!(done.upload_lease_owner.is_none());
    assert!(done.upload_lease_expires_at.is_none());
}

#[tokio::test]
async fn expired_claims_are_stealable_and_reapable() {
    let store = fresh_store().await;
    let t = tenant();

    // Steal: a zero-TTL lease is immediately expired, and a second
    // claimant takes it over without waiting for the reaper.
    let stolen = create(&store, &t, "steal.bin").await;
    file::claim_upload(&store, &t, &stolen.id, "dead-instance", 0)
        .await
        .unwrap();
    let taken = file::claim_upload(&store, &t, &stolen.id, "live-instance", 900)
        .await
        .unwrap();
    assert_eq!(taken.state, FileState::Uploading);
    assert_eq!(taken.upload_lease_owner.as_deref(), Some("live-instance"));

    // Reap: an expired claim nobody steals sweeps to failed (retryable),
    // lease cleared. The live lease from above must NOT be reaped.
    let reapable = create(&store, &t, "reap.bin").await;
    file::claim_upload(&store, &t, &reapable.id, "dead-instance", 0)
        .await
        .unwrap();
    let reaped = file::reap_expired_uploads(&store).await.unwrap();
    assert_eq!(reaped.len(), 1, "only the expired claim reaps: {reaped:?}");
    assert_eq!(reaped[0].id, reapable.id);
    assert_eq!(reaped[0].state, FileState::Failed);
    assert!(reaped[0].upload_lease_owner.is_none());

    // The reaped file is claimable again via the failed path.
    let retried = file::claim_upload(&store, &t, &reapable.id, "retry", 900)
        .await
        .unwrap();
    assert_eq!(retried.state, FileState::Uploading);
}

#[tokio::test]
async fn completion_links_blob_and_recount_derives_references() {
    let store = fresh_store().await;
    let t = tenant();
    let digest = copal_core::ContentDigest::of_bytes(b"hello world");

    // Sighting is idempotent: first call creates, replay is a no-op.
    blob::record_sighting(&store, &digest, 11, "local", &digest.storage_key())
        .await
        .unwrap();
    blob::record_sighting(&store, &digest, 11, "local", &digest.storage_key())
        .await
        .unwrap();
    let loc = blob::get_location(&store, &digest).await.unwrap().unwrap();
    assert_eq!(loc.0, "local");
    assert_eq!(loc.1, digest.storage_key());

    // Two files completing onto the same content: the authoritative
    // reference count is DERIVED from the links, immune to the
    // crash-between-sighting-and-link drift an increment would suffer.
    for path in ["hello-a.txt", "hello-b.txt"] {
        let f = create(&store, &t, path).await;
        file::claim_upload(&store, &t, &f.id, "w", 900)
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
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(done.state, FileState::Ready);
        assert_eq!(done.digest.as_ref().unwrap(), &digest);
    }
    assert_eq!(
        blob::recount_inbound_links(&store, &digest).await.unwrap(),
        2
    );
}
