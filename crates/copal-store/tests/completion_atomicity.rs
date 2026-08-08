//! Completion is one transaction, proved on the embedded mem:// engine.
//!
//! The old completion was five to ten guarded statements, each atomic
//! alone and none atomic together, and the gap that mattered most was
//! between the CAS that increments `version_count` and the UPDATE that
//! points `current_version` at the version the increment named. These
//! tests hold that gap shut: one asks whether a reader can ever catch
//! the file between the two, and one races two writers at the same
//! claim to check that the compare-and-swap still decides the winner
//! and that the loser leaves nothing behind.

use serde::Deserialize;
use serde_json::json;

use copal_core::{ContentDigest, CopalError, FileId, FileSpec, FileState, TenantId};
use copal_store::repo::{blob, completion, file, tenant, version};
use copal_store::{Store, StoreConfig};

async fn fresh_store() -> Store {
    Store::connect(StoreConfig::memory())
        .await
        .expect("mem:// store connects and applies schema")
}

fn acme() -> TenantId {
    TenantId::parse("acme").unwrap()
}

/// A distinct digest per byte, so successive versions of one file link
/// distinct blobs the way real re-uploads do.
fn digest(seed: u8) -> ContentDigest {
    ContentDigest::parse(format!("{seed:02x}").repeat(32)).unwrap()
}

async fn drafted(store: &Store, path: &str) -> FileId {
    let spec = FileSpec {
        path: path.to_owned(),
        content_type: "application/pdf".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: json!({"source": "test"}),
        idempotency_key: None,
    };
    file::create_file(store, &acme(), &spec, "tester")
        .await
        .expect("create")
        .record
        .id
}

/// Claim, sight the blob, and complete: one whole upload, the way every
/// face performs it.
async fn upload(store: &Store, id: &FileId, seed: u8) -> copal_core::Result<()> {
    let digest = digest(seed);
    blob::record_sighting(store, &digest, 4, "local", &format!("p/{seed}")).await?;
    file::claim_upload(store, &acme(), id, "tester", 900).await?;
    completion::complete_upload(
        store,
        &acme(),
        id,
        "local",
        &digest,
        4,
        "tester",
        FileState::Ready,
    )
    .await?;
    Ok(())
}

/// What a reader sees of the two columns that must agree: the count of
/// completed versions and the number of the version being served.
#[derive(Debug, Clone, Copy, Deserialize)]
struct Linkage {
    version_count: u64,
    #[serde(default)]
    current_number: Option<u64>,
}

/// Read the pair the way any face would, through the record id. The
/// version number is traversed through the link, so a `current_version`
/// pointing at a row that does not exist would read as absent rather
/// than as agreement.
async fn linkage(store: &Store, id: &FileId) -> Linkage {
    let raw = store
        .raw()
        .query(&format!(
            "SELECT version_count, current_version.number AS current_number FROM file:{id};",
        ))
        .await
        .expect("linkage read");
    serde_json::from_value::<Vec<Linkage>>(raw[0].clone())
        .expect("linkage shape")
        .pop()
        .expect("the file exists")
}

impl Linkage {
    /// The invariant: an unuploaded file serves nothing and counts
    /// nothing, and every other file serves the version it counted to.
    /// There is no third state, and the point of the transaction is
    /// that no reader can ever be shown one.
    fn agrees(&self) -> bool {
        self.current_number.unwrap_or(0) == self.version_count
    }
}

/// A completed upload leaves the file, its version row and its outbox
/// event all describing the same version.
#[tokio::test]
async fn completion_lands_whole() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    upload(&store, &id, 1).await.expect("first upload");

    let record = file::get_file(&store, &acme(), &id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(record.state, FileState::Ready);
    assert_eq!(record.version_count, 1);
    assert_eq!(record.digest.as_ref(), Some(&digest(1)));

    let seen = linkage(&store, &id).await;
    assert_eq!(seen.current_number, Some(1), "the link is armed: {seen:?}");
    assert!(seen.agrees(), "{seen:?}");

    let versions = version::list_versions(&store, &acme(), &id, 10, None)
        .await
        .expect("versions");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].number, 1);
    assert_eq!(versions[0].digest, digest(1));

    // The file table's outbox event fires inside the same transaction,
    // so a completed upload and its event are one write or neither.
    let events = store
        .raw()
        .query("SELECT action, payload.version AS version FROM file_event;")
        .await
        .expect("events");
    assert_eq!(events[0][0]["action"], "file.ready");
    assert_eq!(events[0][0]["version"], 1);
}

/// Re-uploading chains the version rows: the new version's `prior` is
/// the one the file was serving, read inside the transaction from the
/// same statement that overwrote it.
#[tokio::test]
async fn versions_chain_through_prior() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    upload(&store, &id, 1).await.expect("first upload");
    upload(&store, &id, 2).await.expect("second upload");

    let seen = linkage(&store, &id).await;
    assert_eq!(seen.version_count, 2);
    assert!(seen.agrees(), "{seen:?}");

    let chain = store
        .raw()
        .query(
            "SELECT number, prior.number AS prior_number, armed FROM file_version ORDER BY number;",
        )
        .await
        .expect("chain");
    assert_eq!(chain[0][0]["number"], 1);
    assert!(chain[0][0]["prior_number"].is_null(), "the first has none");
    assert_eq!(chain[0][0]["armed"], true, "rows are born armed");
    assert_eq!(chain[0][1]["number"], 2);
    assert_eq!(chain[0][1]["prior_number"], 1);
    assert_eq!(chain[0][1]["armed"], true);
}

/// Sample the file in a tight loop until the returned flag is set, so a
/// writer running beside it is watched rather than merely followed.
fn watcher(store: &Store, id: &FileId) -> (Stop, ReaderTask) {
    let stop: Stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let store = store.clone();
    let id = id.clone();
    let watching = stop.clone();
    let task = tokio::spawn(async move {
        let mut seen = Vec::new();
        while !watching.load(std::sync::atomic::Ordering::Relaxed) {
            seen.push(linkage(&store, &id).await);
            tokio::task::yield_now().await;
        }
        seen
    });
    (stop, task)
}

type Stop = std::sync::Arc<std::sync::atomic::AtomicBool>;
type ReaderTask = tokio::task::JoinHandle<Vec<Linkage>>;

/// End a [`watcher`] and collect everything it saw.
async fn stop(stop: Stop, task: ReaderTask) -> Vec<Linkage> {
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    task.await.expect("reader")
}

/// The gap this whole change exists to close. A reader hammering the
/// file while uploads complete must never catch `version_count` ahead
/// of the version being served.
#[tokio::test]
async fn no_reader_catches_the_file_mid_completion() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    let (flag, watching) = watcher(&store, &id);

    for seed in 1..=8u8 {
        upload(&store, &id, seed).await.expect("upload");
    }
    let seen = stop(flag, watching).await;

    let counts: std::collections::BTreeSet<u64> = seen.iter().map(|s| s.version_count).collect();
    assert!(
        counts.len() > 2,
        "the reader has to have watched the uploads happen to prove anything: {counts:?}",
    );
    for state in &seen {
        assert!(
            state.agrees(),
            "a reader saw the file between the increment and the link: {state:?}",
        );
    }
}

/// The test above only means something if that reader can catch a gap
/// at all. This opens one deliberately, writing the file the way
/// completion used to: increment `version_count` in one statement and
/// arm `current_version` in another. The same reader must catch the
/// file in between. If this ever stops catching it, the test above
/// stopped proving anything and both need rewriting.
#[tokio::test]
async fn the_reader_would_catch_a_gap_if_one_were_left() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    blob::record_sighting(&store, &digest(1), 4, "local", "p/1")
        .await
        .expect("sighting");
    file::claim_upload(&store, &acme(), &id, "tester", 900)
        .await
        .expect("claim");
    let (flag, watching) = watcher(&store, &id);

    store
        .raw()
        .query(&format!(
            "UPDATE file:{id} SET state = 'ready', version_count = version_count + 1 \
             WHERE state = 'uploading';",
        ))
        .await
        .expect("the old first write");
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    store
        .raw()
        .query(&format!(
            "CREATE file_version:gap CONTENT {{ tenant_id: 'acme', number: 1, \
             content_type: 'application/pdf', size_bytes: 4, digest: '{}', \
             metadata_snapshot: {{}}, created_by: 'tester', file: file:{id}, armed: true }}; \
             UPDATE file:{id} SET current_version = file_version:gap;",
            digest(1).as_str(),
        ))
        .await
        .expect("the old last write");

    let seen = stop(flag, watching).await;
    assert!(
        seen.iter().any(|s| !s.agrees()),
        "the reader missed a gap it was left wide open: {seen:?}",
    );
}

/// Two writers completing the same claim: the compare-and-swap decides,
/// exactly as it did when completion was a single statement. The loser
/// gets a conflict and writes nothing at all, which is the part the
/// transaction adds: it used to be able to lose the CAS only before
/// anything else had run, and now everything else is inside the same
/// guard.
#[tokio::test]
async fn racing_completions_have_one_winner_and_leave_no_debris() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    blob::record_sighting(&store, &digest(1), 4, "local", "p/1")
        .await
        .expect("sighting");
    file::claim_upload(&store, &acme(), &id, "tester", 900)
        .await
        .expect("claim");

    let racers = (0..2).map(|_| {
        let store = store.clone();
        let id = id.clone();
        tokio::spawn(async move {
            completion::complete_upload(
                &store,
                &acme(),
                &id,
                "local",
                &digest(1),
                4,
                "tester",
                FileState::Ready,
            )
            .await
        })
    });
    let mut winners = 0;
    let mut losers = 0;
    for racer in racers.collect::<Vec<_>>() {
        match racer.await.expect("join") {
            Ok(record) => {
                winners += 1;
                assert_eq!(record.version_count, 1);
            }
            Err(CopalError::Conflict(_)) => losers += 1,
            Err(other) => panic!("a lost race must read as a conflict, not {other}"),
        }
    }
    assert_eq!((winners, losers), (1, 1), "exactly one completion applies");

    // The loser's whole transaction was conditional on the CAS, so it
    // left no half-written version row and no stray outbox event.
    let versions = version::list_versions(&store, &acme(), &id, 10, None)
        .await
        .expect("versions");
    assert_eq!(versions.len(), 1, "one completion, one version row");
    let seen = linkage(&store, &id).await;
    assert_eq!(seen.version_count, 1);
    assert!(seen.agrees(), "{seen:?}");
    let events = store
        .raw()
        .query("SELECT count() FROM file_event GROUP ALL;")
        .await
        .expect("events");
    assert_eq!(events[0][0]["count"], 1, "one event, from the winner");
}

/// Completing a file nobody claimed loses the same way, and the
/// transaction that follows the losing CAS writes nothing.
#[tokio::test]
async fn completing_an_unclaimed_file_conflicts_and_writes_nothing() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    blob::record_sighting(&store, &digest(1), 4, "local", "p/1")
        .await
        .expect("sighting");

    let refused = completion::complete_upload(
        &store,
        &acme(),
        &id,
        "local",
        &digest(1),
        4,
        "tester",
        FileState::Ready,
    )
    .await;
    assert!(
        matches!(refused, Err(CopalError::Conflict(_))),
        "a draft is not completable: {refused:?}",
    );

    let record = file::get_file(&store, &acme(), &id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(record.state, FileState::Draft, "the file did not move");
    assert_eq!(record.version_count, 0);
    assert!(record.digest.is_none(), "no payload column was written");
    let versions = version::list_versions(&store, &acme(), &id, 10, None)
        .await
        .expect("versions");
    assert!(versions.is_empty(), "no version row: {versions:?}");
}

/// Another tenant's completion is a conflict too, indistinguishably:
/// the tenant rides the same WHERE clause as the state.
#[tokio::test]
async fn a_foreign_tenant_cannot_complete() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    blob::record_sighting(&store, &digest(1), 4, "local", "p/1")
        .await
        .expect("sighting");
    file::claim_upload(&store, &acme(), &id, "tester", 900)
        .await
        .expect("claim");

    let other = TenantId::parse("globex").unwrap();
    let refused = completion::complete_upload(
        &store,
        &other,
        &id,
        "local",
        &digest(1),
        4,
        "tester",
        FileState::Ready,
    )
    .await;
    assert!(
        matches!(refused, Err(CopalError::Conflict(_))),
        "reaching across a tenant reads as a lost race: {refused:?}",
    );
    let versions = version::list_versions(&store, &acme(), &id, 10, None)
        .await
        .expect("versions");
    assert!(versions.is_empty());
}

/// The retention policy as it stands at the completion stamps the
/// version, in the same transaction that creates it, so there is no
/// moment where a fresh version is prunable by a policy it should have
/// been born under.
#[tokio::test]
async fn the_policy_at_completion_stamps_the_version() {
    let store = fresh_store().await;
    tenant::set_retention_policy(
        &store,
        &acme(),
        &tenant::RetentionPolicy {
            seconds: Some(3600),
            mode: Some("compliance".to_owned()),
            keep_last: None,
        },
    )
    .await
    .expect("policy");

    let id = drafted(&store, "docs/plan.pdf").await;
    upload(&store, &id, 1).await.expect("upload");

    let stamped = store
        .raw()
        .query("SELECT number, retention_mode, retain_until FROM file_version;")
        .await
        .expect("stamped");
    assert_eq!(stamped[0][0]["retention_mode"], "compliance");
    assert!(!stamped[0][0]["retain_until"].is_null());

    // Clearing the policy afterwards must not reach back: what was
    // stamped stays stamped, which is the property that made the read
    // a snapshot rather than something recomputed later.
    tenant::clear_retention_policy(&store, &acme())
        .await
        .expect("clear");
    let after = store
        .raw()
        .query("SELECT retention_mode FROM file_version;")
        .await
        .expect("after");
    assert_eq!(after[0][0]["retention_mode"], "compliance");
}

/// `keep_last` prunes erasable history inside the completion, and the
/// event announcing it rides the same transaction rather than a
/// follow-up write that a crash could lose.
#[tokio::test]
async fn keep_last_prunes_and_announces_inside_the_transaction() {
    let store = fresh_store().await;
    tenant::set_retention_policy(
        &store,
        &acme(),
        &tenant::RetentionPolicy {
            seconds: None,
            mode: None,
            keep_last: Some(2),
        },
    )
    .await
    .expect("policy");

    let id = drafted(&store, "docs/plan.pdf").await;
    for seed in 1..=4u8 {
        upload(&store, &id, seed).await.expect("upload");
    }

    let kept: Vec<u64> = version::list_versions(&store, &acme(), &id, 10, None)
        .await
        .expect("versions")
        .into_iter()
        .map(|v| v.number)
        .collect();
    assert_eq!(kept, vec![4, 3], "the newest two survive: {kept:?}");

    let seen = linkage(&store, &id).await;
    assert_eq!(seen.version_count, 4);
    assert!(seen.agrees(), "{seen:?}");

    let pruned = store
        .raw()
        .query(
            "SELECT payload, created_at FROM file_event WHERE action = 'version.pruned' \
             ORDER BY created_at;",
        )
        .await
        .expect("pruned events");
    let announced = pruned[0].as_array().expect("array");
    assert_eq!(announced.len(), 2, "versions 3 and 4 each pruned one");
    assert_eq!(announced[1]["payload"]["removed"], 1);
    assert_eq!(announced[1]["payload"]["kept"], 2);
    assert_eq!(announced[1]["payload"]["newest"], 4);
}

/// A legal hold survives `keep_last`: the erasability rule the
/// completion renders is the same one the retention repository states,
/// so pruning cannot become the way around a hold.
#[tokio::test]
async fn a_held_version_survives_pruning() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    upload(&store, &id, 1).await.expect("upload");
    version::set_legal_hold(&store, &acme(), &id, 1, true)
        .await
        .expect("hold");
    tenant::set_retention_policy(
        &store,
        &acme(),
        &tenant::RetentionPolicy {
            seconds: None,
            mode: None,
            keep_last: Some(1),
        },
    )
    .await
    .expect("policy");

    for seed in 2..=3u8 {
        upload(&store, &id, seed).await.expect("upload");
    }
    let kept: Vec<u64> = version::list_versions(&store, &acme(), &id, 10, None)
        .await
        .expect("versions")
        .into_iter()
        .map(|v| v.number)
        .collect();
    assert_eq!(kept, vec![3, 1], "the held version outlives the policy");
}

/// Completion refuses to land anywhere but ready or scanning, and it
/// refuses before opening a transaction.
#[tokio::test]
async fn completion_only_lands_in_ready_or_scanning() {
    let store = fresh_store().await;
    let id = drafted(&store, "docs/plan.pdf").await;
    let refused = completion::complete_upload(
        &store,
        &acme(),
        &id,
        "local",
        &digest(1),
        4,
        "tester",
        FileState::Quarantined,
    )
    .await;
    assert!(
        matches!(refused, Err(CopalError::Validation(_))),
        "a caller bug, not a conflict: {refused:?}",
    );
}
