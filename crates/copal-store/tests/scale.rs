//! Scale measurements for the metadata plane: the F16 vector index
//! rebuild at corpus size, and the repository round trips the
//! embedded-tier decision is parked on.
//!
//! Everything here is a measurement rather than a check. Assertions
//! guard only that the measured path did what it claims (rows landed,
//! the index reached ready, the CAS won); none compare a timing
//! against a number, because a timing gate on shared hardware is a
//! flaky test with extra steps. The numbers land in
//! docs/operations.md with their conditions.
//!
//! Every test is `#[ignore]`d: the index rebuild alone holds a
//! six-figure row population, which the ordinary suite must never
//! pay. `bench/scale.sh` runs them one per process in release mode
//! and collects the printed `name value` lines.

use copal_core::{FileSpec, FileState, TenantId};
use copal_store::repo::file;
use copal_store::{Store, StoreConfig};
use surql::schema::{hnsw_index, HnswDistanceType, MTreeVectorType};

/// Process memory as the OS accounts it; same helper as the server
/// scale bench carries, duplicated because integration tests cannot
/// share a module across crates and forty lines do not justify
/// shipping bench plumbing in a library.
#[cfg(windows)]
mod rss {
    /// PROCESS_MEMORY_COUNTERS as the API fills it on x64.
    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> isize;
        fn K32GetProcessMemoryInfo(
            process: isize,
            counters: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
    }

    /// (peak working set, current working set), both in MiB.
    pub fn peak_and_current_mib() -> (u64, u64) {
        let mut counters = ProcessMemoryCounters {
            cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
            page_fault_count: 0,
            peak_working_set_size: 0,
            working_set_size: 0,
            quota_peak_paged_pool_usage: 0,
            quota_paged_pool_usage: 0,
            quota_peak_non_paged_pool_usage: 0,
            quota_non_paged_pool_usage: 0,
            pagefile_usage: 0,
            peak_pagefile_usage: 0,
        };
        let ok =
            unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) };
        assert!(ok != 0, "GetProcessMemoryInfo failed");
        (
            (counters.peak_working_set_size as u64) >> 20,
            (counters.working_set_size as u64) >> 20,
        )
    }
}

#[cfg(not(windows))]
mod rss {
    /// VmHWM (the peak) and VmRSS from /proc/self/status, in MiB.
    pub fn peak_and_current_mib() -> (u64, u64) {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        let read = |key: &str| -> u64 {
            status
                .lines()
                .find(|line| line.starts_with(key))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
                .unwrap_or(0)
                >> 10
        };
        (read("VmHWM:"), read("VmRSS:"))
    }
}

/// One `name value` line, the shape the container bench records.
fn record(name: &str, value: impl std::fmt::Display) {
    println!("{name} {value}");
}

fn record_memory(prefix: &str) {
    let (peak, current) = rss::peak_and_current_mib();
    record(&format!("{prefix}_peak_rss_mib"), peak);
    record(&format!("{prefix}_current_rss_mib"), current);
}

/// The embedding width the deployment defaults ship
/// (`COPAL_EMBEDDING_DIMENSION`).
const DIMENSION: usize = 768;

/// A deterministic generator, xorshift64, so the corpus is the same
/// on every run without a random-number dependency in this crate.
fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// One embedding literal: DIMENSION components in [-1, 1), four
/// decimals, which is more resolution than F16 keeps and therefore
/// enough to exercise it.
fn embedding_literal(state: &mut u64) -> String {
    let mut out = String::with_capacity(DIMENSION * 8 + 2);
    out.push('[');
    for i in 0..DIMENSION {
        if i > 0 {
            out.push(',');
        }
        let unit = (next_random(state) >> 11) as f64 / (1u64 << 53) as f64;
        let value = unit * 2.0 - 1.0;
        out.push_str(&format!("{value:.4}"));
    }
    out.push(']');
    out
}

/// How many passages the rebuild is measured over. Overridable so the
/// runner can probe smaller corpora without editing code; the default
/// is the documented measurement.
fn chunk_count() -> usize {
    std::env::var("COPAL_SCALE_CHUNKS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(100_000)
}

/// Fill `text_chunk` with `count` embedded passages in batched
/// INSERTs. Batching matters: one statement per row would measure the
/// wire loop rather than the engine, and the pipeline's own writes
/// are batched per document.
async fn populate_chunks(store: &Store, count: usize) -> u128 {
    let batch = 200;
    let mut state = 0x9E3779B97F4A7C15u64;
    let started = std::time::Instant::now();
    let mut ordinal = 0usize;
    while ordinal < count {
        let take = batch.min(count - ordinal);
        let mut statement = String::with_capacity(take * (DIMENSION * 8 + 96));
        statement.push_str("INSERT INTO text_chunk [");
        for row in 0..take {
            if row > 0 {
                statement.push(',');
            }
            statement.push_str(&format!(
                "{{tenant_id:'bench',digest:'benchdigest',ordinal:{},body:'passage {}',embedding:{}}}",
                ordinal + row,
                ordinal + row,
                embedding_literal(&mut state),
            ));
        }
        statement.push_str("];");
        store.raw().query(&statement).await.expect("insert batch");
        ordinal += take;
    }
    started.elapsed().as_millis()
}

/// Wait for the CONCURRENTLY build to finish, polling INFO FOR INDEX
/// the way the boot log tells an operator to. Returns the wall time
/// from `started`, which the caller anchors at the DEFINE.
///
/// Done is either an explicit `ready` status or a report that names
/// no in-flight phase anymore; the second form matters because the
/// engine's report shape is its own and a version that drops the
/// ready marker after the build would otherwise spin this loop for
/// an hour. The grace clause (only trust "nothing in flight" after a
/// busy phase was seen or a few seconds passed) covers the window
/// between the DEFINE returning and the background build
/// registering.
async fn wait_for_index_ready(store: &Store, started: std::time::Instant) -> u128 {
    let deadline = std::time::Duration::from_secs(3600);
    let mut seen_busy = false;
    loop {
        let info = store
            .raw()
            .query("INFO FOR INDEX idx_chunk_embedding ON TABLE text_chunk;")
            .await
            .expect("INFO FOR INDEX answers");
        let text = info.to_string();
        assert!(!text.contains("error"), "index build reported: {text}");
        let busy = [
            "started", "initial", "ingest", "updating", "cleaning", "pending",
        ]
        .iter()
        .any(|phase| text.contains(phase));
        if text.contains("ready") || (!busy && (seen_busy || started.elapsed().as_secs() >= 5)) {
            println!("info_for_index_final {text}");
            return started.elapsed().as_millis();
        }
        seen_busy = seen_busy || busy;
        assert!(
            started.elapsed() < deadline,
            "index build still not ready after an hour: {text}",
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// Time one full rebuild at `vector_type`, DEFINE through ready.
async fn rebuild(store: &Store, vector_type: MTreeVectorType, label: &str) {
    let ddl = hnsw_index(
        "idx_chunk_embedding",
        "embedding",
        DIMENSION as u32,
        HnswDistanceType::Cosine,
        vector_type,
        None,
        None,
    )
    .with_concurrently(true)
    .to_surql_overwrite("text_chunk");
    let started = std::time::Instant::now();
    store.raw().query(&ddl).await.expect("DEFINE INDEX");
    let build_ms = wait_for_index_ready(store, started).await;
    record(&format!("{label}_build_ms"), build_ms);
    record_memory(label);
}

/// The rebuild an operator upgrading past the F16 change performs:
/// the same corpus, indexed once at F16 (the shipping type) and once
/// at F32 (what the index was before), both through the CONCURRENTLY
/// path boot uses. mem:// keeps the engine in-process so the recorded
/// working set covers rows plus index and the F16/F32 delta is
/// visible in memory as well as in time.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: six-figure row population; run via bench/scale.sh"]
async fn scale_vector_index_rebuild() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let count = chunk_count();
    record("index_chunks", count);
    record("index_dimension", DIMENSION);

    let populate_ms = populate_chunks(&store, count).await;
    record("index_populate_ms", populate_ms);
    let counted = store
        .raw()
        .query("SELECT count() FROM text_chunk GROUP ALL;")
        .await
        .unwrap();
    assert!(
        counted.to_string().contains(&count.to_string()),
        "population landed: {counted}",
    );
    record_memory("index_after_populate");

    rebuild(&store, MTreeVectorType::F16, "index_f16").await;

    store
        .raw()
        .query("REMOVE INDEX idx_chunk_embedding ON TABLE text_chunk;")
        .await
        .expect("REMOVE INDEX");
    record_memory("index_after_remove");

    rebuild(&store, MTreeVectorType::F32, "index_f32").await;
}

fn tenant() -> TenantId {
    TenantId::parse("bench").unwrap()
}

fn spec(path: &str) -> FileSpec {
    FileSpec {
        path: path.to_owned(),
        content_type: "application/octet-stream".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: serde_json::json!({"source": "bench"}),
        idempotency_key: None,
    }
}

/// Sort and report a sample set the way the container bench reports a
/// timing: the interesting figures are the middle and the tail, and a
/// mean would let one scheduler hiccup rewrite the story.
fn summarize(label: &str, mut samples: Vec<u128>) {
    samples.sort_unstable();
    record(&format!("{label}_p50_us"), samples[samples.len() / 2]);
    record(
        &format!("{label}_p95_us"),
        samples[samples.len() * 95 / 100],
    );
}

/// The three round trips the roadmap's embedded-tier item names: a
/// point read, a hundred-row listing, and the claim/transition CAS
/// pair, each timed one call at a time so a sample is one repository
/// round trip and nothing else.
async fn round_trip_scenario(store: &Store, label: &str) {
    let t = tenant();
    let mut ids = Vec::new();
    for i in 0..150 {
        let created =
            file::create_file(store, &t, &spec(&format!("bench/file-{i:04}.bin")), "bench")
                .await
                .expect("create");
        ids.push(created.record.id);
    }

    // Warm the connection and any lazily-built statement machinery
    // before sampling; the first call pays setup costs that are not
    // the round trip.
    for _ in 0..10 {
        file::get_file(store, &t, &ids[0]).await.unwrap().unwrap();
    }

    let mut samples = Vec::with_capacity(500);
    for i in 0..500 {
        let id = &ids[i % ids.len()];
        let started = std::time::Instant::now();
        let fetched = file::get_file(store, &t, id).await.unwrap();
        samples.push(started.elapsed().as_micros());
        assert!(fetched.is_some());
    }
    summarize(&format!("{label}_get_file"), samples);

    let mut samples = Vec::with_capacity(100);
    for _ in 0..100 {
        let started = std::time::Instant::now();
        let listed = file::list_files(store, &t, 100, None, false, None)
            .await
            .unwrap();
        samples.push(started.elapsed().as_micros());
        assert_eq!(listed.len(), 100);
    }
    summarize(&format!("{label}_list_100"), samples);

    // Claim then release, alternating, so every claim starts from a
    // claimable state (draft the first time, failed afterwards) and
    // both halves of the CAS pair are sampled separately.
    let mut claims = Vec::with_capacity(100);
    let mut transitions = Vec::with_capacity(100);
    for _ in 0..100 {
        let id = &ids[0];
        let started = std::time::Instant::now();
        file::claim_upload(store, &t, id, "bench-owner", 60)
            .await
            .unwrap();
        claims.push(started.elapsed().as_micros());
        let started = std::time::Instant::now();
        file::transition(
            store,
            &t,
            id,
            FileState::Uploading,
            FileState::Failed,
            file::TransitionSets::default(),
        )
        .await
        .unwrap();
        transitions.push(started.elapsed().as_micros());
    }
    summarize(&format!("{label}_claim"), claims);
    summarize(&format!("{label}_transition"), transitions);
}

/// The embedded half: the engine inside the process, no wire at all.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: run via bench/scale.sh"]
async fn scale_round_trips_embedded() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    round_trip_scenario(&store, "embedded").await;
}

/// The remote half: the same calls over a websocket to a SurrealDB
/// server, which is what every deployed copal pays per repository
/// call today. Needs a reachable engine; bench/scale.sh stands one up
/// with the repo's compose file and points this test at it through
/// `COPAL_BENCH_DB_URL`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: needs a running SurrealDB; run via bench/scale.sh"]
async fn scale_round_trips_remote() {
    let url =
        std::env::var("COPAL_BENCH_DB_URL").unwrap_or_else(|_| "ws://127.0.0.1:8000".to_owned());
    let database = format!(
        "bench_{}",
        ulid::Ulid::new().to_string().to_ascii_lowercase()
    );
    let config = StoreConfig {
        url,
        namespace: "copal_bench".to_owned(),
        database: database.clone(),
        username: Some("root".to_owned()),
        password: Some("root".to_owned()),
        ..StoreConfig::memory()
    };
    let store = Store::connect(config)
        .await
        .expect("a SurrealDB server answers at COPAL_BENCH_DB_URL");
    round_trip_scenario(&store, "remote").await;
    // The bench database is scratch; leave the server the way the
    // run found it.
    let _ = store
        .raw()
        .query(&format!("REMOVE DATABASE {database};"))
        .await;
}
