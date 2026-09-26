//! Background maintenance: one pass of independent sweeps.
//!
//! 1. Reap expired upload claims to `failed` (retryable), return
//!    expired run claims to `pending`, and fail scans a crash left
//!    behind.
//! 2. Drop rate windows that no longer count.
//! 3. Discard abandoned S3 multipart and tus sessions with their
//!    staged bytes.
//! 4. Clear staging entries past their TTL on every backend: local,
//!    each named residency, and each tier. Aborted and oversize
//!    uploads leave inert staging garbage by design, and this is where
//!    it leaves the disk.
//! 5. Recount tenant usage, the cache the quota check reads.
//! 6. Garbage-collect unreferenced content: mark blobs whose derived
//!    link count is zero, then (a full grace period later, and only
//!    after a FRESH recount) delete the row and then the object.
//!    Referenced blobs get their advisory refcount cache refreshed on
//!    the way past.
//!
//! Every step is independent and failure-isolated: a store hiccup in
//! one sweep logs and leaves the others running. The loop then runs
//! the tiering classifier and the mover under the same leader lease.

use std::time::Duration;

use copal_blob::BlobStore;
use copal_store::repo::{blob as blob_repo, file as file_repo, flow as flow_repo};
use copal_store::Store;

/// Sweep cadence and retention knobs.
#[derive(Debug, Clone, Copy)]
pub struct SweepConfig {
    /// Seconds between passes.
    pub interval_secs: u64,
    /// Staging entries older than this are deleted.
    pub staging_ttl_secs: u64,
    /// A blob must be continuously unreferenced this long before its
    /// bytes go.
    pub gc_grace_secs: u32,
    /// Blob rows per GC batch query; the pass loops batches until a
    /// short page, so the whole population is visited every pass.
    pub gc_batch: i64,
    /// Files stuck in `scanning` longer than this are failed
    /// (retryable). Covers the crash window between upload completion
    /// and pipeline enqueue; live pipelines finish far sooner.
    pub scan_stale_secs: u32,
    /// Resumable-upload sessions idle longer than this are discarded
    /// with their staged bytes.
    pub tus_session_ttl_secs: u64,
    /// Tier moves initiated per mover pass. Each is a full
    /// read-write-read of the object, so this bounds the pass's IO,
    /// not its correctness: what one pass defers, the next resumes.
    pub tier_move_batch: usize,
    /// How long a displaced copy survives its placement flip before
    /// the mover erases it. Must be at least the backup cadence: a
    /// flip landing between a backend's backup and the metadata
    /// export must leave the bytes findable in that backup.
    pub tier_erase_grace_secs: u32,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            interval_secs: 60,
            staging_ttl_secs: 86_400,
            gc_grace_secs: 86_400,
            gc_batch: 1_000,
            scan_stale_secs: 3_600,
            tus_session_ttl_secs: 86_400,
            tier_move_batch: 100,
            tier_erase_grace_secs: 86_400,
        }
    }
}

/// Outcome of one pass, for logging and tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub reaped_uploads: u64,
    pub reaped_runs: u64,
    pub stale_scans_failed: u64,
    pub tus_sessions_swept: u64,
    /// Abandoned S3 multipart sessions discarded with their parts.
    pub multipart_swept: u64,
    /// Tenant usage counters recomputed from their files.
    pub usage_reconciled: u64,
    pub staging_removed: u64,
    pub blobs_marked: u64,
    pub blobs_collected: u64,
    pub blobs_refreshed: u64,
}

/// Run one maintenance pass. Deterministic and directly testable; the
/// interval loop is just this on a timer.
pub async fn run_pass<B: BlobStore>(
    store: &Store,
    residencies: &crate::app::Residencies<B>,
    config: &SweepConfig,
) -> SweepReport {
    let blobs = &residencies.local;
    let mut report = SweepReport::default();

    match file_repo::reap_expired_uploads(store).await {
        Ok(reaped) => report.reaped_uploads = reaped.len() as u64,
        Err(err) => tracing::warn!(error = %err, "reap sweep failed"),
    }

    match flow_repo::reap_expired_runs(store).await {
        Ok(reaped) => report.reaped_runs = reaped,
        Err(err) => tracing::warn!(error = %err, "run reap sweep failed"),
    }

    match file_repo::reap_stale_scans(store, config.scan_stale_secs).await {
        Ok(failed) => report.stale_scans_failed = failed,
        Err(err) => tracing::warn!(error = %err, "stale scan sweep failed"),
    }

    // Rate windows only ever read the current minute; anything two
    // minutes back is done counting whichever ledger is configured,
    // and a deployment on the in-memory ledger simply has no rows.
    let current_minute = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() / 60)
        .unwrap_or(0);
    if let Err(err) =
        copal_store::repo::rate::cleanup_windows(store, current_minute.saturating_sub(2)).await
    {
        tracing::warn!(error = %err, "rate window sweep failed");
    }

    match crate::s3::multipart::sweep_expired(store, blobs, config.tus_session_ttl_secs).await {
        Ok(swept) => report.multipart_swept = swept,
        Err(err) => tracing::warn!(error = %err, "multipart sweep failed"),
    }

    match crate::tus::sweep_expired(store, blobs, config.tus_session_ttl_secs).await {
        Ok(swept) => report.tus_sessions_swept = swept,
        Err(err) => tracing::warn!(error = %err, "tus session sweep failed"),
    }

    // Every backend stages its writes before landing them: uploads into
    // a named residency, and the mover's copies into a tier. So every
    // backend collects its own staging garbage, each on its own.
    let staging_ttl = Duration::from_secs(config.staging_ttl_secs);
    // Collected before the loop: a lazy chain of borrowing closures
    // held across the awaits would keep this future from being Send.
    let backends: Vec<(String, &B)> = std::iter::once(("local".to_owned(), blobs))
        .chain(
            residencies
                .named
                .iter()
                .map(|(name, backend)| (name.clone(), backend)),
        )
        .chain(residencies.tiers.iter().flat_map(|(residency, tiers)| {
            tiers
                .iter()
                .map(move |(tier, backend)| (format!("{residency}/{tier}"), backend))
        }))
        .collect();
    for (name, backend) in backends {
        match backend.sweep_staging(staging_ttl).await {
            Ok(removed) => report.staging_removed += removed,
            Err(err) => tracing::warn!(backend = %name, error = %err, "staging sweep failed"),
        }
    }

    // The usage counter is a cache; this recount is what keeps a
    // crashed upload or a missed release from drifting it forever.
    match copal_store::repo::tenant::tenants_with_usage(store, 500).await {
        Ok(tenants) => {
            for raw in tenants {
                let Ok(tenant) = copal_core::TenantId::parse(&raw) else {
                    continue;
                };
                match copal_store::repo::tenant::reconcile_usage(store, &tenant).await {
                    Ok(_) => report.usage_reconciled += 1,
                    Err(err) => tracing::warn!(error = %err, "usage reconcile failed"),
                }
            }
        }
        Err(err) => tracing::warn!(error = %err, "usage sweep failed"),
    }

    match gc_pass(store, residencies, config, &mut report).await {
        Ok(()) => {}
        Err(err) => tracing::warn!(error = %err, "gc sweep failed"),
    }

    crate::metrics::add("copal_blobs_collected_total", report.blobs_collected);
    crate::metrics::add("copal_reaped_uploads_total", report.reaped_uploads);
    crate::metrics::add("copal_reaped_runs_total", report.reaped_runs);
    report
}

async fn gc_pass<B: BlobStore>(
    store: &Store,
    residencies: &crate::app::Residencies<B>,
    config: &SweepConfig,
    report: &mut SweepReport,
) -> copal_core::Result<()> {
    // Keyset batches until a short page: the whole population is
    // visited every pass, whatever its size.
    let mut after: Option<String> = None;
    loop {
        let rows = blob_repo::list_blobs(store, config.gc_batch, after.as_deref()).await?;
        let drained = (rows.len() as i64) < config.gc_batch;
        let mut last = None;
        for row in rows {
            last = Some(row.bare_id());
            let (residency, digest) = match row.location() {
                Ok(location) => location,
                Err(err) => {
                    tracing::warn!(id = %row.id, error = %err, "unparseable blob id; skipping");
                    continue;
                }
            };
            // The derived truth, freshly computed; the cache plays no part.
            let live = blob_repo::recount_inbound_links(store, &residency, &digest).await?;
            if live > 0 {
                if row.unreferenced_since.is_some() || row.refcount != live {
                    blob_repo::clear_unreferenced(store, &residency, &digest, live).await?;
                    report.blobs_refreshed += 1;
                }
                continue;
            }
            match row.unreferenced_since {
                None => {
                    blob_repo::mark_unreferenced(store, &residency, &digest).await?;
                    report.blobs_marked += 1;
                }
                Some(_) => {
                    // Row first, then object: the guarded DELETE
                    // re-checks the aged mark, and a row that no longer
                    // exists cannot be linked by a completing upload.
                    // Before the bytes go, one final existence check: a
                    // row RE-CREATED since the delete (identical
                    // content re-uploaded in the window) aborts the
                    // collection; the fresh row's bytes stay.
                    if blob_repo::collect_expired(store, &residency, &digest, config.gc_grace_secs)
                        .await?
                    {
                        if blob_repo::row_exists(store, &residency, &digest).await? {
                            tracing::info!(digest = %digest, "collection aborted: blob resurrected");
                            continue;
                        }
                        // An unconfigured residency cannot reach its
                        // bytes from this instance; the row is gone
                        // and the orphan is that operator's cleanup.
                        match residencies.get(&residency) {
                            Ok(backend) => {
                                backend.delete(&digest).await?;
                                // Collection erases from every tier
                                // the residency configures: delete is
                                // a no-op on absent paths, so the
                                // unconditional sweep across tiers is
                                // replay-safe and covers a
                                // grace-window double copy.
                                if let Some(tiers) = residencies.tiers.get(&residency) {
                                    for tier in tiers.values() {
                                        tier.delete(&digest).await?;
                                    }
                                }
                                report.blobs_collected += 1;
                            }
                            Err(err) => {
                                tracing::warn!(residency = %residency, error = %err, "bytes unreachable");
                            }
                        }
                    }
                }
            }
        }
        if drained {
            return Ok(());
        }
        match last {
            Some(cursor) => after = Some(cursor),
            // A full page of unparseable ids cannot advance the cursor;
            // stop rather than loop in place.
            None => return Ok(()),
        }
    }
}

/// The interval loop the server spawns. Replicas elect a leader per
/// pass through the `sweeps` coordination lease: losers skip the pass
/// entirely, so a fleet does not multiply GC and reap work (every
/// sweep is CAS-guarded and safe to duplicate; this is about waste
/// and about not widening the GC resurrection window across
/// instances). A crashed leader's lease expires and any replica takes
/// over.
pub async fn run_forever<B: BlobStore>(
    store: Store,
    residencies: crate::app::Residencies<B>,
    config: SweepConfig,
    holder: String,
    tiering: crate::tiering::Topology,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(config.interval_secs.max(1)));
    let lease_ttl = u32::try_from((config.interval_secs * 3).clamp(90, 3600)).unwrap_or(3600);
    loop {
        ticker.tick().await;
        match flow_repo::try_acquire_lease(&store, "sweeps", &holder, lease_ttl).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(err) => {
                tracing::warn!(error = %err, "sweep lease check failed; skipping pass");
                continue;
            }
        }
        let report = run_pass(&store, &residencies, &config).await;
        // The tiering classifier rides the same leader election: it
        // walks the whole blob population, so a fleet must not
        // multiply it. Observe-only -- it counts and reports, and
        // with no policy set it returns before the walk.
        crate::tiering::observe_pass(&store, &tiering).await;
        // Then the mover acts on what the observer just published:
        // same lease, same walk discipline, moves bounded per pass.
        let moves = crate::mover::move_pass(
            &store,
            &residencies,
            &tiering,
            config.tier_move_batch,
            config.tier_erase_grace_secs,
        )
        .await;
        if moves != crate::mover::MoveReport::default() {
            tracing::info!(
                demoted = moves.demoted,
                promoted = moves.promoted,
                hot_erased = moves.hot_erased,
                cold_erased = moves.cold_erased,
                verify_failures = moves.verify_failures,
                move_failures = moves.move_failures,
                "tier mover pass",
            );
        }
        if report != SweepReport::default() {
            tracing::info!(
                reaped = report.reaped_uploads,
                runs = report.reaped_runs,
                stale_scans = report.stale_scans_failed,
                staging = report.staging_removed,
                marked = report.blobs_marked,
                collected = report.blobs_collected,
                refreshed = report.blobs_refreshed,
                "maintenance pass",
            );
        }
    }
}
