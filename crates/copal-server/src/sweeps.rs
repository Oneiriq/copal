//! Background maintenance: one pass, three sweeps.
//!
//! 1. Reap expired upload claims to `failed` (retryable).
//! 2. Clear staging entries past their TTL; aborted and oversize
//!    uploads leave inert staging garbage by design; this is where it
//!    leaves the disk.
//! 3. Garbage-collect unreferenced content: mark blobs whose derived
//!    link count is zero, then (a full grace period later, and only
//!    after a FRESH recount) delete the row and then the object.
//!    Referenced blobs get their advisory refcount cache refreshed on
//!    the way past.
//!
//! Every step is independent and failure-isolated: a store hiccup in
//! one sweep logs and leaves the others running.

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

    match crate::tus::sweep_expired(store, blobs, config.tus_session_ttl_secs).await {
        Ok(swept) => report.tus_sessions_swept = swept,
        Err(err) => tracing::warn!(error = %err, "tus session sweep failed"),
    }

    match blobs
        .sweep_staging(Duration::from_secs(config.staging_ttl_secs))
        .await
    {
        Ok(removed) => report.staging_removed = removed,
        Err(err) => tracing::warn!(error = %err, "staging sweep failed"),
    }

    match gc_pass(store, residencies, config, &mut report).await {
        Ok(()) => {}
        Err(err) => tracing::warn!(error = %err, "gc sweep failed"),
    }

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
