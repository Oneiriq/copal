//! Background maintenance: one pass, three sweeps.
//!
//! 1. Reap expired upload claims to `failed` (retryable).
//! 2. Clear staging entries past their TTL — aborted and oversize
//!    uploads leave inert staging garbage by design; this is where it
//!    leaves the disk.
//! 3. Garbage-collect unreferenced content: mark blobs whose derived
//!    link count is zero, then — a full grace period later, and only
//!    after a FRESH recount — delete the row and then the object.
//!    Referenced blobs get their advisory refcount cache refreshed on
//!    the way past.
//!
//! Every step is independent and failure-isolated: a store hiccup in
//! one sweep logs and leaves the others running.

use std::time::Duration;

use copal_blob::BlobStore;
use copal_store::repo::{blob as blob_repo, file as file_repo};
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
    /// Blob rows visited per pass; larger populations continue next
    /// tick rather than stalling a pass.
    pub gc_batch: i64,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            interval_secs: 60,
            staging_ttl_secs: 86_400,
            gc_grace_secs: 86_400,
            gc_batch: 1_000,
        }
    }
}

/// Outcome of one pass, for logging and tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub reaped_uploads: u64,
    pub staging_removed: u64,
    pub blobs_marked: u64,
    pub blobs_collected: u64,
    pub blobs_refreshed: u64,
}

/// Run one maintenance pass. Deterministic and directly testable; the
/// interval loop is just this on a timer.
pub async fn run_pass<B: BlobStore>(store: &Store, blobs: &B, config: &SweepConfig) -> SweepReport {
    let mut report = SweepReport::default();

    match file_repo::reap_expired_uploads(store).await {
        Ok(reaped) => report.reaped_uploads = reaped.len() as u64,
        Err(err) => tracing::warn!(error = %err, "reap sweep failed"),
    }

    match blobs
        .sweep_staging(Duration::from_secs(config.staging_ttl_secs))
        .await
    {
        Ok(removed) => report.staging_removed = removed,
        Err(err) => tracing::warn!(error = %err, "staging sweep failed"),
    }

    match gc_pass(store, blobs, config, &mut report).await {
        Ok(()) => {}
        Err(err) => tracing::warn!(error = %err, "gc sweep failed"),
    }

    report
}

async fn gc_pass<B: BlobStore>(
    store: &Store,
    blobs: &B,
    config: &SweepConfig,
    report: &mut SweepReport,
) -> copal_core::Result<()> {
    let rows = blob_repo::list_blobs(store, config.gc_batch).await?;
    for row in rows {
        let digest = match row.digest() {
            Ok(digest) => digest,
            Err(err) => {
                tracing::warn!(id = %row.id, error = %err, "unparseable blob id; skipping");
                continue;
            }
        };
        // The derived truth, freshly computed — never the cache.
        let live = blob_repo::recount_inbound_links(store, &digest).await?;
        if live > 0 {
            if row.unreferenced_since.is_some() || row.refcount != live {
                blob_repo::clear_unreferenced(store, &digest, live).await?;
                report.blobs_refreshed += 1;
            }
            continue;
        }
        match row.unreferenced_since {
            None => {
                blob_repo::mark_unreferenced(store, &digest).await?;
                report.blobs_marked += 1;
            }
            Some(_) => {
                // Row first, then object: the guarded DELETE re-checks
                // the aged mark, and a row that no longer exists cannot
                // be linked by a completing upload.
                if blob_repo::collect_expired(store, &digest, config.gc_grace_secs).await? {
                    blobs.delete(&digest).await?;
                    report.blobs_collected += 1;
                }
            }
        }
    }
    Ok(())
}

/// The interval loop the server spawns.
pub async fn run_forever<B: BlobStore>(store: Store, blobs: B, config: SweepConfig) {
    let mut ticker = tokio::time::interval(Duration::from_secs(config.interval_secs.max(1)));
    loop {
        ticker.tick().await;
        let report = run_pass(&store, &blobs, &config).await;
        if report != SweepReport::default() {
            tracing::info!(
                reaped = report.reaped_uploads,
                staging = report.staging_removed,
                marked = report.blobs_marked,
                collected = report.blobs_collected,
                refreshed = report.blobs_refreshed,
                "maintenance pass",
            );
        }
    }
}
