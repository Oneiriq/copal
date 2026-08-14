//! The mover: the four motions that change where a blob's bytes
//! live, and the discipline that makes them safe.
//!
//! Per blob: copy the raw object -- envelope and all, opaque -- to
//! the destination's staging and land it at the same content
//! address; verify by reading the copy back through the ordinary
//! open path and comparing the digest to the row id (the digest is
//! the name; nothing weaker is verification); flip the row in one
//! guarded UPDATE, which is the only commitment; and erase the
//! displaced copy a full grace period later, on a later pass. Mark,
//! grace, erase -- the GC's own discipline, applied to a copy
//! instead of a corpse. At no instant does the row name a placement
//! whose object is absent, and every crash window converges by
//! re-copy and re-verify.
//!
//! Promotion is the same four motions in reverse, when eligibility
//! lapses: a pin lands, a policy tightens or leaves, or reads make
//! the blob ineligible for cold. Reads themselves never promote --
//! one monthly audit read must not yank a corpus hot.

use copal_blob::tier::TierClass;
use copal_blob::BlobStore;
use copal_core::{ContentDigest, DigestBuilder};
use copal_store::repo::tier as tier_repo;
use copal_store::Store;
use futures::StreamExt as _;

use crate::app::Residencies;
use crate::tiering::{placement, Placement, Topology};

/// What one mover pass did, for logging and tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MoveReport {
    pub demoted: u64,
    pub promoted: u64,
    /// Displaced copies erased after their grace: hot copies of
    /// settled-cold rows, cold copies of settled-hot rows.
    pub hot_erased: u64,
    pub cold_erased: u64,
    /// Copies whose read-back digest did not match the row id. The
    /// bad copy is deleted, the row keeps its placement, and the
    /// next pass retries from the copy step.
    pub verify_failures: u64,
    /// Copy or flip motions that errored; logged and retried next
    /// pass.
    pub move_failures: u64,
}

/// Read the copy back through the ordinary open path -- decrypt,
/// stream, hash -- and compare to the digest the address claims.
pub(crate) async fn verified<B: BlobStore>(
    backend: &B,
    digest: &ContentDigest,
) -> copal_core::Result<bool> {
    let (_, mut stream) = backend.open_read(digest).await?;
    let mut hasher = DigestBuilder::new();
    while let Some(chunk) = stream.next().await {
        hasher.update(&chunk?);
    }
    let (computed, _) = hasher.finish();
    Ok(&computed == digest)
}

/// Copy raw from `source` to `destination`, verify at the
/// destination, then run `flip`. Returns whether the flip landed.
async fn move_one<B: BlobStore>(
    source: &B,
    destination: &B,
    digest: &ContentDigest,
    report: &mut MoveReport,
    flip: impl std::future::Future<Output = copal_core::Result<bool>>,
) -> copal_core::Result<bool> {
    let (_, raw) = source.open_raw(digest).await?;
    destination.put_raw(digest, raw).await?;
    if !verified(destination, digest).await? {
        // Never erase a source on the strength of an unchecked copy;
        // never leave wrong bytes at a content address either.
        destination.delete(digest).await?;
        report.verify_failures += 1;
        return Ok(false);
    }
    flip.await
}

/// One mover pass: walk every blob row exactly as the GC does, erase
/// what past flips displaced once the grace has aged, and move what
/// the policies say -- demotions bounded by `move_batch` because
/// each is a full read-write-read of the object.
pub async fn move_pass<B: BlobStore>(
    store: &Store,
    residencies: &Residencies<B>,
    topology: &Topology,
    move_batch: usize,
    erase_grace_secs: u32,
) -> MoveReport {
    let mut report = MoveReport::default();
    // No tiers configured: nothing can be placed anywhere else, and
    // nothing can need bringing home.
    if topology.is_empty() {
        return report;
    }
    let policies: std::collections::HashMap<String, tier_repo::TieringPolicy> =
        match tier_repo::all_policies(store).await {
            Ok(policies) => policies.into_iter().collect(),
            Err(err) => {
                tracing::warn!(error = %err, "mover could not load policies; skipping pass");
                return report;
            }
        };
    let mut moved = 0usize;
    let mut after: Option<String> = None;
    loop {
        let rows = match tier_repo::list_for_classify(store, 1_000, after.as_deref()).await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "mover walk failed; resuming next pass");
                return report;
            }
        };
        let drained = (rows.len() as i64) < 1_000;
        let mut last = None;
        for row in &rows {
            last = Some(row.bare_id());
            if let Err(err) = handle_row(
                store,
                residencies,
                topology,
                &policies,
                row,
                &mut moved,
                move_batch,
                erase_grace_secs,
                &mut report,
            )
            .await
            {
                report.move_failures += 1;
                tracing::warn!(id = %row.id, error = %err, "mover motion failed; will retry");
            }
        }
        if drained {
            break;
        }
        match last {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    crate::metrics::add("copal_tiering_demoted_total", report.demoted);
    crate::metrics::add("copal_tiering_promoted_total", report.promoted);
    crate::metrics::add(
        "copal_tiering_erased_total{copy=\"hot\"}",
        report.hot_erased,
    );
    crate::metrics::add(
        "copal_tiering_erased_total{copy=\"cold\"}",
        report.cold_erased,
    );
    crate::metrics::add(
        "copal_tiering_verify_failures_total",
        report.verify_failures,
    );
    report
}

#[allow(clippy::too_many_arguments)]
async fn handle_row<B: BlobStore>(
    store: &Store,
    residencies: &Residencies<B>,
    topology: &Topology,
    policies: &std::collections::HashMap<String, tier_repo::TieringPolicy>,
    row: &tier_repo::BlobClassifyRow,
    moved: &mut usize,
    move_batch: usize,
    erase_grace_secs: u32,
    report: &mut MoveReport,
) -> copal_core::Result<()> {
    let (residency, digest) = row.location()?;

    // Erase first: settle what an earlier pass's flip displaced,
    // once the marker has aged past the grace. The grace exists so a
    // backup taken before the flip can still find the bytes; the
    // operator docs bind it to the backup cadence.
    if let Some(age) = row.demoted_age_secs {
        if age >= i64::from(erase_grace_secs) {
            match row.tier.as_deref() {
                // Settled cold: the hot copy is the displaced one,
                // and so is any copy on OTHER tiers an older policy
                // left behind (delete is a no-op on absent paths).
                Some(tier) => {
                    residencies.get(&residency)?.delete(&digest).await?;
                    if let Some(tiers) = residencies.tiers.get(&residency) {
                        for (name, backend) in tiers {
                            if name != tier {
                                backend.delete(&digest).await?;
                            }
                        }
                    }
                    tier_repo::settle_erase(store, &residency, &digest, Some(tier)).await?;
                    report.hot_erased += 1;
                }
                // Hot again: every tier copy is displaced. The sweep
                // across all tiers is replay-safe (delete is a no-op
                // on absent paths) and covers a copy stranded on a
                // tier an older policy named.
                None => {
                    if let Some(tiers) = residencies.tiers.get(&residency) {
                        for backend in tiers.values() {
                            backend.delete(&digest).await?;
                        }
                    }
                    tier_repo::settle_erase(store, &residency, &digest, None).await?;
                    report.cold_erased += 1;
                }
            }
        }
    }

    let judged = placement(row, policies, topology);
    match (&row.tier, judged) {
        // Hot and judged cold: demote. Both classes move -- an
        // archive-destined object is written readable and verified
        // BEFORE the bucket's lifecycle archives it; recall answers
        // for it afterward.
        (None, Placement::Cold { tier }) if *moved < move_batch => {
            if topology.class_of(&residency, &tier).is_none() {
                return Ok(());
            }
            let hot = residencies.get(&residency)?.clone();
            let cold = residencies.tier_backend(&residency, &tier)?.clone();
            let flip = tier_repo::demote_flip(store, &residency, &digest, &tier);
            if move_one(&hot, &cold, &digest, report, flip).await? {
                *moved += 1;
                report.demoted += 1;
            }
        }
        // Cold and no longer judged exactly there: promote. A blob
        // judged cold for a DIFFERENT tier comes home first and
        // demotes to the new tier on a later pass -- one motion per
        // pass, never a cold-to-cold hop that skips verification
        // against the primary.
        (Some(current), judged) if *moved < move_batch => {
            let stay = matches!(&judged, Placement::Cold { tier } if tier == current);
            // Unreferenced rows are the GC's: it erases from every
            // tier at collection, and promoting a corpse first would
            // only delay it.
            if stay || judged == Placement::Unreferenced {
                return Ok(());
            }
            // Archive-cold bytes cannot be read directly, so their
            // promotion IS a recall: enqueue the journaled run (which
            // issues the restore and polls) instead of a copy the
            // backend would refuse. Idempotent; every pass until the
            // run lands re-enqueues onto the same key.
            if topology.class_of(&residency, current) == Some(TierClass::Archive) {
                if let Some(referent) = row.referencing_tenants().next() {
                    if let Ok(tenant) = copal_core::TenantId::parse(referent) {
                        crate::recall::enqueue(store, &tenant, &residency, current, &digest)
                            .await?;
                    }
                }
                return Ok(());
            }
            let cold = residencies.tier_backend(&residency, current)?.clone();
            let hot = residencies.get(&residency)?.clone();
            let flip = tier_repo::promote_flip(store, &residency, &digest, current);
            if move_one(&cold, &hot, &digest, report, flip).await? {
                *moved += 1;
                report.promoted += 1;
            }
        }
        _ => {}
    }
    Ok(())
}
