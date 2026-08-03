//! Master key rotation: the boot pass over sealed secrets and the
//! background sweep over sealed objects.
//!
//! The operator's runbook is three motions. Set the new key as
//! `COPAL_BLOB_ENCRYPTION_KEY` and the old one as
//! `COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS`, then restart: reads fall
//! back to the retiring key while everything re-seals. Watch
//! `copal_resealed_total` go quiet. Drop the previous key and restart
//! again. Content addressing makes the object sweep safe in place:
//! the digest covers plaintext, so a re-sealed object keeps its
//! address and its references.

use std::time::Duration;

use base64::Engine as _;
use copal_blob::crypto::BlobCipher;
use copal_blob::ObjectStore;
use copal_store::Store;

/// How often the object sweep looks for work.
const SWEEP_EVERY: Duration = Duration::from_secs(300);
/// Objects re-sealed per residency per pass.
const SWEEP_BATCH: usize = 512;

/// Re-seal database secrets sitting under the retiring key: S3
/// credentials, webhook endpoints, edge keys. Runs once at boot,
/// before traffic. A secret no configured key opens is left alone; it
/// was already unserveable and the surface using it reports so.
pub async fn reseal_secrets(store: &Store, cipher: &BlobCipher) -> usize {
    let engine = &base64::engine::general_purpose::STANDARD;
    let outcome = copal_store::repo::rotate::reseal_secrets(store, |sealed| {
        let bytes = engine.decode(sealed).ok()?;
        match cipher.reseal(&bytes) {
            Ok(Some(fresh)) => Some(engine.encode(fresh)),
            Ok(None) => None,
            Err(_) => {
                tracing::warn!("a sealed secret opens under no configured key");
                None
            }
        }
    })
    .await;
    match outcome {
        Ok(0) => 0,
        Ok(rewritten) => {
            crate::metrics::add("copal_secrets_resealed_total", rewritten as u64);
            tracing::info!(rewritten, "rotation re-sealed database secrets");
            rewritten
        }
        Err(e) => {
            tracing::warn!(error = %e, "secret re-seal pass failed");
            0
        }
    }
}

/// The object sweep: every five minutes, re-seal a batch in each
/// rotating residency until passes come back empty. The loop keeps
/// ticking after the drain because late writes from an instance still
/// holding the old key as current must not strand; dropping
/// `COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS` is what ends the rotation.
pub async fn run_sweep(stores: Vec<(String, ObjectStore)>) {
    let mut tick = tokio::time::interval(SWEEP_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        for (name, store) in &stores {
            match store.reseal_pass(SWEEP_BATCH).await {
                Ok(0) => {}
                Ok(resealed) => {
                    crate::metrics::add("copal_resealed_total", resealed as u64);
                    tracing::info!(
                        residency = %name,
                        resealed,
                        "rotation sweep re-sealed objects",
                    );
                }
                Err(e) => {
                    tracing::warn!(residency = %name, error = %e, "rotation sweep failed");
                }
            }
        }
    }
}
