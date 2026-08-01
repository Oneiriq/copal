//! The store-backed consumption ledger, for fleets.
//!
//! `MemoryRateStore` meters one process, which silently multiplies
//! every declared budget by the replica count. This implementation
//! keeps the window in the database every replica shares, with the
//! check and the increment as one guarded statement, so racing
//! replicas serialize instead of overspending. The cost is one store
//! round trip per operation, which is why the in-memory ledger stays
//! the single-node default.

use std::time::{SystemTime, UNIX_EPOCH};

use janus::runtime::RateStore;

use copal_store::repo::rate as rate_repo;
use copal_store::Store;

/// A [`RateStore`] over the shared database.
pub struct SurrealRateStore {
    store: Store,
}

impl SurrealRateStore {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    fn minute() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 60)
            .unwrap_or(0)
    }
}

impl RateStore for SurrealRateStore {
    fn charge<'a>(
        &'a self,
        bucket: &'a str,
        units: u64,
        per_minute: u64,
    ) -> janus::runtime::BoxFuture<'a, Result<bool, janus::runtime::JanusError>> {
        Box::pin(async move {
            let minute = Self::minute();
            let internal =
                |e: copal_core::CopalError| janus::runtime::JanusError::Internal(e.to_string());
            let key = rate_repo::window_key(bucket, minute).map_err(internal)?;
            rate_repo::ensure_window(&self.store, &key, minute)
                .await
                .map_err(internal)?;
            rate_repo::try_charge(&self.store, &key, units, per_minute)
                .await
                .map_err(internal)
        })
    }
}
