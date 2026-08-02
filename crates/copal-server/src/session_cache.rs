//! Caller sessions, reused across requests.
//!
//! Opening one costs two engine round trips (authenticate, then the
//! identity check that refuses tokens `PERMISSIONS` would not
//! filter), and a caller making many small reads pays that on every
//! one. The cache holds open sessions keyed by the identity that
//! minted them.
//!
//! What the cache may safely reuse follows from where it sits: every
//! request authenticates BEFORE reaching here, so a revoked or
//! expired key never gets a session at all, cached or fresh. What the
//! key must therefore carry is what the token claims say: the tenant,
//! the caller, and the scopes. A key whose scopes change mints a
//! different cache key and a different session, so a widened or
//! narrowed key never rides an old one.
//!
//! Sessions carry no engine-side expiry, so lifetime here is Copal
//! policy alone: a bounded TTL and a bounded count, both
//! configuration.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use copal_store::Store;

struct Entry {
    store: Store,
    opened: Instant,
}

/// Open caller sessions, keyed by minting identity.
pub struct SessionCache {
    entries: Mutex<HashMap<String, Entry>>,
    ttl: Duration,
    capacity: usize,
}

impl SessionCache {
    /// A cache holding at most `capacity` sessions for `ttl_secs`
    /// each. Either bound at zero disables reuse, which is the
    /// configuration for a deployment that would rather pay the open
    /// than hold sessions.
    pub fn new(ttl_secs: u64, capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
            capacity,
        }
    }

    /// The cache key for one minting identity. Scopes ride it because
    /// they ride the token: the same caller with different scopes is
    /// a different session.
    pub fn key(tenant: &str, key_id: &str, scopes: &[String]) -> String {
        format!("{tenant}|{key_id}|{}", scopes.join(","))
    }

    fn enabled(&self) -> bool {
        self.capacity > 0 && !self.ttl.is_zero()
    }

    /// A live session for this identity, when one is held and still
    /// inside its TTL.
    pub fn get(&self, key: &str) -> Option<Store> {
        if !self.enabled() {
            return None;
        }
        let mut entries = self.entries.lock().ok()?;
        match entries.get(key) {
            Some(entry) if entry.opened.elapsed() < self.ttl => Some(entry.store.clone()),
            Some(_) => {
                // Expired: dropping it ends the engine session.
                entries.remove(key);
                None
            }
            None => None,
        }
    }

    /// Hold a session for reuse. Two requests racing the same
    /// identity may both open one; the loser's session drops when its
    /// request ends, which costs an open rather than correctness.
    pub fn put(&self, key: String, store: Store) {
        if !self.enabled() {
            return;
        }
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        if entries.len() >= self.capacity && !entries.contains_key(&key) {
            // Evict the oldest rather than the least recently used:
            // sessions are interchangeable in cost, and age is the
            // one thing already recorded.
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.opened)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            Entry {
                store,
                opened: Instant::now(),
            },
        );
    }

    /// How many sessions are held. For tests and metrics.
    pub fn len(&self) -> usize {
        self.entries.lock().map(|e| e.len()).unwrap_or(0)
    }

    /// Whether the cache holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SessionCache {
    fn default() -> Self {
        Self::new(60, 256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_separates_tenants_callers_and_scopes() {
        let read = vec!["read".to_owned()];
        let both = vec!["read".to_owned(), "write".to_owned()];
        let acme = SessionCache::key("acme", "k1", &read);
        assert_ne!(acme, SessionCache::key("rival", "k1", &read));
        assert_ne!(acme, SessionCache::key("acme", "k2", &read));
        assert_ne!(
            acme,
            SessionCache::key("acme", "k1", &both),
            "a scope change must not reuse a session",
        );
    }

    #[test]
    fn zero_bounds_disable_reuse() {
        let cache = SessionCache::new(0, 256);
        assert!(!cache.enabled());
        let cache = SessionCache::new(60, 0);
        assert!(!cache.enabled());
    }
}
