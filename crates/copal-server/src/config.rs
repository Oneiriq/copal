//! Server configuration from environment variables.
//!
//! Defaults target the docker-compose development topology; every value
//! is overridable, none are required.

use copal_store::StoreConfig;

/// Runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bind address, e.g. `127.0.0.1:8080`.
    pub bind: String,
    /// Optional separate bind for the admin surface; when set, admin
    /// routes exist ONLY on this listener, keeping key custody off the
    /// tenant-facing network.
    pub admin_bind: Option<String>,
    /// Metadata plane connection.
    pub store: StoreConfig,
    /// Root directory of the filesystem blob store.
    pub blob_root: String,
    /// Upload body ceiling in bytes.
    pub max_upload_bytes: usize,
    /// Upload claim lease TTL; expired claims are stealable and reaped.
    pub upload_lease_secs: u32,
    /// Background maintenance cadence and retention.
    pub sweeps: crate::sweeps::SweepConfig,
    /// Comma-separated blocked extension list for the upload pipeline.
    pub blocked_extensions: Option<String>,
    /// Quarantine declared-type lies instead of merely annotating them.
    pub enforce_type_match: bool,
    /// Request authentication mode and the admin gate.
    pub auth: crate::auth::AuthConfig,
    /// Deadline for ordinary requests.
    pub request_timeout_secs: u64,
    /// Deadline for streaming byte routes.
    pub transfer_timeout_secs: u64,
    /// CORS allowlist origins; unset means no CORS layer at all.
    pub cors_origins: Option<Vec<String>>,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    /// Read configuration from `COPAL_*` environment variables.
    pub fn from_env() -> Self {
        let username = std::env::var("COPAL_DB_USER").ok();
        let password = std::env::var("COPAL_DB_PASS").ok();
        // Development default: the compose SurrealDB with root/root.
        let (username, password) = match (username, password) {
            (None, None) => (Some("root".to_owned()), Some("root".to_owned())),
            pair => pair,
        };
        Self {
            bind: env_or("COPAL_BIND", "127.0.0.1:8080"),
            admin_bind: std::env::var("COPAL_ADMIN_BIND").ok(),
            store: StoreConfig {
                url: env_or("COPAL_DB_URL", "ws://127.0.0.1:8000"),
                namespace: env_or("COPAL_DB_NS", "copal"),
                database: env_or("COPAL_DB_NAME", "copal"),
                username,
                password,
            },
            blob_root: env_or("COPAL_BLOB_ROOT", "./data/blobs"),
            max_upload_bytes: env_parse("COPAL_MAX_UPLOAD_BYTES", 1 << 30),
            upload_lease_secs: env_parse("COPAL_UPLOAD_LEASE_SECS", 900),
            blocked_extensions: std::env::var("COPAL_BLOCKED_EXTENSIONS").ok(),
            enforce_type_match: env_parse("COPAL_ENFORCE_TYPE_MATCH", false),
            auth: crate::auth::AuthConfig {
                // "header" (development default until 1.0) or "keys".
                mode: std::env::var("COPAL_AUTH_MODE")
                    .ok()
                    .and_then(|raw| crate::auth::AuthMode::parse(&raw))
                    .unwrap_or_default(),
                admin_token: std::env::var("COPAL_ADMIN_TOKEN").ok(),
                admin_token_previous: std::env::var("COPAL_ADMIN_TOKEN_PREVIOUS").ok(),
            },
            request_timeout_secs: env_parse("COPAL_REQUEST_TIMEOUT_SECS", 30),
            transfer_timeout_secs: env_parse("COPAL_TRANSFER_TIMEOUT_SECS", 3_600),
            cors_origins: std::env::var("COPAL_CORS_ORIGINS").ok().map(|raw| {
                raw.split(',')
                    .map(|o| o.trim().to_owned())
                    .filter(|o| !o.is_empty())
                    .collect()
            }),
            sweeps: crate::sweeps::SweepConfig {
                interval_secs: env_parse("COPAL_SWEEP_INTERVAL_SECS", 60),
                staging_ttl_secs: env_parse("COPAL_STAGING_TTL_SECS", 86_400),
                gc_grace_secs: env_parse("COPAL_GC_GRACE_SECS", 86_400),
                gc_batch: env_parse("COPAL_GC_BATCH", 1_000),
                scan_stale_secs: env_parse("COPAL_SCAN_STALE_SECS", 3_600),
            },
        }
    }
}
