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
    /// Optional bind for the S3-compatible gateway, which serves
    /// path-style bucket routes at its own root. Requires
    /// `blob_encryption_key`: SigV4 needs the shared secret back, and
    /// credentials are stored sealed under that key or not at all.
    pub s3_bind: Option<String>,
    /// Metadata plane connection.
    pub store: StoreConfig,
    /// Root directory of the filesystem blob store.
    pub blob_root: String,
    /// Optional 64-hex master key enabling encryption at rest for new
    /// objects; existing plaintext objects keep serving.
    pub blob_encryption_key: Option<String>,
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
    /// Resumable-upload session lifetime.
    pub tus_session_ttl_secs: u64,
    /// CORS allowlist origins; unset means no CORS layer at all.
    pub cors_origins: Option<Vec<String>>,
    /// Named storage residencies beyond `local`: a JSON map of name to
    /// backend config. Tenants pin to one via the admin surface.
    pub residencies: std::collections::HashMap<String, copal_blob::BackendConfig>,
    /// Permit webhook endpoints on private addresses.
    pub allow_private_webhook_targets: bool,
    /// `host:port` of a clamd instance. Set enables malware scanning
    /// in the upload pipeline AND stops content serving while a scan
    /// is outstanding.
    pub clamav_addr: Option<String>,
    /// Address (or full URL) of a text extractor speaking Tika's
    /// shape. Text and JSON extract natively either way; this covers
    /// the formats Copal declines to parse itself.
    pub extractor_addr: Option<String>,
    /// Embedding service (OpenAI `/v1/embeddings` shape), the model
    /// to ask it for, and the width that model emits. All three are
    /// needed: the vector index is defined at that dimension.
    pub embedding_addr: Option<String>,
    pub embedding_model: String,
    pub embedding_dimension: u32,
    /// Cosine-distance ceiling for a semantic match.
    pub max_semantic_distance: f64,
    /// Seconds one subscription may stay open before the server ends
    /// it; clients re-subscribe through full authentication.
    pub subscription_max_secs: u64,
    /// Which consumption ledger meters requests: `memory` (default,
    /// one process) or `store` (shared, so a fleet holds one budget).
    pub rate_ledger: String,
    /// Path to a JSON object of sha256 hash to GraphQL document. Set,
    /// the GraphQL face runs listed operations only.
    pub persisted_operations: Option<String>,
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
            s3_bind: std::env::var("COPAL_S3_BIND").ok(),
            store: StoreConfig {
                url: env_or("COPAL_DB_URL", "ws://127.0.0.1:8000"),
                namespace: env_or("COPAL_DB_NS", "copal"),
                database: env_or("COPAL_DB_NAME", "copal"),
                username,
                password,
            },
            blob_root: env_or("COPAL_BLOB_ROOT", "./data/blobs"),
            blob_encryption_key: std::env::var("COPAL_BLOB_ENCRYPTION_KEY").ok(),
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
            tus_session_ttl_secs: env_parse("COPAL_TUS_SESSION_TTL_SECS", 86_400),
            transfer_timeout_secs: env_parse("COPAL_TRANSFER_TIMEOUT_SECS", 3_600),
            cors_origins: std::env::var("COPAL_CORS_ORIGINS").ok().map(|raw| {
                raw.split(',')
                    .map(|o| o.trim().to_owned())
                    .filter(|o| !o.is_empty())
                    .collect()
            }),
            clamav_addr: std::env::var("COPAL_CLAMAV_ADDR").ok(),
            extractor_addr: std::env::var("COPAL_EXTRACTOR_ADDR").ok(),
            embedding_addr: std::env::var("COPAL_EMBEDDING_ADDR").ok(),
            embedding_model: env_or("COPAL_EMBEDDING_MODEL", "nomic-embed-text"),
            embedding_dimension: env_parse("COPAL_EMBEDDING_DIMENSION", 768),
            max_semantic_distance: env_parse("COPAL_MAX_SEMANTIC_DISTANCE", 0.65),
            subscription_max_secs: env_parse("COPAL_SUBSCRIPTION_MAX_SECS", 900),
            rate_ledger: env_or("COPAL_RATE_LEDGER", "memory"),
            persisted_operations: std::env::var("COPAL_PERSISTED_OPERATIONS").ok(),
            allow_private_webhook_targets: env_parse("COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS", false),
            residencies: std::env::var("COPAL_RESIDENCIES")
                .ok()
                .and_then(|raw| match serde_json::from_str(&raw) {
                    Ok(parsed) => Some(parsed),
                    Err(err) => {
                        tracing::error!(error = %err, "COPAL_RESIDENCIES does not parse; ignoring");
                        None
                    }
                })
                .unwrap_or_default(),
            sweeps: crate::sweeps::SweepConfig {
                interval_secs: env_parse("COPAL_SWEEP_INTERVAL_SECS", 60),
                staging_ttl_secs: env_parse("COPAL_STAGING_TTL_SECS", 86_400),
                gc_grace_secs: env_parse("COPAL_GC_GRACE_SECS", 86_400),
                gc_batch: env_parse("COPAL_GC_BATCH", 1_000),
                scan_stale_secs: env_parse("COPAL_SCAN_STALE_SECS", 3_600),
                tus_session_ttl_secs: env_parse("COPAL_TUS_SESSION_TTL_SECS", 86_400),
            },
        }
    }
}
