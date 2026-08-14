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
    /// Named external transformers: operator-run HTTP services that
    /// derive new content from stored bytes through the transform
    /// action. Parsed from `COPAL_TRANSFORMERS` JSON.
    pub transformers: std::collections::HashMap<String, TransformerConfig>,
    /// The retiring master key during a rotation. Opens fall back to
    /// it while the re-seal sweep moves everything under the current
    /// key; nothing seals under it. Unset it once the sweep drains.
    pub blob_encryption_key_previous: Option<String>,
    /// External key custody. Set, the master key is fetched from here
    /// at boot and never read from the environment; a deployment that
    /// cannot reach custody refuses to start rather than come up
    /// unable to open its own content.
    pub kms_addr: Option<String>,
    /// Which key custody should hand over.
    pub kms_key_id: String,
    /// Bearer token presented to custody, when it wants one.
    pub kms_token: Option<String>,
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
    /// backend config, each optionally carrying a `tiers` block.
    /// Tenants pin to one via the admin surface.
    pub residencies: std::collections::HashMap<String, copal_blob::tier::ResidencyConfig>,
    /// Tiers of the `local` residency, the sibling knob to the
    /// `tiers` block named residencies nest. Parsed from
    /// `COPAL_LOCAL_TIERS` JSON.
    pub local_tiers: std::collections::HashMap<String, copal_blob::tier::TierConfig>,
    /// Permit webhook endpoints on private addresses.
    pub allow_private_webhook_targets: bool,
    /// Whether URL ingestion may pull from private address space.
    /// The same question webhooks answer, asked of fetch sources.
    pub allow_private_fetch_targets: bool,
    /// Whether the console shows the fleet view: sibling namespaces
    /// on the shared engine, read-only. Off by default because root
    /// reach over the engine is a deployment property the operator
    /// asserts, never one copal assumes.
    pub console_fleet: bool,
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
    /// Reranking service, which reads each shortlisted passage
    /// against the query instead of scoring it by term statistics or
    /// vector distance. Absent means search ranks by fusion alone.
    /// The model and token are what individual services ask for.
    pub rerank_addr: Option<String>,
    pub rerank_model: Option<String>,
    pub rerank_token: Option<String>,
    /// How many fused candidates to hand the reranker. Reading pairs
    /// with a model costs per pair, so this bounds the work rather
    /// than the corpus.
    pub rerank_depth: usize,
    /// Cosine-distance ceiling for a semantic match.
    pub max_semantic_distance: f64,
    /// Seconds one subscription may stay open before the server ends
    /// it; clients re-subscribe through full authentication.
    pub subscription_max_secs: u64,
    /// Which consumption ledger meters requests: `memory` (default,
    /// one process) or `store` (shared, so a fleet holds one budget).
    pub rate_ledger: String,
    /// `off` (default) or `on`: whether request handlers run their
    /// repository calls through caller-bound engine sessions.
    pub engine_sessions: String,
    /// How long an open caller session may be reused, and how many
    /// to hold. Either at zero pays the open on every request.
    pub session_cache_secs: u64,
    pub session_cache_size: usize,
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
                engine_access_key: std::env::var("COPAL_ENGINE_ACCESS_KEY").ok(),
                embedding_dimension: std::env::var("COPAL_EMBEDDING_ADDR")
                    .ok()
                    .map(|_| env_parse("COPAL_EMBEDDING_DIMENSION", 768)),
                // Derived from the contract in main, where a guard
                // without an engine clause can refuse the boot.
                engine_policy: copal_store::schema::EnginePolicy::default(),
            },
            blob_root: env_or("COPAL_BLOB_ROOT", "./data/blobs"),
            blob_encryption_key: std::env::var("COPAL_BLOB_ENCRYPTION_KEY").ok(),
            transformers: std::env::var("COPAL_TRANSFORMERS")
                .ok()
                .map(|raw| match serde_json::from_str(&raw) {
                    Ok(map) => map,
                    Err(err) => {
                        tracing::error!(error = %err, "COPAL_TRANSFORMERS does not parse; ignoring");
                        std::collections::HashMap::new()
                    }
                })
                .unwrap_or_default(),
            blob_encryption_key_previous: std::env::var("COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS").ok(),
            kms_addr: std::env::var("COPAL_KMS_ADDR").ok(),
            kms_key_id: env_or("COPAL_KMS_KEY_ID", "blob"),
            kms_token: std::env::var("COPAL_KMS_TOKEN").ok(),
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
                operator_header: std::env::var("COPAL_OPERATOR_HEADER").ok(),
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
            rerank_addr: std::env::var("COPAL_RERANK_ADDR").ok(),
            rerank_model: std::env::var("COPAL_RERANK_MODEL").ok(),
            rerank_token: std::env::var("COPAL_RERANK_TOKEN").ok(),
            rerank_depth: env_parse("COPAL_RERANK_DEPTH", 50usize).clamp(1, 200),
            embedding_addr: std::env::var("COPAL_EMBEDDING_ADDR").ok(),
            embedding_model: env_or("COPAL_EMBEDDING_MODEL", "nomic-embed-text"),
            embedding_dimension: env_parse("COPAL_EMBEDDING_DIMENSION", 768),
            max_semantic_distance: env_parse("COPAL_MAX_SEMANTIC_DISTANCE", 0.65),
            subscription_max_secs: env_parse("COPAL_SUBSCRIPTION_MAX_SECS", 900),
            rate_ledger: env_or("COPAL_RATE_LEDGER", "memory"),
            engine_sessions: env_or("COPAL_ENGINE_SESSIONS", "off"),
            session_cache_secs: env_parse("COPAL_SESSION_CACHE_SECS", 60),
            session_cache_size: env_parse("COPAL_SESSION_CACHE_SIZE", 256),
            persisted_operations: std::env::var("COPAL_PERSISTED_OPERATIONS").ok(),
            allow_private_webhook_targets: env_parse("COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS", false),
            allow_private_fetch_targets: env_parse("COPAL_FETCH_ALLOW_PRIVATE_TARGETS", false),
            console_fleet: env_parse("COPAL_CONSOLE_FLEET", false),
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
            local_tiers: std::env::var("COPAL_LOCAL_TIERS")
                .ok()
                .and_then(|raw| match serde_json::from_str(&raw) {
                    Ok(parsed) => Some(parsed),
                    Err(err) => {
                        tracing::error!(error = %err, "COPAL_LOCAL_TIERS does not parse; ignoring");
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
                tier_move_batch: env_parse("COPAL_TIER_MOVE_BATCH", 100),
                tier_erase_grace_secs: env_parse("COPAL_TIER_ERASE_GRACE_SECS", 86_400),
            },
        }
    }
}

/// One external transformer: an HTTP service receiving source bytes
/// and answering with derived bytes. Operator-configured, so the URL
/// is trusted the way `COPAL_CLAMAV_ADDR` and the extractor address
/// are; the outbound-policy guard on tenant-supplied webhook targets
/// does not apply here.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TransformerConfig {
    /// Endpoint receiving `POST` with the source bytes as the body.
    pub url: String,
    /// Request timeout in seconds (default 60, clamped to 1..=600).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Shared secret sent as `x-copal-transform-secret`, so the
    /// service can refuse calls that are not from this deployment.
    #[serde(default)]
    pub secret: Option<String>,
    /// Source size ceiling in bytes (default 64 MiB).
    #[serde(default)]
    pub max_source_bytes: Option<u64>,
}
