//! Router and handlers: the vertical slice of the file service.
//!
//! create -> stream bytes in -> complete -> read metadata -> stream out.
//! Handlers stay thin: tenant extraction, one or two repo/blob calls,
//! HTTP mapping. The state machine and tenancy rules live below.

use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use futures::StreamExt as _;

use copal_blob::{BlobStore, StoredBlob};
use copal_core::{CopalError, FileId, FileSpec, FileState, TenantId};
use copal_flow::{FlowEngine, FlowRegistry, RunSpec};
use copal_sign::GrantToken;
use copal_store::repo::{
    blob as blob_repo, completion as completion_repo, file as file_repo, flow as flow_repo,
    grant as grant_repo, version as version_repo,
};
use copal_store::Store;
use serde::Deserialize;
use serde_json::json;

use crate::error::ApiError;

/// Request-shaping limits, separable from infrastructure config so
/// tests can build a router with tiny ceilings.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_upload_bytes: usize,
    pub upload_lease_secs: u32,
    /// Deadline for ordinary (non-streaming) requests.
    pub request_timeout_secs: u64,
    /// Deadline for the byte routes: uploads, downloads, redemptions.
    /// Generous by default; its job is ending slow-drip connections,
    /// never legitimate transfers.
    pub transfer_timeout_secs: u64,
    /// How long a resumable-upload session may live between appends
    /// before the sweep discards it.
    pub tus_session_ttl_secs: u32,
    /// How far a passage may be from a query and still count as a
    /// match, in cosine distance: 0 is identical, 1 is unrelated.
    /// Without a floor, nearest-neighbor search returns its nearest
    /// results however far away they are, so no query ever misses.
    pub max_semantic_distance: f64,
    /// How long one subscription may stay open before the server ends
    /// it with a normal completion. Re-subscribing runs the full
    /// authentication path again, which is how key revocation and
    /// expiry reach streams that are already running.
    pub subscription_max_secs: u64,
    /// Smallest acceptable multipart part, last part exempt. S3's
    /// own floor by default, because clients written against S3 rely
    /// on the rejection; deployments with different needs can lower
    /// it knowingly.
    pub min_multipart_part_bytes: i64,
    /// Allow webhook endpoints that resolve to private addresses.
    /// Off by default: a tenant-supplied URL pointing inside the
    /// deployment is server-side request forgery. Deployments whose
    /// receivers are genuinely internal turn it on knowingly.
    pub allow_private_webhook_targets: bool,
    /// Whether URL ingestion may pull from private address space.
    pub allow_private_fetch_targets: bool,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_upload_bytes: 1 << 30,
            upload_lease_secs: 900,
            request_timeout_secs: 30,
            transfer_timeout_secs: 3_600,
            tus_session_ttl_secs: 86_400,
            max_semantic_distance: 0.65,
            subscription_max_secs: 900,
            min_multipart_part_bytes: 5 * 1024 * 1024,
            allow_private_webhook_targets: false,
            allow_private_fetch_targets: false,
        }
    }
}

/// The configured storage residencies: `local` always exists, named
/// backends join from configuration. Blob rows record the residency
/// their content landed in, so resolution happens per row on reads
/// and per tenant on writes.
#[derive(Clone)]
pub struct Residencies<B: BlobStore> {
    pub local: B,
    pub named: std::collections::HashMap<String, B>,
    /// Tier backends by residency then tier name, each opened under
    /// its residency's cipher: hot and cold copies of one digest are
    /// the same object and seal under the same key.
    pub tiers: std::collections::HashMap<String, std::collections::HashMap<String, B>>,
}

impl<B: BlobStore> Residencies<B> {
    /// Only the local backend.
    pub fn local_only(local: B) -> Self {
        Self {
            local,
            named: std::collections::HashMap::new(),
            tiers: std::collections::HashMap::new(),
        }
    }

    /// The backend for a residency name: its PRIMARY backend, where
    /// new content lands and untiered rows serve from.
    pub fn get(&self, name: &str) -> copal_core::Result<&B> {
        if name == "local" {
            return Ok(&self.local);
        }
        self.named.get(name).ok_or_else(|| {
            CopalError::Store(format!(
                "residency {name} is not configured on this instance"
            ))
        })
    }

    /// Whether a residency name is configured.
    pub fn contains(&self, name: &str) -> bool {
        name == "local" || self.named.contains_key(name)
    }

    /// The backend for a named tier of a residency. A row naming a
    /// tier this instance does not configure is unreachable content,
    /// reported loudly rather than silently served from the wrong
    /// place.
    pub fn tier_backend(&self, residency: &str, tier: &str) -> copal_core::Result<&B> {
        self.tiers
            .get(residency)
            .and_then(|tiers| tiers.get(tier))
            .ok_or_else(|| {
                CopalError::Store(format!(
                    "tier {tier} of residency {residency} is not configured on this instance"
                ))
            })
    }

    /// The backend currently holding a digest's bytes: residency,
    /// then tier. One projected point-read of the blob row when the
    /// residency configures tiers; a residency without any skips the
    /// read entirely, so deployments that never tier pay nothing.
    ///
    /// Archive-class placements PROBE rather than assume: an object
    /// still readable in an archive tier (written before the
    /// bucket's lifecycle transitioned it, or temporarily restored)
    /// answers [`ContentResolution::Ready`] and serves directly --
    /// exactly how S3 itself treats restored objects. Only bytes
    /// that genuinely cannot answer resolve
    /// [`ContentResolution::ArchiveCold`], and what each face does
    /// with that is the face's dialect: 202-and-run on REST,
    /// `InvalidObjectState` on S3, a retryable refusal in pipelines.
    pub async fn resolve_content(
        &self,
        store: &Store,
        topology: &crate::tiering::Topology,
        residency: &str,
        digest: &copal_core::ContentDigest,
    ) -> copal_core::Result<ContentResolution<B>> {
        if self.tiers.get(residency).is_none_or(|t| t.is_empty()) {
            return Ok(ContentResolution::Ready(self.get(residency)?.clone()));
        }
        let tier = copal_store::repo::blob::get_location(store, residency, digest)
            .await?
            .and_then(|location| location.tier);
        let Some(tier) = tier else {
            return Ok(ContentResolution::Ready(self.get(residency)?.clone()));
        };
        let backend = self.tier_backend(residency, &tier)?.clone();
        if topology.class_of(residency, &tier) == Some(copal_blob::tier::TierClass::Archive)
            && !backend.read_probe(digest).await?
        {
            return Ok(ContentResolution::ArchiveCold { tier });
        }
        Ok(ContentResolution::Ready(backend))
    }
}

/// Where a read finds its bytes: a backend that answers now, or an
/// archive placement whose bytes cannot answer until a recall.
pub enum ContentResolution<B> {
    Ready(B),
    ArchiveCold { tier: String },
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState<B: BlobStore> {
    pub store: Store,
    /// The local backend, shorthand for `residencies.local`.
    pub blobs: B,
    /// Every configured backend, local included.
    pub residencies: Residencies<B>,
    pub limits: Limits,
    /// Durable execution over the same store; empty registry by
    /// default -- unknown workflows are refused at the door.
    pub flow: FlowEngine,
    /// Identifies this process as an upload-claim owner, so stale
    /// leases name the instance that died holding them.
    pub instance_id: String,
    /// How requests prove their tenant, plus the admin gate.
    pub auth: crate::auth::AuthConfig,
    /// Known GraphQL documents by sha256, when a deployment locks the
    /// face down to them. Absent means any document runs, which is
    /// the development default; configured, unknown operations refuse
    /// before parsing costs anything.
    pub persisted_operations: Option<std::sync::Arc<std::collections::HashMap<String, String>>>,
    /// The consumption ledger BOTH faces charge, so a caller cannot
    /// dodge a budget by switching protocols. In-memory by default;
    /// COPAL_RATE_LEDGER=store swaps in the shared implementation so
    /// a fleet holds ONE budget instead of one per replica.
    pub rate_store: std::sync::Arc<dyn kayak::runtime::RateStore>,
    /// Whether a pending scan withholds content. Set with malware
    /// scanning: serving bytes that no scanner has cleared would make
    /// the scanner decorative.
    pub scan_gates_serving: bool,
    /// Embedding service address and model, when semantic retrieval
    /// is configured.
    pub embedding: Option<(String, String)>,
    /// Reranking service: address, optional model, optional token, and
    /// how many fused candidates it sees. Absent means search ranks by
    /// fusion alone.
    pub reranker: Option<crate::rerank::Reranker>,
    /// Named external transformers, by name; the transform action
    /// validates against this map before enqueueing.
    pub transformers: std::collections::HashMap<String, crate::config::TransformerConfig>,
    /// Store configuration for the console's fleet walk; present only
    /// when the operator turned the fleet view on.
    pub fleet: Option<copal_store::StoreConfig>,
    /// The master cipher, when one is configured. Sealed secrets (S3
    /// credentials, webhook signing keys, edge keys) open under it,
    /// and the surfaces that mint them exist only when it does.
    pub cipher: Option<copal_blob::crypto::BlobCipher>,
    /// Caller-token minting parameters, present when the deployment
    /// set `COPAL_ENGINE_ACCESS_KEY` and the store defined the record
    /// access method those tokens authenticate against.
    pub engine_access: Option<crate::engine::EngineAccess>,
    /// Whether request handlers run repository calls through
    /// caller-bound engine sessions. Off by default so the layer can
    /// be watched before it is trusted; boot refuses `on` without the
    /// access key.
    pub engine_sessions: bool,
    /// Open caller sessions, reused across requests by the identity
    /// that minted them.
    pub sessions: std::sync::Arc<crate::session_cache::SessionCache>,
    /// Which tiers each residency configures: the tiering policy
    /// surface validates names against it, and the classifier asks it
    /// whether a blob's residency carries the named tier. Empty means
    /// no tiering anywhere, which is every deployment that has not
    /// configured a `tiers` block.
    pub tiering: crate::tiering::Topology,
}

impl<B: BlobStore> AppState<B> {
    /// State with default limits, an empty flow registry, a fresh
    /// instance id, and trusted-header auth (the development default).
    pub fn new(store: Store, blobs: B) -> Self {
        Self {
            flow: FlowEngine::new(store.clone(), FlowRegistry::new()),
            store,
            residencies: Residencies::local_only(blobs.clone()),
            blobs,
            limits: Limits::default(),
            instance_id: ulid::Ulid::generate().to_string().to_ascii_lowercase(),
            auth: crate::auth::AuthConfig::default(),
            scan_gates_serving: false,
            embedding: None,
            reranker: None,
            transformers: std::collections::HashMap::new(),
            fleet: None,
            cipher: None,
            rate_store: std::sync::Arc::new(kayak::runtime::MemoryRateStore::new()),
            persisted_operations: None,
            engine_access: None,
            engine_sessions: false,
            sessions: std::sync::Arc::new(crate::session_cache::SessionCache::default()),
            tiering: crate::tiering::Topology::default(),
        }
    }

    /// Install the tier topology validated at boot.
    pub fn with_tiering(mut self, tiering: crate::tiering::Topology) -> Self {
        self.tiering = tiering;
        self
    }

    /// Install the caller-session cache.
    pub fn with_session_cache(mut self, ttl_secs: u64, capacity: usize) -> Self {
        self.sessions =
            std::sync::Arc::new(crate::session_cache::SessionCache::new(ttl_secs, capacity));
        self
    }

    /// Turn caller-bound engine sessions on for request handlers.
    pub fn with_engine_sessions(mut self, on: bool) -> Self {
        self.engine_sessions = on;
        self
    }

    /// Install caller-token minting.
    pub fn with_engine_access(mut self, access: Option<crate::engine::EngineAccess>) -> Self {
        self.engine_access = access;
        self
    }

    /// Install the master cipher.
    pub fn with_cipher(mut self, cipher: Option<copal_blob::crypto::BlobCipher>) -> Self {
        self.cipher = cipher;
        self
    }

    /// The cipher, or the refusal every sealed-secret surface gives
    /// when a deployment has not configured one.
    pub(crate) fn require_cipher(&self) -> Result<&copal_blob::crypto::BlobCipher, ApiError> {
        self.cipher.as_ref().ok_or_else(|| {
            CopalError::conflict(
                "this surface stores secrets sealed under COPAL_BLOB_ENCRYPTION_KEY, which is \
                 not configured",
            )
            .into()
        })
    }

    /// Install the embedding service semantic search asks.
    /// Point search at a reranking service.
    pub fn with_reranker(mut self, reranker: Option<crate::rerank::Reranker>) -> Self {
        self.reranker = reranker;
        self
    }

    pub fn with_embedding(mut self, embedding: Option<(String, String)>) -> Self {
        self.embedding = embedding;
        self
    }

    /// Withhold content while a scan is outstanding.
    pub fn with_scan_gate(mut self, gates: bool) -> Self {
        self.scan_gates_serving = gates;
        self
    }

    /// Whether this record's bytes are withheld for want of a scan.
    ///
    /// Copal otherwise serves on the digest alone, so content is
    /// readable while its pipeline runs. That is the wrong default
    /// once a malware scanner exists: bytes no scanner has cleared
    /// would still reach readers, and the scanner would only ever
    /// quarantine content that had already been served.
    ///
    /// The question is about CONTENT, not lifecycle: has the digest
    /// this record currently serves been cleared? Reading state
    /// instead gets two cases wrong. A run that failed leaves the
    /// record in `failed` with unscanned bytes, which "still
    /// scanning" would serve; and a re-upload in flight still points
    /// at the PREVIOUS digest until completion, which a `ready`-only
    /// rule would withhold even though that content was scanned.
    /// Comparing digests answers both.
    pub(crate) fn withholds_pending_scan(&self, record: &copal_core::FileRecord) -> bool {
        self.scan_gates_serving && !crate::pipeline::content_cleared(record)
    }

    /// Install a populated activity/workflow registry.
    pub fn with_fleet(mut self, fleet: Option<copal_store::StoreConfig>) -> Self {
        self.fleet = fleet;
        self
    }

    pub fn with_transformers(
        mut self,
        transformers: std::collections::HashMap<String, crate::config::TransformerConfig>,
    ) -> Self {
        self.transformers = transformers;
        self
    }

    pub fn with_flow(mut self, registry: FlowRegistry) -> Self {
        self.flow = FlowEngine::new(self.store.clone(), registry);
        self
    }

    /// Install authentication configuration.
    pub fn with_auth(mut self, auth: crate::auth::AuthConfig) -> Self {
        self.auth = auth;
        self
    }

    /// Install named residency backends beside the local one.
    pub fn with_residencies(mut self, named: std::collections::HashMap<String, B>) -> Self {
        self.residencies.named = named;
        self
    }

    /// The residency a tenant's new content lands in, with its backend.
    pub async fn residency_for(&self, tenant: &TenantId) -> Result<(String, B), ApiError> {
        let name = copal_store::repo::tenant::get_residency(&self.store, tenant).await?;
        let backend = self.residencies.get(&name)?.clone();
        Ok((name, backend))
    }

    /// Where a record's landed content answers from, resolved
    /// residency-then-tier: a demoted blob serves from its tier's
    /// backend, and archive-cold bytes resolve as such for the face
    /// to answer in its own dialect. Costs nothing extra when the
    /// record's residency configures no tiers.
    pub async fn resolve_record(
        &self,
        record: &copal_core::FileRecord,
    ) -> Result<ContentResolution<B>, ApiError> {
        let name = record.blob_residency.as_deref().unwrap_or("local");
        match record.digest.as_ref() {
            Some(digest) => Ok(self
                .residencies
                .resolve_content(&self.store, &self.tiering, name, digest)
                .await?),
            None => Ok(ContentResolution::Ready(
                self.residencies.get(name)?.clone(),
            )),
        }
    }

    /// [`Self::resolve_record`] for faces that must have bytes now or
    /// answer 202: archive-cold content enqueues the recall
    /// idempotently and yields the run id to poll.
    pub async fn backend_or_recall(
        &self,
        tenant: &TenantId,
        record: &copal_core::FileRecord,
    ) -> Result<Result<B, String>, ApiError> {
        match self.resolve_record(record).await? {
            ContentResolution::Ready(backend) => Ok(Ok(backend)),
            ContentResolution::ArchiveCold { tier } => {
                let residency = record.blob_residency.as_deref().unwrap_or("local");
                let digest = record
                    .digest
                    .as_ref()
                    .ok_or_else(|| CopalError::Store("archive-cold without digest".into()))?;
                let (run, _) =
                    crate::recall::enqueue(&self.store, tenant, residency, &tier, digest).await?;
                Ok(Err(run))
            }
        }
    }

    /// Enforce the tenant quota before bytes move: refuses outright
    /// when usage already meets the ceiling or a declared size would
    /// cross it, and returns the remaining headroom so streaming
    /// ceilings can clamp to it. `None` means unlimited.
    pub async fn quota_headroom(
        &self,
        tenant: &TenantId,
        declared: Option<u64>,
    ) -> Result<Option<u64>, ApiError> {
        use copal_store::repo::tenant as tenant_repo;

        let quota = tenant_repo::get_quota(&self.store, tenant).await?;
        // The counter is the cheap read; the aggregate initializes it
        // once per tenant and the sweep keeps it honest thereafter.
        let (used, _) = tenant_repo::usage_counter(&self.store, tenant).await?;
        let Some(quota) = quota else {
            // No ceiling: still account the bytes so the counter stays
            // usable the moment a quota is set.
            if let Some(declared) = declared {
                tenant_repo::reserve_usage(&self.store, tenant, declared as i64, None).await?;
            }
            return Ok(None);
        };

        let remaining = (quota - used).max(0) as u64;
        if remaining == 0 {
            crate::metrics::incr("copal_quota_refusals_total");
            return Err(CopalError::conflict(format!(
                "storage quota reached: {used} of {quota} bytes used",
            ))
            .into());
        }
        if let Some(declared) = declared {
            // Guard and increment in ONE statement: concurrent uploads
            // cannot each see the same headroom and collectively
            // overshoot, because the loser's condition no longer holds.
            if !tenant_repo::reserve_usage(&self.store, tenant, declared as i64, Some(quota))
                .await?
            {
                crate::metrics::incr("copal_quota_refusals_total");
                return Err(CopalError::conflict(format!(
                    "upload of {declared} bytes exceeds remaining quota of {remaining} bytes",
                ))
                .into());
            }
        }
        Ok(Some(remaining))
    }

    /// Settle a reservation once the real size is known: releases the
    /// difference when the body came in smaller, adds the remainder
    /// when it came in larger or arrived undeclared.
    pub(crate) async fn settle_reservation(
        &self,
        tenant: &TenantId,
        reserved: Option<u64>,
        actual: u64,
    ) {
        use copal_store::repo::tenant as tenant_repo;
        let reserved = reserved.unwrap_or(0) as i64;
        let actual = actual as i64;
        let _ = if actual >= reserved {
            tenant_repo::reserve_usage(&self.store, tenant, actual - reserved, None).await
        } else {
            tenant_repo::release_usage(&self.store, tenant, reserved - actual)
                .await
                .map(|()| true)
        };
    }

    /// Give a whole reservation back: the upload never landed.
    pub(crate) async fn abandon_reservation(&self, tenant: &TenantId, reserved: Option<u64>) {
        if let Some(reserved) = reserved {
            let _ = copal_store::repo::tenant::release_usage(&self.store, tenant, reserved as i64)
                .await;
        }
    }
}

/// Build the full router (tenant API plus admin surface) over any
/// blob backend. Deployments that set `COPAL_ADMIN_BIND` serve
/// [`api_router`] and [`admin_router`] on separate listeners instead.
///
/// The upload ceiling is enforced inside the upload handler's stream;
/// `DefaultBodyLimit` guards extractor-based bodies only, and the
/// upload path consumes the raw request stream. JSON routes keep axum's
/// small built-in default limit.
pub fn build_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    build_router_with_cors(state, None)
}

/// Count every served response and its latency. One layer at the
/// outermost edge, so what it measures is what clients experienced,
/// timeouts and refusals included.
async fn measure(request: Request, next: axum::middleware::Next) -> Response {
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    crate::metrics::observe_request(response.status().as_u16(), started.elapsed().as_secs_f64());
    response
}

/// [`build_router`] with an optional CORS allowlist for browser
/// clients. `None` attaches no CORS layer at all: closed by default.
pub fn build_router_with_cors<B: BlobStore + 'static>(
    state: AppState<B>,
    cors_origins: Option<&[String]>,
) -> Router {
    let mut router = api_router(state.clone()).merge(admin_router(state));
    if let Some(origins) = cors_origins {
        router = router.layer(cors_layer(origins));
    }
    router.layer(axum::middleware::from_fn(measure))
}

/// [`api_router`] with the same optional CORS allowlist, for the
/// split-listener deployment shape.
pub fn api_router_with_cors<B: BlobStore + 'static>(
    state: AppState<B>,
    cors_origins: Option<&[String]>,
) -> Router {
    let mut router = api_router(state);
    if let Some(origins) = cors_origins {
        router = router.layer(cors_layer(origins));
    }
    router.layer(axum::middleware::from_fn(measure))
}

/// The strict allowlist CORS layer for configured origins.
fn cors_layer(origins: &[String]) -> tower_http::cors::CorsLayer {
    use axum::http::{HeaderName, HeaderValue, Method};
    let origins: Vec<HeaderValue> = origins.iter().filter_map(|o| o.parse().ok()).collect();
    tower_http::cors::CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ])
        .allow_headers([
            axum::http::header::AUTHORIZATION,
            axum::http::header::CONTENT_TYPE,
            axum::http::header::IF_NONE_MATCH,
            axum::http::header::RANGE,
            HeaderName::from_static("x-copal-tenant"),
            HeaderName::from_static("x-copal-digest"),
            HeaderName::from_static("tus-resumable"),
            HeaderName::from_static("upload-offset"),
            HeaderName::from_static("upload-length"),
            HeaderName::from_static("upload-metadata"),
        ])
        .expose_headers([
            axum::http::header::ETAG,
            axum::http::header::CONTENT_RANGE,
            axum::http::header::ACCEPT_RANGES,
            axum::http::header::CONTENT_DISPOSITION,
            axum::http::header::LOCATION,
            HeaderName::from_static("tus-resumable"),
            HeaderName::from_static("tus-version"),
            HeaderName::from_static("tus-extension"),
            HeaderName::from_static("upload-offset"),
            HeaderName::from_static("upload-length"),
        ])
        .max_age(std::time::Duration::from_secs(3600))
}

/// The tenant-facing API: files, runs, grants, and both faces.
///
/// Two timeout classes: byte routes get the transfer deadline, every
/// other route gets the request deadline. Both exist to end slow-drip
/// connections, and a timed-out request answers 408.
pub fn api_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    let request_timeout = state.limits.request_timeout_secs;
    let request_deadline = tower_http::timeout::TimeoutLayer::with_status_code(
        StatusCode::REQUEST_TIMEOUT,
        std::time::Duration::from_secs(request_timeout),
    );
    let transfer_deadline = tower_http::timeout::TimeoutLayer::with_status_code(
        StatusCode::REQUEST_TIMEOUT,
        std::time::Duration::from_secs(state.limits.transfer_timeout_secs),
    );

    let transfer_routes = Router::new()
        .route(
            "/v1/files/{id}/content",
            put(upload_content::<B>).get(download_content::<B>),
        )
        .route(
            "/v1/files/{id}/versions/{number}/content",
            get(download_version::<B>),
        )
        .route(
            "/v1/grants/{grant_ref}",
            get(redeem_grant::<B>)
                .put(redeem_upload_grant::<B>)
                .delete(revoke_grant::<B>),
        )
        .layer(transfer_deadline)
        .with_state(state.clone());

    Router::new()
        .route("/", get(index::<B>))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz::<B>))
        .route("/v1/files", post(create_file::<B>).get(list_files::<B>))
        .route("/v1/usage", get(tenant_usage::<B>))
        .route("/v1/search", get(search_text::<B>))
        .route("/v1/files/{id}/text", get(file_text::<B>))
        .route(
            "/v1/files/{id}",
            get(get_file::<B>).delete(delete_file::<B>),
        )
        .route("/mcp", post(crate::mcp::mcp_endpoint::<B>))
        .route("/v1/events", get(list_tenant_events::<B>))
        .route("/v1/events/{id}", get(get_tenant_event::<B>))
        .route("/v1/runs", post(start_run::<B>).get(list_runs::<B>))
        .route("/v1/runs/{id}", get(get_run::<B>))
        .route("/v1/runs/{id}/retry", post(retry_run::<B>))
        .route("/v1/files/{id}/versions", get(list_versions::<B>))
        .route("/v1/files/{id}/url", post(issue_grant::<B>))
        .route("/v1/files/{id}/upload-url", post(issue_upload_grant::<B>))
        .route(
            "/v1/files/{id}/renditions",
            post(request_rendition::<B>).get(list_renditions::<B>),
        )
        .route("/v1/files/{id}/transform", post(request_transform::<B>))
        .route("/v1/files/fetch", post(fetch_file::<B>))
        .route(
            "/v1c/{*rest}",
            axum::routing::any(crate::rest_runtime::serve::<B>),
        )
        .route("/v1/files/{id}/renditions/{spec}", get(get_rendition::<B>))
        .layer(request_deadline)
        .with_state(state.clone())
        .merge(transfer_routes)
        // Resumable uploads: byte-bearing, so the transfer deadline.
        .merge(crate::tus::tus_router(state.clone()).layer(
            tower_http::timeout::TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                std::time::Duration::from_secs(state.limits.transfer_timeout_secs),
            ),
        ))
        // The second face: /graphql, served by Kayak from the same
        // contract, dispatching into the same repositories. Queries are
        // ordinary requests, so the request deadline applies.
        .merge(crate::graphql::graphql_router(state).layer(
            tower_http::timeout::TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                std::time::Duration::from_secs(request_timeout),
            ),
        ))
}

/// The operator surface alone: key custody and the audit trail,
/// guarded by the admin token (disabled entirely when none is
/// configured). Servable on its own listener (`COPAL_ADMIN_BIND`) so
/// deployments can keep it off the tenant-facing network; without a
/// separate bind, [`build_router`] merges it into the main router.
pub fn admin_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    Router::new()
        .route("/admin/console", get(crate::console::home::<B>))
        .route(
            "/admin/console/t/{tenant}",
            axum::routing::any(crate::console::tenant_home::<B>),
        )
        // A trailing slash is what a browser produces from a bare
        // base, and a wildcard route matches no empty segment, so the
        // overview answers under both spellings.
        .route(
            "/admin/console/t/{tenant}/",
            axum::routing::any(crate::console::tenant_home::<B>),
        )
        .route(
            "/admin/console/t/{tenant}/{*rest}",
            axum::routing::any(crate::console::tenant_pages::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/keys",
            post(mint_key::<B>).get(list_keys::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/keys/{key_id}",
            axum::routing::delete(revoke_key::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/principals",
            post(create_principal::<B>).get(list_principals::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/principals/{handle}",
            axum::routing::delete(disable_principal::<B>),
        )
        .route("/v1/admin/tenants/{tenant}/audit", get(list_audit::<B>))
        .route("/v1/admin/audit/export", get(export_audit::<B>))
        .route("/v1/admin/tenants", get(list_tenants::<B>))
        .route(
            "/v1/admin/tenants/{tenant}/retention",
            put(set_retention_policy::<B>)
                .get(get_retention_policy::<B>)
                .delete(clear_retention_policy::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/files/{file}/versions/{number}/retention",
            put(set_version_retention::<B>).delete(clear_version_retention::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/files/{file}/versions/{number}/hold",
            put(apply_version_hold::<B>).delete(release_version_hold::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/storage",
            put(assign_storage::<B>).get(get_storage::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/quota",
            put(set_quota::<B>)
                .get(get_quota::<B>)
                .delete(clear_quota::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/tiering",
            put(crate::tiering::set_policy::<B>)
                .get(crate::tiering::get_policy::<B>)
                .delete(crate::tiering::clear_policy::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/files/{file}/tier",
            put(crate::tiering::set_pin::<B>).delete(crate::tiering::clear_pin::<B>),
        )
        .route("/v1/admin/tiering/report", get(crate::tiering::report::<B>))
        .route("/metrics", get(metrics_scrape::<B>))
        .with_state(state)
}

/// Prometheus scrape. Guarded by the admin token like everything else
/// on this surface: request volumes and error rates are operator
/// data, not public.
async fn metrics_scrape<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
) -> Result<([(axum::http::HeaderName, &'static str); 1], String), ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        crate::metrics::render(),
    ))
}

/// Characters of passage returned with a hit. A window, not the
/// document: search results should not become a bulk text-export
/// channel.
const EXCERPT_CHARS: usize = 400;

/// Search query parameters. `mode` selects retrieval: `lexical`
/// (words), `semantic` (meaning), or `hybrid` (both, fused).
#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: String,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
    /// Fields to count the match set by, comma separated.
    #[serde(default)]
    facets: Option<String>,
}

/// Search a tenant's extracted text.
///
/// Hits carry the file id and the matched text, in the engine's
/// full-text relevance order. There is no score field: SurrealDB 3.x
/// does not report per-row BM25 values, and a column that is always
/// zero would read as relevance without being it.
/// Retrieval, shared by the REST handler and the contract query
/// resolver so the two faces cannot answer differently.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn search_core<B: BlobStore>(
    state: &AppState<B>,
    store: &copal_store::Store,
    tenant: &TenantId,
    q: &str,
    mode: Option<&str>,
    limit: i64,
    filters: &copal_store::repo::text::SearchFilters,
    cursor: Option<&str>,
    facets: Option<&str>,
) -> Result<serde_json::Value, ApiError> {
    // Parsed before any retrieval runs, so a misspelled field is a
    // validation error rather than a search the caller pays for and
    // then cannot read the counts of.
    let facet_fields = match facets {
        Some(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(copal_store::repo::text::FacetField::parse)
            .collect::<copal_core::Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    // The cursor is a ranking offset, opaque on the wire. Rankings
    // shift as content changes, so continuation is best-effort: a
    // page boundary may repeat or skip a result that moved between
    // requests. The depth cap is the over-fetch envelope both legs
    // already pay for.
    let offset = match cursor {
        Some(raw) => raw
            .strip_prefix("o:")
            .and_then(|n| n.parse::<usize>().ok())
            .filter(|n| *n + limit as usize <= 500)
            .ok_or_else(|| CopalError::validation("malformed or too-deep cursor"))?,
        None => 0,
    };
    // One past the page, so the ranking can say whether more
    // remains without a second query.
    let fetch = (offset as i64 + limit + 1).min(500);
    let requested = mode.unwrap_or("hybrid");
    // Semantic modes need an embedding service; without one, asking
    // for meaning gets words rather than an error, and the response
    // says which retrieval actually ran.
    let semantic_available = state.embedding.is_some();
    let mode = match (requested, semantic_available) {
        ("lexical", _) => "lexical",
        ("semantic", true) => "semantic",
        ("semantic", false) => "lexical",
        ("hybrid", true) => "hybrid",
        ("hybrid", false) => "lexical",
        (other, _) => {
            return Err(CopalError::validation(format!(
                "mode must be lexical, semantic, or hybrid, not {other}",
            ))
            .into())
        }
    };

    let lexical = if mode != "semantic" {
        copal_store::repo::text::search(store, tenant, q, fetch, filters).await?
    } else {
        Vec::new()
    };
    let semantic = if mode == "lexical" {
        Vec::new()
    } else {
        let (addr, model) = state.embedding.clone().expect("checked above");
        let vector = crate::embed::embed(&addr, &model, q).await?;
        copal_store::repo::text::semantic_search(
            store,
            tenant,
            &vector,
            fetch,
            state.limits.max_semantic_distance,
            filters,
        )
        .await?
    };

    // Fuse by rank, then render from whichever list carried the hit.
    let mut bodies: std::collections::HashMap<String, (i64, String)> =
        std::collections::HashMap::new();
    // A document can match in several passages; the first one each
    // ranking offers is its best, so later ones add nothing.

    let mut rank = |hits: &[copal_store::repo::text::SearchHit]| -> Vec<String> {
        hits.iter()
            .filter_map(|hit| {
                let id = hit.file_id()?;
                bodies
                    .entry(id.clone())
                    .or_insert_with(|| (hit.ordinal, hit.body.clone()));
                Some(id)
            })
            .collect()
    };
    let lexical_ids = rank(&lexical);
    let semantic_ids = rank(&semantic);
    let mut ordered = match mode {
        "lexical" => lexical_ids,
        "semantic" => semantic_ids,
        _ => crate::embed::reciprocal_rank_fusion(&[lexical_ids, semantic_ids], 60.0),
    };

    // Reranking, when a service is configured. Retrieval decides which
    // passages contain the words or sit near the vector; neither reads
    // a passage against the question. This does, over the head of the
    // ranking, because reading pairs with a model costs per pair.
    //
    // The tail below `depth` keeps its fused order. A search deeper
    // than the reranker saw is then a reranked head followed by a
    // fused remainder, which is coherent to page through, and the
    // response says how far the reranking reached.
    let mut reranked = None;
    if let Some(reranker) = &state.reranker {
        let head = ordered.len().min(reranker.depth);
        let documents: Vec<String> = ordered[..head]
            .iter()
            .filter_map(|id| bodies.get(id).map(|(_, body)| body.clone()))
            .collect();
        if documents.len() == head && head > 1 {
            match crate::rerank::rerank(
                &reranker.addr,
                reranker.model.as_deref(),
                reranker.token.as_deref(),
                q,
                &documents,
            )
            .await
            {
                Ok(order) => {
                    let head_ids: Vec<String> =
                        order.into_iter().map(|i| ordered[i].clone()).collect();
                    ordered = head_ids
                        .into_iter()
                        .chain(ordered[head..].to_vec())
                        .collect();
                    reranked = Some(head);
                }
                // A reranker is an improvement on an answer that
                // already exists, so losing it costs relevance rather
                // than the search. Semantic retrieval degrades to
                // lexical the same way.
                Err(error) => {
                    tracing::warn!(%error, "reranking failed; ranking by fusion alone");
                    reranked = Some(0);
                }
            }
        }
    }

    let page: Vec<_> = ordered
        .iter()
        .skip(offset)
        .take(limit as usize)
        .filter_map(|id| {
            let (ordinal, body) = bodies.get(id)?;
            // Chosen with the analyzer that decided the match, so a
            // passage matched through a stem shows the word that
            // matched. The spans say where, for a caller that marks
            // them.
            let found = copal_core::excerpt(body, q, EXCERPT_CHARS);
            Some(json!({
                "file": id,
                // Which passage matched, so a caller can point at it.
                "passage": ordinal,
                "excerpt": found.text,
                "matches": found
                    .matches
                    .iter()
                    .map(|(start, end)| json!([start, end]))
                    .collect::<Vec<_>>(),
            }))
        })
        .collect();
    let next_cursor = if ordered.len() > offset + page.len() && !page.is_empty() {
        Some(format!("o:{}", offset + page.len()))
    } else {
        None
    };

    let mut response = json!({ "mode": mode, "items": page, "next_cursor": next_cursor });
    // How far the reranking reached, so a caller can tell a reranked
    // head from a fused one. Zero means a reranker is configured and
    // did not answer. Absent means none is configured, or that fewer
    // than two documents matched and none was called.
    if let Some(depth) = reranked {
        response["reranked"] = json!(depth);
    }
    // Counted over the whole match set rather than the ranked window,
    // so a facet says how many documents match and the page says which
    // ones rank. Lexical matching decides membership either way: the
    // semantic leg has no match set to count, only a neighbourhood.
    if !facet_fields.is_empty() {
        let mut counts = serde_json::Map::new();
        for field in facet_fields {
            let buckets: Vec<serde_json::Value> =
                copal_store::repo::text::facet_counts(store, tenant, q, field, filters)
                    .await?
                    .into_iter()
                    .map(|bucket| json!({ "value": bucket.value, "files": bucket.files }))
                    .collect();
            counts.insert(field.as_str().to_owned(), json!(buckets));
        }
        response["facets"] = serde_json::Value::Object(counts);
    }
    Ok(response)
}

async fn search_text<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<SearchQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = params.limit.unwrap_or(20).clamp(1, 100);
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, limit as u64)
            .await?;
    let filters = copal_store::repo::text::SearchFilters {
        path_prefix: params.prefix.clone(),
        content_type: params.content_type.clone(),
    };
    let answer = search_core(
        &state,
        &auth.store,
        &auth.tenant,
        &params.q,
        params.mode.as_deref(),
        limit,
        &filters,
        params.cursor.as_deref(),
        params.facets.as_deref(),
    )
    .await?;
    Ok(Json(answer))
}

/// One file's extracted text, shared by both faces.
///
/// The stored body contains the marked spans, so this read is the
/// second door beside search and must close with it. The response
/// elides the withheld spans and carries `withheld`, a count of
/// elided regions, so a caller knows the text is partial; refusing
/// the whole document instead would recreate exactly the
/// all-or-nothing behavior per-chunk authorization exists to remove.
/// Span positions are never disclosed: the length of a secret is
/// part of the secret, so `chars` counts the SERVED text and the
/// count says how many gaps there are, not where or how wide.
///
/// The spans persist all four access levels; only `grant` is
/// operative here, because this read is tenant-authenticated and
/// read-scoped, so the other three levels admit every caller who can
/// reach it today. When principals split the read path, the filter
/// below is where `private` and `tenant` spans start to bite.
pub(crate) async fn file_text_core(
    store: &copal_store::Store,
    tenant: &TenantId,
    id: &FileId,
) -> Result<serde_json::Value, ApiError> {
    let row = copal_store::repo::text::get_text(store, tenant, id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("no extracted text for file {id}")))?;
    let operative: Vec<(usize, usize)> = row
        .withheld
        .iter()
        .filter(|span| span.access == copal_core::AccessLevel::Grant.as_str())
        .map(|span| (span.start, span.end))
        .collect();
    let (text, withheld) = copal_core::marker::elide(&row.body, &operative);
    Ok(json!({
        "file": id.as_str(),
        "digest": row.digest,
        "chars": text.chars().count(),
        "withheld": withheld,
        "extractor": row.extractor,
        "text": text,
        "updated_at": row.updated_at,
    }))
}

async fn file_text<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let id = parse_id(&id)?;
    Ok(Json(file_text_core(&auth.store, &auth.tenant, &id).await?))
}

/// A tenant's own usage and ceiling.
async fn tenant_usage<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let tenant = &auth.tenant;
    let (bytes, files) = copal_store::repo::tenant::usage_counter(&auth.store, tenant).await?;
    let quota = copal_store::repo::tenant::get_quota(&auth.store, tenant).await?;
    // Retained bytes ride beside the total so a tenant can tell
    // "full" apart from "full of things I may not remove".
    let retained = copal_store::repo::tenant::retained_bytes(&auth.store, tenant).await?;
    Ok(Json(json!({
        "bytes": bytes,
        "files": files,
        "retained_bytes": retained,
        "quota_bytes": quota,
    })))
}

/// Quota assignment body.
#[derive(Debug, Deserialize)]
struct SetQuotaRequest {
    max_bytes: i64,
}

/// Set a tenant's storage ceiling.
async fn set_quota<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Json(request): Json<SetQuotaRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    copal_store::repo::tenant::set_quota(&state.store, &tenant, request.max_bytes).await?;
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "tenant.quota_set",
        &request.max_bytes.to_string(),
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(Json(json!({ "max_bytes": request.max_bytes })))
}

#[derive(serde::Deserialize)]
struct RetentionPolicyRequest {
    #[serde(default)]
    seconds: Option<u64>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    keep_last: Option<u32>,
}

/// Set the tenant's default retention: a clock and mode stamped onto
/// every new version, a history depth pruned to, or both.
async fn set_retention_policy<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Json(request): Json<RetentionPolicyRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if request.seconds.is_none() && request.keep_last.is_none() {
        return Err(CopalError::validation("a policy needs seconds, keep_last, or both").into());
    }
    if let Some(mode) = request.mode.as_deref() {
        if mode != "governance" && mode != "compliance" {
            return Err(CopalError::validation("mode must be governance or compliance").into());
        }
    }
    if request.keep_last == Some(0) {
        return Err(CopalError::validation("keep_last must be at least 1").into());
    }
    let policy = copal_store::repo::tenant::RetentionPolicy {
        seconds: request.seconds,
        mode: request.mode.clone(),
        keep_last: request.keep_last,
    };
    copal_store::repo::tenant::set_retention_policy(&state.store, &tenant, &policy).await?;
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "tenant.retention_policy_set",
        tenant.as_str(),
        forwarded_origin(&headers).as_deref(),
        Some(json!({
            "seconds": request.seconds,
            "mode": request.mode,
            "keep_last": request.keep_last,
        })),
    )
    .await?;
    Ok(Json(json!({
        "seconds": request.seconds,
        "mode": request.mode,
        "keep_last": request.keep_last,
    })))
}

async fn get_retention_policy<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let policy = copal_store::repo::tenant::get_retention_policy(&state.store, &tenant).await?;
    match policy {
        Some(policy) => Ok(Json(json!({
            "seconds": policy.seconds,
            "mode": policy.mode,
            "keep_last": policy.keep_last,
        }))),
        None => Err(CopalError::not_found("no retention policy").into()),
    }
}

/// Remove the default. Versions already stamped keep their clocks,
/// because the stamp is never recomputed.
async fn clear_retention_policy<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    copal_store::repo::tenant::clear_retention_policy(&state.store, &tenant).await?;
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "tenant.retention_policy_cleared",
        tenant.as_str(),
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
struct RetentionRequest {
    seconds: u64,
    #[serde(default)]
    mode: Option<String>,
}

#[derive(serde::Deserialize)]
struct HoldRequest {
    reason: String,
}

/// Set a version's retention clock. Compliance mode accepts only
/// extensions of itself; the refusal is a 409 because the row is in
/// a state the request may not move it out of, admin or no admin,
/// and that authority line is the whole of WORM.
async fn set_version_retention<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, file, number)): Path<(String, String, u64)>,
    Json(request): Json<RetentionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let file = parse_id(&file)?;
    let mode = request.mode.as_deref().unwrap_or("governance");
    if mode != "governance" && mode != "compliance" {
        return Err(CopalError::validation("mode must be governance or compliance").into());
    }
    let applied = copal_store::repo::version::set_retention(
        &state.store,
        &tenant,
        &file,
        number,
        request.seconds,
        mode,
    )
    .await?;
    if !applied {
        crate::metrics::incr("copal_retention_refusals_total");
        return Err(
            CopalError::conflict("compliance retention only extends; wait for the clock").into(),
        );
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "version.retention_set",
        &format!("{file}#v{number}"),
        forwarded_origin(&headers).as_deref(),
        Some(json!({ "seconds": request.seconds, "mode": mode })),
    )
    .await?;
    copal_store::repo::eventing::emit_event(
        &state.store,
        &tenant,
        Some(file.as_str()),
        "version.retention_set",
        json!({ "number": number, "seconds": request.seconds, "mode": mode }),
    )
    .await?;
    Ok(Json(json!({ "seconds": request.seconds, "mode": mode })))
}

/// Clear a version's retention. Refuses on an unexpired compliance
/// clock, exactly as shortening does.
async fn clear_version_retention<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, file, number)): Path<(String, String, u64)>,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let file = parse_id(&file)?;
    let applied =
        copal_store::repo::version::clear_retention(&state.store, &tenant, &file, number).await?;
    if !applied {
        crate::metrics::incr("copal_retention_refusals_total");
        return Err(
            CopalError::conflict("compliance retention only extends; wait for the clock").into(),
        );
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "version.retention_cleared",
        &format!("{file}#v{number}"),
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    copal_store::repo::eventing::emit_event(
        &state.store,
        &tenant,
        Some(file.as_str()),
        "version.retention_cleared",
        json!({ "number": number }),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Apply a legal hold. The reason is required because a hold is a
/// statement someone made on purpose, and the audit trail is where
/// that statement lives.
async fn apply_version_hold<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, file, number)): Path<(String, String, u64)>,
    Json(request): Json<HoldRequest>,
) -> Result<StatusCode, ApiError> {
    version_hold(
        &state,
        &headers,
        &tenant,
        &file,
        number,
        &request.reason,
        true,
    )
    .await
}

/// Release a legal hold, with the reason recorded beside the apply.
async fn release_version_hold<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, file, number)): Path<(String, String, u64)>,
    Json(request): Json<HoldRequest>,
) -> Result<StatusCode, ApiError> {
    version_hold(
        &state,
        &headers,
        &tenant,
        &file,
        number,
        &request.reason,
        false,
    )
    .await
}

async fn version_hold<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    tenant: &str,
    file: &str,
    number: u64,
    reason: &str,
    held: bool,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(state, headers)?;
    if reason.trim().is_empty() {
        return Err(CopalError::validation("a hold change requires a reason").into());
    }
    let tenant = TenantId::parse(tenant)?;
    let file = parse_id(file)?;
    copal_store::repo::version::set_legal_hold(&state.store, &tenant, &file, number, held).await?;
    let action = if held {
        "version.hold_applied"
    } else {
        "version.hold_released"
    };
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        action,
        &format!("{file}#v{number}"),
        forwarded_origin(headers).as_deref(),
        Some(json!({ "reason": reason })),
    )
    .await?;
    copal_store::repo::eventing::emit_event(
        &state.store,
        &tenant,
        Some(file.as_str()),
        action,
        json!({ "number": number, "reason": reason }),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// A tenant's quota and current usage, admin view.
async fn get_quota<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let quota = copal_store::repo::tenant::get_quota(&state.store, &tenant).await?;
    let (bytes, files) = copal_store::repo::tenant::usage(&state.store, &tenant).await?;
    let retained = copal_store::repo::tenant::retained_bytes(&state.store, &tenant).await?;
    Ok(Json(json!({
        "max_bytes": quota,
        "retained_bytes": retained,
        "bytes": bytes,
        "files": files,
    })))
}

/// Return a tenant to unlimited; no quota reads as 404.
async fn clear_quota<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !copal_store::repo::tenant::clear_quota(&state.store, &tenant).await? {
        return Err(CopalError::not_found("no quota is set").into());
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "tenant.quota_cleared",
        "",
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Storage assignment body.
#[derive(Debug, Deserialize)]
struct AssignStorageRequest {
    residency: String,
}

/// Pin a tenant's NEW content to a configured residency. Existing
/// content is untouched: blob rows carry their residency and serving
/// resolves from the row.
async fn assign_storage<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Json(request): Json<AssignStorageRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !state.residencies.contains(&request.residency) {
        return Err(CopalError::validation(format!(
            "residency {} is not configured on this instance",
            request.residency,
        ))
        .into());
    }
    copal_store::repo::tenant::set_residency(&state.store, &tenant, &request.residency).await?;
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "tenant.storage_assigned",
        &request.residency,
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(Json(json!({ "residency": request.residency })))
}

/// The residency a tenant's new content lands in.
async fn get_storage<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let residency = copal_store::repo::tenant::get_residency(&state.store, &tenant).await?;
    Ok(Json(json!({ "residency": residency })))
}

async fn healthz() -> &'static str {
    "ok"
}

/// The root: what this service is and where its surfaces are.
///
/// A person who types the host into a browser deserves better than a
/// 404, and a program that probes the root deserves a document rather
/// than prose. Both get the same list, in the shape they asked for.
/// Nothing here is secret: every path named already announces itself
/// by answering, and the console appears only when a token exists to
/// guard it.
async fn index<B: BlobStore>(State(state): State<AppState<B>>, headers: HeaderMap) -> Response {
    use axum::response::IntoResponse as _;

    let mut surfaces = vec![
        ("liveness", "/healthz"),
        ("readiness", "/readyz"),
        ("rest", "/v1/files"),
        ("rest (from the contract)", "/v1c/files"),
        ("graphql", "/graphql"),
        ("mcp", "/mcp"),
        ("search", "/v1/search"),
    ];
    if state.auth.admin_token.is_some() {
        surfaces.push(("console", "/admin/console"));
    }

    let wants_html = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"));

    if !wants_html {
        let listed: serde_json::Map<String, serde_json::Value> = surfaces
            .iter()
            .map(|(name, path)| ((*name).to_owned(), json!(path)))
            .collect();
        return Json(json!({
            "service": "copal",
            "version": env!("CARGO_PKG_VERSION"),
            "surfaces": listed,
        }))
        .into_response();
    }

    let page = maud::html! {
        (maud::DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "copal" }
                style {
                    (maud::PreEscaped(
                        "body{margin:0;padding:3rem 1.5rem;background:#101014;\
                         color:#d6d6dc;font:15px/1.6 ui-monospace,Menlo,monospace}\
                         main{max-width:34rem;margin:0 auto}\
                         h1{font-size:1.2rem;margin:0 0 .25rem;color:#f2f2f6}\
                         p{color:#7d838c;margin:0 0 2rem}\
                         ul{list-style:none;padding:0;margin:0}\
                         li{display:flex;gap:1rem;padding:.45rem 0;\
                         border-bottom:1px solid #26262e}\
                         span{color:#7d838c;min-width:12rem}\
                         a{color:#8fb8ff;text-decoration:none}\
                         a:hover{text-decoration:underline}"
                    ))
                }
            }
            body {
                main {
                    h1 { "copal " (env!("CARGO_PKG_VERSION")) }
                    p { "A self-hosted file service. These surfaces answer here." }
                    ul {
                        @for (name, path) in &surfaces {
                            li {
                                span { (name) }
                                a href=(path) { (path) }
                            }
                        }
                    }
                }
            }
        }
    };
    axum::response::Html(page.into_string()).into_response()
}

/// Readiness: both planes must answer. Liveness stays `/healthz`.
async fn readyz<B: BlobStore>(
    State(state): State<AppState<B>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.store.ping().await?;
    // Any digest works: an absent object still proves the plane
    // answers, and errors surface as 500.
    let probe = copal_core::ContentDigest::parse("0".repeat(64).as_str())
        .map_err(|e| ApiError(CopalError::Store(e.to_string())))?;
    state.blobs.exists(&probe).await?;
    Ok(Json(json!({ "store": "ok", "blobs": "ok" })))
}

/// Request body for minting an API key.
#[derive(Debug, Deserialize)]
struct MintKeyRequest {
    name: String,
    /// Scope names from [`crate::auth::KEY_SCOPES`]; empty mints an
    /// unscoped key, which holds all of them.
    #[serde(default)]
    scopes: Vec<String>,
    /// Seconds until the key expires; absent means it never does.
    #[serde(default)]
    ttl_secs: Option<u32>,
    #[serde(default)]
    principal: Option<String>,
}

#[derive(serde::Deserialize)]
struct CreatePrincipalRequest {
    handle: String,
    kind: String,
    #[serde(default)]
    scopes: Vec<String>,
}

/// Create a named actor. Keys minted under it answer to its scope
/// ceiling and to its disabled switch.
async fn create_principal<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Json(request): Json<CreatePrincipalRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !matches!(request.kind.as_str(), "human" | "service" | "agent") {
        return Err(CopalError::validation("kind must be human, service, or agent").into());
    }
    for scope in &request.scopes {
        if !crate::auth::KEY_SCOPES.contains(&scope.as_str()) {
            return Err(CopalError::validation(format!(
                "unknown scope {scope:?}; scopes are read, write, admin",
            ))
            .into());
        }
    }
    let row = copal_store::repo::principal::create_principal(
        &state.store,
        &tenant,
        &request.handle,
        &request.kind,
        &request.scopes,
    )
    .await?;
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "principal.created",
        &row.handle,
        forwarded_origin(&headers).as_deref(),
        Some(json!({ "kind": row.kind, "scopes": request.scopes })),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "handle": row.handle,
            "kind": row.kind,
            "scopes": request.scopes,
            "created_at": row.created_at,
        })),
    ))
}

async fn list_principals<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let rows = copal_store::repo::principal::list_principals(&state.store, &tenant).await?;
    let items: Vec<_> = rows
        .iter()
        .map(|row| {
            json!({
                "handle": row.handle,
                "kind": row.kind,
                "scopes": row.scope_list(),
                "disabled_at": row.disabled_at,
                "created_at": row.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "items": items })))
}

/// Disable a principal: every key under it refuses from this moment.
/// Disabling rather than deleting, because the audit trail keeps
/// naming the actor.
async fn disable_principal<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, handle)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let disabled =
        copal_store::repo::principal::disable_principal(&state.store, &tenant, &handle).await?;
    if !disabled {
        return Err(CopalError::not_found(format!("no live principal {handle:?}")).into());
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "principal.disabled",
        &handle,
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Mint a tenant API key. The bearer token appears exactly once, in
/// this response; the store keeps only its hash.
async fn mint_key<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Json(request): Json<MintKeyRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    for scope in &request.scopes {
        if !crate::auth::KEY_SCOPES.contains(&scope.as_str()) {
            return Err(CopalError::validation(format!(
                "unknown scope {scope:?}; scopes are read, write, admin",
            ))
            .into());
        }
    }
    if request.ttl_secs == Some(0) {
        return Err(CopalError::validation("ttl_secs must be at least 1").into());
    }
    // A key under a principal answers to its ceiling at mint time
    // too: scopes beyond it refuse loudly here rather than silently
    // shrinking at authentication.
    let principal_id = match request.principal.as_deref() {
        Some(handle) => {
            let principal =
                copal_store::repo::principal::get_by_handle(&state.store, &tenant, handle)
                    .await?
                    .ok_or_else(|| CopalError::not_found(format!("no principal {handle:?}")))?;
            if principal.disabled_at.is_some() {
                return Err(
                    CopalError::validation(format!("principal {handle:?} is disabled",)).into(),
                );
            }
            let ceiling = principal.scope_list();
            if !ceiling.is_empty() {
                for scope in &request.scopes {
                    if !ceiling.contains(scope) {
                        return Err(CopalError::validation(format!(
                            "scope {scope:?} exceeds principal {handle:?}",
                        ))
                        .into());
                    }
                }
            }
            Some(principal.principal_id())
        }
        None => None,
    };
    let token = copal_sign::ApiKeyToken::mint();
    let row = copal_store::repo::auth::create_key(
        &state.store,
        &tenant,
        &request.name,
        &token.key_id,
        &token.secret_hash(),
        &request.scopes,
        principal_id.as_deref(),
    )
    .await?;
    if let Some(ttl) = request.ttl_secs {
        // A key that was asked to expire must never outlive a failure
        // to arm that expiry: revoke it rather than leave it eternal.
        if let Err(err) =
            copal_store::repo::auth::arm_key_expiry(&state.store, &row.key_id(), ttl).await
        {
            let _ = copal_store::repo::auth::revoke_key(&state.store, &tenant, &row.key_id()).await;
            return Err(err.into());
        }
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "key.minted",
        &row.key_id(),
        forwarded_origin(&headers).as_deref(),
        Some(json!({ "name": row.name, "principal": request.principal })),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "key_id": row.key_id(),
            "name": row.name,
            "principal": request.principal,
            "scopes": request.scopes,
            "expires_in_secs": request.ttl_secs,
            "token": token.encode(),
            "created_at": row.created_at,
        })),
    ))
}

/// List a tenant's keys (never their hashes).
async fn list_keys<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let keys = copal_store::repo::auth::list_keys(&state.store, &tenant).await?;
    Ok(Json(json!({ "items": keys })))
}

/// Revoke a key; already-revoked and unknown both read as 404 so the
/// admin surface is not a key-id oracle either.
async fn revoke_key<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, key_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !copal_store::repo::auth::revoke_key(&state.store, &tenant, &key_id).await? {
        return Err(CopalError::not_found(format!("key {key_id}")).into());
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "key.revoked",
        &key_id,
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Audit-listing query parameters.
#[derive(Debug, Deserialize)]
struct AuditListQuery {
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(serde::Deserialize)]
struct AuditExportQuery {
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    tenant: Option<String>,
}

/// A tenant's audit trail, newest first (admin surface).
async fn list_audit<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    axum::extract::Query(params): axum::extract::Query<AuditListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let limit = params.limit.unwrap_or(200).clamp(1, 1_000);
    let events = copal_store::repo::auth::list_audit(&state.store, &tenant, limit).await?;
    Ok(Json(json!({ "items": events })))
}

/// Every tenant that has stored anything, with files and bytes:
/// the deployment's population in one grouped query.
async fn list_tenants<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let items = copal_store::repo::tenant::known_tenants(&state.store).await?;
    Ok(Json(json!({ "items": items })))
}

/// The deployment audit trail as NDJSON, ascending, keyset-cursored:
/// the SIEM face. The next cursor rides the `x-copal-next-cursor`
/// header on every page carrying rows, so the body stays pure
/// line-delimited events and a partial page still advances the
/// checkpoint. An empty body with no header means the checkpoint is
/// current.
async fn export_audit<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<AuditExportQuery>,
) -> Result<(StatusCode, HeaderMap, String), ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let limit = params.limit.unwrap_or(500).clamp(1, 1_000);
    let tenant = params.tenant.as_deref().map(TenantId::parse).transpose()?;
    let rows = copal_store::repo::auth::export_audit_page(
        &state.store,
        tenant.as_ref().map(TenantId::as_str),
        limit,
        params.cursor.as_deref(),
    )
    .await?;
    let next = rows.last().and_then(|last| {
        let at = last.get("created_at").and_then(serde_json::Value::as_str)?;
        let id = last.get("id").and_then(serde_json::Value::as_str)?;
        Some(format!("{at}~{id}"))
    });
    if !rows.is_empty() {
        crate::metrics::add("copal_audit_exported_total", rows.len() as u64);
    }
    let mut body = String::new();
    for row in &rows {
        body.push_str(&row.to_string());
        body.push('\n');
    }
    let mut out = HeaderMap::new();
    out.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/x-ndjson"),
    );
    if let Some(next) = next {
        let value = axum::http::HeaderValue::from_str(&next)
            .map_err(|_| CopalError::Store("cursor not header-safe".into()))?;
        out.insert(
            axum::http::HeaderName::from_static("x-copal-next-cursor"),
            value,
        );
    }
    Ok((StatusCode::OK, out, body))
}

/// The proxy-forwarded client origin, first hop only, for audit
/// forensics. Never used for authorization.
pub(crate) fn forwarded_origin(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.split(',').next())
        .map(|first| first.trim().to_owned())
        .filter(|first| !first.is_empty())
}

fn parse_id(raw: &str) -> Result<FileId, ApiError> {
    Ok(FileId::parse(raw)?)
}

async fn create_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(spec): Json<FileSpec>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let created = file_repo::create_file(&auth.store, &auth.tenant, &spec, "api").await?;
    // 201 for a fresh record, 200 for an idempotency-key replay that
    // returned the original, so retries read as success.
    let status = if created.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(crate::wire::wire_file(&created.record))))
}

/// Listing query parameters, matching the generated OpenAPI document:
/// `state` filters (indexed), `sort` is `created_at` or `-created_at`.
#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    sort: Option<String>,
}

/// Encode a page position as an opaque cursor. Hex over a delimited
/// triple (direction, timestamp, id) -- opacity is the point; clients
/// never construct these, and the embedded direction lets decode
/// refuse a cursor replayed under a different sort order (which would
/// silently return wrong pages).
pub(crate) fn encode_cursor(position: &file_repo::ListPosition, ascending: bool) -> String {
    let dir = if ascending { 'a' } else { 'd' };
    hex::encode(format!("{dir}|{}|{}", position.created_at, position.id))
}

pub(crate) fn decode_cursor(
    raw: &str,
    ascending: bool,
) -> Result<file_repo::ListPosition, ApiError> {
    let invalid = || CopalError::validation("malformed cursor");
    let bytes = hex::decode(raw).map_err(|_| invalid())?;
    let text = String::from_utf8(bytes).map_err(|_| invalid())?;
    let mut parts = text.splitn(3, '|');
    let (Some(dir), Some(created_at), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(invalid().into());
    };
    let cursor_ascending = match dir {
        "a" => true,
        "d" => false,
        _ => return Err(invalid().into()),
    };
    if cursor_ascending != ascending {
        return Err(CopalError::validation("cursor was issued for a different sort order").into());
    }
    Ok(file_repo::ListPosition {
        created_at: created_at.to_owned(),
        id: FileId::parse(id)?,
    })
}

fn parse_state_param(raw: &str) -> Result<FileState, ApiError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned()))
        .map_err(|_| CopalError::validation(format!("unknown state {raw:?}")).into())
}

async fn list_files<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = params.limit.unwrap_or(100).clamp(1, 100);
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, limit as u64)
            .await?;
    let ascending = match params.sort.as_deref() {
        None | Some("-created_at") => false,
        Some("created_at") => true,
        Some(other) => {
            return Err(CopalError::validation(format!("unknown sort {other:?}")).into());
        }
    };
    let after = params
        .cursor
        .as_deref()
        .map(|raw| decode_cursor(raw, ascending))
        .transpose()?;
    let state_filter = params.state.as_deref().map(parse_state_param).transpose()?;
    let records = file_repo::list_files(
        &auth.store,
        &auth.tenant,
        limit,
        after.as_ref(),
        ascending,
        state_filter,
    )
    .await?;
    // A full page may have more behind it; the cursor points past the
    // last row either way and a drained next page returns empty.
    let next_cursor = if records.len() as i64 == limit {
        records.last().map(|last| {
            encode_cursor(
                &file_repo::ListPosition {
                    created_at: last.created_at.clone(),
                    id: last.id.clone(),
                },
                ascending,
            )
        })
    } else {
        None
    };
    let items: Vec<serde_json::Value> = records.iter().map(crate::wire::wire_file).collect();
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

async fn get_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let id = parse_id(&id)?;
    let record = file_repo::get_file(&auth.store, &auth.tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    Ok(Json(crate::wire::wire_file(&record)))
}

/// Soft-delete: tombstone the record and free its live path. Bytes go
/// later, via garbage collection, once nothing references them;
/// deletion is a metadata act, reclamation is a sweep.
async fn delete_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let id = parse_id(&id)?;
    remove_file_core(
        &auth.store,
        &auth.tenant,
        &id,
        forwarded_origin(&headers).as_deref(),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Soft-delete plus its audit event, shared by the REST handler and
/// the GraphQL action resolver.
pub(crate) async fn remove_file_core(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    origin: Option<&str>,
) -> Result<(), ApiError> {
    // Read the size before the tombstone hides it, so the counter can
    // give the bytes back; the sweep's recount corrects any drift.
    let released = file_repo::get_file(store, tenant, id)
        .await?
        .and_then(|record| record.size_bytes);
    file_repo::soft_delete(store, tenant, id).await?;
    // Search indexes what exists; a tombstoned file's text would keep
    // answering queries with content nobody can fetch.
    let _ = copal_store::repo::text::delete_text(store, id).await;
    let _ = copal_store::repo::text::delete_chunks(store, id).await;
    if let Some(bytes) = released {
        let _ = copal_store::repo::tenant::release_usage(store, tenant, bytes as i64).await;
    }

    copal_store::repo::auth::record_audit(
        store,
        tenant,
        tenant.as_str(),
        "file.removed",
        id.as_str(),
        origin,
        None,
    )
    .await?;
    Ok(())
}

/// Rendition request body. Defaults produce a 256-square jpeg thumb.
#[derive(Debug, Deserialize)]
struct RenditionRequest {
    #[serde(default = "default_rendition_kind")]
    kind: String,
    #[serde(default = "default_rendition_dim")]
    width: u32,
    #[serde(default = "default_rendition_dim")]
    height: u32,
    #[serde(default = "default_rendition_format")]
    format: String,
}

fn default_rendition_kind() -> String {
    "thumb".to_owned()
}

fn default_rendition_dim() -> u32 {
    256
}

fn default_rendition_format() -> String {
    "jpeg".to_owned()
}

/// Request a rendition: create the derived record at its deterministic
/// path, link it to the source, and enqueue the render. A repeat with
/// the same parameters returns the existing record instead of a
/// duplicate, because the rendition path is unique per live file.
/// Bounds and charset shared by both rendition faces.
fn validate_rendition_spec(request: &RenditionSpec) -> Result<(), ApiError> {
    if !(16..=4096).contains(&request.width) || !(16..=4096).contains(&request.height) {
        return Err(CopalError::validation("width and height must be within 16..=4096").into());
    }
    if !matches!(request.format.as_str(), "jpeg" | "png") {
        return Err(CopalError::validation("format must be jpeg or png").into());
    }
    let kind_ok = !request.kind.is_empty()
        && request.kind.len() <= 32
        && request
            .kind
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'));
    if !kind_ok {
        return Err(CopalError::validation(
            "kind must be 1..=32 characters of letters, digits, hyphen, underscore",
        )
        .into());
    }
    Ok(())
}

/// The parameter digest and derived path one rendition spec maps to,
/// shared by the request action and the on-the-fly URL face.
fn rendition_artifacts(source_path: &str, spec: &RenditionSpec) -> (String, String) {
    let params = format!("w={}&h={}&f={}", spec.width, spec.height, spec.format);
    let full_digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(params.as_bytes()))
    };
    let path = format!(
        "{}@{}-{}x{}.{}",
        source_path, spec.kind, spec.width, spec.height, spec.format
    );
    (full_digest[..16].to_owned(), path)
}

/// Parse the URL segment `{kind}-{width}x{height}.{format}`.
fn parse_rendition_spec(raw: &str) -> Result<RenditionSpec, ApiError> {
    let malformed = || {
        ApiError::from(CopalError::validation(
            "rendition spec reads kind-WIDTHxHEIGHT.format",
        ))
    };
    let (stem, format) = raw.rsplit_once('.').ok_or_else(malformed)?;
    let (kind, dims) = stem.rsplit_once('-').ok_or_else(malformed)?;
    let (w, h) = dims.split_once('x').ok_or_else(malformed)?;
    let width: u32 = w.parse().map_err(|_| malformed())?;
    let height: u32 = h.parse().map_err(|_| malformed())?;
    Ok(RenditionSpec {
        kind: kind.to_owned(),
        width,
        height,
        format: format.to_owned(),
    })
}

/// Serve a derived record under its own access level, the same
/// enforcement `/content` applies.
async fn serve_rendition<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    record: copal_core::FileRecord,
) -> Result<Response, ApiError> {
    let cache = match record.access {
        copal_core::AccessLevel::Public => crate::serve::CacheClass::Public,
        access => {
            let tenant =
                crate::auth::authenticate_scoped(state, headers, crate::auth::Scope::Read, 1)
                    .await?;
            if record.tenant_id != tenant {
                return Err(CopalError::not_found(format!("file {}", record.id)).into());
            }
            if access == copal_core::AccessLevel::Grant {
                return Err(
                    CopalError::forbidden("file is grant-only; redeem an issued URL").into(),
                );
            }
            crate::serve::CacheClass::Private
        }
    };
    if !record.servable_content() {
        return Err(CopalError::conflict(format!(
            "rendition is {} with no servable content",
            record.state.as_str(),
        ))
        .into());
    }
    if state.withholds_pending_scan(&record) {
        return Err(CopalError::conflict("content is awaiting a malware scan").into());
    }
    let digest = record
        .digest
        .as_ref()
        .ok_or_else(|| CopalError::Store("servable file without digest".into()))?;
    let backend = match state.backend_or_recall(&record.tenant_id, &record).await? {
        Ok(backend) => backend,
        Err(run) => return Ok(crate::recall::accepted_response(&run)),
    };
    crate::tiering::note_blob_read(
        &state.store,
        record.blob_residency.as_deref().unwrap_or("local"),
        digest,
    );
    crate::serve::serve_blob(
        &backend,
        headers,
        crate::serve::ServeSpec {
            content_type: &record.content_type,
            digest,
            path: &record.path,
            cache,
        },
    )
    .await
}

/// Serve a rendition straight from its URL, deriving on first
/// request. The spec segment reads `{kind}-{width}x{height}.{format}`,
/// the shape the derived path already carries. An existing rendition
/// serves under its own access level, so public thumbnails stay
/// anonymous and cacheable; deriving is a write, needs the write
/// scope, and belongs to the owning tenant.
async fn get_rendition<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((id, spec_raw)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id)?;
    let spec = parse_rendition_spec(&spec_raw)?;
    validate_rendition_spec(&spec)?;
    let source = file_repo::get_file_any(&state.store, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    let tenant = source.tenant_id.clone();
    let (params_digest, derived_path) = rendition_artifacts(&source.path, &spec);

    if let Some(existing) = file_repo::find_by_path(&state.store, &tenant, &derived_path).await? {
        if existing.servable_content() {
            return serve_rendition(&state, &headers, existing).await;
        }
    }

    let caller =
        crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    if caller != tenant {
        return Err(CopalError::not_found(format!("file {id}")).into());
    }
    if source.access == copal_core::AccessLevel::Grant {
        return Err(CopalError::forbidden("file is grant-only; redeem an issued URL").into());
    }
    if !source.servable_content() {
        return Err(CopalError::conflict("source has no served content").into());
    }
    if state.withholds_pending_scan(&source) {
        return Err(CopalError::conflict("content is awaiting a malware scan").into());
    }
    if !source.content_type.starts_with("image/") {
        return Err(CopalError::validation("renditions require an image source").into());
    }
    let source_digest = source
        .digest
        .clone()
        .expect("servable content carries a digest");
    if source.size_bytes.unwrap_or(0) > crate::pipeline::MAX_DERIVE_SOURCE_BYTES {
        return Err(CopalError::validation("source exceeds the decode ceiling").into());
    }

    // Hold the derived path before rendering so racing callers
    // converge on one record.
    let file_spec = FileSpec {
        path: derived_path.clone(),
        content_type: format!("image/{}", spec.format),
        access: source.access,
        metadata: serde_json::Value::Null,
        idempotency_key: None,
    };
    let derived = match file_repo::create_file(&state.store, &tenant, &file_spec, "derive").await {
        Ok(created) => created.record,
        Err(CopalError::Conflict(_)) => {
            let existing = file_repo::find_by_path(&state.store, &tenant, &derived_path)
                .await?
                .ok_or_else(|| CopalError::conflict("derivation is in flight; retry shortly"))?;
            if existing.servable_content() {
                return serve_rendition(&state, &headers, existing).await;
            }
            return Err(CopalError::conflict("derivation is in flight; retry shortly").into());
        }
        Err(other) => return Err(other.into()),
    };
    file_repo::mark_rendition(
        &state.store,
        &tenant,
        &derived.id,
        &id,
        &spec.kind,
        &params_digest,
    )
    .await?;

    // An inline derive whose source is archive-cold answers 202 and
    // recalls the source; the derived record is refused (retryable)
    // so the next attempt after the recall re-derives.
    let backend = match state.backend_or_recall(&tenant, &source).await? {
        Ok(backend) => backend,
        Err(run) => {
            crate::pipeline::refuse_derived(
                &state.store,
                &tenant,
                &derived.id,
                format!("source is archive-cold; recall run {run} is in flight"),
            )
            .await?;
            return Ok(crate::recall::accepted_response(&run));
        }
    };
    let bytes = backend.read(&source_digest).await?;
    let rendered =
        match crate::pipeline::render_image(&bytes, spec.width, spec.height, &spec.format) {
            Ok(rendered) => rendered,
            Err(reason) => {
                crate::pipeline::refuse_derived(&state.store, &tenant, &derived.id, reason.clone())
                    .await?;
                return Err(CopalError::validation(reason).into());
            }
        };
    let residency = copal_store::repo::tenant::get_residency(&state.store, &tenant).await?;
    let target = state.residencies.get(&residency)?;
    let body = futures::stream::iter(vec![Ok::<_, String>(bytes::Bytes::from(rendered))]);
    let stored = target.put_streamed(body).await?;
    copal_store::repo::blob::record_sighting(
        &state.store,
        &stored.digest,
        stored.size_bytes,
        &residency,
        &stored.storage_path,
    )
    .await?;
    file_repo::claim_upload(&state.store, &tenant, &derived.id, "derive-inline", 900).await?;
    let finished = crate::pipeline::complete_rendition(
        &state.store,
        &tenant,
        &derived.id,
        &residency,
        &stored,
        &source_digest,
        crate::pipeline::content_cleared(&source),
    )
    .await?;
    crate::metrics::incr("copal_renditions_inline_total");
    serve_rendition(&state, &headers, finished).await
}

#[derive(Debug, Deserialize)]
struct FetchRequest {
    url: String,
    path: String,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    access: Option<copal_core::AccessLevel>,
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default)]
    idempotency_key: Option<String>,
    /// Confidentiality markers for the content the server will pull:
    /// a fetch is an upload the server performs on the caller's
    /// behalf, so it carries the declaration the way the content PUT
    /// carries its header.
    #[serde(default)]
    markers: Option<serde_json::Value>,
}

async fn fetch_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(request): Json<FetchRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant =
        crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let spec = FetchSpec {
        url: request.url,
        path: request.path,
        content_type: request.content_type,
        access: request.access,
        metadata: request.metadata,
        idempotency_key: request.idempotency_key,
        markers: request.markers,
    };
    let (status, body) = fetch_core(&state, &tenant, &spec).await?;
    Ok((status, Json(body)))
}

/// What a fetch request asks for, shared by both faces.
#[derive(Debug, Clone)]
pub(crate) struct FetchSpec {
    pub url: String,
    pub path: String,
    pub content_type: Option<String>,
    pub access: Option<copal_core::AccessLevel>,
    pub metadata: serde_json::Value,
    pub idempotency_key: Option<String>,
    pub markers: Option<serde_json::Value>,
}

/// Create the record and enqueue the ingestion, the shared core
/// behind the REST handler and the GraphQL action resolver. The URL
/// is tenant-supplied, so the outbound policy gates it here for a
/// fast refusal and again inside the worker.
pub(crate) async fn fetch_core<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    request: &FetchSpec,
) -> Result<(StatusCode, serde_json::Value), ApiError> {
    if !state.flow.has_workflow(crate::pipeline::FETCH_WORKFLOW) {
        return Err(CopalError::validation("the ingestion pipeline is not configured").into());
    }
    if !(request.url.starts_with("http://") || request.url.starts_with("https://")) {
        return Err(CopalError::validation("url must be http or https").into());
    }
    if request.url.len() > 2048 {
        return Err(CopalError::validation("url exceeds 2048 characters").into());
    }
    if !state.limits.allow_private_fetch_targets {
        crate::netguard::check_outbound_url(&request.url)
            .map_err(|e| CopalError::validation(format!("outbound policy: {e}")))?;
    }
    if let Some(declared) = request.content_type.as_deref() {
        if declared.len() > 127 || !declared.contains('/') || !declared.is_ascii() {
            return Err(
                CopalError::validation("content_type does not look like a media type").into(),
            );
        }
    }
    // Validated against the level this same request declares, before
    // the record exists and long before any byte moves: a widening
    // or malformed declaration costs nothing but this refusal.
    let access = request.access.unwrap_or(copal_core::AccessLevel::Private);
    let markers = match &request.markers {
        None => None,
        Some(raw) => Some(crate::markers::accept_declaration(raw, access)?),
    };

    let file_spec = FileSpec {
        path: request.path.clone(),
        content_type: request
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_owned()),
        access,
        metadata: request.metadata.clone(),
        idempotency_key: request.idempotency_key.clone(),
    };
    let created = file_repo::create_file(&state.store, tenant, &file_spec, "fetch").await?;
    if !created.created {
        // An idempotency replay: the original enqueue stands.
        return Ok((StatusCode::OK, crate::wire::wire_file(&created.record)));
    }
    let record = created.record;
    let url_digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(request.url.as_bytes()))
    };
    let mut input = json!({
        "tenant": tenant.as_str(),
        "file": record.id.as_str(),
        "url": request.url,
        "declared": request.content_type.is_some(),
    });
    // The canonical declaration rides the run input to the fetch
    // worker, which persists it on the version row at completion the
    // way the byte faces do.
    if let Some(declaration) = &markers {
        input["markers"] = declaration.clone();
    }
    let (run_id, _) = state
        .flow
        .enqueue(
            tenant,
            crate::pipeline::FETCH_WORKFLOW,
            RunSpec {
                input,
                subject: Some(record.id.clone()),
                idempotency_key: Some(crate::pipeline::fetch_run_key(
                    &record.id,
                    &url_digest[..16],
                )),
            },
        )
        .await?;
    let mut body = crate::wire::wire_file(&record);
    body["run"] = json!(run_id);
    Ok((StatusCode::ACCEPTED, body))
}

async fn request_rendition<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RenditionRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant =
        crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let id = parse_id(&id)?;
    let spec = RenditionSpec {
        kind: request.kind,
        width: request.width,
        height: request.height,
        format: request.format,
    };
    let (status, body) = request_rendition_core(&state, &tenant, &id, &spec).await?;
    Ok((status, Json(body)))
}

#[derive(Debug, Deserialize)]
struct TransformRequest {
    transformer: String,
    #[serde(default)]
    params: serde_json::Value,
    #[serde(default)]
    content_type: Option<String>,
}

async fn request_transform<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<TransformRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant =
        crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let id = parse_id(&id)?;
    let spec = TransformSpec {
        transformer: request.transformer,
        params: request.params,
        content_type: request
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_owned()),
    };
    let (status, body) = request_transform_core(&state, &tenant, &id, &spec).await?;
    Ok((status, Json(body)))
}

/// What a transform request asks for, shared by both faces.
#[derive(Debug, Clone)]
pub(crate) struct TransformSpec {
    pub transformer: String,
    pub params: serde_json::Value,
    pub content_type: String,
}

/// Create the derived record and enqueue the external transform, the
/// shared core behind the REST handler and the GraphQL action
/// resolver. Returns 202 for a fresh derivation and 200 for one that
/// already exists.
pub(crate) async fn request_transform_core<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    id: &FileId,
    request: &TransformSpec,
) -> Result<(StatusCode, serde_json::Value), ApiError> {
    if !state.flow.has_workflow(crate::pipeline::TRANSFORM_WORKFLOW) {
        return Err(CopalError::validation("the transform pipeline is not configured").into());
    }
    let name = request.transformer.as_str();
    let name_ok = !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'));
    if !name_ok {
        return Err(CopalError::validation(
            "transformer must be 1..=32 characters of letters, digits, hyphen, underscore",
        )
        .into());
    }
    if !state.transformers.contains_key(name) {
        return Err(CopalError::validation(format!("unknown transformer {name:?}")).into());
    }
    let type_ok = request.content_type.len() <= 127
        && request.content_type.contains('/')
        && request.content_type.is_ascii();
    if !type_ok {
        return Err(CopalError::validation("content_type does not look like a media type").into());
    }

    let source = file_repo::get_file(&state.store, tenant, id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    if !source.servable_content() {
        return Err(CopalError::conflict("source has no served content").into());
    }
    let source_digest = source
        .digest
        .clone()
        .expect("servable content carries a digest");

    let params_json = serde_json::to_string(&request.params)
        .map_err(|_| CopalError::validation("params does not serialize"))?;
    let full_digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(params_json.as_bytes()))
    };
    let params_digest = &full_digest[..16];
    let derived_path = format!("{}@{}-{}", source.path, name, &full_digest[..8]);

    let spec = FileSpec {
        path: derived_path.clone(),
        content_type: request.content_type.clone(),
        access: source.access,
        metadata: serde_json::Value::Null,
        idempotency_key: None,
    };
    let derived = match file_repo::create_file(&state.store, tenant, &spec, "transform").await {
        Ok(created) => created.record,
        Err(CopalError::Conflict(_)) => {
            // The path already holds this derivation; return it.
            let existing = file_repo::find_by_path(&state.store, tenant, &derived_path)
                .await?
                .ok_or_else(|| CopalError::conflict("derived path is contended"))?;
            return Ok((StatusCode::OK, crate::wire::wire_file(&existing)));
        }
        Err(other) => return Err(other.into()),
    };
    file_repo::mark_rendition(&state.store, tenant, &derived.id, id, name, params_digest).await?;

    let input = json!({
        "tenant": tenant.as_str(),
        "derived_file": derived.id.as_str(),
        "source_file": id.as_str(),
        "source_residency": source.blob_residency.as_deref().unwrap_or("local"),
        "source_digest": source_digest.as_str(),
        "source_size": source.size_bytes,
        "source_content_type": source.content_type,
        "transformer": name,
        "params": request.params,
    });
    let (run_id, _) = state
        .flow
        .enqueue(
            tenant,
            crate::pipeline::TRANSFORM_WORKFLOW,
            RunSpec {
                input,
                subject: Some(derived.id.clone()),
                idempotency_key: Some(crate::pipeline::transform_run_key(
                    &derived.id,
                    &source_digest,
                    name,
                    params_digest,
                )),
            },
        )
        .await?;
    let mut body = crate::wire::wire_file(&derived);
    body["run"] = json!(run_id);
    Ok((StatusCode::ACCEPTED, body))
}

/// What a rendition request asks for, shared by both faces.
#[derive(Debug, Clone)]
pub(crate) struct RenditionSpec {
    pub kind: String,
    pub width: u32,
    pub height: u32,
    pub format: String,
}

/// Create the derived record and enqueue its render, the shared core
/// behind the REST handler and the GraphQL action resolver. Returns
/// 202 for a fresh rendition and 200 for one that already exists.
pub(crate) async fn request_rendition_core<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    id: &FileId,
    request: &RenditionSpec,
) -> Result<(StatusCode, serde_json::Value), ApiError> {
    if !state.flow.has_workflow(crate::pipeline::DERIVE_WORKFLOW) {
        return Err(CopalError::validation("the derivatives pipeline is not configured").into());
    }
    validate_rendition_spec(request)?;

    let source = file_repo::get_file(&state.store, tenant, id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    if !source.servable_content() {
        return Err(CopalError::conflict("source has no served content").into());
    }
    // A rendition inherits its source's clearance, so a source still
    // awaiting its scan has none to pass on.
    if state.withholds_pending_scan(&source) {
        return Err(CopalError::conflict("content is awaiting a malware scan").into());
    }
    if !source.content_type.starts_with("image/") {
        return Err(CopalError::validation("renditions require an image source").into());
    }
    let source_digest = source
        .digest
        .clone()
        .expect("servable content carries a digest");

    let (params_digest, rendition_path) = rendition_artifacts(&source.path, request);
    let params_digest = params_digest.as_str();

    let spec = FileSpec {
        path: rendition_path.clone(),
        content_type: format!("image/{}", request.format),
        access: source.access,
        metadata: serde_json::Value::Null,
        idempotency_key: None,
    };
    let derived = match file_repo::create_file(&state.store, tenant, &spec, "derive").await {
        Ok(created) => created.record,
        Err(CopalError::Conflict(_)) => {
            // The path already holds this rendition; return it.
            let existing = file_repo::find_by_path(&state.store, tenant, &rendition_path)
                .await?
                .ok_or_else(|| CopalError::conflict("rendition path is contended"))?;
            return Ok((StatusCode::OK, crate::wire::wire_file(&existing)));
        }
        Err(other) => return Err(other.into()),
    };
    file_repo::mark_rendition(
        &state.store,
        tenant,
        &derived.id,
        id,
        &request.kind,
        params_digest,
    )
    .await?;

    let input = json!({
        "tenant": tenant.as_str(),
        "derived_file": derived.id.as_str(),
        "source_file": id.as_str(),
        "source_residency": source.blob_residency.as_deref().unwrap_or("local"),
        "source_digest": source_digest.as_str(),
        "source_size": source.size_bytes,
        "kind": request.kind,
        "width": request.width,
        "height": request.height,
        "format": request.format,
    });
    let (run_id, _) = state
        .flow
        .enqueue(
            tenant,
            crate::pipeline::DERIVE_WORKFLOW,
            RunSpec {
                input,
                subject: Some(derived.id.clone()),
                idempotency_key: Some(crate::pipeline::derive_run_key(
                    &derived.id,
                    &source_digest,
                    params_digest,
                )),
            },
        )
        .await?;
    let mut body = crate::wire::wire_file(&derived);
    body["run"] = json!(run_id);
    Ok((StatusCode::ACCEPTED, body))
}

/// Live renditions of a file, in path order.
async fn list_renditions<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let tenant = &auth.tenant;
    let id = parse_id(&id)?;
    // Tenancy and tombstone filtering ride the file fetch, the way
    // they do for versions. Listing straight from the child table
    // answered an empty page for a parent this caller cannot see,
    // which is the same answer as for a parent with no renditions:
    // it disclosed nothing, and it told a caller nothing either.
    file_repo::get_file(&auth.store, tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    let rows = file_repo::list_renditions(&auth.store, tenant, &id).await?;
    let items: Vec<_> = rows.iter().map(crate::wire::wire_file).collect();
    Ok(Json(json!({ "items": items })))
}

/// Stream bytes in and finish the upload in one request.
///
/// The claim (draft or failed-retry, or stealing an expired lease from
/// a dead uploader) lands before any byte, so a concurrent second
/// uploader loses the CAS instead of interleaving writes. On success
/// the record is ready, digest-verified, and linked to its deduped
/// blob row.
async fn upload_content<B: BlobStore>(
    State(state): State<AppState<B>>,
    Path(id): Path<String>,
    request: Request,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (tenant, identity) = crate::auth::authenticate_scoped_with_identity(
        &state,
        request.headers(),
        crate::auth::Scope::Write,
        1,
    )
    .await?;
    // Attribution: the version records the actor. A principal's
    // handle when one exists; the source label otherwise, so rows
    // from principal-less keys keep reading as they always did.
    let actor = identity
        .as_ref()
        .and_then(|id| id.principal.clone())
        .unwrap_or_else(|| "api".to_owned());
    let id = parse_id(&id)?;
    // Optional integrity assertion: the client declares the digest it
    // intends to send, and a mismatch fails the upload after the
    // bytes are hashed. The mistargeted content is inert (content
    // addressing stores it under its TRUE digest; nothing links it,
    // so GC reclaims it) and the record stays retryable.
    let expected_digest = request
        .headers()
        .get("x-copal-digest")
        .and_then(|v| v.to_str().ok())
        .map(|raw| {
            copal_core::ContentDigest::parse(raw)
                .map_err(|_| CopalError::validation("malformed x-copal-digest header"))
        })
        .transpose()?;

    // Confidentiality markers ride the content call, in a header
    // like the digest and the conditionals before them, because they
    // describe the bytes this request carries and nothing else. A
    // malformed or widening declaration refuses HERE, before any
    // byte moves; the narrowing check reads the file's level, which
    // costs one fetch only when markers are present.
    let declared_markers = match crate::markers::from_header(request.headers())? {
        None => None,
        Some(raw) => {
            let record = file_repo::get_file(&state.store, &tenant, &id)
                .await?
                .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
            Some(crate::markers::accept_declaration(&raw, record.access)?)
        }
    };

    let declared_len = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.parse::<u64>().ok());
    let headroom = state.quota_headroom(&tenant, declared_len).await?;

    let (residency, backend) = state.residency_for(&tenant).await?;

    let precondition =
        crate::s3::write_precondition(request.headers()).map_err(CopalError::validation)?;
    let claim = precondition
        .as_ref()
        .map(crate::s3::WritePrecondition::as_claim)
        .unwrap_or_default();
    if let Err(err) = file_repo::claim_upload_if(
        &state.store,
        &tenant,
        &id,
        &state.instance_id,
        state.limits.upload_lease_secs,
        claim,
    )
    .await
    {
        // A refused conditional claim answers 412 when the condition
        // is what failed; a merely busy key keeps its usual answer.
        if let Some(condition) = &precondition {
            if let Ok(Some(record)) = file_repo::get_file(&state.store, &tenant, &id).await {
                let violated = match condition {
                    crate::s3::WritePrecondition::AbsentContent => record.digest.is_some(),
                    crate::s3::WritePrecondition::DigestIs(digest) => {
                        record.digest.as_ref().map(|d| d.as_str()) != Some(digest.as_str())
                    }
                };
                if violated {
                    crate::metrics::incr("copal_precondition_refusals_total");
                    return Err(CopalError::PreconditionFailed(
                        "the file's current content does not satisfy the write condition"
                            .to_owned(),
                    )
                    .into());
                }
            }
        }
        return Err(err.into());
    }

    // Enforce the ceiling in the stream itself: past the limit the
    // stream yields an error, the writer aborts, and only inert staging
    // garbage remains. The sentinel message is what the error arm
    // below maps to 413.
    let max = match headroom {
        Some(remaining) => (state.limits.max_upload_bytes as u64).min(remaining),
        None => state.limits.max_upload_bytes as u64,
    };
    let mut running_total = 0u64;
    let body = request
        .into_body()
        .into_data_stream()
        .map(move |chunk| match chunk {
            Ok(bytes) => {
                running_total += bytes.len() as u64;
                if running_total > max {
                    Err("length limit exceeded".to_owned())
                } else {
                    Ok(bytes)
                }
            }
            Err(err) => Err(format!("body: {err}")),
        });
    let stored = match backend.put_streamed(body).await {
        Ok(stored) => stored,
        Err(err) => {
            state.abandon_reservation(&tenant, declared_len).await;
            // Leave the record retryable; the claim owner reports the
            // original failure even if the fallback transition fails too.
            let _ = file_repo::transition(
                &state.store,
                &tenant,
                &id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
            // The body-limit layer surfaces as a stream error; report it
            // as 413 rather than an internal failure. The record stays
            // in `failed`, so retrying with a smaller payload is legal.
            let text = err.to_string();
            if text.contains("length limit") {
                return Err(CopalError::PayloadTooLarge(format!(
                    "upload exceeds {} bytes",
                    state.limits.max_upload_bytes,
                ))
                .into());
            }
            return Err(err.into());
        }
    };

    let StoredBlob {
        digest,
        size_bytes,
        storage_path,
    } = stored;

    if let Some(expected) = expected_digest {
        if expected != digest {
            // The record returns to retryable; the stray object sits
            // unlinked until GC collects it.
            let _ = file_repo::transition(
                &state.store,
                &tenant,
                &id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
            return Err(CopalError::validation(format!(
                "digest mismatch: client declared {expected}, content hashed to {digest}",
            ))
            .into());
        }
    }

    state
        .settle_reservation(&tenant, declared_len, size_bytes)
        .await;
    let record = finalize_new_content(
        &state,
        &tenant,
        &id,
        &residency,
        &digest,
        size_bytes,
        &storage_path,
        &actor,
        declared_markers.as_ref(),
    )
    .await?;
    Ok(Json(crate::wire::wire_file(&record)))
}

/// Finish landed content: register the blob, complete the upload CAS,
/// enqueue the pipeline, and resolve dedupe hits. Shared by the single
/// PUT path and resumable-session completion, so both finish
/// identically.
///
/// `markers` is the validated declaration for these bytes, from
/// whichever face carried it (the PUT header, or a tus session's
/// creation metadata); faces that carry none - the S3 gateway,
/// upload-grant redemption - pass `None`, and absence means the
/// file's level, which is today's behavior.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_new_content<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    id: &FileId,
    residency: &str,
    digest: &copal_core::ContentDigest,
    size_bytes: u64,
    storage_path: &str,
    actor: &str,
    markers: Option<&serde_json::Value>,
) -> Result<copal_core::FileRecord, ApiError> {
    crate::metrics::incr("copal_uploads_completed_total");
    crate::metrics::add("copal_uploaded_bytes_total", size_bytes);
    blob_repo::record_sighting(&state.store, digest, size_bytes, residency, storage_path).await?;

    // One CAS finishes the upload and mints the version number; the
    // frozen version row and current_version link follow inside. With a
    // processing pipeline configured, completion lands in scanning and
    // the pipeline's finalize step performs the ready/quarantine call.
    let pipelined = state.flow.has_workflow(crate::pipeline::UPLOAD_WORKFLOW);
    let final_state = if pipelined {
        FileState::Scanning
    } else {
        FileState::Ready
    };
    let record = completion_repo::complete_upload(
        &state.store,
        tenant,
        id,
        residency,
        digest,
        size_bytes,
        actor,
        final_state,
        markers,
    )
    .await?;

    let mut record = record;
    if pipelined {
        // Deterministic idempotency key: a crashed or repeated enqueue
        // dedupes instead of double-processing this content.
        let input = json!({
            "tenant": tenant.as_str(),
            "file": id.as_str(),
            "residency": residency,
            "digest": record.digest.as_ref().map(|d| d.as_str()),
            "declared_type": record.content_type,
            "path": record.path,
        });
        let (run_id, created) = state
            .flow
            .enqueue(
                tenant,
                crate::pipeline::UPLOAD_WORKFLOW,
                RunSpec {
                    input,
                    subject: Some(id.clone()),
                    idempotency_key: Some(crate::pipeline::upload_run_key(id, digest)),
                },
            )
            .await?;
        if !created {
            // A dedupe hit means this exact content was processed (or is
            // being processed) for this file before. A COMPLETED run will
            // never finalize the fresh scan, so resolve it here: same file,
            // same path, same bytes means the recorded verdict stands and
            // the annotations are already on the metadata. (A quarantined
            // verdict cannot reach this path: quarantined files refuse
            // upload claims.) A FAILED run gets retried so a worker
            // finishes the scan.
            match flow_repo::get_run(&state.store, tenant, &run_id).await? {
                Some(run) if run.status == "completed" => {
                    match file_repo::transition(
                        &state.store,
                        tenant,
                        id,
                        FileState::Scanning,
                        FileState::Ready,
                        Default::default(),
                    )
                    .await
                    {
                        Ok(updated) => record = updated,
                        Err(CopalError::Conflict(_)) => {}
                        Err(other) => return Err(other.into()),
                    }
                }
                Some(run) if run.status == "failed" => {
                    let _ = flow_repo::retry_failed(&state.store, &run_id).await?;
                }
                _ => {}
            }
        }
    }
    Ok(record)
}

/// Serve `/content` under the file's ACCESS LEVEL, the enforcement
/// point of the access model:
///
/// - `public`: anonymous, and cacheable hard (immutable by digest).
/// - `private` / `tenant`: the owning tenant, `no-store`. (The two
///   levels coincide until principals-within-a-tenant exist; keys ARE
///   tenant identities today.)
/// - `grant`: bytes flow ONLY through issued URLs; direct download
///   refuses even for the owner, because "grant" means every access
///   is an auditable, revocable capability.
async fn download_content<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id)?;
    // The public path cannot know a tenant before reading the row, so
    // the row is fetched unscoped and non-public falls back to the
    // authenticated, tenant-checked path.
    let record = file_repo::get_file_any(&state.store, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;

    let cache = match record.access {
        copal_core::AccessLevel::Public => crate::serve::CacheClass::Public,
        access => {
            let tenant =
                crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Read, 1)
                    .await?;
            if record.tenant_id != tenant {
                // Cross-tenant reads as absent, never as forbidden.
                return Err(CopalError::not_found(format!("file {id}")).into());
            }
            if access == copal_core::AccessLevel::Grant {
                return Err(
                    CopalError::forbidden("file is grant-only; redeem an issued URL").into(),
                );
            }
            crate::serve::CacheClass::Private
        }
    };

    // Digest-based servability: a re-upload in flight (or failed) keeps
    // the previous version serving; only quarantine and never-uploaded
    // block.
    if !record.servable_content() {
        return Err(CopalError::conflict(format!(
            "file is {} with no servable content",
            record.state.as_str(),
        ))
        .into());
    }
    if state.withholds_pending_scan(&record) {
        return Err(CopalError::conflict("content is awaiting a malware scan").into());
    }
    let digest = record
        .digest
        .as_ref()
        .ok_or_else(|| CopalError::Store("servable file without digest".into()))?;
    let backend = match state.backend_or_recall(&record.tenant_id, &record).await? {
        Ok(backend) => backend,
        Err(run) => return Ok(crate::recall::accepted_response(&run)),
    };
    crate::tiering::note_blob_read(
        &state.store,
        record.blob_residency.as_deref().unwrap_or("local"),
        digest,
    );
    crate::serve::serve_blob(
        &backend,
        &headers,
        crate::serve::ServeSpec {
            content_type: &record.content_type,
            digest,
            path: &record.path,
            cache,
        },
    )
    .await
}

/// Request body for grant issuance.
#[derive(Debug, Deserialize)]
struct IssueGrantRequest {
    /// Seconds until the URL stops working. Bounded to a year.
    #[serde(default = "default_grant_ttl")]
    ttl_secs: u32,
    /// Cap on redemptions; one-time links use 1. Unset = unlimited
    /// within the TTL.
    #[serde(default)]
    max_uses: Option<u32>,
}

fn default_grant_ttl() -> u32 {
    900
}

/// Upload-URL request body.
#[derive(Debug, Deserialize)]
struct IssueUploadRequest {
    #[serde(default = "default_upload_ttl")]
    ttl_secs: u32,
}

fn default_upload_ttl() -> u32 {
    900
}

/// Issue a write capability for a file awaiting content, so a browser
/// can upload straight to Copal without holding a tenant key.
///
/// The record must exist and be claimable (`draft`, or `failed`
/// retryable, or `ready` for a new version): the caller's backend
/// creates it, decides the path and access level, and hands the
/// browser only this URL. Upload grants are single-use by
/// construction; a retry needs a fresh URL, which is the property
/// that makes handing one to an untrusted client safe.
async fn issue_upload_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<IssueUploadRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let tenant = &auth.tenant;
    let id = parse_id(&id)?;
    let body = issue_upload_grant_core(
        &auth.store,
        tenant,
        &id,
        request.ttl_secs,
        forwarded_origin(&headers).as_deref(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(body)))
}

/// Mint a write capability, the shared core behind the REST handler
/// and the GraphQL action resolver.
pub(crate) async fn issue_upload_grant_core(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    ttl_secs: u32,
    origin: Option<&str>,
) -> Result<serde_json::Value, ApiError> {
    if ttl_secs == 0 || ttl_secs > 86_400 {
        return Err(CopalError::validation(
            "ttl_secs must be between 1 and 86400: an upload URL is a write capability",
        )
        .into());
    }
    let record = file_repo::get_file(store, tenant, id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    if matches!(record.state, FileState::Quarantined | FileState::Deleted) {
        return Err(CopalError::conflict(format!(
            "file is {} and cannot accept content",
            record.state.as_str(),
        ))
        .into());
    }

    let token = GrantToken::mint();
    let grant = grant_repo::issue(
        store,
        tenant,
        id,
        &token.grant_id,
        &token.secret_hash(),
        &grant_repo::GrantSpec {
            ttl_secs,
            max_uses: Some(1),
            created_by: "api".to_owned(),
            op: "put".to_owned(),
        },
    )
    .await?;
    copal_store::repo::auth::record_audit(
        store,
        tenant,
        tenant.as_str(),
        "grant.upload_issued",
        &token.grant_id,
        origin,
        Some(json!({ "file": id.as_str(), "ttl_secs": ttl_secs })),
    )
    .await?;
    Ok(json!({
        "grant_id": token.grant_id,
        "token": token.encode(),
        "url": format!("/v1/grants/{}", token.encode()),
        "expires_at": grant.expires_at,
    }))
}

/// Redeem a write capability: stream bytes into the granted file.
///
/// Refusals are the same uniform 404 the read path uses, and the use
/// is consumed BEFORE the bytes land, so a token cannot be replayed
/// while its first upload is still in flight. A failed upload leaves
/// the record retryable through a freshly issued URL.
async fn redeem_upload_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    Path(grant_ref): Path<String>,
    request: Request,
) -> Result<Json<serde_json::Value>, ApiError> {
    // A grant is the authority here; no key, no principal. The
    // version records the source until grants carry actors.
    let actor = "grant".to_owned();
    let refused = || CopalError::not_found("unknown or unusable grant");

    let token = GrantToken::parse(&grant_ref).map_err(|_| refused())?;
    let grant = grant_repo::fetch(&state.store, &token.grant_id)
        .await?
        .ok_or_else(refused)?;
    if !copal_sign::verify_secret(&token.secret, &grant.secret_hash) || grant.op != "put" {
        return Err(refused().into());
    }
    let tenant = TenantId::parse(&grant.tenant_id).map_err(|_| refused())?;
    let id = grant.file_id().map_err(|_| refused())?;
    let record = file_repo::get_file(&state.store, &tenant, &id)
        .await?
        .ok_or_else(refused)?;
    if matches!(record.state, FileState::Quarantined | FileState::Deleted) {
        return Err(refused().into());
    }

    let declared_len = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.parse::<u64>().ok());
    let headroom = state.quota_headroom(&tenant, declared_len).await?;
    let (residency, backend) = state.residency_for(&tenant).await?;

    file_repo::claim_upload(
        &state.store,
        &tenant,
        &id,
        &state.instance_id,
        state.limits.upload_lease_secs,
    )
    .await?;

    // The capability burns here, before any byte lands: a replay while
    // the first upload streams must not also be authorized.
    if !grant_repo::consume(&state.store, &token.grant_id).await? {
        return Err(refused().into());
    }

    let max = match headroom {
        Some(remaining) => (state.limits.max_upload_bytes as u64).min(remaining),
        None => state.limits.max_upload_bytes as u64,
    };
    let mut running_total = 0u64;
    let body = request
        .into_body()
        .into_data_stream()
        .map(move |chunk| match chunk {
            Ok(bytes) => {
                running_total += bytes.len() as u64;
                if running_total > max {
                    Err("length limit exceeded".to_owned())
                } else {
                    Ok(bytes)
                }
            }
            Err(err) => Err(format!("body: {err}")),
        });
    let stored = match backend.put_streamed(body).await {
        Ok(stored) => stored,
        Err(err) => {
            state.abandon_reservation(&tenant, declared_len).await;
            let _ = file_repo::transition(
                &state.store,
                &tenant,
                &id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
            let text = err.to_string();
            if text.contains("length limit") {
                return Err(CopalError::PayloadTooLarge(format!(
                    "upload exceeds {} bytes",
                    state.limits.max_upload_bytes,
                ))
                .into());
            }
            return Err(err.into());
        }
    };
    let StoredBlob {
        digest,
        size_bytes,
        storage_path,
    } = stored;
    state
        .settle_reservation(&tenant, declared_len, size_bytes)
        .await;
    // Upload grants carry no markers: the grant names a record, not
    // content semantics, and the doc of record lists exactly three
    // marker-bearing calls. Absence means the file's level.
    let record = finalize_new_content(
        &state,
        &tenant,
        &id,
        &residency,
        &digest,
        size_bytes,
        &storage_path,
        &actor,
        None,
    )
    .await?;
    Ok(Json(crate::wire::wire_file(&record)))
}

/// Issue a signed URL for a servable file, the shared core behind the
/// REST handler and the GraphQL action resolver.
///
/// The response's `url` is relative -- the deployment's public base is
/// the proxy's business. The token appears exactly once, here; the
/// store keeps only its hash.
pub(crate) async fn issue_grant_core<B: BlobStore>(
    store: &Store,
    state: &AppState<B>,
    tenant: &TenantId,
    id: &FileId,
    ttl_secs: u32,
    max_uses: Option<u32>,
    origin: Option<&str>,
) -> Result<serde_json::Value, ApiError> {
    if ttl_secs == 0 || ttl_secs > 31_536_000 {
        return Err(CopalError::validation("ttl_secs must be between 1 and 31536000").into());
    }
    if max_uses == Some(0) {
        return Err(CopalError::validation("max_uses must be at least 1").into());
    }

    // Only a servable file gets a URL; a draft link would 404 until
    // upload anyway, and issuing it would leak lifecycle state.
    let record = file_repo::get_file(store, tenant, id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    if !record.servable_content() {
        return Err(CopalError::conflict(format!(
            "file is {} with no servable content",
            record.state.as_str(),
        ))
        .into());
    }
    if state.withholds_pending_scan(&record) {
        return Err(CopalError::conflict("content is awaiting a malware scan").into());
    }

    let token = GrantToken::mint();
    let grant = grant_repo::issue(
        store,
        tenant,
        id,
        &token.grant_id,
        &token.secret_hash(),
        &grant_repo::GrantSpec {
            ttl_secs,
            max_uses,
            created_by: "api".to_owned(),
            op: "get".to_owned(),
        },
    )
    .await?;
    copal_store::repo::auth::record_audit(
        store,
        tenant,
        tenant.as_str(),
        "grant.issued",
        &token.grant_id,
        origin,
        Some(json!({ "file": id.as_str(), "ttl_secs": ttl_secs, "max_uses": max_uses })),
    )
    .await?;

    Ok(json!({
        "grant_id": token.grant_id,
        "token": token.encode(),
        "url": format!("/v1/grants/{}", token.encode()),
        "expires_at": grant.expires_at,
        "max_uses": grant.max_uses,
    }))
}

async fn issue_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<IssueGrantRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let auth = crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let tenant = &auth.tenant;
    let id = parse_id(&id)?;
    let issued = issue_grant_core(
        &auth.store,
        &state,
        tenant,
        &id,
        request.ttl_secs,
        request.max_uses,
        forwarded_origin(&headers).as_deref(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(issued)))
}

/// Redeem a grant token: serve the file with no tenant header.
///
/// Every failure -- malformed token, unknown grant, wrong secret,
/// revoked, expired, exhausted, file gone -- is the same 404. A signed
/// URL must not be an oracle for any of those distinctions.
async fn redeem_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(grant_ref): Path<String>,
) -> Result<Response, ApiError> {
    let refused = || CopalError::not_found("unknown or unusable grant");

    let token = GrantToken::parse(&grant_ref).map_err(|_| refused())?;
    let grant = grant_repo::fetch(&state.store, &token.grant_id)
        .await?
        .ok_or_else(refused)?;
    if !copal_sign::verify_secret(&token.secret, &grant.secret_hash) {
        return Err(refused().into());
    }

    // Every precondition runs BEFORE the use is consumed: a token whose
    // file was since deleted or quarantined must refuse without burning
    // a remaining use, and a 304 revalidation is not a new read. A
    // Range request IS a read and does consume; media seeking against
    // counted grants should use TTL-only grants (max_uses unset).
    // A capability authorizes ONE operation: an upload token cannot
    // read, and a download token cannot write.
    if grant.op != "get" {
        return Err(refused().into());
    }
    let tenant = TenantId::parse(&grant.tenant_id).map_err(|_| refused())?;
    let file_id = grant.file_id().map_err(|_| refused())?;
    let record = file_repo::get_file(&state.store, &tenant, &file_id)
        .await?
        .ok_or_else(refused)?;
    // Content that replaced the file after issuance waits for its own
    // scan, like every other read, and the wait burns no use.
    if !record.servable_content() || state.withholds_pending_scan(&record) {
        return Err(refused().into());
    }
    let digest = record.digest.as_ref().ok_or_else(refused)?;
    // Grant bytes are `no-store`: a shared cache retaining a one-time
    // grant's body would outlive the grant itself.
    let spec = crate::serve::ServeSpec {
        content_type: &record.content_type,
        digest,
        path: &record.path,
        cache: crate::serve::CacheClass::Private,
    };
    let etag = format!("\"{digest}\"");
    if crate::serve::if_none_match_hits(&headers, &etag) {
        // The revalidation gate re-checks the grant's guards (revoked,
        // expired, exhausted) without consuming: a dead grant must not
        // keep refreshing a cache it no longer authorizes.
        if !grant_repo::redeemable(&state.store, &token.grant_id).await? {
            return Err(refused().into());
        }
        return Ok(crate::serve::not_modified_response(&spec));
    }

    // Resolve BEFORE consuming: archive-cold content answers 202
    // without burning a use -- no byte was read, and a counted grant
    // must survive until the recall lands. A TTL that expires
    // mid-recall re-issues; the operator docs say so.
    let backend = match state.backend_or_recall(&tenant, &record).await? {
        Ok(backend) => backend,
        Err(run) => return Ok(crate::recall::accepted_response(&run)),
    };
    // Guards live in the UPDATE: two racing redemptions of a one-use
    // grant serialize here, and this stays the single authorization
    // point for actually reading bytes.
    if !grant_repo::consume(&state.store, &token.grant_id).await? {
        return Err(refused().into());
    }
    crate::tiering::note_blob_read(
        &state.store,
        record.blob_residency.as_deref().unwrap_or("local"),
        spec.digest,
    );
    crate::serve::serve_blob(&backend, &headers, spec).await
}

/// Revoke a grant by id. Tenant-authenticated; the bearer token is not
/// required -- losing the token is exactly when revocation matters.
async fn revoke_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(grant_ref): Path<String>,
) -> Result<StatusCode, ApiError> {
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let tenant = &auth.tenant;
    grant_repo::revoke(&auth.store, tenant, &grant_ref).await?;
    copal_store::repo::auth::record_audit(
        &auth.store,
        tenant,
        tenant.as_str(),
        "grant.revoked",
        &grant_ref,
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Version-listing query parameters: bounded pages, keyset on the
/// monotone version number.
#[derive(Debug, Deserialize)]
struct VersionListQuery {
    #[serde(default)]
    limit: Option<i64>,
    /// Opaque cursor from a previous page's next_cursor, matching the
    /// page envelope every other listing uses.
    #[serde(default)]
    cursor: Option<String>,
}

/// List a file's version history, newest first, paginated.
async fn list_versions<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<VersionListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 100);
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, limit as u64)
            .await?;
    let id = parse_id(&id)?;
    // Tenancy and tombstone filtering ride the file fetch.
    file_repo::get_file(&auth.store, &auth.tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    let (mut items, next_cursor) = list_versions_page(
        &auth.store,
        &auth.tenant,
        &id,
        limit,
        params.cursor.as_deref(),
    )
    .await?;
    // The same declarations the dispatcher projects on the GraphQL
    // face, evaluated through the shared API, so the two faces redact
    // identically instead of drifting apart.
    let mut ctx = kayak::runtime::KayakContext::new();
    if let Some(key) = auth.identity {
        // Guards compare actors: a key under a principal answers as
        // its handle, and the key id stays in the identity for
        // audit's "using key" half.
        let subject = key.principal.clone().unwrap_or_else(|| key.key_id.clone());
        ctx.insert(kayak::runtime::Principal::new(subject, key.scopes));
    }
    let guarded = kayak::runtime::guarded_fields(
        &crate::contract::contract(),
        "files",
        Some("versions"),
        &crate::contract::guards(),
    );
    for row in &mut items {
        kayak::runtime::strip_guarded(row, &guarded, &ctx);
    }
    Ok(Json(json!({
        "items": items,
        "next_cursor": next_cursor,
    })))
}

/// One page of the tenant change feed in wire shape, plus the
/// cursor that resumes it. Shared by the REST handler and the
/// GraphQL list resolver, so a down indexer resumes from either face
/// and the rows read identically.
pub(crate) async fn events_page(
    store: &copal_store::Store,
    tenant: &TenantId,
    action: Option<&str>,
    limit: i64,
    cursor: Option<&str>,
    ascending: bool,
) -> Result<(Vec<serde_json::Value>, Option<String>), ApiError> {
    let rows = copal_store::repo::eventing::list_events_page(
        store, tenant, action, limit, cursor, ascending,
    )
    .await?;
    let next_cursor = if rows.len() as i64 == limit {
        rows.last().map(copal_store::repo::eventing::event_cursor)
    } else {
        None
    };
    Ok((
        rows.iter().map(crate::wire::wire_event).collect(),
        next_cursor,
    ))
}

/// The change feed on the REST face: replay forward from a cursor
/// with `order=asc`, or page backward through history with the
/// default newest-first order.
async fn list_tenant_events<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<EventFeedQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 100);
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, limit as u64)
            .await?;
    crate::metrics::incr("copal_feed_reads_total");
    let ascending = match params.order.as_deref() {
        None | Some("desc") => false,
        Some("asc") => true,
        Some(other) => {
            return Err(CopalError::validation(
                format!("order must be asc or desc, got {other:?}",),
            )
            .into())
        }
    };
    let (items, next_cursor) = events_page(
        &auth.store,
        &auth.tenant,
        params.action.as_deref(),
        limit,
        params.cursor.as_deref(),
        ascending,
    )
    .await?;
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(serde::Deserialize)]
struct EventFeedQuery {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    order: Option<String>,
}

/// One event by id.
async fn get_tenant_event<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let row = copal_store::repo::eventing::get_event(&auth.store, &auth.tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("event {id}")))?;
    Ok(Json(crate::wire::wire_event(&row)))
}

/// One page of a file's history in wire shape, plus the cursor that
/// resumes it. Shared by the REST handler and the GraphQL
/// sub-collection so the two cannot render a version differently.
pub(crate) async fn list_versions_page(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    limit: i64,
    cursor: Option<&str>,
) -> Result<(Vec<serde_json::Value>, Option<String>), ApiError> {
    // The cursor is the last version number of the previous page; the
    // keyset resumes strictly below it.
    let before = match cursor {
        Some(raw) => Some(
            raw.parse::<u64>()
                .map_err(|_| CopalError::validation("malformed cursor"))?,
        ),
        None => None,
    };
    let versions = version_repo::list_versions(store, tenant, id, limit, before).await?;
    let next_cursor = if versions.len() as i64 == limit {
        versions.last().map(|v| v.number.to_string())
    } else {
        None
    };
    Ok((
        versions.iter().map(crate::wire::wire_version).collect(),
        next_cursor,
    ))
}

/// Whether a version's bytes were cleared by a scan. The current
/// content answers through the record. An older version answers
/// through the snapshot its successor took at completion: that
/// snapshot is the metadata the older bytes left behind, processing
/// verdict included, and a version replaced before its scan finished
/// left no verdict naming its digest.
async fn version_cleared<B: BlobStore>(
    state: &AppState<B>,
    store: &Store,
    tenant: &TenantId,
    record: &copal_core::FileRecord,
    version: &copal_core::FileVersion,
) -> Result<bool, ApiError> {
    if record.digest.as_ref() == Some(&version.digest) {
        return Ok(!state.withholds_pending_scan(record));
    }
    let successor =
        version_repo::get_version(store, tenant, &record.id, version.number + 1).await?;
    Ok(successor.is_some_and(|next| {
        next.metadata_snapshot
            .get("processing")
            .and_then(|p| p.get("scanned_digest"))
            .and_then(|v| v.as_str())
            == Some(version.digest.as_str())
    }))
}

/// Serve one historical version's bytes.
async fn download_version<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((id, number)): Path<(String, u64)>,
) -> Result<Response, ApiError> {
    let auth = crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let tenant = &auth.tenant;
    let id = parse_id(&id)?;
    let record = file_repo::get_file(&auth.store, tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    // Quarantine blocks the whole record, history included.
    if record.state == FileState::Quarantined {
        return Err(CopalError::conflict("file is quarantined").into());
    }
    let version = version_repo::get_version(&auth.store, tenant, &id, number)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("version {number} of file {id}")))?;
    // Grant-only files serve bytes exclusively through issued URLs,
    // history included.
    if record.access == copal_core::AccessLevel::Grant {
        return Err(CopalError::forbidden("file is grant-only; redeem an issued URL").into());
    }
    // A failed CURRENT version is unscanned content; its bytes do not
    // serve. Historical versions (different digest) passed their own
    // pipelines and keep serving.
    if record.state == FileState::Failed && record.digest.as_ref() == Some(&version.digest) {
        return Err(CopalError::conflict(
            "this version's content failed processing and is not servable",
        )
        .into());
    }
    if state.scan_gates_serving
        && !version_cleared(&state, &auth.store, tenant, &record, &version).await?
    {
        return Err(CopalError::conflict("content is awaiting a malware scan").into());
    }
    let backend = match state
        .residencies
        .resolve_content(
            &state.store,
            &state.tiering,
            &version.blob_residency,
            &version.digest,
        )
        .await?
    {
        ContentResolution::Ready(backend) => backend,
        ContentResolution::ArchiveCold { tier } => {
            let (run, _) = crate::recall::enqueue(
                &state.store,
                tenant,
                &version.blob_residency,
                &tier,
                &version.digest,
            )
            .await?;
            return Ok(crate::recall::accepted_response(&run));
        }
    };
    crate::tiering::note_blob_read(&state.store, &version.blob_residency, &version.digest);
    crate::serve::serve_blob(
        &backend,
        &headers,
        crate::serve::ServeSpec {
            content_type: &version.content_type,
            digest: &version.digest,
            path: &record.path,
            cache: crate::serve::CacheClass::Private,
        },
    )
    .await
}

/// Request body for starting a run.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct StartRunRequest {
    pub(crate) workflow: String,
    #[serde(default)]
    pub(crate) input: serde_json::Value,
    #[serde(default)]
    pub(crate) file: Option<String>,
    #[serde(default)]
    pub(crate) idempotency_key: Option<String>,
    /// "async" (default) enqueues for a worker; "sync" executes
    /// in-request over the same journal.
    #[serde(default)]
    pub(crate) mode: Option<String>,
}

/// Start a workflow run, the shared core behind the REST handler and
/// the GraphQL action resolver.
pub(crate) async fn start_run_core<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    request: StartRunRequest,
) -> Result<(StatusCode, serde_json::Value), ApiError> {
    let subject = match &request.file {
        Some(raw) => Some(FileId::parse(raw)?),
        None => None,
    };
    let spec = RunSpec {
        input: request.input,
        subject,
        idempotency_key: request.idempotency_key,
    };
    match request.mode.as_deref() {
        Some("sync") => {
            let (run_id, output) = state.flow.run_sync(tenant, &request.workflow, spec).await?;
            match output {
                Some(output) => Ok((
                    StatusCode::OK,
                    json!({ "run_id": run_id, "output": output, "status": "completed" }),
                )),
                // A worker raced the claim: pending, poll the run,
                // NOT a completed run with null output.
                None => Ok((
                    StatusCode::ACCEPTED,
                    json!({ "run_id": run_id, "status": "pending" }),
                )),
            }
        }
        None | Some("async") => {
            let (run_id, created) = state.flow.enqueue(tenant, &request.workflow, spec).await?;
            let status = if created {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            Ok((status, json!({ "run_id": run_id })))
        }
        Some(other) => Err(CopalError::validation(format!("unknown mode {other:?}")).into()),
    }
}

/// Start a workflow run.
async fn start_run<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(request): Json<StartRunRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant =
        crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let (status, body) = start_run_core(&state, &tenant, request).await?;
    Ok((status, Json(body)))
}

/// Retry a failed run, the shared core behind the REST handler and
/// the GraphQL action resolver.
///
/// Order matters: the subject file (when failed) flips back to
/// `scanning` BEFORE the run CAS, or a claimed worker could finalize
/// against a still-failed file and read the lost CAS as an idempotent
/// no-op. A crash between the two writes self-heals: the stale-scan
/// sweep fails the file again. The retried run gets one fresh attempt
/// per remaining step (journaled attempts still count toward the
/// ceiling).
pub(crate) async fn retry_run_core<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    run_id: &str,
) -> Result<serde_json::Value, ApiError> {
    let run = flow_repo::get_run(&state.store, tenant, run_id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("run {run_id}")))?;
    if run.status != "failed" {
        return Err(CopalError::conflict(format!(
            "only failed runs can be retried; run is {}",
            run.status,
        ))
        .into());
    }

    if let Some(parsed) = run.file_id() {
        let file_id = parsed?;
        if let Some(record) = file_repo::get_file(&state.store, tenant, &file_id).await? {
            if record.state == FileState::Failed {
                // A run for content that was since re-uploaded must not
                // resurrect: its finalize would stamp stale annotations.
                let run_digest = run.input.get("digest").and_then(|v| v.as_str());
                let file_digest = record.digest.as_ref().map(|d| d.as_str());
                if let (Some(expected), Some(actual)) = (run_digest, file_digest) {
                    if expected != actual {
                        return Err(CopalError::conflict(
                            "run was superseded by a newer upload; re-upload instead",
                        )
                        .into());
                    }
                }
                match file_repo::transition(
                    &state.store,
                    tenant,
                    &file_id,
                    FileState::Failed,
                    FileState::Scanning,
                    Default::default(),
                )
                .await
                {
                    Ok(_) | Err(CopalError::Conflict(_)) => {}
                    Err(other) => return Err(other.into()),
                }
            }
        }
    }

    if !flow_repo::retry_failed(&state.store, run_id).await? {
        return Err(CopalError::conflict(format!(
            "run {run_id} is no longer failed (a racing retry or worker moved it)",
        ))
        .into());
    }
    Ok(json!({ "run_id": run_id, "status": "pending" }))
}

/// Retry a failed run.
async fn retry_run<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant =
        crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Write, 1).await?;
    let body = retry_run_core(&state, &tenant, &id).await?;
    Ok((StatusCode::ACCEPTED, Json(body)))
}

/// Run-listing query parameters, matching the generated document.
#[derive(Debug, Deserialize)]
struct RunListQuery {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    sort: Option<String>,
}

pub(crate) fn encode_run_cursor(position: &flow_repo::RunListPosition, ascending: bool) -> String {
    let dir = if ascending { 'a' } else { 'd' };
    hex::encode(format!("{dir}|{}|{}", position.created_at, position.id))
}

pub(crate) fn decode_run_cursor(
    raw: &str,
    ascending: bool,
) -> Result<flow_repo::RunListPosition, ApiError> {
    let invalid = || CopalError::validation("malformed cursor");
    let bytes = hex::decode(raw).map_err(|_| invalid())?;
    let text = String::from_utf8(bytes).map_err(|_| invalid())?;
    let mut parts = text.splitn(3, '|');
    let (Some(dir), Some(created_at), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(invalid().into());
    };
    let cursor_ascending = match dir {
        "a" => true,
        "d" => false,
        _ => return Err(invalid().into()),
    };
    if cursor_ascending != ascending {
        return Err(CopalError::validation("cursor was issued for a different sort order").into());
    }
    Ok(flow_repo::RunListPosition {
        created_at: created_at.to_owned(),
        id: id.to_owned(),
    })
}

const RUN_STATUSES: &[&str] = &["pending", "running", "completed", "failed", "cancelled"];

/// List a tenant's runs.
async fn list_runs<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<RunListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let limit = params.limit.unwrap_or(100).clamp(1, 100);
    let auth =
        crate::auth::authorize_scoped(&state, &headers, crate::auth::Scope::Read, limit as u64)
            .await?;
    let tenant = &auth.tenant;
    if let Some(status) = params.status.as_deref() {
        if !RUN_STATUSES.contains(&status) {
            return Err(CopalError::validation(format!("unknown status {status:?}")).into());
        }
    }
    let ascending = match params.sort.as_deref() {
        None | Some("-created_at") => false,
        Some("created_at") => true,
        Some(other) => {
            return Err(CopalError::validation(format!("unknown sort {other:?}")).into());
        }
    };
    let after = params
        .cursor
        .as_deref()
        .map(|raw| decode_run_cursor(raw, ascending))
        .transpose()?;
    let runs = flow_repo::list_runs(
        &auth.store,
        tenant,
        limit,
        after.as_ref(),
        ascending,
        params.status.as_deref(),
    )
    .await?;
    let next_cursor = if runs.len() as i64 == limit {
        runs.last().map(|last| {
            encode_run_cursor(
                &flow_repo::RunListPosition {
                    created_at: last.created_at.clone(),
                    id: last.run_id(),
                },
                ascending,
            )
        })
    } else {
        None
    };
    let items: Vec<serde_json::Value> = runs.iter().map(crate::wire::wire_run).collect();
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

/// Run status plus its journal. The run itself renders through the
/// contract wire mapper; the step journal is REST-only detail layered
/// on top (kept as `run_id` alongside `id` for compatibility).
async fn get_run<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant =
        crate::auth::authenticate_scoped(&state, &headers, crate::auth::Scope::Read, 1).await?;
    let (run, steps) = state
        .flow
        .run_state(&tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("run {id}")))?;
    let steps: Vec<serde_json::Value> = steps
        .into_iter()
        .map(|s| {
            json!({
                "step": s.step_key,
                "attempt": s.attempt,
                "status": s.status,
                "error": s.step_error,
                "started_at": s.started_at,
                "ended_at": s.ended_at,
            })
        })
        .collect();
    let mut body = crate::wire::wire_run(&run);
    let map = body.as_object_mut().expect("wire_run is an object");
    map.insert("run_id".into(), json!(run.run_id()));
    map.insert("steps".into(), json!(steps));
    Ok(Json(body))
}
