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
    blob as blob_repo, file as file_repo, flow as flow_repo, grant as grant_repo,
    version as version_repo,
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
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_upload_bytes: 1 << 30,
            upload_lease_secs: 900,
        }
    }
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState<B: BlobStore> {
    pub store: Store,
    pub blobs: B,
    pub limits: Limits,
    /// Durable execution over the same store; empty registry by
    /// default -- unknown workflows are refused at the door.
    pub flow: FlowEngine,
    /// Identifies this process as an upload-claim owner, so stale
    /// leases name the instance that died holding them.
    pub instance_id: String,
    /// How requests prove their tenant, plus the admin gate.
    pub auth: crate::auth::AuthConfig,
}

impl<B: BlobStore> AppState<B> {
    /// State with default limits, an empty flow registry, a fresh
    /// instance id, and trusted-header auth (the development default).
    pub fn new(store: Store, blobs: B) -> Self {
        Self {
            flow: FlowEngine::new(store.clone(), FlowRegistry::new()),
            store,
            blobs,
            limits: Limits::default(),
            instance_id: ulid::Ulid::new().to_string().to_ascii_lowercase(),
            auth: crate::auth::AuthConfig::default(),
        }
    }

    /// Install a populated activity/workflow registry.
    pub fn with_flow(mut self, registry: FlowRegistry) -> Self {
        self.flow = FlowEngine::new(self.store.clone(), registry);
        self
    }

    /// Install authentication configuration.
    pub fn with_auth(mut self, auth: crate::auth::AuthConfig) -> Self {
        self.auth = auth;
        self
    }
}

/// Build the router over any blob backend.
///
/// The upload ceiling is enforced inside the upload handler's stream —
/// `DefaultBodyLimit` guards extractor-based bodies only, and the
/// upload path consumes the raw request stream. JSON routes keep axum's
/// small built-in default limit.
pub fn build_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/files", post(create_file::<B>).get(list_files::<B>))
        .route(
            "/v1/files/{id}",
            get(get_file::<B>).delete(delete_file::<B>),
        )
        .route(
            "/v1/files/{id}/content",
            put(upload_content::<B>).get(download_content::<B>),
        )
        .route("/v1/runs", post(start_run::<B>).get(list_runs::<B>))
        .route("/v1/runs/{id}", get(get_run::<B>))
        .route("/v1/runs/{id}/retry", post(retry_run::<B>))
        .route("/v1/files/{id}/versions", get(list_versions::<B>))
        .route(
            "/v1/files/{id}/versions/{number}/content",
            get(download_version::<B>),
        )
        .route("/v1/files/{id}/url", post(issue_grant::<B>))
        // One pattern, two readings: GET takes the full bearer token,
        // DELETE takes the bare grant id (with tenant auth).
        .route(
            "/v1/grants/{grant_ref}",
            get(redeem_grant::<B>).delete(revoke_grant::<B>),
        )
        // Operator surface: key custody. Guarded by the admin token;
        // disabled entirely when none is configured.
        .route(
            "/v1/admin/tenants/{tenant}/keys",
            post(mint_key::<B>).get(list_keys::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/keys/{key_id}",
            axum::routing::delete(revoke_key::<B>),
        )
        .with_state(state.clone())
        // The second face: /graphql, served by Janus from the same
        // contract, dispatching into the same repositories.
        .merge(crate::graphql::graphql_router(state))
}

async fn healthz() -> &'static str {
    "ok"
}

/// Request body for minting an API key.
#[derive(Debug, Deserialize)]
struct MintKeyRequest {
    name: String,
}

/// Mint a tenant API key. The bearer token appears exactly once, in
/// this response; the store keeps only its hash.
async fn mint_key<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Json(request): Json<MintKeyRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let token = copal_sign::ApiKeyToken::mint();
    let row = copal_store::repo::auth::create_key(
        &state.store,
        &tenant,
        &request.name,
        &token.key_id,
        &token.secret_hash(),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "key_id": row.key_id(),
            "name": row.name,
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
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !copal_store::repo::auth::revoke_key(&state.store, &tenant, &key_id).await? {
        return Err(CopalError::not_found(format!("key {key_id}")).into());
    }
    Ok(StatusCode::NO_CONTENT)
}

fn parse_id(raw: &str) -> Result<FileId, ApiError> {
    Ok(FileId::parse(raw)?)
}

async fn create_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(spec): Json<FileSpec>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let created = file_repo::create_file(&state.store, &tenant, &spec, "api").await?;
    // 201 for a fresh record, 200 for an idempotency-key replay that
    // returned the original — retries read as success, not conflict.
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
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let limit = params.limit.unwrap_or(100).clamp(1, 100);
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
        &state.store,
        &tenant,
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
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let id = parse_id(&id)?;
    let record = file_repo::get_file(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    Ok(Json(crate::wire::wire_file(&record)))
}

/// Soft-delete: tombstone the record and free its live path. Bytes go
/// later, via garbage collection, once nothing references them —
/// deletion is a metadata act, reclamation is a sweep.
async fn delete_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let id = parse_id(&id)?;
    file_repo::soft_delete(&state.store, &tenant, &id).await?;
    Ok(StatusCode::NO_CONTENT)
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
    let tenant = crate::auth::authenticate(&state, request.headers()).await?;
    let id = parse_id(&id)?;

    file_repo::claim_upload(
        &state.store,
        &tenant,
        &id,
        &state.instance_id,
        state.limits.upload_lease_secs,
    )
    .await?;

    // Enforce the ceiling in the stream itself: past the limit the
    // stream yields an error, the writer aborts, and only inert staging
    // garbage remains. The sentinel message is what the error arm
    // below maps to 413.
    let max = state.limits.max_upload_bytes as u64;
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
    let stored = match state.blobs.put_streamed(body).await {
        Ok(stored) => stored,
        Err(err) => {
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
    blob_repo::record_sighting(&state.store, &digest, size_bytes, "local", &storage_path).await?;

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
    let record = file_repo::complete_upload(
        &state.store,
        &tenant,
        &id,
        &digest,
        size_bytes,
        "api",
        final_state,
    )
    .await?;

    let mut record = record;
    if pipelined {
        // Deterministic idempotency key: a crashed or repeated enqueue
        // dedupes instead of double-processing this content.
        let input = json!({
            "tenant": tenant.as_str(),
            "file": id.as_str(),
            "digest": record.digest.as_ref().map(|d| d.as_str()),
            "declared_type": record.content_type,
            "path": record.path,
        });
        let (run_id, created) = state
            .flow
            .enqueue(
                &tenant,
                crate::pipeline::UPLOAD_WORKFLOW,
                RunSpec {
                    input,
                    subject: Some(id.clone()),
                    idempotency_key: Some(crate::pipeline::upload_run_key(&id, &digest)),
                },
            )
            .await?;
        if !created {
            // A dedupe hit means this exact content was processed (or is
            // being processed) for this file before. A COMPLETED run will
            // never finalize the fresh scan — resolve it here: same file,
            // same path, same bytes means the recorded verdict stands and
            // the annotations are already on the metadata. (A quarantined
            // verdict cannot reach this path: quarantined files refuse
            // upload claims.) A FAILED run gets retried so a worker
            // finishes the scan.
            match flow_repo::get_run(&state.store, &tenant, &run_id).await? {
                Some(run) if run.status == "completed" => {
                    match file_repo::transition(
                        &state.store,
                        &tenant,
                        &id,
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
    Ok(Json(crate::wire::wire_file(&record)))
}

async fn download_content<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let id = parse_id(&id)?;
    let record = file_repo::get_file(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
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
    let digest = record
        .digest
        .as_ref()
        .ok_or_else(|| CopalError::Store("servable file without digest".into()))?;
    crate::serve::serve_blob(
        &state.blobs,
        &headers,
        crate::serve::ServeSpec {
            content_type: &record.content_type,
            digest,
            path: &record.path,
            cache: crate::serve::CacheClass::Private,
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

/// Issue a signed URL for a servable file — the shared core behind the
/// REST handler and the GraphQL action resolver.
///
/// The response's `url` is relative -- the deployment's public base is
/// the proxy's business. The token appears exactly once, here; the
/// store keeps only its hash.
pub(crate) async fn issue_grant_core<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    id: &FileId,
    ttl_secs: u32,
    max_uses: Option<u32>,
) -> Result<serde_json::Value, ApiError> {
    if ttl_secs == 0 || ttl_secs > 31_536_000 {
        return Err(CopalError::validation("ttl_secs must be between 1 and 31536000").into());
    }
    if max_uses == Some(0) {
        return Err(CopalError::validation("max_uses must be at least 1").into());
    }

    // Only a servable file gets a URL; a draft link would 404 until
    // upload anyway, and issuing it would leak lifecycle state.
    let record = file_repo::get_file(&state.store, tenant, id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    if !record.servable_content() {
        return Err(CopalError::conflict(format!(
            "file is {} with no servable content",
            record.state.as_str(),
        ))
        .into());
    }

    let token = GrantToken::mint();
    let grant = grant_repo::issue(
        &state.store,
        tenant,
        id,
        &token.grant_id,
        &token.secret_hash(),
        &grant_repo::GrantSpec {
            ttl_secs,
            max_uses,
            created_by: "api".to_owned(),
        },
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
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let id = parse_id(&id)?;
    let issued = issue_grant_core(&state, &tenant, &id, request.ttl_secs, request.max_uses).await?;
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
    // Guards live in the UPDATE: two racing redemptions of a one-use
    // grant serialize here.
    if !grant_repo::consume(&state.store, &token.grant_id).await? {
        return Err(refused().into());
    }

    let tenant = TenantId::parse(&grant.tenant_id).map_err(|_| refused())?;
    let file_id = grant.file_id().map_err(|_| refused())?;
    let record = file_repo::get_file(&state.store, &tenant, &file_id)
        .await?
        .ok_or_else(refused)?;
    if !record.servable_content() {
        return Err(refused().into());
    }
    let digest = record.digest.as_ref().ok_or_else(refused)?;
    // Grant bytes are `no-store`: a shared cache retaining a one-time
    // grant's body would outlive the grant itself.
    crate::serve::serve_blob(
        &state.blobs,
        &headers,
        crate::serve::ServeSpec {
            content_type: &record.content_type,
            digest,
            path: &record.path,
            cache: crate::serve::CacheClass::Private,
        },
    )
    .await
}

/// Revoke a grant by id. Tenant-authenticated; the bearer token is not
/// required -- losing the token is exactly when revocation matters.
async fn revoke_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(grant_ref): Path<String>,
) -> Result<StatusCode, ApiError> {
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    grant_repo::revoke(&state.store, &tenant, &grant_ref).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// List a file's version history, newest first.
async fn list_versions<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Vec<copal_core::FileVersion>>, ApiError> {
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let id = parse_id(&id)?;
    // Tenancy and tombstone filtering ride the file fetch.
    file_repo::get_file(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    let versions = version_repo::list_versions(&state.store, &tenant, &id).await?;
    Ok(Json(versions))
}

/// Serve one historical version's bytes.
async fn download_version<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((id, number)): Path<(String, u64)>,
) -> Result<Response, ApiError> {
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let id = parse_id(&id)?;
    let record = file_repo::get_file(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    // Quarantine blocks the whole record, history included.
    if record.state == FileState::Quarantined {
        return Err(CopalError::conflict("file is quarantined").into());
    }
    let version = version_repo::get_version(&state.store, &tenant, &id, number)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("version {number} of file {id}")))?;
    // A failed CURRENT version is unscanned content — its bytes do not
    // serve. Historical versions (different digest) passed their own
    // pipelines and keep serving.
    if record.state == FileState::Failed && record.digest.as_ref() == Some(&version.digest) {
        return Err(CopalError::conflict(
            "this version's content failed processing and is not servable",
        )
        .into());
    }
    crate::serve::serve_blob(
        &state.blobs,
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

/// Start a workflow run — the shared core behind the REST handler and
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
                // A worker raced the claim: pending, poll the run —
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
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let (status, body) = start_run_core(&state, &tenant, request).await?;
    Ok((status, Json(body)))
}

/// Retry a failed run — the shared core behind the REST handler and
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
    let tenant = crate::auth::authenticate(&state, &headers).await?;
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
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let limit = params.limit.unwrap_or(100).clamp(1, 100);
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
        &state.store,
        &tenant,
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
    let tenant = crate::auth::authenticate(&state, &headers).await?;
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
