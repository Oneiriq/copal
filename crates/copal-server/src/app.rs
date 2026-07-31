//! Router and handlers: the vertical slice of the file service.
//!
//! create -> stream bytes in -> complete -> read metadata -> stream out.
//! Handlers stay thin: tenant extraction, one or two repo/blob calls,
//! HTTP mapping. The state machine and tenancy rules live below.

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use futures::StreamExt as _;

use copal_blob::{BlobStore, StoredBlob};
use copal_core::{CopalError, FileId, FileSpec, FileState, TenantId};
use copal_flow::{FlowEngine, FlowRegistry, RunSpec};
use copal_sign::GrantToken;
use copal_store::repo::{
    blob as blob_repo, file as file_repo, grant as grant_repo, version as version_repo,
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
}

impl<B: BlobStore> AppState<B> {
    /// State with default limits, an empty flow registry, and a fresh
    /// instance id.
    pub fn new(store: Store, blobs: B) -> Self {
        Self {
            flow: FlowEngine::new(store.clone(), FlowRegistry::new()),
            store,
            blobs,
            limits: Limits::default(),
            instance_id: ulid::Ulid::new().to_string().to_ascii_lowercase(),
        }
    }

    /// Install a populated activity/workflow registry.
    pub fn with_flow(mut self, registry: FlowRegistry) -> Self {
        self.flow = FlowEngine::new(self.store.clone(), registry);
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
        .route("/v1/runs", post(start_run::<B>))
        .route("/v1/runs/{id}", get(get_run::<B>))
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
        .with_state(state.clone())
        // The second face: /graphql, served by Janus from the same
        // contract, dispatching into the same repositories.
        .merge(crate::graphql::graphql_router(state))
}

async fn healthz() -> &'static str {
    "ok"
}

/// Tenant comes from the `x-copal-tenant` header. This is the
/// development seam where verified authentication plugs in; the header
/// is validated as an identifier either way.
fn tenant_from(headers: &HeaderMap) -> Result<TenantId, ApiError> {
    let raw = headers
        .get("x-copal-tenant")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| CopalError::validation("missing x-copal-tenant header"))?;
    Ok(TenantId::parse(raw)?)
}

fn parse_id(raw: &str) -> Result<FileId, ApiError> {
    Ok(FileId::parse(raw)?)
}

async fn create_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(spec): Json<FileSpec>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant = tenant_from(&headers)?;
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
/// pair -- opacity is the point; clients never construct these.
pub(crate) fn encode_cursor(position: &file_repo::ListPosition) -> String {
    hex::encode(format!("{}|{}", position.created_at, position.id))
}

pub(crate) fn decode_cursor(raw: &str) -> Result<file_repo::ListPosition, ApiError> {
    let invalid = || CopalError::validation("malformed cursor");
    let bytes = hex::decode(raw).map_err(|_| invalid())?;
    let text = String::from_utf8(bytes).map_err(|_| invalid())?;
    let (created_at, id) = text.split_once('|').ok_or_else(invalid)?;
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
    let tenant = tenant_from(&headers)?;
    let limit = params.limit.unwrap_or(100).clamp(1, 100);
    let after = params.cursor.as_deref().map(decode_cursor).transpose()?;
    let state_filter = params.state.as_deref().map(parse_state_param).transpose()?;
    let ascending = match params.sort.as_deref() {
        None | Some("-created_at") => false,
        Some("created_at") => true,
        Some(other) => {
            return Err(CopalError::validation(format!("unknown sort {other:?}")).into());
        }
    };
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
            encode_cursor(&file_repo::ListPosition {
                created_at: last.created_at.clone(),
                id: last.id.clone(),
            })
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
    let tenant = tenant_from(&headers)?;
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
    let tenant = tenant_from(&headers)?;
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
    let tenant = tenant_from(request.headers())?;
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
        state
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
    }
    Ok(Json(crate::wire::wire_file(&record)))
}

async fn download_content<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let tenant = tenant_from(&headers)?;
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
    // Stream: bytes never buffer in the server, while the length —
    // known up front for immutable content-addressed objects — still
    // rides Content-Length.
    let (len, stream) = state.blobs.open_read(digest).await?;

    let response = (
        [
            (header::CONTENT_TYPE, record.content_type.clone()),
            (header::CONTENT_LENGTH, len.to_string()),
            (header::ETAG, format!("\"{digest}\"")),
        ],
        Body::from_stream(stream),
    );
    Ok(response.into_response())
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
    let tenant = tenant_from(&headers)?;
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
    let (len, stream) = state.blobs.open_read(digest).await?;
    let response = (
        [
            (header::CONTENT_TYPE, record.content_type.clone()),
            (header::CONTENT_LENGTH, len.to_string()),
            (header::ETAG, format!("\"{digest}\"")),
        ],
        Body::from_stream(stream),
    );
    Ok(response.into_response())
}

/// Revoke a grant by id. Tenant-authenticated; the bearer token is not
/// required -- losing the token is exactly when revocation matters.
async fn revoke_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(grant_ref): Path<String>,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_from(&headers)?;
    grant_repo::revoke(&state.store, &tenant, &grant_ref).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// List a file's version history, newest first.
async fn list_versions<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Vec<copal_core::FileVersion>>, ApiError> {
    let tenant = tenant_from(&headers)?;
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
    let tenant = tenant_from(&headers)?;
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
    let (len, stream) = state.blobs.open_read(&version.digest).await?;
    let response = (
        [
            (header::CONTENT_TYPE, version.content_type.clone()),
            (header::CONTENT_LENGTH, len.to_string()),
            (header::ETAG, format!("\"{}\"", version.digest)),
        ],
        Body::from_stream(stream),
    );
    Ok(response.into_response())
}

/// Request body for starting a run.
#[derive(Debug, Deserialize)]
struct StartRunRequest {
    workflow: String,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    /// "async" (default) enqueues for a worker; "sync" executes
    /// in-request over the same journal.
    #[serde(default)]
    mode: Option<String>,
}

/// Start a workflow run.
async fn start_run<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(request): Json<StartRunRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant = tenant_from(&headers)?;
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
            let (run_id, output) = state
                .flow
                .run_sync(&tenant, &request.workflow, spec)
                .await?;
            Ok((
                StatusCode::OK,
                Json(json!({ "run_id": run_id, "output": output })),
            ))
        }
        None | Some("async") => {
            let (run_id, created) = state.flow.enqueue(&tenant, &request.workflow, spec).await?;
            let status = if created {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            Ok((status, Json(json!({ "run_id": run_id }))))
        }
        Some(other) => Err(CopalError::validation(format!("unknown mode {other:?}")).into()),
    }
}

/// Run status plus its journal.
async fn get_run<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = tenant_from(&headers)?;
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
    Ok(Json(json!({
        "run_id": run.run_id(),
        "workflow": run.workflow_key,
        "status": run.status,
        "output": run.output,
        "error": run.run_error,
        "created_at": run.created_at,
        "ended_at": run.ended_at,
        "steps": steps,
    })))
}
