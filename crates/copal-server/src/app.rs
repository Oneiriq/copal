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
use copal_core::{CopalError, FileId, FileRecord, FileSpec, FileState, TenantId};
use copal_sign::GrantToken;
use copal_store::repo::{blob as blob_repo, file as file_repo, grant as grant_repo};
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
    /// Identifies this process as an upload-claim owner, so stale
    /// leases name the instance that died holding them.
    pub instance_id: String,
}

impl<B: BlobStore> AppState<B> {
    /// State with default limits and a fresh instance id.
    pub fn new(store: Store, blobs: B) -> Self {
        Self {
            store,
            blobs,
            limits: Limits::default(),
            instance_id: ulid::Ulid::new().to_string().to_ascii_lowercase(),
        }
    }
}

/// Build the router over any blob backend.
///
/// The upload ceiling is enforced inside the upload handler's stream —
/// `DefaultBodyLimit` guards extractor-based bodies only, and the
/// upload path consumes the raw request stream. JSON routes keep axum's
/// small built-in default limit.
pub fn build_router<B: BlobStore>(state: AppState<B>) -> Router {
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
        .route("/v1/files/{id}/url", post(issue_grant::<B>))
        // One pattern, two readings: GET takes the full bearer token,
        // DELETE takes the bare grant id (with tenant auth).
        .route(
            "/v1/grants/{grant_ref}",
            get(redeem_grant::<B>).delete(revoke_grant::<B>),
        )
        .with_state(state)
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
) -> Result<(StatusCode, Json<FileRecord>), ApiError> {
    let tenant = tenant_from(&headers)?;
    let created = file_repo::create_file(&state.store, &tenant, &spec, "api").await?;
    // 201 for a fresh record, 200 for an idempotency-key replay that
    // returned the original — retries read as success, not conflict.
    let status = if created.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(created.record)))
}

async fn list_files<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
) -> Result<Json<Vec<FileRecord>>, ApiError> {
    let tenant = tenant_from(&headers)?;
    let records = file_repo::list_files(&state.store, &tenant, 100).await?;
    Ok(Json(records))
}

async fn get_file<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<FileRecord>, ApiError> {
    let tenant = tenant_from(&headers)?;
    let id = parse_id(&id)?;
    let record = file_repo::get_file(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    Ok(Json(record))
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
) -> Result<Json<FileRecord>, ApiError> {
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

    let record = file_repo::transition(
        &state.store,
        &tenant,
        &id,
        FileState::Uploading,
        FileState::Ready,
        file_repo::TransitionSets {
            digest: Some(digest.clone()),
            size_bytes: Some(size_bytes),
            link_blob: Some(digest),
        },
    )
    .await?;
    Ok(Json(record))
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
    if !record.state.servable() {
        return Err(CopalError::conflict(format!(
            "file is {}, not servable",
            record.state.as_str(),
        ))
        .into());
    }
    let digest = record
        .digest
        .as_ref()
        .ok_or_else(|| CopalError::Store("ready file without digest".into()))?;
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

/// Issue a signed URL for a servable file.
///
/// The response's `url` is relative -- the deployment's public base is
/// the proxy's business. The token appears exactly once, here; the
/// store keeps only its hash.
async fn issue_grant<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<IssueGrantRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant = tenant_from(&headers)?;
    let id = parse_id(&id)?;
    if request.ttl_secs == 0 || request.ttl_secs > 31_536_000 {
        return Err(CopalError::validation("ttl_secs must be between 1 and 31536000").into());
    }
    if request.max_uses == Some(0) {
        return Err(CopalError::validation("max_uses must be at least 1").into());
    }

    // Only a servable file gets a URL; a draft link would 404 until
    // upload anyway, and issuing it would leak lifecycle state.
    let record = file_repo::get_file(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    if !record.state.servable() {
        return Err(CopalError::conflict(format!(
            "file is {}, not servable",
            record.state.as_str(),
        ))
        .into());
    }

    let token = GrantToken::mint();
    let grant = grant_repo::issue(
        &state.store,
        &tenant,
        &id,
        &token.grant_id,
        &token.secret_hash(),
        &grant_repo::GrantSpec {
            ttl_secs: request.ttl_secs,
            max_uses: request.max_uses,
            created_by: "api".to_owned(),
        },
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "grant_id": token.grant_id,
            "token": token.encode(),
            "url": format!("/v1/grants/{}", token.encode()),
            "expires_at": grant.expires_at,
            "max_uses": grant.max_uses,
        })),
    ))
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
    if !record.state.servable() {
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
