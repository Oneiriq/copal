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

use copal_blob::{BlobStore, StoredBlob};
use copal_core::{CopalError, FileId, FileRecord, FileSpec, FileState, TenantId};
use copal_store::repo::{blob as blob_repo, file as file_repo};
use copal_store::Store;

use crate::error::ApiError;

/// Shared application state.
#[derive(Clone)]
pub struct AppState<B: BlobStore> {
    pub store: Store,
    pub blobs: B,
}

/// Build the router over any blob backend.
pub fn build_router<B: BlobStore>(state: AppState<B>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/files", post(create_file::<B>).get(list_files::<B>))
        .route("/v1/files/{id}", get(get_file::<B>))
        .route(
            "/v1/files/{id}/content",
            put(upload_content::<B>).get(download_content::<B>),
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
    let record = file_repo::create_file(&state.store, &tenant, &spec, "api").await?;
    Ok((StatusCode::CREATED, Json(record)))
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

/// Stream bytes in and finish the upload in one request.
///
/// The record moves draft -> uploading (or failed -> uploading on a
/// retry) before any byte lands, so a concurrent second uploader loses
/// the CAS instead of interleaving writes. On success the record is
/// ready, digest-verified, and linked to its deduped blob row.
async fn upload_content<B: BlobStore>(
    State(state): State<AppState<B>>,
    Path(id): Path<String>,
    request: Request,
) -> Result<Json<FileRecord>, ApiError> {
    let tenant = tenant_from(request.headers())?;
    let id = parse_id(&id)?;

    // Claim the record. A fresh draft claims from Draft; a retry after
    // failure claims from Failed.
    let claim = file_repo::transition(
        &state.store,
        &tenant,
        &id,
        FileState::Draft,
        FileState::Uploading,
        Default::default(),
    )
    .await;
    match claim {
        Ok(_) => {}
        Err(CopalError::Conflict(_)) => {
            file_repo::transition(
                &state.store,
                &tenant,
                &id,
                FileState::Failed,
                FileState::Uploading,
                Default::default(),
            )
            .await?;
        }
        Err(other) => return Err(other.into()),
    }

    let body = request.into_body().into_data_stream();
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
    let bytes = state.blobs.read(digest).await?;

    let response = (
        [
            (header::CONTENT_TYPE, record.content_type.clone()),
            (header::ETAG, format!("\"{digest}\"")),
        ],
        Body::from(bytes),
    );
    Ok(response.into_response())
}
