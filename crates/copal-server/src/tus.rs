//! Resumable uploads: the tus 1.0.0 core protocol with the creation
//! and termination extensions.
//!
//! A session is a claimed draft file plus a growing staged object. The
//! offset column in the store is the protocol's truth; every PATCH
//! claims a single-writer lease, appends, and advances the offset with
//! a CAS from the previous value. When the offset reaches the declared
//! length, the staged bytes promote to their content address (hashing,
//! and sealing when encryption is configured) and the session finishes
//! through the same finalize path a single-PUT upload uses: same
//! digest handling, same pipeline, same dedupe resolution.
//!
//! Interrupted sessions cost nothing: HEAD recovers the offset and the
//! client resumes; abandoned sessions are swept with their staged
//! bytes after the session TTL, and the claimed file fails retryably.

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, options};
use axum::Router;
use base64::Engine as _;
use futures::StreamExt as _;

use copal_blob::BlobStore;
use copal_core::{AccessLevel, CopalError, FileSpec, FileState};
use copal_store::repo::{file as file_repo, tus as tus_repo};

use crate::app::{finalize_new_content, AppState};
use crate::error::ApiError;

const TUS_VERSION: &str = "1.0.0";

/// The `/v1/tus` router.
pub fn tus_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    Router::new()
        .route("/v1/tus", options(capabilities).post(create_session::<B>))
        .route(
            "/v1/tus/{id}",
            get(head_alias)
                .head(session_status::<B>)
                .patch(append::<B>)
                .delete(terminate::<B>),
        )
        .with_state(state)
}

fn tus_headers(response: &mut Response) {
    let headers = response.headers_mut();
    headers.insert("tus-resumable", TUS_VERSION.parse().expect("static"));
    headers.insert(header::CACHE_CONTROL, "no-store".parse().expect("static"));
}

/// Every non-OPTIONS request must speak the protocol version.
fn require_version(headers: &HeaderMap) -> Result<(), ApiError> {
    let ok = headers
        .get("tus-resumable")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == TUS_VERSION);
    if ok {
        Ok(())
    } else {
        Err(CopalError::validation("unsupported or missing Tus-Resumable version").into())
    }
}

/// OPTIONS: the capability advertisement.
async fn capabilities() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    let headers = response.headers_mut();
    headers.insert("tus-resumable", TUS_VERSION.parse().expect("static"));
    headers.insert("tus-version", TUS_VERSION.parse().expect("static"));
    headers.insert(
        "tus-extension",
        "creation,termination".parse().expect("static"),
    );
    response
}

/// GET on a session is not part of the core protocol.
async fn head_alias() -> StatusCode {
    StatusCode::METHOD_NOT_ALLOWED
}

/// Decode `Upload-Metadata`: comma-separated `key base64value` pairs.
fn parse_metadata(
    headers: &HeaderMap,
) -> Result<std::collections::BTreeMap<String, String>, ApiError> {
    let mut out = std::collections::BTreeMap::new();
    let Some(raw) = headers.get("upload-metadata").and_then(|v| v.to_str().ok()) else {
        return Ok(out);
    };
    for pair in raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, ' ');
        let key = parts.next().unwrap_or_default().to_owned();
        let value = match parts.next() {
            Some(encoded) => base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .ok_or_else(|| CopalError::validation("malformed Upload-Metadata value"))?,
            None => String::new(),
        };
        out.insert(key, value);
    }
    Ok(out)
}

/// POST /v1/tus: create a session (creation extension). The metadata
/// must carry `path`; `content_type` and `access` are optional.
async fn create_session<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    require_version(&headers)?;
    let tenant = crate::auth::authenticate(&state, &headers).await?;

    let upload_length: u64 = headers
        .get("upload-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| CopalError::validation("Upload-Length is required"))?;
    if upload_length as usize > state.limits.max_upload_bytes {
        return Err(CopalError::PayloadTooLarge(format!(
            "upload exceeds {} bytes",
            state.limits.max_upload_bytes,
        ))
        .into());
    }

    let metadata = parse_metadata(&headers)?;
    let path = metadata
        .get("path")
        .cloned()
        .ok_or_else(|| CopalError::validation("Upload-Metadata must carry a path"))?;
    let access = match metadata.get("access").map(String::as_str) {
        None => AccessLevel::Private,
        Some(raw) => serde_json::from_value(serde_json::Value::String(raw.to_owned()))
            .map_err(|_| CopalError::validation(format!("unknown access {raw:?}")))?,
    };
    let spec = FileSpec {
        path,
        content_type: metadata
            .get("content_type")
            .cloned()
            .unwrap_or_else(|| "application/octet-stream".to_owned()),
        access,
        metadata: serde_json::Value::Null,
        idempotency_key: None,
    };

    // The file exists and is claimed for the whole session, so a rival
    // single-PUT upload loses its CAS instead of interleaving.
    let created = file_repo::create_file(&state.store, &tenant, &spec, "tus").await?;
    let file_id = created.record.id.clone();
    file_repo::claim_upload(
        &state.store,
        &tenant,
        &file_id,
        &state.instance_id,
        state.limits.tus_session_ttl_secs,
    )
    .await?;

    let staging_key = format!("tus/{}", ulid::Ulid::new().to_string().to_ascii_lowercase());
    let session_id =
        tus_repo::create_session(&state.store, &tenant, &file_id, upload_length, &staging_key)
            .await?;

    let mut response = StatusCode::CREATED.into_response();
    tus_headers(&mut response);
    response.headers_mut().insert(
        header::LOCATION,
        format!("/v1/tus/{session_id}").parse().expect("ulid path"),
    );
    response
        .headers_mut()
        .insert("upload-offset", "0".parse().expect("static"));
    Ok(response)
}

/// HEAD /v1/tus/{id}: where to resume from.
async fn session_status<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    require_version(&headers)?;
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let session = tus_repo::fetch(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("upload {id}")))?;

    let mut response = StatusCode::OK.into_response();
    tus_headers(&mut response);
    let h = response.headers_mut();
    h.insert(
        "upload-offset",
        session.offset.to_string().parse().expect("number"),
    );
    h.insert(
        "upload-length",
        session.upload_length.to_string().parse().expect("number"),
    );
    Ok(response)
}

/// PATCH /v1/tus/{id}: append at the declared offset.
async fn append<B: BlobStore>(
    State(state): State<AppState<B>>,
    Path(id): Path<String>,
    request: Request,
) -> Result<Response, ApiError> {
    require_version(request.headers())?;
    let tenant = crate::auth::authenticate(&state, request.headers()).await?;

    let content_type_ok = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == "application/offset+octet-stream");
    if !content_type_ok {
        return Err(CopalError::validation(
            "PATCH requires content-type application/offset+octet-stream",
        )
        .into());
    }
    let claimed_offset: u64 = request
        .headers()
        .get("upload-offset")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| CopalError::validation("Upload-Offset is required"))?;

    let session = tus_repo::fetch(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("upload {id}")))?;
    if session.offset != claimed_offset {
        return Err(CopalError::conflict(format!(
            "offset is {}, request declared {claimed_offset}",
            session.offset,
        ))
        .into());
    }

    // Single writer: the lease CAS refuses concurrent PATCHes and a
    // stale offset alike.
    let lease_secs = u32::try_from(state.limits.transfer_timeout_secs).unwrap_or(3_600);
    if !tus_repo::claim_patch(
        &state.store,
        &id,
        claimed_offset,
        &state.instance_id,
        lease_secs,
    )
    .await?
    {
        return Err(CopalError::conflict("another append holds this session").into());
    }

    // Enforce the remaining-length ceiling inside the stream.
    let remaining = session.upload_length - session.offset;
    let mut running_total = 0u64;
    let body = request
        .into_body()
        .into_data_stream()
        .map(move |chunk| match chunk {
            Ok(bytes) => {
                running_total += bytes.len() as u64;
                if running_total > remaining {
                    Err("length limit exceeded".to_owned())
                } else {
                    Ok(bytes)
                }
            }
            Err(err) => Err(format!("body: {err}")),
        });

    let staged = match state.blobs.append_staged(&session.staging_key, body).await {
        Ok(len) => len,
        Err(err) => {
            // The staged object may now hold a partial append the
            // offset does not acknowledge; discard the session's bytes
            // past the offset by failing the session entirely. tus
            // clients recover by re-creating; the swept file stays
            // retryable.
            let _ = tus_repo::release_patch(&state.store, &id).await;
            let text = err.to_string();
            if text.contains("length limit") {
                return Err(CopalError::PayloadTooLarge(format!(
                    "append exceeds the declared Upload-Length {}",
                    session.upload_length,
                ))
                .into());
            }
            return Err(err.into());
        }
    };

    tus_repo::advance(&state.store, &id, claimed_offset, staged).await?;

    if staged == session.upload_length {
        // Complete: promote the staged bytes to their content address
        // and finish exactly like a single-PUT upload. Staging always
        // lives on the local backend (appends need it); a remote
        // residency streams the staged bytes into its own store and
        // the local staging entry drops after.
        let file_id = session.file_id()?;
        let (residency, backend) = state.residency_for(&tenant).await?;
        let stored = if residency == "local" {
            state.blobs.promote_staged(&session.staging_key).await?
        } else {
            let (_, plain) = state.blobs.open_staged(&session.staging_key).await?;
            let stored = backend
                .put_streamed(plain.map(|chunk| chunk.map_err(|e| e.to_string())))
                .await?;
            state.blobs.discard_staged(&session.staging_key).await?;
            stored
        };
        finalize_new_content(
            &state,
            &tenant,
            &file_id,
            &residency,
            &stored.digest,
            stored.size_bytes,
            &stored.storage_path,
        )
        .await?;
        tus_repo::delete_session(&state.store, &id).await?;
    }

    let mut response = StatusCode::NO_CONTENT.into_response();
    tus_headers(&mut response);
    response
        .headers_mut()
        .insert("upload-offset", staged.to_string().parse().expect("number"));
    Ok(response)
}

/// DELETE /v1/tus/{id}: termination extension. The staged bytes go,
/// the claimed file fails retryably.
async fn terminate<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    require_version(&headers)?;
    let tenant = crate::auth::authenticate(&state, &headers).await?;
    let session = tus_repo::fetch(&state.store, &tenant, &id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("upload {id}")))?;

    state.blobs.discard_staged(&session.staging_key).await?;
    if let Ok(file_id) = session.file_id() {
        let _ = file_repo::transition(
            &state.store,
            &tenant,
            &file_id,
            FileState::Uploading,
            FileState::Failed,
            Default::default(),
        )
        .await;
    }
    tus_repo::delete_session(&state.store, &id).await?;

    let mut response = StatusCode::NO_CONTENT.into_response();
    tus_headers(&mut response);
    Ok(response)
}

/// Sweep support: expire abandoned sessions, discarding staged bytes
/// and failing their claimed files retryably.
pub async fn sweep_expired<B: BlobStore>(
    store: &copal_store::Store,
    blobs: &B,
    ttl_secs: u64,
) -> copal_core::Result<u64> {
    let expired = tus_repo::list_expired(store, ttl_secs, 500).await?;
    let mut swept = 0u64;
    for session in expired {
        blobs.discard_staged(&session.staging_key).await?;
        if let (Ok(file_id), Ok(tenant)) = (
            session.file_id(),
            copal_core::TenantId::parse(&session.tenant_id),
        ) {
            let _ = file_repo::transition(
                store,
                &tenant,
                &file_id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
        }
        tus_repo::delete_session(store, &session.session_id()).await?;
        swept += 1;
    }
    Ok(swept)
}

// Body type appears in the router signature through axum's generics.
#[allow(dead_code)]
fn _assert_body(_: Body) {}
