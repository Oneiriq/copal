//! cg2 edge tokens: issuance and redemption.
//!
//! An edge token is a stateless HMAC capability under a tenant edge
//! key: verifiable at a CDN worker or reverse proxy holding the same
//! secret, with no database hop. Redemption here is the origin's own
//! verification of the identical scheme. Statelessness trades away
//! per-token revocation; a token lives until it expires or its key is
//! revoked, so issue short TTLs. `cg1` grants stay the revocable,
//! use-counted family.
//!
//! Edge keys follow the sealed-secret custody rule: admin-minted,
//! sealed under the blob master key, returned exactly once (the copy
//! the operator installs at the edge). The whole surface exists only
//! when that key is configured.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine as _;
use serde::Deserialize;
use serde_json::json;

use copal_blob::crypto::BlobCipher;
use copal_blob::BlobStore;
use copal_core::{CopalError, FileId, TenantId};
use copal_store::repo::{edge as edge_repo, file as file_repo};
use copal_store::Store;

use crate::app::{forwarded_origin, AppState};
use crate::error::ApiError;

/// Issued-token TTL ceiling: one day. Edge tokens cannot be revoked
/// individually, so the ceiling stays low; `cg1` covers long-lived
/// needs.
const MAX_TTL_SECS: i64 = 86_400;
const DEFAULT_TTL_SECS: i64 = 300;

/// Edge state: the application plus the sealing cipher.
#[derive(Clone)]
pub struct EdgeState<B: BlobStore> {
    pub app: AppState<B>,
}

/// Tenant issuance plus anonymous redemption.
pub fn edge_router<B: BlobStore + 'static>(app: AppState<B>) -> Router {
    let state = EdgeState { app };
    Router::new()
        .route(
            "/v1/files/{id}/edge-url",
            axum::routing::post(issue_edge_url::<B>),
        )
        .route("/v1/edge/{token}", get(redeem_edge::<B>))
        .with_state(state)
}

/// Edge key custody for the admin surface.
pub fn edge_admin_router<B: BlobStore + 'static>(app: AppState<B>) -> Router {
    let state = EdgeState { app };
    Router::new()
        .route(
            "/v1/admin/tenants/{tenant}/edge-keys",
            axum::routing::post(mint_edge_key::<B>).get(list_edge_keys::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/edge-keys/{key_id}",
            axum::routing::delete(revoke_edge_key::<B>),
        )
        .with_state(state)
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn open_secret(cipher: &BlobCipher, sealed_b64: &str) -> Option<String> {
    let sealed = base64::engine::general_purpose::STANDARD
        .decode(sealed_b64)
        .ok()?;
    let bytes = cipher.open(&sealed).ok()?;
    String::from_utf8(bytes).ok()
}

/// Issuance body.
#[derive(Debug, Deserialize)]
struct IssueEdgeRequest {
    #[serde(default)]
    ttl_secs: Option<i64>,
}

/// Issue a cg2 URL for a servable file, signed by the tenant's newest
/// active edge key.
async fn issue_edge_url<B: BlobStore>(
    State(state): State<EdgeState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<IssueEdgeRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let auth =
        crate::auth::authorize_scoped(&state.app, &headers, crate::auth::Scope::Read, 1).await?;
    let tenant = &auth.tenant;
    let id = FileId::parse(&id)?;
    let body = issue_edge_url_core(
        &auth.store,
        &state.app,
        tenant,
        &id,
        request.ttl_secs.unwrap_or(DEFAULT_TTL_SECS),
        forwarded_origin(&headers).as_deref(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(body)))
}

/// Mint a cg2 token, the shared core behind the REST handler and the
/// GraphQL action resolver.
pub(crate) async fn issue_edge_url_core<B: BlobStore>(
    store: &Store,
    app: &AppState<B>,
    tenant: &copal_core::TenantId,
    id: &FileId,
    ttl: i64,
    origin: Option<&str>,
) -> Result<serde_json::Value, ApiError> {
    if !(1..=MAX_TTL_SECS).contains(&ttl) {
        return Err(CopalError::validation(format!(
            "ttl_secs must be within 1..={MAX_TTL_SECS}: edge tokens cannot be revoked \
             individually",
        ))
        .into());
    }
    let record = file_repo::get_file(store, tenant, id)
        .await?
        .ok_or_else(|| CopalError::not_found(format!("file {id}")))?;
    if !record.servable_content() {
        return Err(CopalError::conflict("file has no servable content").into());
    }

    // The newest active key signs; rotation reads as mint new, revoke
    // old once edge configs have moved.
    let keys = edge_repo::list_keys(store, tenant).await?;
    let signing_key_id = keys
        .iter()
        .rev()
        .find(|row| row.get("revoked_at").map(|v| v.is_null()).unwrap_or(true))
        .and_then(|row| row.get("id").and_then(|v| v.as_str()))
        .map(str::to_owned)
        .ok_or_else(|| CopalError::conflict("no active edge key; mint one on the admin surface"))?;
    let key_row = edge_repo::fetch_key(store, &signing_key_id)
        .await?
        .ok_or_else(|| CopalError::Store("edge key vanished".into()))?;
    let cipher = app.require_cipher()?;
    let secret = open_secret(cipher, &key_row.secret_sealed).ok_or_else(|| {
        CopalError::Store("edge key does not open under the configured key".into())
    })?;

    let claims = copal_sign::EdgeClaims {
        key: signing_key_id,
        tenant: tenant.as_str().to_owned(),
        file: id.as_str().to_owned(),
        exp: now_unix() + ttl,
    };
    let token = copal_sign::EdgeToken::sign(&claims, &secret)?;
    copal_store::repo::auth::record_audit(
        store,
        tenant,
        tenant.as_str(),
        "edge.issued",
        id.as_str(),
        origin,
        Some(json!({ "key": claims.key, "exp": claims.exp })),
    )
    .await?;
    Ok(json!({
        "token": token,
        "url": format!("/v1/edge/{token}"),
        "expires_at": claims.exp,
    }))
}

/// Redeem a cg2 token: the origin-side verification of the edge
/// scheme. Refusals are uniform 404s, exactly like grants.
async fn redeem_edge<B: BlobStore>(
    State(state): State<EdgeState<B>>,
    headers: HeaderMap,
    Path(token): Path<String>,
) -> Result<Response, ApiError> {
    let refused = || CopalError::not_found("unknown or unusable edge token");

    let parsed = copal_sign::EdgeToken::parse(&token).map_err(|_| refused())?;
    let key_row = edge_repo::fetch_key(&state.app.store, &parsed.claims.key)
        .await?
        .ok_or_else(refused)?;
    if key_row.revoked_at.is_some() || key_row.tenant_id != parsed.claims.tenant {
        return Err(refused().into());
    }
    let cipher = state.app.require_cipher().map_err(|_| refused())?;
    let secret = open_secret(cipher, &key_row.secret_sealed).ok_or_else(refused)?;
    if !parsed.verify(&secret, now_unix()) {
        return Err(refused().into());
    }

    let tenant = TenantId::parse(&parsed.claims.tenant).map_err(|_| refused())?;
    let id = FileId::parse(&parsed.claims.file).map_err(|_| refused())?;
    let record = file_repo::get_file(&state.app.store, &tenant, &id)
        .await?
        .ok_or_else(refused)?;
    if !record.servable_content() {
        return Err(refused().into());
    }
    let digest = record.digest.as_ref().expect("servable implies digest");
    let backend = state.app.backend_for_record(&record)?;
    crate::tiering::note_blob_read(
        &state.app.store,
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
            cache: crate::serve::CacheClass::Private,
        },
    )
    .await
}

/// Mint an edge key. The secret appears exactly once, here; install
/// the same value at the CDN edge.
async fn mint_edge_key<B: BlobStore>(
    State(state): State<EdgeState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    crate::auth::require_admin(&state.app, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let token = copal_sign::ApiKeyToken::mint();
    let sealed = state.app.require_cipher()?.seal(token.secret.as_bytes())?;
    let sealed_b64 = base64::engine::general_purpose::STANDARD.encode(sealed);
    let row = edge_repo::create_key(&state.app.store, &tenant, &token.key_id, &sealed_b64).await?;
    copal_store::repo::auth::record_audit(
        &state.app.store,
        &tenant,
        "admin",
        "edgekey.minted",
        &row.key_id(),
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "key_id": row.key_id(),
            "secret": token.secret,
            "created_at": row.created_at,
        })),
    ))
}

/// List a tenant's edge keys (never their secrets).
async fn list_edge_keys<B: BlobStore>(
    State(state): State<EdgeState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state.app, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let rows = edge_repo::list_keys(&state.app.store, &tenant).await?;
    Ok(Json(json!({ "items": rows })))
}

/// Revoke an edge key, ending every token it signed; unknown and
/// already-revoked both read 404.
async fn revoke_edge_key<B: BlobStore>(
    State(state): State<EdgeState<B>>,
    headers: HeaderMap,
    Path((tenant, key_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    crate::auth::require_admin(&state.app, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !edge_repo::revoke_key(&state.app.store, &tenant, &key_id).await? {
        return Err(CopalError::not_found(format!("edge key {key_id}")).into());
    }
    copal_store::repo::auth::record_audit(
        &state.app.store,
        &tenant,
        "admin",
        "edgekey.revoked",
        &key_id,
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
