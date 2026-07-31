//! Request authentication: who is this tenant?
//!
//! Two modes, chosen by configuration:
//!
//! - `TrustedHeader` (development default until 1.0): the
//!   `x-copal-tenant` header IS the identity. The server logs a loud
//!   warning at startup; every deployment document says to turn it off.
//! - `ApiKeys`: the `Authorization: Bearer ck1.<id>.<secret>` key is
//!   the identity. Verification is a record fetch plus a constant-time
//!   hash compare; the tenant comes OUT of the key row, so a caller
//!   cannot name a tenant at all. Every failure is the same uniform
//!   401 — the credential path is not an oracle.
//!
//! Keys are minted and revoked through `/v1/admin/...` routes guarded
//! by the operator token (`COPAL_ADMIN_TOKEN`), compared hash-first in
//! constant time. No admin token configured = admin surface disabled.

use axum::http::HeaderMap;

use copal_blob::BlobStore;
use copal_core::{CopalError, TenantId};
use copal_sign::ApiKeyToken;
use copal_store::repo::auth as auth_repo;

use crate::app::AppState;
use crate::error::ApiError;

/// How requests prove their tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMode {
    /// Trust `x-copal-tenant` (development only).
    #[default]
    TrustedHeader,
    /// Require a `ck1` bearer key.
    ApiKeys,
}

impl AuthMode {
    /// Parse the `COPAL_AUTH_MODE` value.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "header" => Some(Self::TrustedHeader),
            "keys" => Some(Self::ApiKeys),
            _ => None,
        }
    }
}

/// Authentication configuration carried on the app state.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub mode: AuthMode,
    /// Operator token guarding the admin surface; absent = disabled.
    pub admin_token: Option<String>,
}

fn refused() -> ApiError {
    // Uniform: parse failure, unknown key, wrong secret, and revoked
    // key are indistinguishable to the caller.
    CopalError::unauthorized("missing or invalid credentials").into()
}

/// Resolve the request's tenant per the configured mode.
pub async fn authenticate<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
) -> Result<TenantId, ApiError> {
    match state.auth.mode {
        AuthMode::TrustedHeader => {
            let raw = headers
                .get("x-copal-tenant")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| CopalError::validation("missing x-copal-tenant header"))?;
            Ok(TenantId::parse(raw)?)
        }
        AuthMode::ApiKeys => {
            let bearer = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .ok_or_else(refused)?;
            let token = ApiKeyToken::parse(bearer).map_err(|_| refused())?;
            let row = auth_repo::fetch_key(&state.store, &token.key_id)
                .await?
                .ok_or_else(refused)?;
            if !copal_sign::verify_secret(&token.secret, &row.key_hash) {
                return Err(refused());
            }
            if row.revoked_at.is_some() {
                return Err(refused());
            }
            TenantId::parse(&row.tenant_id).map_err(|_| refused())
        }
    }
}

/// Gate an admin route on the operator token, constant-time.
pub fn require_admin<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    let Some(configured) = state.auth.admin_token.as_deref() else {
        return Err(CopalError::unauthorized("admin API is not enabled").into());
    };
    let presented = headers
        .get("x-copal-admin-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    // Hash both sides, then compare in constant time: no length leak,
    // no prefix leak.
    if copal_sign::verify_secret(presented, &copal_sign::hash_secret(configured)) {
        Ok(())
    } else {
        Err(CopalError::unauthorized("missing or invalid admin token").into())
    }
}
