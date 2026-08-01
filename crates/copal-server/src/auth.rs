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
//!   401; the credential path is not an oracle.
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
    /// The outgoing operator token during a rotation window: accepted
    /// beside the current one, so rotation needs no restart and no
    /// moment where neither token works. Unset outside rotations.
    pub admin_token_previous: Option<String>,
}

/// A fixed 64-hex compare target (matching no real key) burned on
/// unknown key ids, so lookup misses cost the same hash-and-compare as
/// secret mismatches.
const DUMMY_HASH: &str = "5f1f0e8a2f2a2f2a5c9d8b7a6f5e4d3c2b1a09f8e7d6c5b4a3928170605f4e3d";

fn refused() -> ApiError {
    // Uniform: parse failure, unknown key, wrong secret, and revoked
    // key are indistinguishable to the caller.
    CopalError::unauthorized("missing or invalid credentials").into()
}

/// The scope vocabulary keys narrow to. An unscoped key holds all of
/// them, which is what every key minted before scoping existed does.
pub const KEY_SCOPES: [&str; 3] = ["read", "write", "admin"];

/// Who a verified key belongs to and what it may do.
#[derive(Debug, Clone)]
pub struct KeyIdentity {
    /// The bare key id, carried as the principal's subject for audit.
    pub key_id: String,
    /// The scopes this key holds, already expanded: an unscoped key
    /// reads as holding every scope in [`KEY_SCOPES`].
    pub scopes: Vec<String>,
}

/// Resolve the request's tenant per the configured mode.
pub async fn authenticate<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
) -> Result<TenantId, ApiError> {
    authenticate_with_identity(state, headers)
        .await
        .map(|(tenant, _)| tenant)
}

/// Resolve the tenant plus, in key mode, the key's identity and
/// scopes. Header mode carries no identity below the tenant, which is
/// exactly what that mode is: development trust.
pub async fn authenticate_with_identity<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
) -> Result<(TenantId, Option<KeyIdentity>), ApiError> {
    match state.auth.mode {
        AuthMode::TrustedHeader => {
            let raw = headers
                .get("x-copal-tenant")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| CopalError::validation("missing x-copal-tenant header"))?;
            Ok((TenantId::parse(raw)?, None))
        }
        AuthMode::ApiKeys => {
            let bearer = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .ok_or_else(refused)?;
            let token = ApiKeyToken::parse(bearer).map_err(|_| refused())?;
            // Expiry rides the fetch, with the engine as the clock: an
            // expired key reads as absent.
            let row = auth_repo::fetch_live_key(&state.store, &token.key_id).await?;
            let Some(row) = row else {
                // Burn the same hash-compare an existing key would
                // cost, so "unknown id" and "wrong secret" are
                // indistinguishable by timing as well as by message.
                let _ = copal_sign::verify_secret(&token.secret, DUMMY_HASH);
                return Err(refused());
            };
            if !copal_sign::verify_secret(&token.secret, &row.key_hash) {
                return Err(refused());
            }
            if row.revoked_at.is_some() {
                return Err(refused());
            }
            let tenant = TenantId::parse(&row.tenant_id).map_err(|_| refused())?;
            let scopes: Vec<String> = if row.scopes.is_empty() {
                KEY_SCOPES.iter().map(|s| s.to_string()).collect()
            } else {
                row.scopes.split(',').map(str::to_owned).collect()
            };
            Ok((
                tenant,
                Some(KeyIdentity {
                    key_id: row.key_id(),
                    scopes,
                }),
            ))
        }
    }
}

/// Gate an admin route on the operator token, constant-time. During a
/// rotation window the previous token is accepted as well; both
/// candidates are always compared, so the answer's timing does not
/// reveal which one matched.
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
    let current = copal_sign::verify_secret(presented, &copal_sign::hash_secret(configured));
    let previous = match state.auth.admin_token_previous.as_deref() {
        Some(prior) => copal_sign::verify_secret(presented, &copal_sign::hash_secret(prior)),
        None => false,
    };
    if current || previous {
        Ok(())
    } else {
        Err(CopalError::unauthorized("missing or invalid admin token").into())
    }
}
