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
            // Header mode IS full trust, so it reads as an identity
            // holding every scope. Declared requirements then apply
            // uniformly to both modes instead of refusing the mode
            // that has no key to carry them.
            Ok((
                TenantId::parse(raw)?,
                Some(KeyIdentity {
                    key_id: "trusted-header".to_owned(),
                    scopes: KEY_SCOPES.iter().map(|s| s.to_string()).collect(),
                }),
            ))
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

/// A scope requirement on a REST route, mirroring the contract's
/// vocabulary so both faces refuse with the same words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Read,
    Write,
    Admin,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }

    /// Which contract rate class this requirement charges. Reads
    /// meter as reads; anything that changes state meters as a
    /// mutation.
    fn rate_class(self) -> &'static str {
        match self {
            Self::Read => "reads",
            Self::Write | Self::Admin => "mutations",
        }
    }
}

/// Authenticate, charge the consumption ledger, and check the scope,
/// in that order: bad credentials are 401, an exhausted budget is 429
/// before the caller learns anything else, and a missing scope is 403
/// naming it. `units` is the operation's cost; listings pass their
/// clamped row limit and everything else passes 1, matching what the
/// dispatcher charges on the GraphQL face.
pub async fn authenticate_scoped<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    scope: Scope,
    units: u64,
) -> Result<TenantId, ApiError> {
    authenticate_scoped_with_identity(state, headers, scope, units)
        .await
        .map(|(tenant, _)| tenant)
}

/// What authorization hands a request: the tenant, and the store its
/// repository calls run through. With engine sessions off that is
/// the service store; on, a caller-bound session the engine filters
/// by `PERMISSIONS`, so a request-path bug that drops or confuses a
/// tenant filter returns nothing instead of another tenant's rows.
pub struct Authorized {
    pub tenant: TenantId,
    pub store: copal_store::Store,
    /// The authenticated key, absent in header mode; handlers that
    /// project guarded fields build their principal from it.
    pub identity: Option<KeyIdentity>,
}

/// [`authenticate_scoped`] plus the request store. Handlers that
/// adopt engine sessions authorize through this and run repositories
/// on `Authorized::store`.
pub async fn authorize_scoped<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    scope: Scope,
    units: u64,
) -> Result<Authorized, ApiError> {
    let (tenant, identity) =
        authenticate_scoped_with_identity(state, headers, scope, units).await?;
    let store = request_store(state, &tenant, identity.as_ref()).await?;
    Ok(Authorized {
        tenant,
        store,
        identity,
    })
}

/// The store a request runs on. Caller sessions cost two engine
/// round trips to open, which is the price of the second enforcement
/// layer; the flag keeps it opt-in until a deployment has watched it.
pub(crate) async fn request_store<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    identity: Option<&KeyIdentity>,
) -> Result<copal_store::Store, ApiError> {
    if !state.engine_sessions {
        return Ok(state.store.clone());
    }
    let access = state.engine_access.as_ref().ok_or_else(|| {
        ApiError::from(CopalError::Store(
            "engine sessions are on without an access key; boot validation should refuse this"
                .to_owned(),
        ))
    })?;
    let (key_id, scopes) = match identity {
        Some(key) => (key.key_id.as_str(), key.scopes.clone()),
        // Header mode is the full-trust development shape; the engine
        // session mirrors that trust.
        None => (
            "trusted-header",
            KEY_SCOPES.iter().map(|s| (*s).to_owned()).collect(),
        ),
    };
    let cache_key = crate::session_cache::SessionCache::key(tenant.as_str(), key_id, &scopes);
    if let Some(store) = state.sessions.get(&cache_key) {
        return Ok(store);
    }
    let token = crate::engine::mint_caller_token(access, tenant, key_id, &scopes);
    let store = state.store.caller(&token).await.map_err(ApiError::from)?;
    state.sessions.put(cache_key, store.clone());
    Ok(store)
}

/// [`authenticate_scoped`], keeping the identity, for handlers that
/// also project guarded fields and need the principal to evaluate
/// the guards.
pub async fn authenticate_scoped_with_identity<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    scope: Scope,
    units: u64,
) -> Result<(TenantId, Option<KeyIdentity>), ApiError> {
    let (tenant, identity) = authenticate_with_identity(state, headers).await?;
    let subject = identity
        .as_ref()
        .map(|id| id.key_id.as_str())
        .unwrap_or("anonymous");

    // The same ledger the GraphQL dispatcher charges, keyed the same
    // way, so switching protocols never dodges a budget.
    let class = scope.rate_class();
    let budget = crate::contract::contract()
        .rate_classes
        .iter()
        .find(|c| c.name == class)
        .map(|c| c.units_per_minute)
        .unwrap_or(u64::MAX);
    let bucket = format!("{class}:{subject}");
    let admitted = state
        .rate_store
        .charge(&bucket, units, budget)
        .await
        .map_err(|e| CopalError::Store(e.to_string()))?;
    if !admitted {
        return Err(CopalError::TooManyRequests(format!(
            "rate class {class} exhausted; retry next minute",
        ))
        .into());
    }

    if let Some(identity) = &identity {
        if !identity.scopes.iter().any(|s| s == scope.as_str()) {
            return Err(
                CopalError::Forbidden(format!("scope {} required", scope.as_str(),)).into(),
            );
        }
    }
    Ok((tenant, identity))
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
