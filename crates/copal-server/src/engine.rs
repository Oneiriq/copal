//! Caller tokens for the engine's second enforcement layer.
//!
//! With `COPAL_ENGINE_ACCESS_KEY` set, the store defines a record
//! access method and this module mints the short-lived tokens that
//! authenticate against it. A caller session opened with such a token
//! is filtered by the schema's `PERMISSIONS`: rows outside the
//! token's tenant do not exist for it, and guarded columns come back
//! absent. The application layer keeps making the decisions callers
//! see; the engine layer exists so a request-path bug that drops a
//! tenant filter returns nothing instead of another tenant's rows.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use copal_core::TenantId;

/// Everything token minting needs, present only when the deployment
/// configured the access key.
#[derive(Clone)]
pub struct EngineAccess {
    pub key: String,
    pub namespace: String,
    pub database: String,
}

/// Engine clauses for named contract guards.
///
/// A guard the contract declares without an entry here refuses the
/// boot: shipping it would silently drop the engine layer for that
/// column while the application layer kept enforcing, and the two
/// layers exist to agree.
fn guard_clause(guard: &str) -> Option<&'static str> {
    match guard {
        "admin_only" => Some("$token.adm = true"),
        // Ownership at the engine: the author's principal handle
        // rides the token as `pr`, so the second layer can say what
        // the application guard says. Tokens without the claim (keys
        // under no principal) fail the comparison, which is the
        // unknown-authorship rule again.
        "owner_or_admin" => Some("$token.adm = true OR created_by = $token.pr"),
        _ => None,
    }
}

/// Derive the engine policy from the contract, so both enforcement
/// layers read one declaration set. Field guards become engine
/// column redactions; a resource's read scopes become a conjunct on
/// its table's select clause, sub-resources included, mirroring how
/// the dispatcher enforces reads.
pub fn engine_policy() -> copal_core::Result<copal_store::schema::EnginePolicy> {
    let contract = crate::contract::contract();
    let mut policy = copal_store::schema::EnginePolicy::default();
    let mut add_guards =
        |table: &str, fields: &[janus::ir::FieldExposure]| -> copal_core::Result<()> {
            for field in fields {
                if let Some(guard) = &field.guard {
                    let clause = guard_clause(guard).ok_or_else(|| {
                        copal_core::CopalError::Store(format!(
                            "contract guard {guard:?} has no engine clause; add one before \
                         shipping the guard",
                        ))
                    })?;
                    policy.field_guards.push((
                        table.to_owned(),
                        field.column.clone(),
                        clause.to_owned(),
                    ));
                }
            }
            Ok(())
        };
    for resource in &contract.resources {
        add_guards(&resource.table, &resource.fields)?;
        for sub in &resource.sub_resources {
            add_guards(&sub.table, &sub.fields)?;
        }
    }
    // Retention is enforceable policy the contract cannot declare
    // yet, so it is stated here explicitly rather than derived: a
    // caller session may delete a version row only when nothing
    // binds it. The service store bypasses this, and version pruning
    // runs there; the clause exists so a request-path bug that
    // reaches version deletion meets a second refusal.
    policy.delete_conjuncts.push((
        "file_version".to_owned(),
        "legal_hold != true AND (retain_until IS NONE OR retain_until < time::now())".to_owned(),
    ));
    for resource in &contract.resources {
        if resource.reads_require.is_empty() {
            continue;
        }
        let conjunct = resource
            .reads_require
            .iter()
            .map(|scope| format!("$token.sc CONTAINS '{scope}'"))
            .collect::<Vec<_>>()
            .join(" AND ");
        policy
            .select_conjuncts
            .push((resource.table.clone(), conjunct.clone()));
        for sub in &resource.sub_resources {
            policy
                .select_conjuncts
                .push((sub.table.clone(), conjunct.clone()));
        }
    }
    Ok(policy)
}

/// Seconds a caller token stays valid. Sessions opened with it keep
/// their own engine-side duration; this bounds how long a stolen
/// token mints new ones.
const TOKEN_TTL_SECS: u64 = 300;

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Mint a caller token for one authenticated principal.
///
/// The `id` claim points into the `api_key` table, which makes the
/// engine bind a record session; the record does not need to exist
/// for verification, it names the caller for `$auth` and audit. The
/// admin claim mirrors the scope model, so engine field guards and
/// contract field guards read the same authority.
pub fn mint_caller_token(
    access: &EngineAccess,
    tenant: &TenantId,
    key_id: &str,
    scopes: &[String],
    principal: Option<&str>,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let claims = serde_json::json!({
        "iss": "copal",
        "iat": now,
        "exp": now + TOKEN_TTL_SECS,
        "ns": access.namespace,
        "db": access.database,
        "ac": "caller",
        "id": format!("api_key:⟨{key_id}⟩"),
        "tn": tenant.as_str(),
        "adm": scopes.iter().any(|s| s == "admin"),
        "sc": scopes,
        "pr": principal,
    });
    let header = b64(serde_json::json!({ "alg": "HS256", "typ": "JWT" })
        .to_string()
        .as_bytes());
    let payload = b64(claims.to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let mut mac = Hmac::<Sha256>::new_from_slice(access.key.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(signing_input.as_bytes());
    let signature = b64(&mac.finalize().into_bytes());
    format!("{signing_input}.{signature}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_carry_the_governance_claims() {
        let access = EngineAccess {
            key: "k".into(),
            namespace: "ns".into(),
            database: "db".into(),
        };
        let tenant = TenantId::parse("acme").unwrap();
        let token = mint_caller_token(
            &access,
            &tenant,
            "01KEY",
            &["read".into(), "admin".into()],
            Some("alice"),
        );
        let payload = token.split('.').nth(1).unwrap();
        let claims: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claims["ac"], "caller");
        assert_eq!(claims["tn"], "acme");
        assert_eq!(claims["id"], "api_key:⟨01KEY⟩");
        assert_eq!(claims["adm"], true);
        assert_eq!(claims["ns"], "ns");
        assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());
    }
}
