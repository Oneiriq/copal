//! Engine facts about per-caller sessions, pinned the same way
//! `engine_assumptions.rs` pins retrieval behavior.
//!
//! The governance roadmap once dispositioned `PERMISSIONS` pushdown
//! with the claim that the engine cannot tell Copal's callers apart.
//! These tests prove the claim wrong and bound the revived design: a
//! cloned handle is its own engine session over the same connection,
//! and a record-access JWT binds a caller identity to it, after which
//! table and field `PERMISSIONS` filter engine side while the root
//! handle beside it keeps full authority.
//!
//! The tests use the surrealdb crate directly because enforcement
//! needs a datastore built with credentials, and the surql-rs
//! connection config has no seam for that yet. Growing one is part of
//! the pushdown project.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use surrealdb::engine::local::Mem;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Config;
use surrealdb::Surreal;

fn b64(json: &serde_json::Value) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json.to_string())
}

/// A hand-rolled HS256 JWT, enough for the engine to verify.
fn jwt(secret: &str, claims: serde_json::Value) -> String {
    let header = b64(&serde_json::json!({ "alg": "HS256", "typ": "JWT" }));
    let payload = b64(&claims);
    let signing_input = format!("{header}.{payload}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signing_input.as_bytes());
    let signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signing_input}.{signature}")
}

fn caller_claims(namespace: &str) -> serde_json::Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    serde_json::json!({
        "iss": "copal-probe",
        "iat": now,
        "exp": now + 3600,
        "ns": namespace,
        "db": namespace,
        "ac": "caller",
        "id": "caller_ident:ck1",
        "tn": "acme",
        "adm": false,
    })
}

async fn engine_with_credentials(namespace: &str) -> Surreal<surrealdb::engine::local::Db> {
    let config = Config::new().user(Root {
        username: "root".to_owned(),
        password: "root".to_owned(),
    });
    let root: Surreal<_> = Surreal::new::<Mem>(config).await.unwrap();
    root.signin(Root {
        username: "root".to_owned(),
        password: "root".to_owned(),
    })
    .await
    .unwrap();
    root.use_ns(namespace).use_db(namespace).await.unwrap();
    root
}

async fn run_all(root: &Surreal<surrealdb::engine::local::Db>, statements: &[&str]) {
    for statement in statements {
        let mut response = root.query(*statement).await.expect(statement);
        let errors = response.take_errors();
        assert!(errors.is_empty(), "{statement}: {errors:?}");
    }
}

/// The mechanism the pushdown project stands on: one connection, two
/// authority levels at once, with the engine filtering rows and
/// fields for the record session.
#[tokio::test]
async fn per_caller_record_sessions_enforce_permissions() {
    let root = engine_with_credentials("sessions").await;
    run_all(
        &root,
        &[
            "DEFINE ACCESS caller ON DATABASE TYPE RECORD \
             SIGNIN (SELECT * FROM caller_ident WHERE key_id = $key) \
             WITH JWT ALGORITHM HS256 KEY 'probe-secret' DURATION FOR SESSION 1h;",
            "DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE tenant = $token.tn \
             FOR create, update, delete NONE;",
            "DEFINE FIELD body ON doc PERMISSIONS FULL;",
            "DEFINE FIELD secret_note ON doc PERMISSIONS FOR select WHERE $token.adm = true;",
            "CREATE doc SET tenant = 'acme', body = 'ours', secret_note = 'attributed';",
            "CREATE doc SET tenant = 'rival', body = 'theirs', secret_note = 'attributed';",
        ],
    )
    .await;

    // A cloned handle is its own session; authenticating it must not
    // touch the root session beside it.
    let caller = root.clone();
    caller
        .authenticate(jwt("probe-secret", caller_claims("sessions")))
        .await
        .expect("record JWT authenticates");

    let mut response = caller.query("SELECT * FROM doc;").await.unwrap();
    let rows: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1, "table permissions filter rows: {rows:?}");
    assert_eq!(rows[0]["tenant"], "acme");
    assert_eq!(rows[0]["body"], "ours");
    assert!(
        rows[0].get("secret_note").is_none(),
        "field permissions redact columns: {:?}",
        rows[0]
    );

    let mut response = root.query("SELECT * FROM doc;").await.unwrap();
    let root_rows: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(root_rows.len(), 2, "the root session keeps full authority");

    // A write the table forbids returns empty rows with NO error, so
    // the application layer stays the face that explains refusals.
    let mut response = caller
        .query("CREATE doc SET tenant = 'acme', body = 'sneaky';")
        .await
        .unwrap();
    assert!(response.take_errors().is_empty());
    let written: Vec<serde_json::Value> = response.take(0).unwrap_or_default();
    assert!(written.is_empty(), "refused write created: {written:?}");
    let mut response = root
        .query("SELECT count() FROM doc GROUP ALL;")
        .await
        .unwrap();
    let counts: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(counts[0]["count"], 2, "the refused write persisted nothing");
}

/// Tokens minted by Copal need no `SIGNIN` or `SIGNUP` clause on the
/// access method: `TYPE RECORD` with only a JWT verifier is kept as
/// record access, and a key-signed token with an `id` claim
/// authenticates against it with permissions enforced. The contrast
/// that matters when generating this DDL: plain `TYPE JWT` yields
/// database-level sessions, which BYPASS table permissions, so the
/// two words between ACCESS and WITH decide whether the layer exists.
#[tokio::test]
async fn record_access_needs_no_signin_clause() {
    let root = engine_with_credentials("no_signin").await;
    run_all(
        &root,
        &[
            "DEFINE ACCESS caller ON DATABASE TYPE RECORD \
             WITH JWT ALGORITHM HS256 KEY 'probe-secret' DURATION FOR SESSION 1h;",
            "DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE tenant = $token.tn \
             FOR create, update, delete NONE;",
            "CREATE doc SET tenant = 'acme', body = 'ours';",
            "CREATE doc SET tenant = 'rival', body = 'theirs';",
        ],
    )
    .await;
    let mut response = root.query("INFO FOR DB;").await.unwrap();
    let info: Vec<serde_json::Value> = response.take(0).unwrap();
    let access = info[0]["accesses"]["caller"].as_str().unwrap();
    assert!(
        access.contains("TYPE RECORD"),
        "access kind changed: {access}"
    );

    let caller = root.clone();
    caller
        .authenticate(jwt("probe-secret", caller_claims("no_signin")))
        .await
        .expect("record JWT authenticates without a SIGNIN clause");
    let mut response = caller.query("SELECT * FROM doc;").await.unwrap();
    let rows: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1, "permissions enforce: {rows:?}");
    assert_eq!(rows[0]["tenant"], "acme");
}

/// On an engine built without credentials the ANONYMOUS session acts
/// as owner and `PERMISSIONS` clauses are skipped for it silently.
/// The exposure is scoped to the anonymous session: enforcement
/// follows the actor, and the next test shows a record session
/// constrained on this same kind of engine.
#[tokio::test]
async fn engine_without_credentials_skips_permissions() {
    let root: Surreal<_> = Surreal::new::<Mem>(()).await.unwrap();
    root.use_ns("open").use_db("open").await.unwrap();
    run_all(
        &root,
        &[
            "DEFINE TABLE doc SCHEMALESS PERMISSIONS NONE;",
            "CREATE doc SET body = 'visible anyway';",
        ],
    )
    .await;
    let mut response = root.query("SELECT * FROM doc;").await.unwrap();
    let rows: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "PERMISSIONS NONE enforced on an open engine, revisit the pins"
    );
}

/// Sessions need no engine-side expiry: `DURATION FOR SESSION NONE`
/// parses, echoes, and authenticates. Copal's own bounds are the
/// lifetime authority: tokens expire in seconds, request sessions
/// drop in milliseconds, and streams end on their ceilings, so an
/// engine clock racing those is a second clock with nothing to add.
#[tokio::test]
async fn record_sessions_accept_no_expiry() {
    let root = engine_with_credentials("no_expiry").await;
    run_all(
        &root,
        &[
            "DEFINE ACCESS caller ON DATABASE TYPE RECORD              WITH JWT ALGORITHM HS256 KEY 'probe-secret' DURATION FOR SESSION NONE;",
            "DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE tenant = $token.tn              FOR create, update, delete NONE;",
            "CREATE doc SET tenant = 'acme', body = 'ours';",
        ],
    )
    .await;
    let mut response = root.query("INFO FOR DB;").await.unwrap();
    let info: Vec<serde_json::Value> = response.take(0).unwrap();
    let access = info[0]["accesses"]["caller"].as_str().unwrap();
    assert!(
        access.contains("FOR SESSION NONE") || !access.contains("FOR SESSION"),
        "session duration echoed unexpectedly: {access}"
    );

    let caller = root.clone();
    caller
        .authenticate(jwt("probe-secret", caller_claims("no_expiry")))
        .await
        .expect("record JWT authenticates against a no-expiry access method");
    let mut response = caller.query("SELECT * FROM doc;").await.unwrap();
    let rows: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1, "permissions still enforce: {rows:?}");
}

/// Enforcement follows the actor, and credentials only decide what
/// the anonymous session may do: a record session is filtered even
/// on an engine built without them. The caller-session layer works
/// on any engine; what an open engine leaves unprotected is its
/// anonymous front door.
#[tokio::test]
async fn record_sessions_enforce_on_credential_less_engines() {
    let root: Surreal<_> = Surreal::new::<Mem>(()).await.unwrap();
    root.use_ns("open_record")
        .use_db("open_record")
        .await
        .unwrap();
    run_all(
        &root,
        &[
            "DEFINE ACCESS caller ON DATABASE TYPE RECORD              WITH JWT ALGORITHM HS256 KEY 'probe-secret' DURATION FOR SESSION 1h;",
            "DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE tenant = $token.tn              FOR create, update, delete NONE;",
            "CREATE doc SET tenant = 'acme', body = 'ours';",
            "CREATE doc SET tenant = 'rival', body = 'theirs';",
        ],
    )
    .await;
    let caller = root.clone();
    caller
        .authenticate(jwt("probe-secret", caller_claims("open_record")))
        .await
        .expect("record JWT authenticates on a credential-less engine");
    let mut response = caller.query("SELECT * FROM doc;").await.unwrap();
    let rows: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1, "the record actor is constrained: {rows:?}");
    assert_eq!(rows[0]["tenant"], "acme");
}

/// The per-chunk conjunct stands on this: a `PERMISSIONS FOR select`
/// clause may traverse a record link and read a column of the row it
/// points at, evaluated per candidate row. That is what lets the
/// engine's second layer state `(access IS NONE OR access != 'grant')
/// AND file.access != 'grant'` on `text_chunk` and filter withheld
/// passages even for a session whose query carries no WHERE at all.
#[tokio::test]
async fn select_permissions_traverse_record_links() {
    let root = engine_with_credentials("link_perm").await;
    run_all(
        &root,
        &[
            "DEFINE ACCESS caller ON DATABASE TYPE RECORD \
             WITH JWT ALGORITHM HS256 KEY 'probe-secret' DURATION FOR SESSION 1h;",
            "DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select FULL \
             FOR create, update, delete NONE;",
            "DEFINE TABLE chunk SCHEMALESS PERMISSIONS FOR select \
             WHERE (access IS NONE OR access != 'grant') AND file.access != 'grant' \
             FOR create, update, delete NONE;",
            "CREATE doc:open SET access = 'private';",
            "CREATE doc:sealed SET access = 'grant';",
            "CREATE chunk:plain SET file = doc:open, body = 'served';",
            "CREATE chunk:marked SET file = doc:open, access = 'grant', body = 'withheld';",
            "CREATE chunk:sealedfile SET file = doc:sealed, body = 'withheld too';",
        ],
    )
    .await;

    let caller = root.clone();
    caller
        .authenticate(jwt("probe-secret", caller_claims("link_perm")))
        .await
        .expect("record JWT authenticates");
    let mut response = caller
        .query("SELECT meta::id(id) AS id FROM chunk;")
        .await
        .unwrap();
    let rows: Vec<serde_json::Value> = response.take(0).unwrap();
    let ids: Vec<&str> = rows.iter().filter_map(|r| r["id"].as_str()).collect();
    assert_eq!(
        ids,
        vec!["plain"],
        "the marked chunk and the sealed file's chunk do not exist for the caller",
    );

    // The root session sees all three, so the filtering above was the
    // permission clause doing work rather than the rows being absent.
    let mut response = root
        .query("SELECT meta::id(id) AS id FROM chunk;")
        .await
        .unwrap();
    let rows: Vec<serde_json::Value> = response.take(0).unwrap();
    assert_eq!(rows.len(), 3, "{rows:?}");
}
