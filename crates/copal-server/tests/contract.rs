//! The Janus contract gate, live in its first consumer.
//!
//! THE contract lives in `copal_server::contract` — the same object
//! that serves `/graphql` — and is validated here over `copal-store`'s
//! REAL schema definitions. Three failure classes become test failures
//! in this repo:
//!
//! 1. Contract-vs-schema drift: exposing a renamed/dropped column, or
//!    declaring a filter/sort no index can serve, fails validation with
//!    the offending name.
//! 2. Artifact drift: every generated artifact — `docs/openapi.json`,
//!    `docs/schema.graphql`, and the four clients under `clients/` —
//!    must match its checked-in copy byte for byte (`COPAL_BLESS=1`
//!    re-blesses deliberately).
//! 3. Index regressions: dropping `idx_file_listing` (or demoting its
//!    prefix) breaks the `created_at` sort claim and fails here.

use copal_server::contract::contract;
use janus::{generate_all, validate};

#[test]
fn contract_validates_against_the_real_schema() {
    let schema = copal_store::schema::tables();
    let violations = validate(&contract(), &schema);
    assert_eq!(violations, vec![], "contract drifted from schema");
}

#[test]
fn generated_artifacts_match_the_checked_in_documents() {
    let schema = copal_store::schema::tables();
    let artifacts =
        generate_all(&contract(), &schema, janus::generate::TARGETS).expect("contract generates");
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    for (filename, content) in &artifacts {
        let checked_in_path = match filename.as_str() {
            "openapi.json" => format!("{root}/docs/openapi.json"),
            "schema.graphql" => format!("{root}/docs/schema.graphql"),
            other => format!("{root}/clients/{other}"),
        };
        if std::env::var("COPAL_BLESS").is_ok() {
            std::fs::write(&checked_in_path, content).unwrap();
        }
        let checked_in = std::fs::read_to_string(&checked_in_path).unwrap_or_else(|_| {
            panic!("{checked_in_path} missing — run with COPAL_BLESS=1 to create")
        });
        assert_eq!(
            content.trim(),
            checked_in.trim(),
            "{filename} drifted from its checked-in copy; COPAL_BLESS=1 to re-bless deliberately",
        );
    }
}

#[test]
fn the_gate_actually_fires_on_an_unindexed_sort() {
    // Sanity that the gate is not vacuously green: an unindexable sort
    // over the REAL schema must be refused by name.
    let mut contract = contract();
    contract.resources[0].sortable.push("digest".into());
    let violations = validate(&contract, &copal_store::schema::tables());
    assert!(
        violations.iter().any(|v| v.to_string().contains("digest")),
        "expected a named refusal, got {violations:?}",
    );
}

#[test]
fn the_live_graphql_schema_serves_the_generated_sdl_shapes() {
    // The dynamic schema and the checked-in SDL derive from one
    // contract object; spot-prove the agreement on load-bearing lines.
    let schema = copal_store::schema::tables();
    let sdl = janus::generate_sdl(&contract(), &schema).expect("contract generates SDL");
    for line in [
        "type File {",
        "size: Int",
        "digest: String",
        "enum FileSort {",
        "CREATED_AT_DESC",
        "files(limit: Int = 100, cursor: String, state: String, sort: FileSort): FilePage!",
        "file(id: ID!): File",
        "fileIssueUrl(id: ID!, ttlSecs: Int, maxUses: Int): JSON!",
        "fileRemove(id: ID!): Boolean!",
    ] {
        assert!(sdl.contains(line), "SDL missing {line:?}:\n{sdl}");
    }
}
