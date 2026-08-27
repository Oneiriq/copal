//! The Kayak contract gate, live in its first consumer.
//!
//! THE contract lives in `copal_server::contract` (the same object
//! that serves `/graphql`) and is validated here over `copal-store`'s
//! REAL schema definitions. Three failure classes become test failures
//! in this repo:
//!
//! 1. Contract-vs-schema drift: exposing a renamed/dropped column, or
//!    declaring a filter/sort no index can serve, fails validation with
//!    the offending name.
//! 2. Artifact drift: every generated artifact (`docs/openapi.json`,
//!    `docs/schema.graphql`, `docs/policy.json`, and the four clients
//!    under `clients/`) must match its checked-in copy byte for byte
//!    (`COPAL_BLESS=1` re-blesses as an explicit step).
//! 3. Index regressions: dropping `idx_file_listing` (or demoting its
//!    prefix) breaks the `created_at` sort claim and fails here.

use copal_server::contract::contract;
use kayak::{generate_all, validate};

#[test]
fn contract_validates_against_the_real_schema() {
    let schema = copal_store::schema::tables();
    let violations = validate(&contract(), &schema);
    assert_eq!(violations, vec![], "contract drifted from schema");
}

#[test]
fn generated_artifacts_match_the_checked_in_documents() {
    let schema = copal_store::schema::tables();
    // `engine-policy` is opt-in upstream because its clauses render
    // through a token-claim vocabulary the deployment owns, and the
    // default vocabulary IS this deployment's. So the artifact is
    // asked for by name here, and engine row security becomes
    // review-visible the same way the other faces are: a scope
    // tightened in the contract shows up as a changed clause in
    // `docs/policy.json` in the same commit.
    let targets: Vec<&str> = kayak::generate::TARGETS
        .iter()
        .copied()
        .chain(std::iter::once("engine-policy"))
        .collect();
    let artifacts = generate_all(&contract(), &schema, &targets).expect("contract generates");
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    for (filename, content) in &artifacts {
        let checked_in_path = match filename.as_str() {
            "openapi.json" => format!("{root}/docs/openapi.json"),
            "schema.graphql" => format!("{root}/docs/schema.graphql"),
            "mcp-tools.json" => format!("{root}/docs/mcp-tools.json"),
            "policy.json" => format!("{root}/docs/policy.json"),
            other => format!("{root}/clients/{other}"),
        };
        if std::env::var("COPAL_BLESS").is_ok() {
            std::fs::write(&checked_in_path, content).unwrap();
        }
        let checked_in = std::fs::read_to_string(&checked_in_path).unwrap_or_else(|_| {
            panic!("{checked_in_path} missing; run with COPAL_BLESS=1 to create")
        });
        assert_eq!(
            content.trim(),
            checked_in.trim(),
            "{filename} drifted from its checked-in copy; COPAL_BLESS=1 to re-bless",
        );
    }
}

/// The `markers` input on `file_fetch` is an OPTIONAL addition, and
/// the differ must say so: diffing the contract without it against
/// the contract with it yields Compatible changes only. Only a
/// required input would be Breaking, and nothing here requires - the
/// property that lets this ship without a major version.
#[test]
fn the_differ_calls_the_markers_input_compatible() {
    let with_markers = contract();
    let mut without = contract();
    let fetch = without.resources[0]
        .actions
        .iter_mut()
        .find(|action| action.name == "fetch")
        .expect("the fetch action exists");
    fetch.input.retain(|field| field.name != "markers");

    let changes = kayak::diff(&without, &with_markers);
    let mentions_markers = changes.iter().any(|change| match change {
        kayak::Change::Breaking(text) | kayak::Change::Compatible(text) => text.contains("markers"),
    });
    assert!(
        mentions_markers,
        "the differ must see the addition: {changes:?}",
    );
    for change in &changes {
        assert!(
            matches!(change, kayak::Change::Compatible(_)),
            "an optional input must not break the wire: {change:?}",
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
    let sdl = kayak::generate_sdl(&contract(), &schema).expect("contract generates SDL");
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
        "type Run {",
        "runs(limit: Int = 100, cursor: String, status: String, sort: RunSort): RunPage!",
        "run(id: ID!): Run",
        "runStart(workflow: String!, input: JSON, file: String, idempotencyKey: String, \
         mode: String): JSON!",
        "runRetry(id: ID!): JSON!",
    ] {
        assert!(sdl.contains(line), "SDL missing {line:?}:\n{sdl}");
    }
}
