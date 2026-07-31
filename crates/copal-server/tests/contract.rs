//! The Janus contract gate, live in its first consumer.
//!
//! The files contract is declared here over `copal-store`'s REAL schema
//! definitions — not a copy — so three failure classes become test
//! failures in this repo:
//!
//! 1. Contract-vs-schema drift: exposing a renamed/dropped column, or
//!    declaring a filter/sort no index can serve, fails validation with
//!    the offending name.
//! 2. Document drift: the generated OpenAPI must match the checked-in
//!    `docs/openapi.json` byte for byte (`COPAL_BLESS=1` re-blesses
//!    deliberately).
//! 3. Index regressions: dropping `idx_file_listing` (or demoting its
//!    prefix) breaks the `created_at` sort claim and fails here.

use janus::{generate_openapi, validate, Contract, FieldExposure, Resource};

fn files_contract() -> Contract {
    Contract {
        name: "copal".into(),
        version: "0.1.0".into(),
        ir_revision: 1,
        resources: vec![Resource {
            name: "files".into(),
            table: "file".into(),
            fields: vec![
                FieldExposure::column("path"),
                FieldExposure::column("state"),
                FieldExposure::column("access"),
                FieldExposure::column("content_type"),
                FieldExposure::renamed("size_bytes", "size"),
                FieldExposure::column("digest"),
                FieldExposure::column("version_count"),
                FieldExposure::column("created_at"),
                FieldExposure::column("updated_at"),
            ],
            // tenant_id is server-bound on every query; it is what
            // lets the prefix rule credit idx_file_listing for the
            // created_at sort.
            pinned: vec!["tenant_id".into()],
            filterable: vec!["state".into()],
            sortable: vec!["created_at".into()],
            max_page_size: 100,
        }],
    }
}

#[test]
fn contract_validates_against_the_real_schema() {
    let schema = copal_store::schema::tables();
    let violations = validate(&files_contract(), &schema);
    assert_eq!(violations, vec![], "contract drifted from schema");
}

#[test]
fn generated_openapi_matches_the_checked_in_document() {
    let schema = copal_store::schema::tables();
    let doc = generate_openapi(&files_contract(), &schema).expect("contract generates");
    let rendered = serde_json::to_string_pretty(&doc).unwrap();

    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/openapi.json");
    if std::env::var("COPAL_BLESS").is_ok() {
        std::fs::write(path, format!("{rendered}\n")).unwrap();
    }
    let checked_in = std::fs::read_to_string(path)
        .expect("docs/openapi.json missing — run with COPAL_BLESS=1 to create");
    assert_eq!(
        rendered.trim(),
        checked_in.trim(),
        "OpenAPI drifted from docs/openapi.json; COPAL_BLESS=1 to re-bless deliberately",
    );
}

#[test]
fn the_gate_actually_fires_on_an_unindexed_sort() {
    // Sanity that the gate is not vacuously green: an unindexable sort
    // over the REAL schema must be refused by name.
    let mut contract = files_contract();
    contract.resources[0].sortable.push("digest".into());
    let violations = validate(&contract, &copal_store::schema::tables());
    assert!(
        violations.iter().any(|v| v.to_string().contains("digest")),
        "expected a named refusal, got {violations:?}",
    );
}
