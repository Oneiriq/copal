//! The planner joins the contract gate.
//!
//! The static gate (`tests/contract.rs`) proves an index EXISTS for
//! every filter and sort claim and that the search backing names a
//! real index; it cannot prove the planner USES them. That gap is
//! where listings degrade without any commit changing anything this
//! repo checks: an engine upgrade re-costs a plan, and a query that
//! still answers correctly starts walking the table to do it. So this
//! test stands up the real schema on the embedded engine and runs
//! kayak's `EXPLAIN` verification over THE contract: one probe per
//! filter claim, per sort claim, and per search backing, convicting
//! on a table walk and on a backing whose plan does not reach its
//! named index. It runs on every pull request and on the scheduled
//! main run, which is the channel that exists precisely for drift
//! arriving without a commit.
//!
//! The search backing is lexical-only today (`idx_chunk_body`), so
//! the static schema the store applies on connect carries everything
//! the probes ask about. The vector index is deliberately outside
//! this gate the same way it is outside the contract: it exists only
//! in deployments that configure an embedding model, and a claim that
//! is true only sometimes is not a claim the contract makes.

use copal_server::contract::contract;
use copal_store::{Store, StoreConfig};
use kayak::verify::verify_contract;

#[tokio::test]
async fn the_planner_serves_every_claim_the_contract_makes() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let violations = verify_contract(store.raw(), &contract())
        .await
        .expect("verification runs against the embedded engine");
    assert_eq!(violations, vec![], "a claim planned as a table walk");
}

/// Sanity that the gate is not vacuously green, mirroring the static
/// gate's own check: a backing whose declared index the plan cannot
/// reach must be convicted by name. The probe still runs `@@` over a
/// column the real full-text index serves, so the conviction is
/// specifically "served, but not by the declared machinery" - the
/// drift this expectation exists to catch.
#[tokio::test]
async fn the_gate_convicts_a_backing_that_misses_its_named_index() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let mut drifted = contract();
    let backing = drifted
        .queries
        .iter_mut()
        .find(|query| query.name == "search")
        .expect("the search query is in the contract")
        .backing
        .first_mut()
        .expect("the search query declares a backing");
    assert_eq!(
        backing.index, "idx_chunk_body",
        "the fixture edits the real claim"
    );
    backing.index = "idx_chunk_body_retired".into();

    let violations = verify_contract(store.raw(), &drifted)
        .await
        .expect("verification runs against the embedded engine");
    let rendered: Vec<String> = violations.iter().map(ToString::to_string).collect();
    assert_eq!(violations.len(), 1, "{rendered:?}");
    assert!(
        violations[0]
            .claim
            .contains("lexical backing text_chunk.body"),
        "{rendered:?}",
    );
    assert!(
        violations[0].operation.contains("idx_chunk_body"),
        "the violation names what answered instead: {rendered:?}",
    );
}
