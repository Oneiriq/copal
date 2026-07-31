//! The durability proofs, on mem://.
//!
//! The test that matters most: a run that fails mid-pipeline resumes
//! with its journal, and the completed step's side effect DOES NOT
//! REPEAT. That is the property that makes this an execution journal
//! rather than a job queue.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};

use copal_core::TenantId;
use copal_flow::{FlowEngine, FlowRegistry, RunSpec};
use copal_store::repo::flow as flow_repo;
use copal_store::{Store, StoreConfig};

async fn fresh_store() -> Store {
    Store::connect(StoreConfig::memory()).await.unwrap()
}

fn tenant() -> TenantId {
    TenantId::parse("acme").unwrap()
}

#[tokio::test]
async fn pipeline_threads_output_to_input() {
    let store = fresh_store().await;
    let registry = FlowRegistry::new()
        .activity("double", |input: Value| async move {
            let n = input["n"].as_i64().unwrap_or(0);
            Ok(json!({"n": n * 2}))
        })
        .activity("add_ten", |input: Value| async move {
            let n = input["n"].as_i64().unwrap_or(0);
            Ok(json!({"n": n + 10}))
        })
        .workflow("math", &["double", "add_ten"], 1);
    let engine = FlowEngine::new(store, registry);

    let (run_id, output) = engine
        .run_sync(
            &tenant(),
            "math",
            RunSpec {
                input: json!({"n": 4}),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(output, json!({"n": 18}));

    let (run, steps) = engine
        .run_state(&tenant(), &run_id)
        .await
        .unwrap()
        .expect("run exists");
    assert_eq!(run.status, "completed");
    assert_eq!(run.output, Some(json!({"n": 18})));
    assert_eq!(steps.len(), 2);
    assert!(steps.iter().all(|s| s.status == "completed"));
}

#[tokio::test]
async fn resume_replays_the_journal_without_repeating_side_effects() {
    let store = fresh_store().await;
    let side_effects = Arc::new(AtomicUsize::new(0));
    let should_fail = Arc::new(AtomicUsize::new(1)); // 1 = step two fails

    let counter = Arc::clone(&side_effects);
    let failer = Arc::clone(&should_fail);
    let registry = FlowRegistry::new()
        .activity("charge_card", move |input: Value| {
            // The activity a journal exists to protect: running it
            // twice would be a real-world incident.
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(input)
            }
        })
        .activity("flaky_notify", move |input: Value| {
            let failer = Arc::clone(&failer);
            async move {
                if failer.load(Ordering::SeqCst) == 1 {
                    Err(copal_core::CopalError::Store("smtp down".into()))
                } else {
                    Ok(input)
                }
            }
        })
        .workflow("payment", &["charge_card", "flaky_notify"], 1);
    let engine = FlowEngine::new(store.clone(), registry);

    // First execution: step one commits its side effect, step two
    // fails, the run fails.
    let (run_id, _) = engine
        .enqueue(
            &tenant(),
            "payment",
            RunSpec {
                input: json!({"amount": 42}),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(engine.tick("worker-1").await.unwrap());
    let (run, _) = engine.run_state(&tenant(), &run_id).await.unwrap().unwrap();
    assert_eq!(run.status, "failed");
    assert!(run.run_error.as_deref().unwrap_or("").contains("smtp down"));
    assert_eq!(side_effects.load(Ordering::SeqCst), 1);

    // The outage ends; the run is requeued WITH ITS JOURNAL.
    should_fail.store(0, Ordering::SeqCst);
    flow_repo::requeue(&store, &run_id).await.unwrap();
    assert!(engine.tick("worker-2").await.unwrap());

    let (run, steps) = engine.run_state(&tenant(), &run_id).await.unwrap().unwrap();
    assert_eq!(run.status, "completed");
    // THE PROOF: charge_card ran exactly once across both executions.
    assert_eq!(side_effects.load(Ordering::SeqCst), 1);
    // Journal shows: one completed charge, one failed notify attempt,
    // one completed notify attempt.
    let charge_rows: Vec<_> = steps
        .iter()
        .filter(|s| s.step_key == "charge_card")
        .collect();
    assert_eq!(charge_rows.len(), 1);
    let notify_rows: Vec<_> = steps
        .iter()
        .filter(|s| s.step_key == "flaky_notify")
        .collect();
    assert_eq!(notify_rows.len(), 2);
    assert_eq!(
        notify_rows.iter().filter(|s| s.status == "failed").count(),
        1
    );
    assert_eq!(
        notify_rows
            .iter()
            .filter(|s| s.status == "completed")
            .count(),
        1
    );
}

#[tokio::test]
async fn per_step_retry_ceiling_applies_within_one_execution() {
    let store = fresh_store().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&attempts);
    let registry = FlowRegistry::new()
        .activity("succeeds_third_try", move |input: Value| {
            let counter = Arc::clone(&counter);
            async move {
                if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(copal_core::CopalError::Store("transient".into()))
                } else {
                    Ok(input)
                }
            }
        })
        .workflow("stubborn", &["succeeds_third_try"], 3);
    let engine = FlowEngine::new(store, registry);

    let (run_id, output) = engine
        .run_sync(
            &tenant(),
            "stubborn",
            RunSpec {
                input: json!({"ok": true}),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(output, json!({"ok": true}));
    let (run, steps) = engine.run_state(&tenant(), &run_id).await.unwrap().unwrap();
    assert_eq!(run.status, "completed");
    assert_eq!(steps.len(), 3, "two failed attempts plus the success");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn idempotent_enqueue_and_lease_reap() {
    let store = fresh_store().await;
    let registry = FlowRegistry::new()
        .activity("noop", |input: Value| async move { Ok(input) })
        .workflow("simple", &["noop"], 1);
    let engine = FlowEngine::new(store.clone(), registry);

    // Same idempotency key -> same run, created once.
    let spec = || RunSpec {
        input: json!({}),
        idempotency_key: Some("evt-123".into()),
        ..Default::default()
    };
    let (first, created) = engine.enqueue(&tenant(), "simple", spec()).await.unwrap();
    assert!(created);
    let (second, created) = engine.enqueue(&tenant(), "simple", spec()).await.unwrap();
    assert!(!created);
    assert_eq!(first, second);

    // Unknown workflows are refused at the door.
    assert!(engine
        .enqueue(&tenant(), "nonexistent", RunSpec::default())
        .await
        .is_err());

    // A claim with an instantly-expired lease reaps back to pending,
    // journal intact, claimable again.
    let mut zero_lease = engine.clone();
    zero_lease.lease_secs = 0;
    let claimed = flow_repo::claim_next_pending(&store, "dying-worker", 0)
        .await
        .unwrap()
        .expect("claims the pending run");
    assert_eq!(claimed.status, "running");
    let reaped = flow_repo::reap_expired_runs(&store).await.unwrap();
    assert_eq!(reaped, 1);
    let (run, _) = engine.run_state(&tenant(), &first).await.unwrap().unwrap();
    assert_eq!(run.status, "pending");
    // And the normal worker finishes it.
    assert!(engine.tick("healthy-worker").await.unwrap());
    let (run, _) = engine.run_state(&tenant(), &first).await.unwrap().unwrap();
    assert_eq!(run.status, "completed");
}
