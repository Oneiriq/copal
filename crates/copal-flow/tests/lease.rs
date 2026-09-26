//! A step that runs past its run's lease keeps the lease. Without
//! renewal the reaper returns the run to pending mid-step, and a
//! second worker starts the same step again.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use copal_core::TenantId;
use copal_flow::{FlowEngine, FlowRegistry, RunSpec};
use copal_store::repo::flow as flow_repo;
use copal_store::{Store, StoreConfig};

#[tokio::test]
async fn a_step_longer_than_the_lease_is_neither_reaped_nor_claimed_twice() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let starts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&starts);
    let registry = FlowRegistry::new()
        .activity("slow", move |input: Value| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
                Ok(input)
            }
        })
        .workflow("slow", &["slow"], 1);
    let mut engine = FlowEngine::new(store.clone(), registry);
    engine.lease_secs = 2;
    let tenant = TenantId::parse("acme").unwrap();
    let (run_id, _) = engine
        .enqueue(&tenant, "slow", RunSpec::default())
        .await
        .unwrap();

    let first = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.tick("first").await })
    };
    // Past the lease the claim took, with the step still running.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        flow_repo::reap_expired_runs(&store).await.unwrap(),
        0,
        "a renewed lease is not expired",
    );
    assert!(
        !engine.tick("second").await.unwrap(),
        "nothing is pending for a second worker",
    );

    assert!(first.await.unwrap().unwrap());
    let (run, _) = engine.run_state(&tenant, &run_id).await.unwrap().unwrap();
    assert_eq!(run.status, "completed");
    assert_eq!(starts.load(Ordering::SeqCst), 1, "the step ran once");
}
