//! Copal durable execution.
//!
//! Activities are plain async functions over JSON values: typed at
//! their edges, idempotent by contract, and unaware of orchestration.
//! A workflow is a registered pipeline of activity names (each step's
//! output is the next step's input), and the engine runs it in two
//! modes over the SAME journal:
//!
//! - sync: claim, execute, and return the output in one call;
//! - async: enqueue now, a worker claims and executes later.
//!
//! Durability lives in the journal, never in the process. Every step attempt is a
//! row; completed steps replay from their recorded output without
//! re-executing, so a worker crash costs a lease TTL and no side
//! effect ever repeats. That property is the entire point of the design and is
//! proven by test with a side-effect counter.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use copal_core::{CopalError, FileId, TenantId};
use copal_store::repo::{file as file_repo, flow as flow_repo};
use copal_store::Store;

pub use copal_store::repo::flow::{RunRow, StepRow};

/// A registered activity: JSON in, JSON out.
pub type Activity = Arc<
    dyn Fn(Value) -> Pin<Box<dyn Future<Output = copal_core::Result<Value>> + Send>> + Send + Sync,
>;

/// One registered workflow: an ordered pipeline of activity names.
#[derive(Clone)]
pub struct WorkflowDef {
    pub key: String,
    pub steps: Vec<String>,
    /// Per-step attempt ceiling before the run fails.
    pub max_attempts: i64,
    /// Whether a caller may start this workflow through the runs API.
    /// Server-only workflows trust their input (the file, the digest,
    /// the residency), because the server built it after its own
    /// checks; a caller-supplied input must never reach them.
    pub caller_startable: bool,
}

/// The registry: activities by name, workflows by key. Built once at
/// startup, shared by handle.
#[derive(Clone, Default)]
pub struct FlowRegistry {
    activities: BTreeMap<String, Activity>,
    workflows: BTreeMap<String, WorkflowDef>,
}

impl FlowRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an activity under `name`.
    pub fn activity<F, Fut>(mut self, name: &str, function: F) -> Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = copal_core::Result<Value>> + Send + 'static,
    {
        let function = Arc::new(function);
        self.activities.insert(
            name.to_owned(),
            Arc::new(move |input| {
                let function = Arc::clone(&function);
                Box::pin(async move { function(input).await })
            }),
        );
        self
    }

    /// Register a server-only workflow as a pipeline of activity
    /// names. Only server code starts it; the runs API refuses it.
    pub fn workflow(self, key: &str, steps: &[&str], max_attempts: i64) -> Self {
        self.register(key, steps, max_attempts, false)
    }

    /// Register a workflow callers may start through the runs API.
    /// Its activities receive caller-supplied input and must treat
    /// every field as untrusted.
    pub fn caller_workflow(self, key: &str, steps: &[&str], max_attempts: i64) -> Self {
        self.register(key, steps, max_attempts, true)
    }

    fn register(mut self, key: &str, steps: &[&str], max_attempts: i64, caller: bool) -> Self {
        self.workflows.insert(
            key.to_owned(),
            WorkflowDef {
                key: key.to_owned(),
                steps: steps.iter().map(|s| (*s).to_owned()).collect(),
                max_attempts: max_attempts.max(1),
                caller_startable: caller,
            },
        );
        self
    }

    fn workflow_def(&self, key: &str) -> copal_core::Result<&WorkflowDef> {
        self.workflows
            .get(key)
            .ok_or_else(|| CopalError::not_found(format!("workflow {key}")))
    }

    fn activity_fn(&self, name: &str) -> copal_core::Result<&Activity> {
        self.activities
            .get(name)
            .ok_or_else(|| CopalError::validation(format!("unregistered activity {name}")))
    }
}

/// The engine: a registry plus the store, cheap to clone.
#[derive(Clone)]
pub struct FlowEngine {
    store: Store,
    registry: Arc<FlowRegistry>,
    /// Claim lease for run execution.
    pub lease_secs: u32,
}

/// Parameters for starting a run.
#[derive(Debug, Clone, Default)]
pub struct RunSpec {
    pub input: Value,
    pub subject: Option<FileId>,
    pub idempotency_key: Option<String>,
}

impl FlowEngine {
    pub fn new(store: Store, registry: FlowRegistry) -> Self {
        Self {
            store,
            registry: Arc::new(registry),
            lease_secs: 300,
        }
    }

    /// Whether a workflow key is registered.
    pub fn has_workflow(&self, key: &str) -> bool {
        self.registry.workflows.contains_key(key)
    }

    /// Refuse a caller-started run of anything but a caller workflow:
    /// unknown keys are not found, server-only keys are forbidden.
    pub fn ensure_caller_startable(&self, key: &str) -> copal_core::Result<()> {
        if self.registry.workflow_def(key)?.caller_startable {
            Ok(())
        } else {
            Err(CopalError::forbidden(format!(
                "workflow {key} is started by the server, not through the runs API",
            )))
        }
    }

    /// Enqueue a run for a worker. Unknown workflows are refused at the
    /// door, not at claim time.
    pub async fn enqueue(
        &self,
        tenant: &TenantId,
        workflow_key: &str,
        spec: RunSpec,
    ) -> copal_core::Result<(String, bool)> {
        self.registry.workflow_def(workflow_key)?;
        flow_repo::enqueue(
            &self.store,
            tenant,
            workflow_key,
            spec.input,
            spec.subject.as_ref(),
            spec.idempotency_key.as_deref(),
        )
        .await
    }

    /// Enqueue, claim, and execute in-process. `Some(output)` means the
    /// run completed here (or an idempotent replay had already
    /// completed); `None` means a worker holds it and the caller polls
    /// (distinct from a run whose output is legitimately null). Same
    /// journal as the async path; sync changes who waits and nothing
    /// about durability.
    pub async fn run_sync(
        &self,
        tenant: &TenantId,
        workflow_key: &str,
        spec: RunSpec,
    ) -> copal_core::Result<(String, Option<Value>)> {
        let (run_id, created) = self.enqueue(tenant, workflow_key, spec).await?;
        if !created {
            // Idempotent replay: report the existing run's state; a
            // still-running original reads as pending, not as output.
            let run = flow_repo::get_run(&self.store, tenant, &run_id)
                .await?
                .ok_or_else(|| CopalError::not_found("run"))?;
            let output = match run.status.as_str() {
                "completed" => Some(run.output.unwrap_or(Value::Null)),
                _ => None,
            };
            return Ok((run_id, output));
        }
        // Claim exactly the run just enqueued, and never a neighbor this
        // path would then abandon to its lease.
        let claimed =
            flow_repo::claim_specific(&self.store, &run_id, "sync", self.lease_secs).await?;
        match claimed {
            Some(run) => {
                let output = self.execute_claimed(&run).await?;
                Ok((run_id, Some(output)))
            }
            // A worker got there first; the caller polls like anyone.
            None => Ok((run_id, None)),
        }
    }

    /// One worker tick: claim the oldest pending run and execute it to
    /// a terminal status. Returns whether anything was claimed.
    pub async fn tick(&self, owner: &str) -> copal_core::Result<bool> {
        let Some(run) = flow_repo::claim_next_pending(&self.store, owner, self.lease_secs).await?
        else {
            return Ok(false);
        };
        // Execution errors land on the run row; the tick itself only
        // fails on store trouble.
        let _ = self.execute_claimed(&run).await;
        Ok(true)
    }

    /// Execute a claimed run through its pipeline, replaying completed
    /// steps from the journal.
    async fn execute_claimed(&self, run: &RunRow) -> copal_core::Result<Value> {
        let run_id = run.run_id();
        let def = match self.registry.workflow_def(&run.workflow_key) {
            Ok(def) => def,
            Err(err) => {
                flow_repo::finish_run(&self.store, &run_id, "failed", None, Some(&err.to_string()))
                    .await?;
                self.fail_scanning_subject(run).await;
                return Err(err);
            }
        };

        let journal = flow_repo::completed_steps(&self.store, &run_id).await?;
        let mut carried = bind_tenant(run.input.clone(), &run.tenant_id);
        for step_key in &def.steps {
            if let Some(recorded) = journal.get(step_key) {
                // Replay: the step already happened; its recorded
                // output is the truth and the activity must not run
                // again.
                carried = recorded.clone();
                continue;
            }
            carried = match self.execute_step(def, &run_id, step_key, carried).await {
                Ok(output) => output,
                Err(err) => {
                    let message = err.to_string();
                    flow_repo::finish_run(&self.store, &run_id, "failed", None, Some(&message))
                        .await?;
                    self.fail_scanning_subject(run).await;
                    return Err(err);
                }
            };
        }

        flow_repo::finish_run(&self.store, &run_id, "completed", Some(&carried), None).await?;
        Ok(carried)
    }

    /// A terminally failed run fails its scanning subject: the file
    /// leaves the transient `scanning` state for retryable `failed`
    /// ahead of the age sweep. Best-effort: a
    /// conflict means the subject was not scanning (non-pipeline run,
    /// or something else already moved it), which is fine.
    async fn fail_scanning_subject(&self, run: &RunRow) {
        let Some(parsed) = run.file_id() else {
            return;
        };
        let (Ok(file), Ok(tenant)) = (parsed, TenantId::parse(&run.tenant_id)) else {
            return;
        };
        match file_repo::transition(
            &self.store,
            &tenant,
            &file,
            copal_core::FileState::Scanning,
            copal_core::FileState::Failed,
            Default::default(),
        )
        .await
        {
            Ok(_) => {
                tracing::warn!(run = %run.run_id(), file = %file, "failed run moved its subject scanning -> failed");
            }
            Err(CopalError::Conflict(_)) => {}
            Err(err) => {
                tracing::warn!(run = %run.run_id(), file = %file, error = %err, "failed-run propagation could not move the subject");
            }
        }
    }

    /// Execute one step with journaled attempts and in-process retry up
    /// to the workflow's ceiling, renewing the run's lease across
    /// every attempt.
    async fn execute_step(
        &self,
        def: &WorkflowDef,
        run_id: &str,
        step_key: &str,
        input: Value,
    ) -> copal_core::Result<Value> {
        self.renewing_lease(run_id, self.attempt_step(def, run_id, step_key, input))
            .await
    }

    /// The journaled attempts of one step.
    async fn attempt_step(
        &self,
        def: &WorkflowDef,
        run_id: &str,
        step_key: &str,
        input: Value,
    ) -> copal_core::Result<Value> {
        let activity = self.registry.activity_fn(step_key)?;
        let mut attempt = flow_repo::last_attempt(&self.store, run_id, step_key).await? + 1;
        loop {
            let step_id = flow_repo::open_step(&self.store, run_id, step_key, attempt).await?;
            match activity(input.clone()).await {
                Ok(output) => {
                    flow_repo::close_step(&self.store, &step_id, "completed", Some(&output), None)
                        .await?;
                    return Ok(output);
                }
                Err(err) => {
                    let message = err.to_string();
                    flow_repo::close_step(&self.store, &step_id, "failed", None, Some(&message))
                        .await?;
                    if attempt >= def.max_attempts {
                        return Err(CopalError::Store(format!(
                            "step {step_key} failed after {attempt} attempts: {message}",
                        )));
                    }
                    attempt += 1;
                }
            }
        }
    }

    /// Drive a step while renewing its run's lease. A step can outlive
    /// the lease (a tier recall retries for hours while an archive
    /// restore completes), and an expired lease is reaped back to
    /// pending, where a second worker would start the same step again.
    /// Renewal comes every third of the lease, so one missed renewal
    /// leaves it live.
    async fn renewing_lease<T>(&self, run_id: &str, work: impl Future<Output = T>) -> T {
        let lease = self.lease_secs.max(1);
        let period = std::time::Duration::from_millis(u64::from(lease) * 1000 / 3);
        let mut beat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(work);
        loop {
            tokio::select! {
                output = &mut work => return output,
                _ = beat.tick() => {
                    match flow_repo::renew_lease(&self.store, run_id, lease).await {
                        Ok(true) => {}
                        Ok(false) => tracing::warn!(run = run_id, "run lease lost while a step ran"),
                        Err(err) => tracing::warn!(run = run_id, error = %err, "run lease renewal failed"),
                    }
                }
            }
        }
    }

    /// Run state, tenant-scoped.
    pub async fn run_state(
        &self,
        tenant: &TenantId,
        run_id: &str,
    ) -> copal_core::Result<Option<(RunRow, Vec<StepRow>)>> {
        let Some(run) = flow_repo::get_run(&self.store, tenant, run_id).await? else {
            return Ok(None);
        };
        let steps = flow_repo::list_steps(&self.store, run_id).await?;
        Ok(Some((run, steps)))
    }
}

/// The run's tenant is the authority over its input's. Activities read
/// the tenant from their input, so an input that names one names the
/// tenant the run was enqueued under, whatever its author wrote. A
/// second wall behind the runs API's refusal: no path that enqueues a
/// run can hand an activity another tenant's scope.
fn bind_tenant(mut input: Value, tenant: &str) -> Value {
    if let Some(named) = input.get_mut("tenant") {
        *named = Value::from(tenant);
    }
    input
}

/// The worker loop the server spawns: tick until empty, then idle for
/// the interval.
pub async fn run_worker(engine: FlowEngine, owner: String, idle_secs: u64) {
    loop {
        match engine.tick(&owner).await {
            Ok(true) => continue,
            Ok(false) => {
                tokio::time::sleep(std::time::Duration::from_secs(idle_secs.max(1))).await;
            }
            Err(err) => {
                tracing::warn!(error = %err, "worker tick failed");
                tokio::time::sleep(std::time::Duration::from_secs(idle_secs.max(1))).await;
            }
        }
    }
}
