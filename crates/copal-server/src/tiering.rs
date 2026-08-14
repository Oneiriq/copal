//! Lifecycle and tiering: configuration, the policy surface, and
//! the observe-only classifier.
//!
//! Nothing in this module moves a byte. The classifier walks blob
//! rows exactly as the GC does, derives eligibility over every
//! reference (the most demanding reference wins), and REPORTS:
//! gauges for candidate blobs and bytes, and an admin listing of what
//! would move per tenant. Observation is not a formality; it is
//! where the recall-rate assumption in the design's cost model gets
//! measured against real traffic before a byte moves.

use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde_json::json;

use copal_blob::tier::TierClass;
use copal_blob::BlobStore;
use copal_core::{CopalError, TenantId};
use copal_store::repo::tier as tier_repo;
use copal_store::Store;

use crate::app::AppState;
use crate::error::ApiError;

/// Which tiers each residency configures, by class. Built at boot
/// from validated configuration and carried in [`AppState`]: policy
/// validation asks it whether a tier name exists, and the classifier
/// asks it whether a blob's own residency configures the named tier.
#[derive(Debug, Clone, Default)]
pub struct Topology {
    residencies: HashMap<String, HashMap<String, TierClass>>,
}

impl Topology {
    /// Register one residency's tiers.
    pub fn insert(&mut self, residency: &str, tiers: HashMap<String, TierClass>) {
        if !tiers.is_empty() {
            self.residencies.insert(residency.to_owned(), tiers);
        }
    }

    /// Whether any configured residency carries this tier name.
    pub fn knows_tier(&self, tier: &str) -> bool {
        self.residencies.values().any(|t| t.contains_key(tier))
    }

    /// Whether one residency carries this tier name.
    pub fn residency_has(&self, residency: &str, tier: &str) -> bool {
        self.residencies
            .get(residency)
            .is_some_and(|t| t.contains_key(tier))
    }

    /// Whether any tiers are configured at all.
    pub fn is_empty(&self) -> bool {
        self.residencies.is_empty()
    }
}

/// Record a byte read for the tiering classifier, fire-and-forget:
/// the response must never wait on recency bookkeeping, and a lost
/// write under-records at day granularity, which fails toward moving
/// content earlier -- latency, never loss. Callers are the byte
/// paths only (content and version GETs, ranges, S3 GETs, grant and
/// edge redemptions, a derive reading its source); listings and
/// metadata reads are answered from the metadata plane and must not
/// keep a corpus hot.
pub fn note_blob_read(store: &Store, residency: &str, digest: &copal_core::ContentDigest) {
    let store = store.clone();
    let residency = residency.to_owned();
    let digest = digest.clone();
    tokio::spawn(async move {
        if let Err(err) = tier_repo::note_read(&store, &residency, &digest).await {
            tracing::debug!(error = %err, "last_read note failed");
        }
    });
}

/// What the tiering policy surface accepts.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct TieringPolicyRequest {
    tier: String,
    after_seconds: u64,
    /// `created` ages from the version's creation; `accessed` (the
    /// default) ages from the last byte read, falling back to
    /// creation age when no read was ever recorded.
    #[serde(default)]
    basis: Option<String>,
    #[serde(default)]
    min_bytes: Option<u64>,
}

/// Set the tenant's tiering policy. Naming a tier no residency
/// configures refuses at validation, so a policy can never point
/// bytes at a backend that does not exist.
pub(crate) async fn set_policy<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    Json(request): Json<TieringPolicyRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !state.tiering.knows_tier(&request.tier) {
        return Err(CopalError::validation(format!(
            "no configured residency carries a tier named {}",
            request.tier,
        ))
        .into());
    }
    let basis = request
        .basis
        .clone()
        .unwrap_or_else(|| "accessed".to_owned());
    if basis != "created" && basis != "accessed" {
        return Err(CopalError::validation("basis must be created or accessed").into());
    }
    let policy = tier_repo::TieringPolicy {
        tier: request.tier.clone(),
        after_seconds: request.after_seconds,
        basis: basis.clone(),
        min_bytes: request.min_bytes.unwrap_or(0),
    };
    tier_repo::set_policy(&state.store, &tenant, &policy).await?;
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "tenant.tiering_policy_set",
        tenant.as_str(),
        crate::app::forwarded_origin(&headers).as_deref(),
        Some(json!({
            "tier": policy.tier,
            "after_seconds": policy.after_seconds,
            "basis": policy.basis,
            "min_bytes": policy.min_bytes,
        })),
    )
    .await?;
    Ok(Json(json!({
        "tier": policy.tier,
        "after_seconds": policy.after_seconds,
        "basis": policy.basis,
        "min_bytes": policy.min_bytes,
    })))
}

/// The tenant's tiering policy, when one is set.
pub(crate) async fn get_policy<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    match tier_repo::get_policy(&state.store, &tenant).await? {
        Some(policy) => Ok(Json(json!({
            "tier": policy.tier,
            "after_seconds": policy.after_seconds,
            "basis": policy.basis,
            "min_bytes": policy.min_bytes,
        }))),
        None => Err(CopalError::not_found("no tiering policy").into()),
    }
}

/// Remove the tenant's tiering policy. The classifier stops naming
/// this tenant's content on its next pass; nothing was moved, so
/// nothing needs moving back.
pub(crate) async fn clear_policy<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    tier_repo::clear_policy(&state.store, &tenant).await?;
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "tenant.tiering_policy_cleared",
        tenant.as_str(),
        crate::app::forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct PinRequest {
    pin: String,
}

/// Pin a file's content hot: the escape hatch for the file that is
/// old, cold by every measure, and needed in milliseconds anyway.
/// Audited like every operator action.
pub(crate) async fn set_pin<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, file)): Path<(String, String)>,
    Json(request): Json<PinRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let file = copal_core::FileId::parse(&file)?;
    if request.pin != "hot" {
        return Err(CopalError::validation("pin must be \"hot\"").into());
    }
    if !tier_repo::set_file_pin(&state.store, &tenant, &file, true).await? {
        return Err(CopalError::not_found(format!("file {file}")).into());
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "file.tier_pinned",
        file.as_str(),
        crate::app::forwarded_origin(&headers).as_deref(),
        Some(json!({ "pin": "hot" })),
    )
    .await?;
    Ok(Json(json!({ "pin": "hot" })))
}

/// Release a file's hot pin; the policy decides again from the next
/// pass.
pub(crate) async fn clear_pin<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path((tenant, file)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let operator = crate::auth::require_admin(&state, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let file = copal_core::FileId::parse(&file)?;
    if !tier_repo::set_file_pin(&state.store, &tenant, &file, false).await? {
        return Err(CopalError::not_found(format!("file {file}")).into());
    }
    copal_store::repo::auth::record_audit(
        &state.store,
        &tenant,
        operator.as_str(),
        "file.tier_pin_released",
        file.as_str(),
        crate::app::forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// One tenant's slice of a classification pass.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct TenantTally {
    /// Blobs every rule marks movable, and their bytes. A shared blob
    /// counts toward every tenant that references it -- the tenant
    /// view -- while the deployment totals count each blob once.
    pub candidate_blobs: u64,
    pub candidate_bytes: u64,
    /// Blobs old enough to move on creation age whose read recency
    /// holds them: the measured stand-in for the recall rate. Had the
    /// policy already demoted them at the threshold, these are the
    /// reads that would have paid retrieval.
    pub would_recall_blobs: u64,
    pub would_recall_bytes: u64,
}

/// What one observe-only pass found.
#[derive(Debug, Clone, Default)]
pub struct ClassifyReport {
    pub per_tenant: HashMap<String, TenantTally>,
    /// Distinct blobs and bytes that would move: the physical truth
    /// the per-tenant view can double-count (dedupe means one blob
    /// can serve many tenants).
    pub total_candidate_blobs: u64,
    pub total_candidate_bytes: u64,
    pub total_would_recall_blobs: u64,
    /// Blobs some rule held back, by reason, for the honest listing:
    /// a silent skip would read as "nothing is cold" when the truth
    /// is "cold but held".
    pub held: HashMap<&'static str, u64>,
}

/// Why a blob with at least one cold-marking policy did not classify
/// as a candidate.
const HELD_PIN: &str = "pinned";
const HELD_NO_POLICY: &str = "referenced_without_policy";
const HELD_TIER_MISMATCH: &str = "tier_not_in_residency";
const HELD_TIER_DISAGREE: &str = "policies_name_different_tiers";
const HELD_MIN_BYTES: &str = "below_min_bytes";
const HELD_YOUNG: &str = "not_yet_cold";

/// How far back a read still counts as "this would have been a
/// recall": one month, the unit tier pricing quotes retrieval in.
const RECALL_WINDOW_SECS: i64 = 30 * 86_400;

/// Classify one blob against the loaded policies. Answers the tally
/// bucket the blob lands in, or the hold reason that kept it out.
fn classify_row(
    row: &tier_repo::BlobClassifyRow,
    policies: &HashMap<String, tier_repo::TieringPolicy>,
    topology: &Topology,
) -> Result<Option<bool>, &'static str> {
    // Already cold, or nothing referencing it: not this feature's
    // business (the GC owns unreferenced rows).
    if row.tier.is_some() || row.referencing_tenants().next().is_none() {
        return Ok(None);
    }
    if !row.pinned_tenants.is_empty() {
        return Err(HELD_PIN);
    }
    // Every referencing tenant must carry a policy: a tenant without
    // one demands hot, and the most demanding reference wins.
    let mut target: Option<&str> = None;
    let mut coldest_min_bytes = 0u64;
    let mut all_cold = true;
    for tenant in row.referencing_tenants() {
        let Some(policy) = policies.get(tenant) else {
            return Err(HELD_NO_POLICY);
        };
        match target {
            None => target = Some(policy.tier.as_str()),
            Some(named) if named != policy.tier => return Err(HELD_TIER_DISAGREE),
            Some(_) => {}
        }
        coldest_min_bytes = coldest_min_bytes.max(policy.min_bytes);
        let created_age = row.created_age_secs.unwrap_or(0);
        let age = match policy.basis.as_str() {
            // No recorded read falls back to creation age: the other
            // honest basis, not "unknown means cold now".
            "accessed" => row.read_age_secs.unwrap_or(created_age),
            _ => created_age,
        };
        if age < policy.after_seconds as i64 {
            all_cold = false;
        }
    }
    let target = target.unwrap_or_default();
    if !topology.residency_has(&row.residency(), target) {
        return Err(HELD_TIER_MISMATCH);
    }
    if (row.size_bytes.max(0) as u64) < coldest_min_bytes {
        return Err(HELD_MIN_BYTES);
    }
    if all_cold {
        return Ok(Some(true));
    }
    // Not cold yet. When creation age alone would have moved it and a
    // recent read is what holds it, that read is a measured would-be
    // recall -- the figure the cost model assumes and observation
    // exists to check.
    let held_by_reads_alone = row.referencing_tenants().all(|tenant| {
        policies
            .get(tenant)
            .is_some_and(|policy| row.created_age_secs.unwrap_or(0) >= policy.after_seconds as i64)
    }) && row
        .read_age_secs
        .is_some_and(|age| age < RECALL_WINDOW_SECS);
    if held_by_reads_alone {
        return Ok(Some(false));
    }
    Err(HELD_YOUNG)
}

/// One observe-only pass: walk every blob row in keyset batches,
/// classify, and tally. Touches nothing; the report is the product.
pub async fn classify_pass(
    store: &Store,
    topology: &Topology,
) -> copal_core::Result<ClassifyReport> {
    let mut report = ClassifyReport::default();
    let policies: HashMap<String, tier_repo::TieringPolicy> =
        tier_repo::all_policies(store).await?.into_iter().collect();
    // No tiering policy: nothing classifies, nothing moves. Today's
    // behavior exactly, including the walk's absence.
    if policies.is_empty() {
        return Ok(report);
    }
    let mut after: Option<String> = None;
    loop {
        let rows = tier_repo::list_for_classify(store, 1_000, after.as_deref()).await?;
        let drained = (rows.len() as i64) < 1_000;
        let mut last = None;
        for row in &rows {
            last = Some(row.bare_id());
            match classify_row(row, &policies, topology) {
                Ok(None) => {}
                Ok(Some(candidate)) => {
                    let bytes = row.size_bytes.max(0) as u64;
                    if candidate {
                        report.total_candidate_blobs += 1;
                        report.total_candidate_bytes += bytes;
                    } else {
                        report.total_would_recall_blobs += 1;
                    }
                    let mut seen: Vec<&str> = Vec::new();
                    for tenant in row.referencing_tenants() {
                        if seen.contains(&tenant) {
                            continue;
                        }
                        seen.push(tenant);
                        let tally = report.per_tenant.entry(tenant.to_owned()).or_default();
                        if candidate {
                            tally.candidate_blobs += 1;
                            tally.candidate_bytes += bytes;
                        } else {
                            tally.would_recall_blobs += 1;
                            tally.would_recall_bytes += bytes;
                        }
                    }
                }
                Err(reason) => {
                    *report.held.entry(reason).or_default() += 1;
                }
            }
        }
        if drained {
            break;
        }
        match last {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    Ok(report)
}

/// The sweep-side pass: classify, then publish the gauges. Skips
/// entirely when no tiers are configured, because no policy can
/// exist without one to name.
pub async fn observe_pass(store: &Store, topology: &Topology) {
    if topology.is_empty() {
        return;
    }
    match classify_pass(store, topology).await {
        Ok(report) => {
            crate::metrics::set_gauge(
                "copal_tiering_candidate_blobs",
                report.total_candidate_blobs,
            );
            crate::metrics::set_gauge(
                "copal_tiering_candidate_bytes",
                report.total_candidate_bytes,
            );
            crate::metrics::set_gauge(
                "copal_tiering_would_recall_blobs",
                report.total_would_recall_blobs,
            );
            for (tenant, tally) in &report.per_tenant {
                crate::metrics::set_gauge(
                    &format!("copal_tiering_candidate_bytes{{tenant=\"{tenant}\"}}"),
                    tally.candidate_bytes,
                );
            }
        }
        Err(err) => tracing::warn!(error = %err, "tiering observe pass failed"),
    }
}

/// The admin listing: what would move, per tenant, with the held-back
/// counts named. Computed fresh so the operator reads the corpus as
/// it stands, not as the last sweep left it.
pub(crate) async fn report<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::auth::require_admin(&state, &headers)?;
    let report = classify_pass(&state.store, &state.tiering).await?;
    let mut tenants: Vec<serde_json::Value> = report
        .per_tenant
        .iter()
        .map(|(tenant, tally)| {
            json!({
                "tenant": tenant,
                "candidate_blobs": tally.candidate_blobs,
                "candidate_bytes": tally.candidate_bytes,
                "would_recall_blobs": tally.would_recall_blobs,
                "would_recall_bytes": tally.would_recall_bytes,
            })
        })
        .collect();
    tenants.sort_by_key(|row| row["tenant"].as_str().unwrap_or_default().to_owned());
    let held: serde_json::Map<String, serde_json::Value> = report
        .held
        .iter()
        .map(|(reason, count)| ((*reason).to_owned(), json!(count)))
        .collect();
    Ok(Json(json!({
        "tenants": tenants,
        "totals": {
            "candidate_blobs": report.total_candidate_blobs,
            "candidate_bytes": report.total_candidate_bytes,
            "would_recall_blobs": report.total_would_recall_blobs,
        },
        "held": held,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> Topology {
        let mut t = Topology::default();
        t.insert(
            "local",
            HashMap::from([("cold".to_owned(), TierClass::Online)]),
        );
        t
    }

    fn policy(tier: &str, after: u64, basis: &str, min_bytes: u64) -> tier_repo::TieringPolicy {
        tier_repo::TieringPolicy {
            tier: tier.to_owned(),
            after_seconds: after,
            basis: basis.to_owned(),
            min_bytes,
        }
    }

    fn row(tenants: &[&str]) -> tier_repo::BlobClassifyRow {
        tier_repo::BlobClassifyRow {
            id: "blob:aaaa".to_owned(),
            size_bytes: 1_000_000,
            tier: None,
            created_age_secs: Some(100 * 86_400),
            read_age_secs: None,
            file_tenants: tenants.iter().map(|t| (*t).to_owned()).collect(),
            pinned_tenants: Vec::new(),
            version_tenants: Vec::new(),
        }
    }

    fn policies(
        entries: &[(&str, tier_repo::TieringPolicy)],
    ) -> HashMap<String, tier_repo::TieringPolicy> {
        entries
            .iter()
            .map(|(tenant, p)| ((*tenant).to_owned(), p.clone()))
            .collect()
    }

    #[test]
    fn an_old_unread_blob_is_a_candidate() {
        let policies = policies(&[("acme", policy("cold", 90 * 86_400, "accessed", 0))]);
        assert_eq!(
            classify_row(&row(&["acme"]), &policies, &topology()),
            Ok(Some(true)),
        );
    }

    #[test]
    fn creation_age_answers_when_no_read_was_recorded() {
        // `accessed` with no recorded read falls back to creation age
        // rather than reading "unknown" as cold-now or hot-forever.
        let policies = policies(&[("acme", policy("cold", 200 * 86_400, "accessed", 0))]);
        assert_eq!(
            classify_row(&row(&["acme"]), &policies, &topology()),
            Err(HELD_YOUNG),
        );
    }

    #[test]
    fn a_recent_read_holds_content_and_counts_as_a_would_be_recall() {
        let policies = policies(&[("acme", policy("cold", 90 * 86_400, "accessed", 0))]);
        let mut read = row(&["acme"]);
        read.read_age_secs = Some(86_400);
        assert_eq!(
            classify_row(&read, &policies, &topology()),
            Ok(Some(false)),
            "old by creation, held by a fresh read: the measured recall",
        );
    }

    #[test]
    fn an_old_read_no_longer_counts_as_a_recall() {
        let policies = policies(&[("acme", policy("cold", 90 * 86_400, "accessed", 0))]);
        let mut read = row(&["acme"]);
        read.read_age_secs = Some(95 * 86_400);
        assert_eq!(
            classify_row(&read, &policies, &topology()),
            Ok(Some(true)),
            "a read older than the threshold does not hold content",
        );
    }

    #[test]
    fn a_pin_holds_every_referent() {
        let policies = policies(&[("acme", policy("cold", 0, "created", 0))]);
        let mut pinned = row(&["acme", "beta"]);
        pinned.pinned_tenants = vec!["beta".to_owned()];
        assert_eq!(classify_row(&pinned, &policies, &topology()), Err(HELD_PIN),);
    }

    #[test]
    fn a_referent_without_a_policy_demands_hot() {
        let policies = policies(&[("acme", policy("cold", 0, "created", 0))]);
        assert_eq!(
            classify_row(&row(&["acme", "beta"]), &policies, &topology()),
            Err(HELD_NO_POLICY),
        );
    }

    #[test]
    fn the_most_demanding_threshold_wins() {
        let policies = policies(&[
            ("acme", policy("cold", 0, "created", 0)),
            ("beta", policy("cold", 500 * 86_400, "created", 0)),
        ]);
        assert_eq!(
            classify_row(&row(&["acme", "beta"]), &policies, &topology()),
            Err(HELD_YOUNG),
        );
    }

    #[test]
    fn policies_naming_different_tiers_cannot_agree_on_a_placement() {
        let policies = policies(&[
            ("acme", policy("cold", 0, "created", 0)),
            ("beta", policy("chill", 0, "created", 0)),
        ]);
        assert_eq!(
            classify_row(&row(&["acme", "beta"]), &policies, &topology()),
            Err(HELD_TIER_DISAGREE),
        );
    }

    #[test]
    fn the_blob_residency_must_configure_the_named_tier() {
        let policies = policies(&[("acme", policy("cold", 0, "created", 0))]);
        let mut foreign = row(&["acme"]);
        foreign.id = "blob:eu-aaaa".to_owned();
        assert_eq!(
            classify_row(&foreign, &policies, &topology()),
            Err(HELD_TIER_MISMATCH),
        );
    }

    #[test]
    fn small_objects_never_move() {
        let policies = policies(&[("acme", policy("cold", 0, "created", 131_072))]);
        let mut small = row(&["acme"]);
        small.size_bytes = 4_096;
        assert_eq!(
            classify_row(&small, &policies, &topology()),
            Err(HELD_MIN_BYTES),
        );
    }

    #[test]
    fn cold_rows_and_unreferenced_rows_are_not_this_features_business() {
        let policies = policies(&[("acme", policy("cold", 0, "created", 0))]);
        let mut demoted = row(&["acme"]);
        demoted.tier = Some("cold".to_owned());
        assert_eq!(classify_row(&demoted, &policies, &topology()), Ok(None));
        assert_eq!(
            classify_row(&row(&[]), &policies, &topology()),
            Ok(None),
            "unreferenced rows belong to the GC",
        );
    }

    #[test]
    fn history_references_hold_content_exactly_like_files() {
        let policies = policies(&[("acme", policy("cold", 0, "created", 0))]);
        let mut history = row(&[]);
        history.version_tenants = vec!["beta".to_owned()];
        assert_eq!(
            classify_row(&history, &policies, &topology()),
            Err(HELD_NO_POLICY),
            "a retained version's tenant counts as a referent",
        );
    }
}
