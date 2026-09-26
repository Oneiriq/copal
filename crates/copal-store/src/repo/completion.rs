//! Finishing an upload, as one transaction.
//!
//! The other repositories are one module per table. This one is not: it
//! is a single operation that has to write `file`, `file_version` and
//! `file_event` together or write none of them, and putting it under
//! any one of those tables would have hidden that from the other two.
//!
//! What it is fixing. Completion used to be a sequence of guarded
//! statements, each atomic on its own and none atomic together: the CAS
//! moved the file out of `uploading` and incremented `version_count`,
//! and only several round trips later did an UPDATE point
//! `current_version` at the version row that increment had named.
//! Between those two writes the file was readable, by every face, in a
//! state that has never been true: `version_count` of N with
//! `current_version` still on N-1. A crash in the window left it that
//! way permanently, and the old code said so out loud, reporting "the
//! current truth" when it found the file had moved underneath it. That
//! window is what this module removes; the fallback is gone because
//! there is nothing left to fall back from.
//!
//! How it works. surql's [`Transaction`] does not stream statements: the
//! Rust SDK rejects a bare `COMMIT` across separate `query()` calls, so
//! `execute` buffers client-side and `commit` flushes everything as one
//! `BEGIN … COMMIT`. That buys atomicity and costs the thing this code
//! most wanted: `execute` answers `Null`, so nothing inside the
//! transaction can steer a Rust `if`. Every branch therefore has to be
//! stated in SurrealQL, and every value the later statements need has
//! to come from a `LET` rather than from the client.
//!
//! Losing the race is a value, not an error. The obvious shape was to
//! `THROW` when the CAS matched nothing and read the message back, and
//! it does not work: probed against the engine, a THROW inside a
//! transaction rolls the transaction back correctly but reaches the
//! client as "The query was not executed due to a failed transaction",
//! with the thrown text discarded. So the CAS lands in `$prior` and
//! every statement after it is wrapped in `IF array::len($prior) > 0`.
//! A lost race commits an empty transaction and the verdict travels
//! home as a number the final `RETURN` reports. Conflict still means
//! conflict, and the guard that decides it is still the one guard the
//! file repository renders for every transition.

use serde::Deserialize;
use serde_json::Value;

use surql::connection::Transaction;
use surql::query::expressions::raw;
use surql::query::helpers::ReturnFormat;
use surql::types::operators::quote_value_public as literal;
use surql::types::RecordID;

use copal_core::{ContentDigest, CopalError, FileId, FileRecord, FileState, TenantId};

use crate::dto::{map_store_err, FileRow};
use crate::store::Store;

use super::tenant::RetentionPolicy;

/// Finish an upload atomically.
///
/// One CAS moves `uploading -> ready` (or `-> scanning`, when a pipeline
/// will finalize), writes the payload columns, links the blob, and
/// increments `version_count`, whose incremented value IS the new
/// version number. In the same transaction the frozen version row is
/// created already armed, the tenant's retention policy stamps it,
/// erasable history beyond `keep_last` goes, and the file's
/// `current_version` points at what was just written. Either a reader
/// sees the file as it was before any of this, or as it is after all of
/// it.
///
/// Two round trips: the retention policy, then the transaction. It was
/// five to ten.
///
/// `markers` is the uploader's confidentiality declaration for
/// exactly these bytes, already validated at the API face; it lands
/// on the version row in the same CREATE that arms it, because the
/// version is the frozen artifact and markers are facts about
/// exactly that content. `None` means none declared, which is
/// file-level behavior: today's, exactly.
#[allow(clippy::too_many_arguments)]
pub async fn complete_upload(
    store: &Store,
    tenant: &TenantId,
    id: &FileId,
    residency: &str,
    digest: &ContentDigest,
    size_bytes: u64,
    created_by: &str,
    final_state: FileState,
    markers: Option<&Value>,
) -> copal_core::Result<FileRecord> {
    // Completion lands in ready (no pipeline) or scanning (a pipeline
    // will finalize); anything else is a caller bug.
    if !matches!(final_state, FileState::Ready | FileState::Scanning) {
        return Err(CopalError::validation(
            "completion must land in ready or scanning",
        ));
    }

    // The policy read stays OUTSIDE the transaction, and it is the only
    // step that does. Its result chooses which statements follow, and a
    // buffered `execute` answers `Null`, so reading it inside would mean
    // restating the whole retention rule in SurrealQL: a mode-dependent
    // WHERE clause that already exists, exactly once, in the version
    // repository. Two renderings of a WORM guard is one rendering that
    // can drift, and the direction it would drift is a compliance clock
    // an upload could shorten. One round trip is the cheaper mistake.
    //
    // Reading it a moment early costs nothing the contract cares about:
    // the retention a version carries is computed from the policy as it
    // stands at this completion and is never recomputed, because a
    // policy change must not shorten what already exists. A policy
    // written between this read and the commit stamps the NEXT version,
    // which is what it would have done had the read come later.
    let policy = super::tenant::get_retention_policy(store, tenant).await?;

    let statements = completion_statements(
        tenant,
        id,
        residency,
        digest,
        size_bytes,
        created_by,
        final_state,
        policy.as_ref(),
        markers,
    )?;

    let mut transaction = Transaction::begin(store.client())
        .await
        .map_err(|e| map_store_err("complete", e))?;
    for statement in &statements {
        transaction
            .execute(statement)
            .await
            .map_err(|e| map_store_err("complete", e))?;
    }
    let results = transaction
        .commit()
        .await
        .map_err(|e| map_store_err("complete", e))?;

    let outcome = read_outcome(&results)?;
    if outcome.moved == 0 {
        return Err(super::file::lost_the_race(tenant, id, FileState::Uploading));
    }
    outcome
        .row
        .into_iter()
        .next()
        .ok_or_else(|| CopalError::Store("completion committed without a file row".into()))?
        .into_domain()
}

/// The verdict the transaction sends home.
#[derive(Debug, Deserialize)]
struct Outcome {
    /// How many rows the CAS matched: one when this caller performed
    /// the completion, zero when another writer got there first.
    moved: u64,
    /// The file as it stands at commit, read inside the transaction so
    /// it reflects every write in it and nothing after.
    row: Vec<FileRow>,
}

/// Pull the verdict out of the commit's per-statement results.
///
/// `commit` answers with one entry per flushed statement, in order,
/// bookended by `BEGIN` and `COMMIT`. The verdict is therefore the
/// entry before last, and it is only reachable there because `RETURN`
/// is deliberately the final statement: probed against the engine, a
/// `RETURN` anywhere earlier ends the transaction where it stands and
/// silently drops everything after it.
fn read_outcome(results: &Value) -> copal_core::Result<Outcome> {
    let entries = results
        .as_array()
        .ok_or_else(|| CopalError::Store("completion commit answered out of shape".into()))?;
    let verdict = entries
        .len()
        .checked_sub(2)
        .and_then(|i| entries.get(i))
        .ok_or_else(|| CopalError::Store("completion commit answered no verdict".into()))?;
    serde_json::from_value(verdict.clone())
        .map_err(|e| CopalError::Store(format!("completion verdict shape: {e}")))
}

/// Render every statement of the completion, in flush order.
///
/// Nothing here talks to the engine. That is what collapses the whole
/// write into one round trip, and it is also what lets the tests below
/// read the statements this would have sent without standing an engine
/// up to receive them.
#[allow(clippy::too_many_arguments)]
fn completion_statements(
    tenant: &TenantId,
    id: &FileId,
    residency: &str,
    digest: &ContentDigest,
    size_bytes: u64,
    created_by: &str,
    final_state: FileState,
    policy: Option<&RetentionPolicy>,
    markers: Option<&Value>,
) -> copal_core::Result<Vec<String>> {
    let file_rid = super::file::rid(id)?;
    let blob_rid = RecordID::<()>::new("blob", super::blob::blob_row_id(residency, digest))
        .map_err(|e| map_store_err("complete", e))?;
    // The version row's id is minted here rather than by the engine so
    // the CAS can point `current_version` at it in the same statement
    // that earns the version number. The row does not exist yet when
    // that link is written; inside the transaction nobody can tell, and
    // by commit it does.
    let version_rid = RecordID::<()>::new(
        super::version::TABLE,
        ulid::Ulid::generate().to_string().to_ascii_lowercase(),
    )
    .map_err(|e| map_store_err("complete", e))?;

    // The CAS is rendered by the file repository, not restated here.
    // It is the compare-and-swap every other transition uses: the WHERE
    // clause carries the tenant and the expected `uploading` state, so
    // two writers completing the same claim resolve at the engine and
    // the loser matches nothing. RETURN BEFORE rather than AFTER
    // because the version row is written from the row as it stood: its
    // number is the old count plus one, and its `prior` is the
    // `current_version` this same statement is about to overwrite.
    let cas =
        super::file::transition_query(tenant, id, FileState::Uploading, final_state, |query| {
            query
                .set("digest", Value::from(digest.as_str()))
                .map_err(|e| map_store_err("complete", e))?
                .set("size_bytes", Value::from(size_bytes))
                .map_err(|e| map_store_err("complete", e))?
                .set_expr("blob", raw(blob_rid.to_string()))
                .map_err(|e| map_store_err("complete", e))?
                .set_expr("version_count", raw("version_count + 1"))
                .map_err(|e| map_store_err("complete", e))?
                .set_expr("current_version", raw(version_rid.to_string()))
                .map_err(|e| map_store_err("complete", e))
        })?
        .return_before();
    let cas = cas.to_surql().map_err(|e| map_store_err("complete", e))?;

    let number = "$prior[0].version_count + 1";
    let mut statements = vec![format!("LET $prior = {cas}")];

    // The version row is born armed. The schema's arming UPDATE existed
    // because a JSON payload cannot carry a record link, and a raw
    // CONTENT literal can, so create and arm collapse into one
    // statement here. That is strictly stronger than what it replaces:
    // the freeze event fires on UPDATE, so a row created armed has no
    // update to be frozen after, and no moment of existing unarmed.
    // String literals go through the query family's own quoting rather
    // than JSON's. The two agree on ordinary text and part company on
    // escapes, and `created_by` is whatever the caller was
    // authenticated as, so the one that matches the language receiving
    // it is the one to use.
    // Markers land inside the same CONTENT literal as everything
    // else: the declaration is a fact about exactly these bytes, and
    // a version row that existed without it would be a marked upload
    // whose re-extraction resolves nothing. Absent means absent - no
    // column written, so unmarked uploads render byte-identical
    // statements to what they rendered before markers existed.
    let markers_clause = match markers {
        Some(declaration) => format!(", markers: {}", literal(declaration)),
        None => String::new(),
    };
    statements.push(guarded(format!(
        "CREATE {version_rid} CONTENT {{ tenant_id: {tenant_lit}, number: {number}, \
         content_type: $prior[0].content_type, size_bytes: {size_bytes}, digest: {digest_lit}, \
         metadata_snapshot: $prior[0].metadata, created_by: {created_by_lit}, file: {file_rid}, \
         blob: {blob_rid}, prior: $prior[0].current_version, armed: true{markers_clause} }}",
        tenant_lit = literal(&Value::from(tenant.as_str())),
        digest_lit = literal(&Value::from(digest.as_str())),
        created_by_lit = literal(&Value::from(created_by)),
    )));

    if let Some(policy) = policy {
        if let Some(seconds) = policy.seconds {
            let mode = policy.mode.as_deref().unwrap_or("governance");
            let stamp = super::version::retention_update(&version_rid.to_string(), seconds, mode)?
                .to_surql()
                .map_err(|e| map_store_err("complete", e))?;
            statements.push(guarded(stamp));
        }
        if let Some(keep) = policy.keep_last {
            // A `keep` of zero would prune the version this transaction
            // just wrote, so the floor is one and the current version
            // always survives.
            let keep = keep.max(1);
            let prune = super::version::prune_query(tenant, id, &format!("{number} - {keep}"))?
                .return_format(ReturnFormat::Before)
                .to_surql()
                .map_err(|e| map_store_err("complete", e))?;
            statements.push(format!(
                "LET $pruned = IF array::len($prior) > 0 THEN ({prune}) ELSE [] END",
            ));
            // Written as a raw outbox row rather than through
            // `eventing::emit_event`, for the same reason the file
            // table's own outbox is an engine event: an event about a
            // change belongs in the change's transaction or it can be
            // lost between the commit and a crash. The helper is two
            // round trips of its own and has no result to give a
            // buffered transaction anyway.
            statements.push(format!(
                "IF array::len($pruned) > 0 {{ CREATE file_event CONTENT {{ \
                 tenant_id: {tenant_lit}, file: {file_rid}, action: 'version.pruned', \
                 payload: {{ removed: array::len($pruned), kept: {keep}, newest: {number} }} }} }}",
                tenant_lit = literal(&Value::from(tenant.as_str())),
            ));
        }
    }

    // Last, always: RETURN ends the transaction where it stands, so
    // nothing may follow it. The file is re-read inside the transaction
    // rather than taken from the CAS, which returned the row as it was
    // before any of this.
    statements.push(format!(
        "RETURN {{ moved: array::len($prior), row: (SELECT * FROM {file_rid}) }}",
    ));
    Ok(statements)
}

/// Wrap a statement so it applies only when the CAS won.
///
/// This is what replaces the `?` after each step of the old sequence.
/// A statement outside this guard would apply on a lost race too,
/// because the transaction commits either way: only the CAS is
/// conditional at the engine, and everything downstream of it borrows
/// that condition.
fn guarded(statement: impl std::fmt::Display) -> String {
    format!("IF array::len($prior) > 0 {{ {statement} }}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(policy: Option<&RetentionPolicy>) -> Vec<String> {
        rendered_with_markers(policy, None)
    }

    fn rendered_with_markers(
        policy: Option<&RetentionPolicy>,
        markers: Option<&Value>,
    ) -> Vec<String> {
        completion_statements(
            &TenantId::parse("acme").unwrap(),
            &FileId::parse("01kzfrwnqy4kb5cz8rj5kax7yr").unwrap(),
            "local",
            &ContentDigest::parse("a".repeat(64)).unwrap(),
            4,
            "tester",
            FileState::Ready,
            policy,
            markers,
        )
        .expect("statements render")
    }

    /// The CAS carries both guards. Losing either one turns completion
    /// into a write that double-applies (the state) or reaches across a
    /// tenant (the tenant), so they are asserted rather than trusted to
    /// the renderer they come from.
    #[test]
    fn the_compare_and_swap_still_guards_state_and_tenant() {
        let first = &rendered(None)[0];
        assert!(first.starts_with("LET $prior = UPDATE file:"), "{first}");
        assert!(first.contains("(state = 'uploading')"), "{first}");
        assert!(first.contains("(tenant_id = 'acme')"), "{first}");
        assert!(first.contains("RETURN BEFORE"), "{first}");
    }

    /// Everything downstream of the CAS borrows its condition. A
    /// statement that escaped the guard would apply on a lost race,
    /// because the transaction commits either way.
    #[test]
    fn every_write_after_the_compare_and_swap_is_guarded() {
        let statements = rendered(Some(&RetentionPolicy {
            seconds: Some(3600),
            mode: Some("compliance".to_owned()),
            keep_last: Some(2),
        }));
        for statement in &statements[1..statements.len() - 1] {
            assert!(
                statement.contains("array::len($prior) > 0")
                    || statement.contains("array::len($pruned) > 0"),
                "unguarded: {statement}",
            );
        }
    }

    /// RETURN ends the transaction where it stands, so anything after
    /// it would be silently dropped rather than refused.
    #[test]
    fn the_return_is_last_and_carries_the_verdict() {
        for policy in [
            None,
            Some(RetentionPolicy {
                seconds: Some(60),
                mode: None,
                keep_last: Some(1),
            }),
        ] {
            let statements = rendered(policy.as_ref());
            let last = statements.last().expect("at least one statement");
            assert!(last.starts_with("RETURN { moved:"), "{last}");
            assert_eq!(
                statements.iter().filter(|s| s.contains("RETURN {")).count(),
                1,
                "exactly one verdict",
            );
        }
    }

    /// No policy means no retention statements at all, rather than a
    /// stamp of zero seconds or a prune that keeps everything.
    #[test]
    fn an_absent_policy_stamps_and_prunes_nothing() {
        let statements = rendered(None);
        assert_eq!(statements.len(), 3, "CAS, version row, verdict");
        assert!(!statements.iter().any(|s| s.contains("retain_until")));
        assert!(!statements.iter().any(|s| s.contains("DELETE")));
    }

    /// Markers ride the version CREATE itself, quoted through the
    /// query family rather than JSON, and an unmarked completion
    /// renders exactly what it rendered before markers existed - the
    /// no-declaration path must stay byte-identical to today's.
    #[test]
    fn markers_land_inside_the_version_create_or_not_at_all() {
        let declaration = serde_json::json!([
            { "access": "grant", "from": "Pricing's Schedule" }
        ]);
        let statements = rendered_with_markers(None, Some(&declaration));
        let create = statements
            .iter()
            .find(|s| s.contains("CREATE file_version:"))
            .expect("a version CREATE");
        assert!(
            create.contains(
                "armed: true, markers: [{ access: 'grant', from: 'Pricing\\'s Schedule' }]"
            ),
            "{create}",
        );

        let unmarked = rendered(None);
        let create = unmarked
            .iter()
            .find(|s| s.contains("CREATE file_version:"))
            .expect("a version CREATE");
        assert!(!create.contains("markers"), "{create}");
    }

    /// A `keep_last` of zero would prune the version the same
    /// transaction just wrote, so the floor is one.
    #[test]
    fn pruning_never_reaches_the_version_it_just_wrote() {
        let statements = rendered(Some(&RetentionPolicy {
            seconds: None,
            mode: None,
            keep_last: Some(0),
        }));
        let prune = statements
            .iter()
            .find(|s| s.contains("DELETE"))
            .expect("a prune statement");
        assert!(prune.contains("$prior[0].version_count + 1 - 1"), "{prune}",);
        assert!(prune.contains(super::super::version::ERASABLE), "{prune}");
    }
}
