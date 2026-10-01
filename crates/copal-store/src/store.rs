//! The store handle.
//!
//! A cheap `Clone` wrapper over the surql-rs client: one connected store
//! is shared across handlers, never a global. `connect` speaks any engine
//! the underlying feature set enables, so `mem://` in tests and
//! `ws://host:8000` in deployment run identical code.

use surql::connection::{ConnectionConfig, DatabaseClient};

use copal_core::CopalError;

use crate::schema;

/// Connection parameters for the metadata plane.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Engine URL: `ws://127.0.0.1:8000`, `mem://`, `surrealkv://path`.
    pub url: String,
    pub namespace: String,
    pub database: String,
    /// Credentials; unset for embedded engines.
    pub username: Option<String>,
    pub password: Option<String>,
    /// HS256 key for the caller access method. Set, the schema apply
    /// defines record access and [`Store::caller`] becomes usable;
    /// unset, no access method exists and the engine serves only the
    /// service session.
    pub engine_access_key: Option<String>,
    /// Embedding width, when semantic retrieval is configured. Folds
    /// the vector index into the schema diff so a changed model
    /// rebuilds the index and an unchanged one leaves it alone.
    pub embedding_dimension: Option<u32>,
    /// Engine policy derived from the contract: field guards and
    /// read-scope conjuncts. The store contributes the mechanical
    /// tenancy rule; the server derives the rest so the schema and
    /// the application enforce one declaration set.
    pub engine_policy: crate::schema::EnginePolicy,
}

impl StoreConfig {
    /// In-memory configuration for tests.
    pub fn memory() -> Self {
        Self {
            url: "mem://".to_owned(),
            namespace: "copal_test".to_owned(),
            database: "copal".to_owned(),
            username: None,
            password: None,
            engine_access_key: None,
            embedding_dimension: None,
            engine_policy: crate::schema::EnginePolicy::default(),
        }
    }

    /// [`StoreConfig::memory`] with the caller access method enabled.
    pub fn memory_with_engine_access(key: impl Into<String>) -> Self {
        Self {
            engine_access_key: Some(key.into()),
            ..Self::memory()
        }
    }
}

/// What a live notification reports about its row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowChange {
    Created,
    Updated,
    /// The row as it was before the delete.
    Deleted,
}

/// Cloneable handle over the metadata plane.
#[derive(Clone)]
pub struct Store {
    client: DatabaseClient,
    engine_policy: std::sync::Arc<crate::schema::EnginePolicy>,
}

impl Store {
    /// Connect and apply the idempotent schema.
    pub async fn connect(cfg: StoreConfig) -> copal_core::Result<Self> {
        let engine_access_key = cfg.engine_access_key.clone();
        let embedding_dimension = cfg.embedding_dimension;
        let engine_policy = std::sync::Arc::new(cfg.engine_policy.clone());
        let mut builder = ConnectionConfig::builder()
            .url(cfg.url)
            .namespace(cfg.namespace)
            .database(cfg.database);
        if let (Some(user), Some(pass)) = (cfg.username, cfg.password) {
            builder = builder.username(user).password(pass);
        }
        let config = builder
            .build()
            .map_err(|e| CopalError::Store(format!("invalid store config: {e}")))?;
        let client =
            DatabaseClient::new(config).map_err(|e| CopalError::Store(format!("client: {e}")))?;
        client
            .connect()
            .await
            .map_err(|e| CopalError::Store(format!("connect: {e}")))?;

        let store = Self {
            client,
            engine_policy,
        };
        store
            .apply_schema(engine_access_key.as_deref(), embedding_dimension)
            .await?;
        Ok(store)
    }

    /// Bring the database up to the code's schema. Returns how many
    /// statements were applied: a database that already matches
    /// applies zero, the boot-loop property the tests pin.
    ///
    /// The database is introspected live (`INFO FOR DB` for modes,
    /// permissions, analyzers, and access methods; `INFO FOR TABLE`
    /// per table for fields, indexes, and events) and diffed against
    /// the code's snapshot. Only the differences apply, as `OVERWRITE`
    /// forms that create absent objects and replace present
    /// definitions while leaving rows untouched, both engine-pinned.
    /// A database that matches runs no DDL at all, and one created by
    /// an older release receives every later definition,
    /// `PERMISSIONS` included, on its first boot under newer code.
    /// Definitions the database holds that the code no longer
    /// declares are logged and left alone: removal is an operator
    /// decision.
    ///
    /// Two departures from a straight replay of the diff:
    ///
    /// - Non-unique index builds run `CONCURRENTLY`, so a new or
    ///   changed index over a big table (the HNSW vector index above
    ///   all) populates behind a boot instead of blocking it. See
    ///   `index_add_statement`.
    /// - The blob table's computed reverse-reference fields apply
    ///   LAST, behind the reference backfill. Their absence from the
    ///   database is the durable sign that pre-existing blob links
    ///   are not yet registered with the engine's reference tracking,
    ///   so a crash anywhere before they exist makes the next boot
    ///   repeat the (idempotent) backfill rather than silently
    ///   undercount and let the GC erase live content. See
    ///   [`schema::reference_backfill_script`].
    ///
    /// Concurrent boots race benignly: identical `OVERWRITE`
    /// statements are idempotent, and a DDL conflict retries once. An
    /// index the engine refuses while it reclaims the table's document
    /// ids retries on a backoff; see [`Store::run_ddl`].
    pub async fn apply_schema(
        &self,
        engine_access_key: Option<&str>,
        embedding_dimension: Option<u32>,
    ) -> copal_core::Result<usize> {
        let db = self.introspect().await?;
        let code = schema::code_snapshot(embedding_dimension, &self.engine_policy);
        let diffs = surql::migration::diff::diff_schemas(&code, &db);
        let (mut apply, reverse_fields) = plan_schema(&code, &diffs);
        if let Some(key) = engine_access_key {
            // Applied whenever configured rather than diffed: the
            // engine redacts keys in its echo, so access definitions
            // can never compare equal.
            apply.push(schema::access_overwrite(key)?);
        }
        let applied = apply.len() + reverse_fields.len();
        if applied == 0 {
            return Ok(0);
        }
        tracing::info!(
            statements = applied,
            "bringing the database up to the code's schema",
        );
        // One statement at a time, so a retry repeats only the statement
        // that was refused. Re-running the whole script would overwrite
        // every index ahead of the refused one again, and on SurrealDB
        // 3.3 an overwritten index is what starts the document-id
        // reclaim that refused it.
        for statement in &apply {
            self.run_ddl(statement, "apply_schema").await?;
        }
        if !reverse_fields.is_empty() {
            // Backfill first, computed fields after: once the fields
            // exist the recount trusts the inbound sets, so they may
            // only come into being over a database whose every link
            // is registered.
            tracing::info!(
                "registering pre-existing blob links with the engine's reference tracking",
            );
            self.run_ddl(&schema::reference_backfill_script(), "reference backfill")
                .await?;
            self.run_ddl(&reverse_fields.join("\n"), "reverse reference fields")
                .await?;
        }
        Ok(applied)
    }

    /// Run one DDL script, retrying what a later attempt can fix.
    ///
    /// An engine-level conflict retries once: two replicas booting at
    /// the same moment race the same idempotent statements. A
    /// `DEFINE INDEX` that SurrealDB 3.3 refuses because the table's
    /// document ids are "still being reclaimed" (the background cleanup
    /// a removed or overwritten index starts) retries on the backoff in
    /// [`RECLAIM_WAITS`], about half a minute in all, because the
    /// refusal ends when the cleanup does.
    async fn run_ddl(&self, script: &str, what: &str) -> copal_core::Result<()> {
        let mut reclaim_waits = RECLAIM_WAITS.iter();
        let mut conflict_retried = false;
        let mut retried = false;
        loop {
            let error = match self.client.query(script).await {
                Ok(_) => return Ok(()),
                Err(error) => error.to_string(),
            };
            let wait = if is_reclaim_refusal(&error) {
                reclaim_waits.next().copied()
            } else if error.contains("conflict") && !conflict_retried {
                conflict_retried = true;
                Some(std::time::Duration::from_millis(250))
            } else {
                None
            };
            let Some(wait) = wait else {
                let what = if retried {
                    format!("{what} retry")
                } else {
                    what.to_owned()
                };
                return Err(CopalError::Store(format!("{what}: {error}")));
            };
            if is_reclaim_refusal(&error) {
                tracing::info!(
                    step = what,
                    wait_ms = wait.as_millis() as u64,
                    "the engine is still reclaiming a table's document ids; retrying the index",
                );
            }
            retried = true;
            tokio::time::sleep(wait).await;
        }
    }

    /// The database's schema as it actually stands, from both INFO
    /// levels: the database level alone reports fieldless tables.
    async fn introspect(&self) -> copal_core::Result<surql::migration::diff::SchemaSnapshot> {
        let info = self
            .client
            .query("INFO FOR DB;")
            .await
            .map_err(|e| CopalError::Store(format!("introspect: {e}")))?;
        // Both INFO parsers unwrap the client's statement-result
        // wrapper themselves, so the responses pass through as the
        // client returned them.
        let parsed = surql::schema::parser::parse_db_info(&info)
            .map_err(|e| CopalError::Store(format!("introspect: {e}")))?;
        let mut tables = Vec::new();
        for (name, shallow) in &parsed.tables {
            let table_info = self
                .client
                .query(&format!("INFO FOR TABLE {name};"))
                .await
                .map_err(|e| CopalError::Store(format!("introspect {name}: {e}")))?;
            let define = shallow.to_surql();
            let full = surql::schema::parser::parse_table_full(name, &define, &table_info)
                .map_err(|e| CopalError::Store(format!("introspect {name}: {e}")))?;
            tables.push(full);
        }
        Ok(surql::migration::diff::SchemaSnapshot {
            tables,
            analyzers: parsed.analyzers.into_values().collect(),
            ..Default::default()
        })
    }

    /// Open a caller-bound engine session over the same connection.
    ///
    /// The returned store runs every repository call through a session
    /// the engine filters by `PERMISSIONS`: rows outside the token's
    /// tenant do not exist for it, and guarded columns come back
    /// absent. The service store beside it keeps full authority. The
    /// session ends when the returned store drops.
    pub async fn caller(&self, token: &str) -> copal_core::Result<Store> {
        let client = self
            .client
            .caller_session(token)
            .await
            .map_err(|e| CopalError::Store(format!("caller session: {e}")))?;
        Ok(Store {
            client,
            engine_policy: self.engine_policy.clone(),
        })
    }

    /// Apply the vector index at the configured width, through the
    /// same live diff as everything else: a changed dimension
    /// rebuilds, an unchanged one is a no-op.
    ///
    /// Separate from [`Store::connect`] because the width is not a
    /// property of the schema: a deployment without embeddings never
    /// defines this index, and one that changes models redefines it.
    pub async fn ensure_vector_index(&self, dimension: u32) -> copal_core::Result<()> {
        self.apply_schema(None, Some(dimension)).await.map(|_| ())
    }

    /// One cheap round trip proving the metadata plane answers.
    /// Readiness probes call this; it carries no schema assumptions.
    pub async fn ping(&self) -> copal_core::Result<()> {
        self.client
            .query("RETURN 1;")
            .await
            .map_err(|e| CopalError::Store(format!("ping: {e}")))?;
        Ok(())
    }

    /// Wrap an already-connected client without applying anything.
    ///
    /// For embedders and tests that manage the schema themselves;
    /// [`Store::apply_schema`] is available when they want the
    /// reconciliation pass.
    pub fn from_connected(client: DatabaseClient) -> Self {
        Self {
            client,
            engine_policy: std::sync::Arc::new(crate::schema::EnginePolicy::default()),
        }
    }

    /// Borrow the underlying client (advanced usage: introspection,
    /// raw statements the repositories deliberately do not offer).
    pub fn raw(&self) -> &DatabaseClient {
        &self.client
    }

    /// Borrow the underlying client for repository functions.
    pub(crate) fn client(&self) -> &DatabaseClient {
        &self.client
    }

    /// A live-query wake stream for a table: one unit item per change
    /// notification. Carries no payload on purpose; consumers read
    /// their own durable rows on wake, so a dropped or coalesced
    /// notification costs latency and never data. The stream ends when
    /// the connection drops; callers restart it.
    pub async fn watch(
        &self,
        table: &str,
    ) -> copal_core::Result<impl futures::Stream<Item = ()> + Send + Unpin> {
        use futures::StreamExt as _;
        let live = surql::connection::LiveQuery::<serde_json::Value>::start(&self.client, table)
            .await
            .map_err(|e| CopalError::Store(format!("live query: {e}")))?;
        Ok(live.map(|_| ()))
    }

    /// A live-query row stream for a table, narrowed by `conditions`.
    ///
    /// The engine evaluates the conditions per notification, so a
    /// subscriber never sees a row it did not ask for. That is what
    /// keeps tenant scoping out of application code, where a missed
    /// check leaks another tenant's rows.
    ///
    /// Unlike [`Store::watch`], the payload IS the delivery here: a
    /// notification the connection drops is a row the subscriber never
    /// learns about. Use this to serve subscriptions, never to drive
    /// durable work.
    ///
    /// Each item names what happened to its row, because which changes
    /// are news depends on the table: an append-only table's rows are
    /// news once, when created, and every later write to them is
    /// bookkeeping.
    pub async fn watch_rows(
        &self,
        table: &str,
        conditions: Vec<surql::query::Condition>,
    ) -> copal_core::Result<
        impl futures::Stream<Item = copal_core::Result<(RowChange, serde_json::Value)>> + Send + Unpin,
    > {
        use futures::StreamExt as _;
        let live = surql::connection::LiveQuery::<serde_json::Value>::start_where(
            &self.client,
            table,
            conditions,
        )
        .await
        .map_err(|e| CopalError::Store(format!("live query: {e}")))?;
        // CREATE and UPDATE carry the row; DELETE carries it as it was.
        // A killed or failing live query travels as an error item, so
        // the subscriber learns the feed broke rather than decoding an
        // error message as a row.
        Ok(live.map(|item| {
            let notification =
                item.map_err(|error| CopalError::Store(format!("live query: {error}")))?;
            // The action type lives in the engine crate, which this one
            // names only behind the `embedded` feature; its display
            // names are the SurrealQL keywords.
            let change = match notification.action.to_string().as_str() {
                "CREATE" => RowChange::Created,
                "UPDATE" => RowChange::Updated,
                "DELETE" => RowChange::Deleted,
                other => {
                    return Err(CopalError::Store(format!(
                        "live query {other}: {}",
                        notification.data,
                    )));
                }
            };
            Ok((change, notification.data))
        }))
    }
}

/// How long [`Store::run_ddl`] waits between attempts at an index the
/// engine refused while reclaiming its table's document ids. The
/// cleanup's length grows with the table, and boot is the only thing
/// waiting, so the waits double up to about half a minute in all
/// before the refusal is reported.
const RECLAIM_WAITS: [std::time::Duration; 6] = [
    std::time::Duration::from_millis(500),
    std::time::Duration::from_secs(1),
    std::time::Duration::from_secs(2),
    std::time::Duration::from_secs(4),
    std::time::Duration::from_secs(8),
    std::time::Duration::from_secs(15),
];

/// Whether an engine error is SurrealDB 3.3's refusal of a
/// `DEFINE INDEX` while the table's shared document-id space is being
/// reclaimed: "The shared document-ID space for table `t` is still
/// being reclaimed; retry DEFINE INDEX after cleanup completes".
fn is_reclaim_refusal(error: &str) -> bool {
    error.contains("still being reclaimed")
}

/// The statements that bring the database to the code's schema, in
/// order, and apart from them the computed reverse-reference fields
/// that must wait for the reference backfill.
///
/// Analyzers come first: a full-text index names one, so index
/// creation cannot precede it, and the script is not a transaction.
/// Definitions only the database holds are logged and skipped.
fn plan_schema(
    code: &surql::migration::diff::SchemaSnapshot,
    diffs: &[surql::migration::models::SchemaDiff],
) -> (Vec<String>, Vec<String>) {
    let mut analyzer_ops: Vec<String> = Vec::new();
    let mut apply: Vec<String> = Vec::new();
    let mut reverse_fields: Vec<String> = Vec::new();
    for diff in diffs {
        use surql::migration::models::DiffOperation as Op;
        match diff.operation {
            Op::DropTable
            | Op::DropField
            | Op::DropIndex
            | Op::DropEvent
            | Op::DropAnalyzer
            | Op::DropBucket
            | Op::DropSequence
            | Op::DropFunction
            | Op::DropParam => {
                tracing::warn!(
                    change = %diff.description,
                    "the database defines this and the code no longer does; remove it \
                     manually if it is truly retired",
                );
            }
            Op::AddAnalyzer | Op::ModifyAnalyzer => {
                analyzer_ops.push(diff.forward_sql.clone());
            }
            // A changed index arrives as ModifyIndex (surql 0.34) and
            // rebuilds like a new one, so both take the backgrounded path.
            Op::AddIndex | Op::ModifyIndex => apply.push(index_add_statement(code, diff)),
            Op::AddField if adds_reverse_reference(code, diff) => {
                reverse_fields.push(diff.forward_sql.clone());
            }
            _ => apply.push(diff.forward_sql.clone()),
        }
    }
    analyzer_ops.extend(apply);
    (analyzer_ops, reverse_fields)
}

/// The statement for an index the database lacks or defines
/// differently.
///
/// Non-unique indexes build `CONCURRENTLY`: the `DEFINE` returns at
/// once and the engine populates the index behind it, so a new or
/// changed index over a big table (the HNSW vector index above all)
/// no longer holds boot hostage to the rebuild. Probed on mem://:
/// `OVERWRITE` composes with `CONCURRENTLY`, the build reaches
/// `status: ready`, and `INFO FOR INDEX <name> ON <table>` reports
/// its progress, which the log line names for whoever wants to
/// watch. Unique indexes stay synchronous ON PURPOSE: they are
/// constraints, not accelerators, and a backgrounded constraint is
/// silently unenforced for the width of its build -
/// `uniq_file_live_path` is what the completion CAS leans on, so
/// that window must not exist.
fn index_add_statement(
    code: &surql::migration::diff::SchemaSnapshot,
    diff: &surql::migration::models::SchemaDiff,
) -> String {
    let definition = diff.index.as_deref().and_then(|name| {
        code.tables
            .iter()
            .find(|table| table.name == diff.table)
            .and_then(|table| table.indexes.iter().find(|index| index.name == name))
    });
    match definition {
        Some(index) if index.index_type != surql::schema::IndexType::Unique => {
            tracing::info!(
                index = %index.name,
                table = %diff.table,
                "index build backgrounded (CONCURRENTLY); watch it with INFO FOR INDEX",
            );
            index
                .clone()
                .with_concurrently(true)
                .to_surql_overwrite(&diff.table)
        }
        _ => diff.forward_sql.clone(),
    }
}

/// Whether this diff adds a computed reverse-reference field (`<~`):
/// the fields the reference backfill must precede, held out of the
/// main script by [`Store::apply_schema`].
fn adds_reverse_reference(
    code: &surql::migration::diff::SchemaSnapshot,
    diff: &surql::migration::models::SchemaDiff,
) -> bool {
    diff.field
        .as_deref()
        .and_then(|name| {
            code.tables
                .iter()
                .find(|table| table.name == diff.table)?
                .fields
                .iter()
                .find(|field| field.name == name)
        })
        .and_then(|field| field.computed.as_deref())
        .is_some_and(|expression| expression.trim_start().starts_with("<~"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_diffs() -> (
        surql::migration::diff::SchemaSnapshot,
        Vec<surql::migration::models::SchemaDiff>,
    ) {
        let code = crate::schema::code_snapshot(Some(384), &crate::schema::EnginePolicy::default());
        let empty = surql::migration::diff::SchemaSnapshot::default();
        let diffs = surql::migration::diff::diff_schemas(&code, &empty);
        (code, diffs)
    }

    #[test]
    fn non_unique_index_adds_are_backgrounded_and_unique_ones_are_not() {
        let (code, diffs) = fresh_diffs();
        let hnsw = diffs
            .iter()
            .find(|d| d.index.as_deref() == Some("idx_chunk_embedding"))
            .expect("the vector index is in the fresh diff");
        let sql = index_add_statement(&code, hnsw);
        assert!(sql.contains("HNSW"), "{sql}");
        assert!(sql.contains(" OVERWRITE "), "{sql}");
        assert!(sql.ends_with("CONCURRENTLY;"), "{sql}");

        let unique = diffs
            .iter()
            .find(|d| d.index.as_deref() == Some("uniq_file_live_path"))
            .expect("the live-path constraint is in the fresh diff");
        let sql = index_add_statement(&code, unique);
        assert!(!sql.contains("CONCURRENTLY"), "{sql}");
    }

    /// A new embedding width rebuilds the vector index, and the rebuild
    /// must stay behind boot the way a first build does. surql 0.34
    /// reports the change as ModifyIndex where it used to be an add, so
    /// routing only adds sent the rebuild down the synchronous path.
    #[test]
    fn a_changed_index_is_backgrounded_like_a_new_one() {
        let policy = crate::schema::EnginePolicy::default();
        let code = crate::schema::code_snapshot(Some(768), &policy);
        let db = crate::schema::code_snapshot(Some(384), &policy);
        let diffs = surql::migration::diff::diff_schemas(&code, &db);
        let changed = diffs
            .iter()
            .find(|d| d.index.as_deref() == Some("idx_chunk_embedding"))
            .expect("the width change reaches the vector index");
        assert_eq!(
            changed.operation,
            surql::migration::models::DiffOperation::ModifyIndex,
        );

        let (apply, _) = plan_schema(&code, &diffs);
        let rebuilt: Vec<&String> = apply
            .iter()
            .filter(|sql| sql.contains("idx_chunk_embedding"))
            .collect();
        assert_eq!(rebuilt.len(), 1, "{apply:#?}");
        assert!(rebuilt[0].contains("DIMENSION 768"), "{}", rebuilt[0]);
        assert!(rebuilt[0].ends_with("CONCURRENTLY;"), "{}", rebuilt[0]);
    }

    #[test]
    fn the_reclaim_refusal_is_told_apart_from_other_failures() {
        assert!(is_reclaim_refusal(
            "The shared document-ID space for table `text_chunk` is still being \
             reclaimed; retry DEFINE INDEX after cleanup completes",
        ));
        assert!(!is_reclaim_refusal(
            "Transaction conflict: Resource busy. This transaction can be retried",
        ));
        assert!(!is_reclaim_refusal(
            "The index 'idx_chunk_embedding' already exists"
        ));
    }

    #[test]
    fn only_the_computed_reverse_fields_are_deferred() {
        let (code, diffs) = fresh_diffs();
        use surql::migration::models::DiffOperation;
        let deferred: Vec<&str> = diffs
            .iter()
            .filter(|d| d.operation == DiffOperation::AddField && adds_reverse_reference(&code, d))
            .filter_map(|d| d.field.as_deref())
            .collect();
        assert_eq!(deferred, ["inbound_files", "inbound_versions"]);
    }
}
