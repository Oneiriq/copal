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

    /// Bring the database up to the code's schema.
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
    /// Concurrent boots race benignly: identical `OVERWRITE`
    /// statements are idempotent, and a DDL conflict retries once.
    pub async fn apply_schema(
        &self,
        engine_access_key: Option<&str>,
        embedding_dimension: Option<u32>,
    ) -> copal_core::Result<()> {
        let db = self.introspect().await?;
        let code = schema::code_snapshot(embedding_dimension, &self.engine_policy);
        let diffs = surql::migration::diff::diff_schemas(&code, &db);

        // Analyzers first: a full-text index names one, so index
        // creation cannot precede it, and the script is not a
        // transaction.
        let mut analyzer_ops: Vec<String> = Vec::new();
        let mut apply: Vec<String> = Vec::new();
        for diff in &diffs {
            use surql::migration::models::DiffOperation as Op;
            match diff.operation {
                Op::DropTable
                | Op::DropField
                | Op::DropIndex
                | Op::DropEvent
                | Op::DropAnalyzer
                | Op::DropBucket => {
                    tracing::warn!(
                        change = %diff.description,
                        "the database defines this and the code no longer does; remove it \
                         manually if it is truly retired",
                    );
                }
                Op::AddAnalyzer | Op::ModifyAnalyzer => {
                    analyzer_ops.push(diff.forward_sql.clone());
                }
                _ => apply.push(diff.forward_sql.clone()),
            }
        }
        let mut apply = {
            analyzer_ops.extend(apply);
            analyzer_ops
        };
        if let Some(key) = engine_access_key {
            // Applied whenever configured rather than diffed: the
            // engine redacts keys in its echo, so access definitions
            // can never compare equal.
            apply.push(schema::access_overwrite(key)?);
        }
        if apply.is_empty() {
            return Ok(());
        }
        tracing::info!(
            statements = apply.len(),
            "bringing the database up to the code's schema",
        );
        let script = apply.join("\n");
        if let Err(first) = self.client.query(&script).await {
            if first.to_string().contains("conflict") {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                self.client
                    .query(&script)
                    .await
                    .map_err(|e| CopalError::Store(format!("apply_schema retry: {e}")))?;
            } else {
                return Err(CopalError::Store(format!("apply_schema: {first}")));
            }
        }
        Ok(())
    }

    /// The database's schema as it actually stands, from both INFO
    /// levels: the database level alone reports fieldless tables.
    async fn introspect(&self) -> copal_core::Result<surql::migration::diff::SchemaSnapshot> {
        let info = self
            .client
            .query("INFO FOR DB;")
            .await
            .map_err(|e| CopalError::Store(format!("introspect: {e}")))?;
        let parsed = surql::schema::parser::parse_db_info(&info[0])
            .map_err(|e| CopalError::Store(format!("introspect: {e}")))?;
        let mut tables = Vec::new();
        for (name, shallow) in &parsed.tables {
            let table_info = self
                .client
                .query(&format!("INFO FOR TABLE {name};"))
                .await
                .map_err(|e| CopalError::Store(format!("introspect {name}: {e}")))?;
            let define = shallow.to_surql();
            let full = surql::schema::parser::parse_table_full(name, &define, &table_info[0])
                .map_err(|e| CopalError::Store(format!("introspect {name}: {e}")))?;
            tables.push(full);
        }
        Ok(surql::migration::diff::SchemaSnapshot {
            tables,
            edges: Vec::new(),
            buckets: Vec::new(),
            analyzers: parsed.analyzers.into_values().collect(),
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
        self.apply_schema(None, Some(dimension)).await
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
    pub async fn watch_rows(
        &self,
        table: &str,
        conditions: Vec<surql::query::Condition>,
    ) -> copal_core::Result<
        impl futures::Stream<Item = copal_core::Result<serde_json::Value>> + Send + Unpin,
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
        // Every action is a fact worth relaying. A notification error
        // travels as an item so the subscriber learns the feed broke.
        Ok(live.map(|item| match item {
            Ok(notification) => Ok(notification.data),
            Err(error) => Err(CopalError::Store(format!("live query: {error}"))),
        }))
    }
}
