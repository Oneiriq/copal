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
}

impl Store {
    /// Connect and apply the idempotent schema.
    pub async fn connect(cfg: StoreConfig) -> copal_core::Result<Self> {
        let engine_access_key = cfg.engine_access_key.clone();
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

        let store = Self { client };
        store.ensure_schema().await?;
        if let Some(key) = engine_access_key {
            store.ensure_caller_access(&key).await?;
        }
        Ok(store)
    }

    /// Apply the caller access method, replacing any prior key.
    async fn ensure_caller_access(&self, key: &str) -> copal_core::Result<()> {
        let script = schema::access_statements(key)?.join("\n");
        self.client
            .query(&script)
            .await
            .map_err(|e| CopalError::Store(format!("caller access: {e}")))?;
        Ok(())
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
        Ok(Store { client })
    }

    /// Apply the generated DDL. Statements are `IF NOT EXISTS`, so this
    /// is safe on every boot; versioned migrations supersede it once the
    /// schema starts evolving in production.
    async fn ensure_schema(&self) -> copal_core::Result<()> {
        let script = schema::schema_statements().join("\n");
        self.client
            .query(&script)
            .await
            .map_err(|e| CopalError::Store(format!("ensure_schema: {e}")))?;
        Ok(())
    }

    /// Define the vector index over embeddings, at the dimension the
    /// configured model emits.
    ///
    /// Separate from [`Store::connect`] because the width is not a
    /// property of the schema: a deployment without embeddings never
    /// defines this index, and one that changes models redefines it.
    pub async fn ensure_vector_index(&self, dimension: u32) -> copal_core::Result<()> {
        let ddl = schema::text::vector_index(dimension).to_surql_with_options("text_chunk", true);
        self.client
            .query(&ddl)
            .await
            .map_err(|e| CopalError::Store(format!("vector index: {e}")))?;
        Ok(())
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
