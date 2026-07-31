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
        Ok(store)
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

    /// Borrow the underlying client for repository functions.
    pub(crate) fn client(&self) -> &DatabaseClient {
        &self.client
    }
}
