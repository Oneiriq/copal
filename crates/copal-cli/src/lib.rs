//! The library behind `copalctl`: one thin, uniform HTTP layer over
//! the Copal API and admin surfaces.
//!
//! The generated Rust SDK covers resources and actions; queries,
//! content faces, and bearer authentication are still ahead of it,
//! and a CLI split across two transports would confuse every error
//! it prints. Until the SDK carries those faces, everything here
//! goes through one [`Api`] with one authentication story, and the
//! CLI converges on the SDK when the generated client catches up.

use anyhow::{bail, Context as _};
use serde_json::Value;

/// How requests identify themselves.
#[derive(Debug, Clone)]
pub enum Auth {
    /// Dev-mode header identity (`x-copal-tenant`).
    Tenant(String),
    /// A minted key (`authorization: Bearer ...`).
    Bearer(String),
    /// No identity: public reads and health checks only.
    Anonymous,
}

/// One configured endpoint.
#[derive(Debug, Clone)]
pub struct Api {
    pub base: String,
    pub auth: Auth,
    pub admin_token: Option<String>,
    http: reqwest::Client,
}

impl Api {
    pub fn new(base: String, auth: Auth, admin_token: Option<String>) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            auth,
            admin_token,
            http: reqwest::Client::new(),
        }
    }

    /// Read the standard environment: `COPAL_URL`, then `COPAL_TOKEN`
    /// or `COPAL_TENANT` for identity, `COPAL_ADMIN_TOKEN` for the
    /// admin surface.
    pub fn from_env() -> Self {
        let base =
            std::env::var("COPAL_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_owned());
        let auth = match std::env::var("COPAL_TOKEN") {
            Ok(token) => Auth::Bearer(token),
            Err(_) => match std::env::var("COPAL_TENANT") {
                Ok(tenant) => Auth::Tenant(tenant),
                Err(_) => Auth::Anonymous,
            },
        };
        Self::new(base, auth, std::env::var("COPAL_ADMIN_TOKEN").ok())
    }

    fn identified(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Auth::Tenant(tenant) => request.header("x-copal-tenant", tenant),
            Auth::Bearer(token) => request.header("authorization", format!("Bearer {token}")),
            Auth::Anonymous => request,
        }
    }

    fn admin(&self, request: reqwest::RequestBuilder) -> anyhow::Result<reqwest::RequestBuilder> {
        let token = self
            .admin_token
            .as_deref()
            .context("COPAL_ADMIN_TOKEN is required for admin commands")?;
        Ok(request.header("x-copal-admin-token", token))
    }

    async fn json_of(response: reqwest::Response) -> anyhow::Result<Value> {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("{status}: {body}");
        }
        if body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&body).with_context(|| format!("unparseable answer: {body}"))
    }

    /// GET a JSON endpoint under the API face.
    pub async fn get(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<Value> {
        let request = self
            .identified(self.http.get(format!("{}{path}", self.base)))
            .query(query);
        Self::json_of(request.send().await?).await
    }

    /// POST a JSON body under the API face.
    pub async fn post(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        let request = self
            .identified(self.http.post(format!("{}{path}", self.base)))
            .json(&body);
        Self::json_of(request.send().await?).await
    }

    /// DELETE under the API face.
    pub async fn delete(&self, path: &str) -> anyhow::Result<Value> {
        let request = self.identified(self.http.delete(format!("{}{path}", self.base)));
        Self::json_of(request.send().await?).await
    }

    /// PUT raw bytes to a content face.
    pub async fn put_bytes(&self, path: &str, bytes: Vec<u8>) -> anyhow::Result<Value> {
        let request = self
            .identified(self.http.put(format!("{}{path}", self.base)))
            .body(bytes);
        Self::json_of(request.send().await?).await
    }

    /// GET raw bytes from a content face.
    pub async fn get_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .identified(self.http.get(format!("{}{path}", self.base)))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            bail!("{status}: {}", response.text().await.unwrap_or_default());
        }
        Ok(response.bytes().await?.to_vec())
    }

    /// GET a JSON endpoint under the admin surface.
    pub async fn admin_get(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<Value> {
        let request = self
            .admin(self.http.get(format!("{}{path}", self.base)))?
            .query(query);
        Self::json_of(request.send().await?).await
    }

    /// POST a JSON body under the admin surface.
    pub async fn admin_post(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        let request = self
            .admin(self.http.post(format!("{}{path}", self.base)))?
            .json(&body);
        Self::json_of(request.send().await?).await
    }

    /// DELETE under the admin surface.
    pub async fn admin_delete(&self, path: &str) -> anyhow::Result<Value> {
        let request = self.admin(self.http.delete(format!("{}{path}", self.base)))?;
        Self::json_of(request.send().await?).await
    }

    /// GET a text endpoint under the admin surface (the audit export
    /// serves NDJSON).
    pub async fn admin_text(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> anyhow::Result<(String, Option<String>)> {
        let request = self
            .admin(self.http.get(format!("{}{path}", self.base)))?
            .query(query);
        let response = request.send().await?;
        let status = response.status();
        let cursor = response
            .headers()
            .get("x-copal-next-cursor")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("{status}: {body}");
        }
        Ok((body, cursor))
    }
}
