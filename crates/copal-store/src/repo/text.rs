//! Extracted text: store it, read it, search it.
//!
//! Search runs in the database that already holds the file records,
//! so a query can filter by tenant and rank by relevance in one
//! statement. The alternative shape, an object store beside a search
//! cluster, needs both systems to agree about what exists; here they
//! cannot disagree because there is one system.

use serde::Deserialize;
use serde_json::{json, Value};

use surql::query::builder::Query;
use surql::query::crud::{create_record, query_records};
use surql::query::expressions::raw;
use surql::types::operators::eq;
use surql::types::RecordID;

use copal_core::{CopalError, FileId, TenantId};

use crate::dto::{map_store_err, strip_record_prefix};
use crate::store::Store;

const TABLE: &str = "file_text";

/// One stored extraction.
#[derive(Debug, Clone, Deserialize)]
pub struct TextRow {
    pub id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub file: Option<String>,
    pub digest: String,
    pub body: String,
    pub chars: i64,
    pub extractor: String,
    pub updated_at: String,
}

impl TextRow {
    /// The bare file id this text came from.
    pub fn file_id(&self) -> Option<String> {
        self.file
            .as_deref()
            .map(|raw| strip_record_prefix(raw, "file").to_owned())
    }
}

/// Store or replace a file's extracted text. One row per file, so a
/// re-extraction overwrites rather than accumulating.
pub async fn put_text(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    digest: &str,
    body: &str,
    extractor: &str,
) -> copal_core::Result<()> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("put_text", e))?;
    let chars = body.chars().count() as i64;

    let update = Query::new()
        .update_set(TABLE)
        .map_err(|e| map_store_err("put_text", e))?
        .set("digest", Value::from(digest))
        .map_err(|e| map_store_err("put_text", e))?
        .set("body", Value::from(body))
        .map_err(|e| map_store_err("put_text", e))?
        .set("chars", Value::from(chars))
        .map_err(|e| map_store_err("put_text", e))?
        .set("extractor", Value::from(extractor))
        .map_err(|e| map_store_err("put_text", e))?
        .where_str(format!("file = {file_rid}"))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &update)
        .await
        .map_err(|e| map_store_err("put_text", e))?;
    if !rows.is_empty() {
        return Ok(());
    }

    let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
    let rid = RecordID::<()>::new(TABLE, id.as_str()).map_err(|e| map_store_err("put_text", e))?;
    let payload = json!({
        "tenant_id": tenant.as_str(),
        "digest": digest,
        "body": body,
        "chars": chars,
        "extractor": extractor,
    });
    create_record(store.client(), &rid.to_string(), payload)
        .await
        .map_err(|e| map_store_err("put_text", e))?;
    // Record links arm via UPDATE; the unique file index lands here,
    // so a racing extractor retries into the update path above.
    let arm = Query::new()
        .update_set(rid.to_string())
        .map_err(|e| map_store_err("put_text", e))?
        .set_expr("file", raw(file_rid.to_string()))
        .map_err(|e| map_store_err("put_text", e))?
        .return_after();
    match query_records::<Value>(store.client(), &arm).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let mapped = map_store_err("put_text", err);
            let cleanup = Query::new()
                .delete(rid.to_string())
                .map_err(|e| map_store_err("put_text", e))?;
            query_records::<Value>(store.client(), &cleanup)
                .await
                .map_err(|e| map_store_err("put_text", e))?;
            if matches!(mapped, CopalError::Conflict(_)) {
                Box::pin(put_text(store, tenant, file, digest, body, extractor)).await
            } else {
                Err(mapped)
            }
        }
    }
}

/// A file's extracted text, tenant-scoped.
pub async fn get_text(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
) -> copal_core::Result<Option<TextRow>> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("get_text", e))?;
    let query = Query::new()
        .select(None)
        .from_table(TABLE)
        .map_err(|e| map_store_err("get_text", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_str(format!("file = {file_rid}"))
        .limit(1)
        .map_err(|e| map_store_err("get_text", e))?;
    let mut rows: Vec<TextRow> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("get_text", e))?;
    Ok(rows.pop())
}

/// Drop a file's extracted text (its content went away).
pub async fn delete_text(store: &Store, file: &FileId) -> copal_core::Result<()> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("delete_text", e))?;
    let query = Query::new()
        .delete(TABLE)
        .map_err(|e| map_store_err("delete_text", e))?
        .where_str(format!("file = {file_rid}"));
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("delete_text", e))?;
    Ok(())
}

/// Attach an embedding to a file's stored text.
///
/// Guarded on the digest: an embedding computed for content that has
/// since been replaced must not attach to the new text.
pub async fn put_embedding(
    store: &Store,
    file: &FileId,
    digest: &str,
    embedding: &[f64],
    model: &str,
) -> copal_core::Result<bool> {
    let file_rid = RecordID::<()>::new("file", file.as_str())
        .map_err(|e| map_store_err("put_embedding", e))?;
    let rendered = format!(
        "[{}]",
        embedding
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
    let update = Query::new()
        .update_set(TABLE)
        .map_err(|e| map_store_err("put_embedding", e))?
        .set_expr("embedding", raw(rendered))
        .map_err(|e| map_store_err("put_embedding", e))?
        .set("embedding_model", Value::from(model))
        .map_err(|e| map_store_err("put_embedding", e))?
        .where_str(format!("file = {file_rid}"))
        .where_(eq("digest", digest))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &update)
        .await
        .map_err(|e| map_store_err("put_embedding", e))?;
    Ok(!rows.is_empty())
}

/// Nearest documents to a query vector, tenant-scoped.
///
/// The `k` nearest come from the index; the tenant equality is a
/// residual filter, so a tenant with few documents in a large corpus
/// can see fewer than `k` results. Over-fetching and trimming keeps
/// that from reading as "no matches".
pub async fn semantic_search(
    store: &Store,
    tenant: &TenantId,
    embedding: &[f64],
    limit: i64,
) -> copal_core::Result<Vec<SearchHit>> {
    if embedding.is_empty() {
        return Err(CopalError::validation("query embedding must not be empty"));
    }
    let over_fetch = (limit * 10).clamp(limit, 500);
    let query = Query::new()
        .select(Some(vec![
            "file".to_owned(),
            "body".to_owned(),
            "chars".to_owned(),
        ]))
        .from_table(TABLE)
        .map_err(|e| map_store_err("semantic_search", e))?
        .vector_search(
            "embedding",
            embedding.to_vec(),
            over_fetch,
            surql::query::helpers::VectorDistanceType::Cosine,
            None,
        )
        .map_err(|e| map_store_err("semantic_search", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .limit(limit)
        .map_err(|e| map_store_err("semantic_search", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("semantic_search", e))
}

/// One search hit: the file and the text that matched.
///
/// There is no score field on purpose. SurrealDB 3.x does not plumb
/// per-row BM25 values through the full-text scan (`search::score`
/// returns 0), so a score column would be a constant pretending to be
/// relevance. The scan itself returns rows in relevance order, which
/// is the real signal, so hits arrive ranked and unlabelled.
#[derive(Debug, Clone, Deserialize)]
pub struct SearchHit {
    #[serde(default)]
    pub file: Option<String>,
    pub body: String,
    pub chars: i64,
}

impl SearchHit {
    /// The bare file id.
    pub fn file_id(&self) -> Option<String> {
        self.file
            .as_deref()
            .map(|raw| strip_record_prefix(raw, "file").to_owned())
    }
}

/// Lexical search over a tenant's extracted text, in relevance order.
///
/// The tenant predicate and the search predicate are one statement,
/// so a hit that is not this tenant's cannot be returned and then
/// filtered: it is never a hit. EXPLAIN shows the full-text scan
/// driving with the tenant equality as a residual filter, which keeps
/// the scan's ordering intact.
///
/// No ORDER BY: the scan already yields rows by relevance, and
/// sorting on `search::score` (which this engine returns as 0) would
/// replace that ordering with an arbitrary one.
pub async fn search(
    store: &Store,
    tenant: &TenantId,
    terms: &str,
    limit: i64,
) -> copal_core::Result<Vec<SearchHit>> {
    if terms.trim().is_empty() {
        return Err(CopalError::validation("search terms must not be empty"));
    }
    let query = Query::new()
        .select(Some(vec![
            "file".to_owned(),
            "body".to_owned(),
            "chars".to_owned(),
        ]))
        .from_table(TABLE)
        .map_err(|e| map_store_err("search", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .fulltext_search("body", 1, terms)
        .map_err(|e| map_store_err("search", e))?
        .limit(limit)
        .map_err(|e| map_store_err("search", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("search", e))
}
