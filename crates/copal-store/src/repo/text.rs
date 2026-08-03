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

const CHUNK_TABLE: &str = "text_chunk";

/// Replace a file's passages.
///
/// Every chunk of the file goes before the new ones land, so a
/// re-extraction cannot leave passages of superseded content behind
/// to answer queries.
pub async fn put_chunks(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    digest: &str,
    passages: &[String],
) -> copal_core::Result<()> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("put_chunks", e))?;
    delete_chunks(store, file).await?;
    for (ordinal, body) in passages.iter().enumerate() {
        let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
        let rid = RecordID::<()>::new(CHUNK_TABLE, id.as_str())
            .map_err(|e| map_store_err("put_chunks", e))?;
        let payload = json!({
            "tenant_id": tenant.as_str(),
            "digest": digest,
            "ordinal": ordinal,
            "body": body,
        });
        create_record(store.client(), &rid.to_string(), payload)
            .await
            .map_err(|e| map_store_err("put_chunks", e))?;
        let arm = Query::new()
            .update_set(rid.to_string())
            .map_err(|e| map_store_err("put_chunks", e))?
            .set_expr("file", raw(file_rid.to_string()))
            .map_err(|e| map_store_err("put_chunks", e))?
            .return_after();
        query_records::<Value>(store.client(), &arm)
            .await
            .map_err(|e| map_store_err("put_chunks", e))?;
    }
    Ok(())
}

/// Drop every passage of a file.
pub async fn delete_chunks(store: &Store, file: &FileId) -> copal_core::Result<()> {
    let file_rid = RecordID::<()>::new("file", file.as_str())
        .map_err(|e| map_store_err("delete_chunks", e))?;
    let query = Query::new()
        .delete(CHUNK_TABLE)
        .map_err(|e| map_store_err("delete_chunks", e))?
        .where_str(format!("file = {file_rid}"));
    query_records::<Value>(store.client(), &query)
        .await
        .map_err(|e| map_store_err("delete_chunks", e))?;
    Ok(())
}

/// One stored passage.
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkRow {
    pub id: String,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub digest: String,
    pub ordinal: i64,
    pub body: String,
}

impl ChunkRow {
    /// The bare chunk id.
    pub fn chunk_id(&self) -> String {
        strip_record_prefix(&self.id, CHUNK_TABLE).to_owned()
    }

    /// The bare file id this passage belongs to.
    pub fn file_id(&self) -> Option<String> {
        self.file
            .as_deref()
            .map(|raw| strip_record_prefix(raw, "file").to_owned())
    }
}

/// A file's passages that still lack an embedding, oldest first.
pub async fn chunks_without_embedding(
    store: &Store,
    file: &FileId,
) -> copal_core::Result<Vec<ChunkRow>> {
    let file_rid = RecordID::<()>::new("file", file.as_str())
        .map_err(|e| map_store_err("pending_chunks", e))?;
    let query = Query::new()
        .select(None)
        .from_table(CHUNK_TABLE)
        .map_err(|e| map_store_err("pending_chunks", e))?
        .where_str(format!("file = {file_rid}"))
        .where_str("embedding IS NONE")
        .order_by("ordinal", "ASC")
        .map_err(|e| map_store_err("pending_chunks", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("pending_chunks", e))
}

/// Attach an embedding to one passage.
///
/// Guarded on the digest: an embedding computed for content that has
/// since been replaced must not attach to the new text.
pub async fn put_embedding(
    store: &Store,
    chunk_id: &str,
    digest: &str,
    embedding: &[f64],
    model: &str,
) -> copal_core::Result<bool> {
    let rid = RecordID::<()>::new(CHUNK_TABLE, chunk_id)
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
        .update_set(rid.to_string())
        .map_err(|e| map_store_err("put_embedding", e))?
        .set_expr("embedding", raw(rendered))
        .map_err(|e| map_store_err("put_embedding", e))?
        .set("embedding_model", Value::from(model))
        .map_err(|e| map_store_err("put_embedding", e))?
        // Guarded on the digest: a vector computed for content that
        // has since been replaced must not attach to new passages.
        .where_(eq("digest", digest))
        .return_after();
    let rows: Vec<Value> = query_records(store.client(), &update)
        .await
        .map_err(|e| map_store_err("put_embedding", e))?;
    Ok(!rows.is_empty())
}

/// How hard the index searches. Higher explores more of the graph
/// for better recall at more work; this is the server's own default
/// range and needs no tuning until a corpus is large.
const HNSW_EF: i64 = 64;

/// Nearest passages to a query vector, tenant-scoped, no further away
/// than `max_distance` in cosine distance (0 is identical, 1 is
/// unrelated, 2 is opposite).
///
/// The floor is what lets a semantic query say "nothing matches".
/// Nearest-neighbour search otherwise returns its k nearest however
/// far away they are, so in a small corpus every passage is
/// somebody's neighbour.
///
/// `vector_search_indexed` renders `<|k,EF|>`, the form that reaches
/// the HNSW index (EXPLAIN: KnnScan). `Query::vector_search` renders
/// `<|k,METRIC|>`, which on SurrealDB 3.x scans the whole table
/// (EXPLAIN: KnnTopK over TableScan).
/// Narrowing a retrieval to a slice of the corpus. Both legs apply
/// these at the engine, through the chunk's file link, so a filtered
/// search never pays to rank passages it must then discard.
#[derive(Debug, Clone, Default)]
pub struct SearchFilters {
    /// Keep passages whose file path starts with this.
    pub path_prefix: Option<String>,
    /// Keep passages whose file carries this content type.
    pub content_type: Option<String>,
}

impl SearchFilters {
    fn clauses(&self) -> Vec<String> {
        let mut clauses = Vec::new();
        if let Some(prefix) = &self.path_prefix {
            clauses.push(format!(
                "string::starts_with(file.path, '{}')",
                escape_single(prefix),
            ));
        }
        if let Some(content_type) = &self.content_type {
            clauses.push(format!(
                "file.content_type = '{}'",
                escape_single(content_type),
            ));
        }
        clauses
    }
}

fn escape_single(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Chunks whose vector is absent or was produced by a different
/// model. The backfill drains this set after a model change, so old
/// geometry never serves under a new model's index.
pub async fn stale_chunks(
    store: &Store,
    model: &str,
    batch: i64,
) -> copal_core::Result<Vec<ChunkRow>> {
    let query = Query::new()
        .select(None)
        .from_table(CHUNK_TABLE)
        .map_err(|e| map_store_err("stale_chunks", e))?
        .where_str(format!(
            "(embedding IS NONE OR embedding_model IS NONE OR embedding_model != '{}')",
            escape_single(model),
        ))
        .limit(batch)
        .map_err(|e| map_store_err("stale_chunks", e))?;
    query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("stale_chunks", e))
}

pub async fn semantic_search(
    store: &Store,
    tenant: &TenantId,
    embedding: &[f64],
    limit: i64,
    max_distance: f64,
    filters: &SearchFilters,
) -> copal_core::Result<Vec<SearchHit>> {
    if embedding.is_empty() {
        return Err(CopalError::validation("query embedding must not be empty"));
    }
    // The tenant equality is a residual filter over the index's k
    // nearest, so a tenant with few passages in a large corpus would
    // see fewer than `limit`; over-fetch and let the limit trim.
    let over_fetch = (limit * 10).clamp(limit, 500);
    let mut query = Query::new()
        .select(Some(vec![
            "file".to_owned(),
            "body".to_owned(),
            "ordinal".to_owned(),
        ]))
        .from_table(CHUNK_TABLE)
        .map_err(|e| map_store_err("semantic_search", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .vector_search_indexed("embedding", embedding.to_vec(), over_fetch, HNSW_EF)
        .map_err(|e| map_store_err("semantic_search", e))?;
    for clause in filters.clauses() {
        query = query.where_str(clause);
    }
    let query = query
        .where_str(format!("vector::distance::knn() <= {max_distance}"))
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
    /// Which passage of the document matched.
    #[serde(default)]
    pub ordinal: i64,
}

impl SearchHit {
    /// The bare file id.
    pub fn file_id(&self) -> Option<String> {
        self.file
            .as_deref()
            .map(|raw| strip_record_prefix(raw, "file").to_owned())
    }
}

/// How many matches to pull back before scoring them. The engine
/// finds matches through the index but returns them unranked, so the
/// best hit can sit anywhere in the match set; taking `limit` rows
/// straight from the scan would be taking the oldest ones. This bounds
/// how much of a large match set gets rescored.
const RESCORE_WINDOW: i64 = 500;

/// Lexical search over a tenant's extracted text, in relevance order.
///
/// The tenant predicate and the search predicate are one statement,
/// so a hit that is not this tenant's cannot be returned and then
/// filtered: it is never a hit.
///
/// Ranking happens in this function. SurrealDB 3.x uses the
/// BM25 index to decide which rows match and then yields them in
/// insertion order, reporting `search::score` as 0 for every one
/// (both pinned by tests in `tests/engine_assumptions.rs`). So the
/// engine does the selective part through the index and this function
/// rescores the candidate window, which is the two-stage shape
/// production search uses anyway.
///
/// A corpus with more than `RESCORE_WINDOW` matches for one query
/// ranks the window rather than the whole match set. That is a real
/// bound, and it is the honest one to take: the alternative is
/// reading every match into memory to rank it.
pub async fn search(
    store: &Store,
    tenant: &TenantId,
    terms: &str,
    limit: i64,
    filters: &SearchFilters,
) -> copal_core::Result<Vec<SearchHit>> {
    if terms.trim().is_empty() {
        return Err(CopalError::validation("search terms must not be empty"));
    }
    // Over-fetch to rescore, capped, and never fewer rows than the
    // caller asked for.
    let window = limit
        .saturating_mul(10)
        .min(RESCORE_WINDOW)
        .max(limit)
        .max(1);
    let mut query = Query::new()
        .select(Some(vec![
            "file".to_owned(),
            "body".to_owned(),
            "ordinal".to_owned(),
        ]))
        .from_table(CHUNK_TABLE)
        .map_err(|e| map_store_err("search", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .fulltext_search("body", 1, terms)
        .map_err(|e| map_store_err("search", e))?;
    for clause in filters.clauses() {
        query = query.where_str(clause);
    }
    let query = query
        .limit(window)
        .map_err(|e| map_store_err("search", e))?;
    let candidates: Vec<SearchHit> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("search", e))?;

    let bodies: Vec<String> = candidates.iter().map(|hit| hit.body.clone()).collect();
    Ok(copal_core::rank_lexical(terms, &bodies)
        .into_iter()
        .take(limit.max(0) as usize)
        .map(|scored| candidates[scored.index].clone())
        .collect())
}
