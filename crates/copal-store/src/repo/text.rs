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
    /// The resolved marker spans over `body`, empty on rows written
    /// before markers existed or on uploads that declared none.
    #[serde(default)]
    pub withheld: Vec<WithheldSpan>,
    pub extractor: String,
    pub updated_at: String,
}

/// One resolved span over the stored body, in character offsets. The
/// full-text read path elides from these rather than re-resolving
/// anchors, so what it withholds is exactly what extraction resolved
/// against this digest's text.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
pub struct WithheldSpan {
    pub start: usize,
    pub end: usize,
    pub access: String,
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
///
/// `withheld` is the resolved marker spans over `body`, written in
/// the SAME statement as the body on both paths below: a body that
/// exists without its spans would be a marked document serving whole
/// through the full-text read, so the two land or fail together.
pub async fn put_text(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    digest: &str,
    body: &str,
    extractor: &str,
    withheld: &[WithheldSpan],
) -> copal_core::Result<()> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("put_text", e))?;
    let chars = body.chars().count() as i64;
    let spans = serde_json::to_value(withheld)
        .map_err(|e| CopalError::Store(format!("withheld spans encode: {e}")))?;

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
        .set("withheld", spans.clone())
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
        "withheld": spans,
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
                Box::pin(put_text(
                    store, tenant, file, digest, body, extractor, withheld,
                ))
                .await
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

/// One passage to store: its text and, when a marker touched it, its
/// own access level. `None` means the file's level, which is today's
/// behavior and the fail-safe for every unmarked upload.
#[derive(Debug, Clone)]
pub struct ChunkInput {
    pub body: String,
    pub access: Option<copal_core::AccessLevel>,
}

impl ChunkInput {
    /// An unmarked passage, for callers with no markers in hand.
    pub fn plain(body: String) -> Self {
        Self { body, access: None }
    }
}

/// Replace a file's passages.
///
/// Every chunk of the file goes before the new ones land, so a
/// re-extraction cannot leave passages of superseded content behind
/// to answer queries. A chunk's level lands in the same CREATE
/// payload as its body, so no crash between two writes can leave a
/// restricted passage readable: there is no unmarked window, and the
/// delete-first ordering above keeps serving that property across
/// re-uploads whose markers changed.
pub async fn put_chunks(
    store: &Store,
    tenant: &TenantId,
    file: &FileId,
    digest: &str,
    passages: &[ChunkInput],
) -> copal_core::Result<()> {
    let file_rid =
        RecordID::<()>::new("file", file.as_str()).map_err(|e| map_store_err("put_chunks", e))?;
    delete_chunks(store, file).await?;
    for (ordinal, chunk) in passages.iter().enumerate() {
        let id = ulid::Ulid::new().to_string().to_ascii_lowercase();
        let rid = RecordID::<()>::new(CHUNK_TABLE, id.as_str())
            .map_err(|e| map_store_err("put_chunks", e))?;
        let mut payload = json!({
            "tenant_id": tenant.as_str(),
            "digest": digest,
            "ordinal": ordinal,
            "body": chunk.body,
        });
        if let Some(access) = chunk.access {
            payload["access"] = json!(access.as_str());
        }
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
    /// The passage's own level when a marker touched it; absent
    /// means the file's level.
    #[serde(default)]
    pub access: Option<String>,
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
/// Nearest-neighbor search otherwise returns its k nearest however
/// far away they are, so in a small corpus every passage is
/// somebody's neighbor.
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
    semantic_search_impl(store, tenant, embedding, limit, max_distance, filters, true).await
}

/// Test oracle: the semantic leg with the chunk conjunct dropped, so
/// a test can prove a withheld passage WOULD have been a nearest
/// neighbor - that the filter does work rather than the corpus
/// lacking a hit. Never called by production code.
#[doc(hidden)]
pub async fn semantic_search_ignoring_chunk_levels(
    store: &Store,
    tenant: &TenantId,
    embedding: &[f64],
    limit: i64,
    max_distance: f64,
    filters: &SearchFilters,
) -> copal_core::Result<Vec<SearchHit>> {
    semantic_search_impl(
        store,
        tenant,
        embedding,
        limit,
        max_distance,
        filters,
        false,
    )
    .await
}

async fn semantic_search_impl(
    store: &Store,
    tenant: &TenantId,
    embedding: &[f64],
    limit: i64,
    max_distance: f64,
    filters: &SearchFilters,
    enforce_chunk_levels: bool,
) -> copal_core::Result<Vec<SearchHit>> {
    if embedding.is_empty() {
        return Err(CopalError::validation("query embedding must not be empty"));
    }
    // The tenant equality is a residual filter over the index's k
    // nearest, so a tenant with few passages in a large corpus would
    // see fewer than `limit`; over-fetch and let the limit trim. The
    // chunk-level clause is a residual the same way, and the same
    // over-fetch absorbs it: the trimmed limit means result counts
    // reveal nothing about how many neighbors were withheld.
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
        .where_str(FILE_DISCLOSABLE)
        .vector_search_indexed("embedding", embedding.to_vec(), over_fetch, HNSW_EF)
        .map_err(|e| map_store_err("semantic_search", e))?;
    if enforce_chunk_levels {
        query = query.where_str(CHUNK_DISCLOSABLE);
    }
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
/// is the real signal, so hits arrive ranked and unlabeled.
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

/// A field a caller may ask for counts on.
///
/// Closed, because the field name reaches the engine inside a
/// projection this builds by hand. A caller names one of these or gets
/// a validation error; nothing a caller types becomes query text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FacetField {
    ContentType,
    Access,
}

impl FacetField {
    /// The wire name, which is also the file column.
    pub fn as_str(self) -> &'static str {
        match self {
            FacetField::ContentType => "content_type",
            FacetField::Access => "access",
        }
    }

    pub fn parse(raw: &str) -> copal_core::Result<Self> {
        match raw.trim() {
            "content_type" => Ok(FacetField::ContentType),
            "access" => Ok(FacetField::Access),
            other => Err(CopalError::validation(format!(
                "cannot facet on {other}; fields are content_type and access",
            ))),
        }
    }
}

/// One bucket: a value, and how many FILES in the match set carry it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FacetBucket {
    pub value: String,
    pub files: i64,
}

/// Counts over the whole match set, grouped by one field of the file.
///
/// Files rather than passages. A document matching in five passages is
/// one document, and a caller reading "12 PDFs" means twelve of them.
/// The engine's `count()` counts rows, so this groups the linked file
/// ids and measures the distinct set, which is the form that survives
/// `GROUP BY` here (pinned in `tests/engine_assumptions.rs`).
///
/// No limit, deliberately. The ranked page comes from a rescore window
/// and is bounded by it; a count that inherited the same bound would be
/// a number that quietly meant "of the first five hundred". These
/// counts are exact over everything the query matches, which is the
/// only reading of a facet that is worth showing.
pub async fn facet_counts(
    store: &Store,
    tenant: &TenantId,
    terms: &str,
    field: FacetField,
    filters: &SearchFilters,
) -> copal_core::Result<Vec<FacetBucket>> {
    facet_counts_impl(store, tenant, terms, field, filters, true).await
}

/// Test oracle: the same counts with the chunk conjunct dropped, so a
/// test can prove a file whose only matching passages are withheld
/// WOULD have counted - that a bucket's number cannot reveal that a
/// match exists. Never called by production code.
#[doc(hidden)]
pub async fn facet_counts_ignoring_chunk_levels(
    store: &Store,
    tenant: &TenantId,
    terms: &str,
    field: FacetField,
    filters: &SearchFilters,
) -> copal_core::Result<Vec<FacetBucket>> {
    facet_counts_impl(store, tenant, terms, field, filters, false).await
}

async fn facet_counts_impl(
    store: &Store,
    tenant: &TenantId,
    terms: &str,
    field: FacetField,
    filters: &SearchFilters,
    enforce_chunk_levels: bool,
) -> copal_core::Result<Vec<FacetBucket>> {
    if terms.trim().is_empty() {
        return Err(CopalError::validation("search terms must not be empty"));
    }
    #[derive(serde::Deserialize)]
    struct Row {
        value: Option<String>,
        files: Option<i64>,
    }

    // The same WHERE the ranked page runs, chunk clause included, so
    // the counts are computed over the caller-visible match set: a
    // file whose only matching passages are withheld contributes to
    // no bucket, and a count cannot reveal that a match exists.
    let mut query = Query::new()
        .select(Some(vec![
            format!("file.{} AS value", field.as_str()),
            "array::len(array::distinct(array::group(file))) AS files".to_owned(),
        ]))
        .from_table(CHUNK_TABLE)
        .map_err(|e| map_store_err("facet_counts", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_str(FILE_DISCLOSABLE)
        .fulltext_search("body", 1, terms)
        .map_err(|e| map_store_err("facet_counts", e))?;
    if enforce_chunk_levels {
        query = query.where_str(CHUNK_DISCLOSABLE);
    }
    for clause in filters.clauses() {
        query = query.where_str(clause);
    }
    let query = query.group_by(["value"]);

    let rows: Vec<Row> = query_records(store.client(), &query)
        .await
        .map_err(|e| map_store_err("facet_counts", e))?;

    // Largest first, then by value, so a caller rendering the top few
    // gets the same few on every identical request.
    let mut buckets: Vec<FacetBucket> = rows
        .into_iter()
        .filter_map(|row| {
            Some(FacetBucket {
                value: row.value?,
                files: row.files.unwrap_or(0),
            })
        })
        .filter(|bucket| bucket.files > 0)
        .collect();
    buckets.sort_by(|a, b| b.files.cmp(&a.files).then_with(|| a.value.cmp(&b.value)));
    Ok(buckets)
}

/// Files whose extracted text must never surface in retrieval.
///
/// The download path already refuses these, and the soft-delete path
/// already states the principle: search indexes what exists, so text
/// that keeps answering queries is content nobody can fetch leaking
/// through a second door. An excerpt IS the content for a grant-only
/// file, whose bytes flow exclusively through issued URLs.
///
/// The rule mirrors what a read-scoped caller of the same tenant meets
/// on `GET /v1/files/{id}/content`: grant is refused, and
/// `servable_content()` refuses a quarantined record. State otherwise
/// plays no part there, because a failed re-upload keeps the previous
/// version serving, so a failed record is not excluded here either.
/// Deleted records have their chunks purged at deletion; the purge
/// ignores its own errors, so they are named here as well.
///
/// This lives on the queries rather than on [`SearchFilters`] because
/// a filter is something a caller chooses and this is not.
///
/// Public beside [`CHUNK_DISCLOSABLE`] because the engine's second
/// layer composes its `text_chunk` conjunct from these same strings:
/// one statement of the rule, read by the application queries here
/// and by the compiled `PERMISSIONS` clause, so the two enforcement
/// layers cannot drift apart.
pub const FILE_DISCLOSABLE: &str = "file.access != 'grant' \
                           AND file.state != 'quarantined' \
                           AND file.state != 'deleted'";

/// The chunk half of the same rule: a passage whose own level is
/// `grant` answers no search, whatever its file's level admits. NONE
/// means the file's level, so the file clause above already decided
/// for unmarked passages, and every row from before markers existed
/// reads NONE.
///
/// Stated honestly, as the design does: the vocabulary persists all
/// four levels, and only `grant` is operative here, because search
/// is tenant-authenticated and read-scoped on every face, so
/// `public`, `private`, and `tenant` passages all answer the same
/// callers today. When principals split the read path, the divergence
/// lands in this constant, once, and both enforcement layers and both
/// retrieval legs follow.
pub const CHUNK_DISCLOSABLE: &str = "(access IS NONE OR access != 'grant')";

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
    search_impl(store, tenant, terms, limit, filters, true).await
}

/// Test oracle: the same retrieval with the chunk conjunct dropped,
/// so a test can prove a withheld passage WOULD have matched - that
/// the filter is doing work rather than the corpus lacking a hit.
/// Never called by production code, like `tamper_for_test` beside
/// the version freeze.
#[doc(hidden)]
pub async fn search_ignoring_chunk_levels(
    store: &Store,
    tenant: &TenantId,
    terms: &str,
    limit: i64,
    filters: &SearchFilters,
) -> copal_core::Result<Vec<SearchHit>> {
    search_impl(store, tenant, terms, limit, filters, false).await
}

async fn search_impl(
    store: &Store,
    tenant: &TenantId,
    terms: &str,
    limit: i64,
    filters: &SearchFilters,
    enforce_chunk_levels: bool,
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
    // Enforcement sits INSIDE the query, chunk clause beside the file
    // clause, so a withheld passage is never a candidate: it
    // contributes no snippet and no rank, it never reaches the
    // rescore below or the rerank window downstream, and a file whose
    // only matching passages are withheld never surfaces at all.
    let mut query = Query::new()
        .select(Some(vec![
            "file".to_owned(),
            "body".to_owned(),
            "ordinal".to_owned(),
        ]))
        .from_table(CHUNK_TABLE)
        .map_err(|e| map_store_err("search", e))?
        .where_(eq("tenant_id", tenant.as_str()))
        .where_str(FILE_DISCLOSABLE)
        .fulltext_search("body", 1, terms)
        .map_err(|e| map_store_err("search", e))?;
    if enforce_chunk_levels {
        query = query.where_str(CHUNK_DISCLOSABLE);
    }
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
