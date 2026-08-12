//! Text cluster: extracted document text and its search index.
//!
//! Text lives in its own table rather than on the file row: a
//! document's text can be megabytes, and file records are read on
//! every listing. The row records WHICH content it came from, so a
//! re-upload's stale text is detectable the same way a stale scan
//! verdict is.
//!
//! The BM25 index is what makes stored files searchable without a
//! second datastore. SurrealDB analyzes and scores in the database
//! Copal already runs on, so lexical recall costs an index
//! definition rather than a search cluster.

use surql::schema::{
    bm25_index, datetime_field, field, hnsw_index, index, int_field, record_field,
    standard_analyzer, string_field, table_schema, unique_index, AnalyzerDefinition,
    FieldDefinition, FieldType, HnswDistanceType, IndexDefinition, MTreeVectorType,
    TableDefinition, TableMode, TokenFilter,
};

/// The analyzer the text index uses: class tokenizer, lowercased and
/// ASCII-folded, with English stemming so "running" finds "run".
pub fn analyzers() -> Vec<AnalyzerDefinition> {
    vec![standard_analyzer("copal_text").with_filter(TokenFilter::snowball("english"))]
}

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![file_text_table(), text_chunk_table()]
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

fn file_text_table() -> TableDefinition {
    table_schema("file_text")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(record_field("file", Some("file")).nullable(true)),
            // The content this text was extracted from; a re-upload
            // leaves the old row detectable rather than silently
            // authoritative.
            built(string_field("digest").assertion("$value != ''")),
            built(string_field("body")),
            built(int_field("chars").default("0")),
            // The resolved marker spans over the stored body, as
            // `{start, end, access}` character offsets, written in
            // the same statement as the body: the full-text read
            // path enforces from these without re-resolving anchors,
            // and no crash between two writes can leave a marked
            // body readable with its spans missing. `any` rather
            // than `array`, deliberately: a schemafull `array` field
            // refuses object items outright, `FLEXIBLE` attaches
            // only to object-bearing types the builder cannot spell,
            // and `any` carries the array of objects verbatim (all
            // three probed on mem:// and pinned in
            // `tests/engine_assumptions.rs`). Absent means no spans,
            // which is every row written before markers existed.
            built(field("withheld", FieldType::Any)),
            // What produced it: `native` for text Copal decoded
            // itself, or the extractor's name.
            built(string_field("extractor").default("'native'")),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").value("time::now()")),
        ])
        .with_indexes([
            // One text row per file; a re-extraction replaces it.
            unique_index("uniq_text_file", ["file"]),
            index("idx_text_tenant", ["tenant_id", "created_at"]),
        ])
}

/// A passage of a document: the unit retrieval actually returns.
///
/// One embedding for a fifty-page document points at its average
/// meaning, which is nobody's question. Chunks are what make
/// retrieval answer "which passage says this" rather than "which
/// file is vaguely about this", so both indexes live here and the
/// document row keeps only the full text for reading back.
fn text_chunk_table() -> TableDefinition {
    table_schema("text_chunk")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(record_field("file", Some("file")).nullable(true)),
            // The content this passage came from; a re-extraction
            // replaces every chunk of the file.
            built(string_field("digest").assertion("$value != ''")),
            // Position in the document, so a hit can be located.
            built(int_field("ordinal").assertion("$value >= 0")),
            // The passage's own access level, when an upload marker
            // touched it; NONE means the file's level, which is
            // today's behavior. The NONE IS the migration: the boot
            // reconciler adds the column, every existing row reads
            // NONE, and NONE defers to the file, so a deployment
            // upgrades into exactly the behavior it had - no
            // backfill, nothing recomputed, the same additive shape
            // principals used for `principal_id` on `api_key`. The
            // vocabulary is the file vocabulary so the read-path
            // divergence principals are building toward reaches
            // chunks with no schema change.
            built(string_field("access").nullable(true).assertion(
                "$value == NONE OR $value INSIDE ['public', 'private', 'tenant', 'grant']",
            )),
            built(string_field("body")),
            built(surql::schema::array_field("embedding").nullable(true)),
            built(string_field("embedding_model").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([
            unique_index("uniq_chunk_position", ["file", "ordinal"]),
            index("idx_chunk_tenant", ["tenant_id", "created_at"]),
            bm25_index("idx_chunk_body", ["body"], "copal_text"),
        ])
}

/// The vector index over stored embeddings.
///
/// HNSW needs its dimension at definition time, and the dimension is
/// whatever the configured embedding model emits. So this is not part
/// of the static schema: a deployment with embeddings configured
/// applies it at startup, and one without never defines it.
pub fn vector_index(dimension: u32) -> IndexDefinition {
    hnsw_index(
        "idx_chunk_embedding",
        "embedding",
        dimension,
        // Cosine is the metric the common embedding models are
        // trained for; their vectors are direction, not magnitude.
        HnswDistanceType::Cosine,
        // F16: embedding models emit single precision at best, and
        // similarity survives half precision because the vectors are
        // compared by direction, so F32 spent double the index memory
        // on digits that never mattered. The engine accepts F16 as of
        // surrealdb 3.2.4 (surql 0.33); a deployment upgrading in
        // place rebuilds this index once, in the background, through
        // the same CONCURRENTLY path every non-unique index takes.
        //
        // DISKANN parses now too, and stays deliberately unadopted:
        // it trades the in-memory bound for disk-resident search, a
        // different recall and latency profile that a deployment
        // should choose knowingly rather than inherit from a default.
        // The seam is this function; the day a deployment needs the
        // bound lifted, the decision lands here, named.
        MTreeVectorType::F16,
        None,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passages_carry_both_search_indexes() {
        // Retrieval happens over chunks, so both the lexical and the
        // vector index belong to that table.
        let ddl = surql::schema::generate_table_sql(&text_chunk_table(), false).join("\n");
        assert!(
            ddl.contains("FULLTEXT ANALYZER copal_text BM25"),
            "passages are analyzed and BM25-scored: {ddl}",
        );
        let vector = vector_index(768).to_surql("text_chunk");
        assert!(vector.contains("HNSW DIMENSION 768"), "{vector}");
        assert!(vector.contains("DIST COSINE"), "{vector}");
        assert!(
            vector.contains("TYPE F16"),
            "half precision carries direction whole: {vector}",
        );

        // The document row keeps the text for reading back, not for
        // searching: two lexical indexes over the same words would
        // return the same file twice under different rankings.
        let document = surql::schema::generate_table_sql(&file_text_table(), false).join("\n");
        assert!(!document.contains("BM25"), "{document}");
    }

    #[test]
    fn the_analyzer_stems() {
        let sql = analyzers()[0].to_surql();
        assert!(sql.contains("snowball(english)"), "{sql}");
        assert!(sql.contains("lowercase"), "{sql}");
    }

    #[test]
    fn chunk_levels_are_nullable_and_speak_the_file_vocabulary() {
        // NONE means the file's level; the assertion admits exactly
        // the four words the file row admits, so a chunk can never
        // hold a level no enforcement clause understands.
        let ddl = surql::schema::generate_table_sql(&text_chunk_table(), false).join("\n");
        assert!(
            ddl.contains("DEFINE FIELD access ON TABLE text_chunk TYPE option<string>"),
            "{ddl}",
        );
        assert!(
            ddl.contains("'public', 'private', 'tenant', 'grant'"),
            "{ddl}",
        );
    }

    #[test]
    fn withheld_spans_ride_the_document_row_as_any() {
        // `any` is what carries an array of span objects through a
        // schemafull table (probed; pinned in engine_assumptions.rs).
        let ddl = surql::schema::generate_table_sql(&file_text_table(), false).join("\n");
        assert!(
            ddl.contains("DEFINE FIELD withheld ON TABLE file_text TYPE any"),
            "{ddl}",
        );
    }
}
