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
    bm25_index, datetime_field, hnsw_index, index, int_field, record_field, standard_analyzer,
    string_field, table_schema, unique_index, AnalyzerDefinition, FieldDefinition,
    HnswDistanceType, IndexDefinition, MTreeVectorType, TableDefinition, TableMode, TokenFilter,
};

/// The analyzer the text index uses: class tokenizer, lowercased and
/// ASCII-folded, with English stemming so "running" finds "run".
pub fn analyzers() -> Vec<AnalyzerDefinition> {
    vec![standard_analyzer("copal_text").with_filter(TokenFilter::snowball("english"))]
}

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![file_text_table()]
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
            // The document's embedding, when one was computed. Its
            // width is the deployment's model's business, so the
            // vector index is defined at startup rather than here.
            built(surql::schema::array_field("embedding").nullable(true)),
            built(string_field("embedding_model").nullable(true)),
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
            bm25_index("idx_text_body", ["body"], "copal_text"),
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
        "idx_text_embedding",
        "embedding",
        dimension,
        // Cosine is the metric the common embedding models are
        // trained for; their vectors are direction, not magnitude.
        HnswDistanceType::Cosine,
        MTreeVectorType::F64,
        None,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_index_is_analyzed_and_scored() {
        let ddl = surql::schema::generate_table_sql(&file_text_table(), false).join("\n");
        assert!(
            ddl.contains("FULLTEXT ANALYZER copal_text BM25"),
            "the text index must be analyzed and BM25-scored: {ddl}",
        );
    }

    #[test]
    fn the_vector_index_carries_its_dimension() {
        let ddl = vector_index(768).to_surql("file_text");
        assert!(ddl.contains("HNSW DIMENSION 768"), "{ddl}");
        assert!(ddl.contains("DIST COSINE"), "{ddl}");
    }

    #[test]
    fn the_analyzer_stems() {
        let sql = analyzers()[0].to_surql();
        assert!(sql.contains("snowball(english)"), "{sql}");
        assert!(sql.contains("lowercase"), "{sql}");
    }
}
