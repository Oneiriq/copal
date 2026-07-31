//! S3 multipart cluster: `s3_multipart` and `s3_multipart_part`.
//!
//! The aws CLI switches to multipart above 8 MiB, so the gateway
//! needs it for ordinary large-file copies. A session holds staged
//! parts under its own prefix; parts arrive in any order, in
//! parallel, and may be re-sent (S3 semantics replace a part number),
//! so part rows carry a unique pair and the bytes live at a key
//! derived from the part number.
//!
//! No file record exists until completion. S3 allows concurrent
//! multipart uploads to one key, while Copal allows one live file per
//! path; deferring the record to completion keeps both true and
//! leaves abandoned sessions with nothing to clean but bytes.

use surql::schema::{
    datetime_field, index, int_field, record_field, string_field, table_schema, unique_index,
    FieldDefinition, TableDefinition, TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![multipart_table(), multipart_part_table()]
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

fn multipart_table() -> TableDefinition {
    table_schema("s3_multipart")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            // The object key the completed upload will land at.
            built(string_field("object_key").assertion("$value != ''")),
            built(string_field("content_type").default("'application/octet-stream'")),
            built(string_field("staging_prefix").assertion("$value != ''")),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").value("time::now()")),
        ])
        .with_indexes([index("idx_mpu_tenant", ["tenant_id", "created_at"])])
}

fn multipart_part_table() -> TableDefinition {
    table_schema("s3_multipart_part")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(record_field("upload", Some("s3_multipart")).nullable(true)),
            built(int_field("part_number").assertion("$value >= 1 AND $value <= 10000")),
            built(int_field("size_bytes").assertion("$value >= 0")),
            // The part's own digest, returned as its ETag and checked
            // against the completion manifest.
            built(string_field("digest").assertion("$value != ''")),
            built(string_field("staging_key").assertion("$value != ''")),
            built(datetime_field("updated_at").value("time::now()")),
        ])
        .with_indexes([unique_index("uniq_mpu_part", ["upload", "part_number"])])
}
