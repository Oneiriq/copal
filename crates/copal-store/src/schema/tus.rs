//! Resumable-upload cluster: `tus_upload` sessions.
//!
//! A session ties a claimed draft file to a growing staging object.
//! The offset column is the protocol's source of truth; a PATCH lease
//! (same CAS discipline as upload claims) keeps writers exclusive, and
//! expired sessions are swept with their staging bytes.

use surql::schema::{
    datetime_field, field, int_field, record_field, string_field, table_schema, FieldDefinition,
    FieldType, TableDefinition, TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![tus_upload_table()]
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

fn tus_upload_table() -> TableDefinition {
    table_schema("tus_upload")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(record_field("file", Some("file")).nullable(true)),
            built(int_field("upload_length").assertion("$value >= 0")),
            built(int_field("offset").assertion("$value >= 0").default("0")),
            built(string_field("staging_key").assertion("$value != ''")),
            // The session's declared markers, validated at creation
            // and held until the final PATCH: tus carries the
            // declaration in `Upload-Metadata` when the session is
            // born, but the version row it must land on is only born
            // at completion, so the session row is the bridge
            // between those two moments. `any` for the same reason
            // `file_version.markers` is; absent means none declared.
            built(field("markers", FieldType::Any)),
            // Single-writer exclusion for PATCH bodies.
            built(string_field("patch_owner").nullable(true)),
            built(datetime_field("patch_expires_at").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").value("time::now()")),
        ])
}
