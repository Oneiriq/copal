//! Core cluster: `file`, `blob`, `file_version`.
//!
//! Translated verbatim from the probed design. Receipts that shaped it,
//! all verified live against SurrealDB v3.0.5:
//!
//! - `file` is a valid table name even with the files capability enabled.
//! - Unique indexes ignore absent/NONE values (single and composite), so
//!   `idempotency_key` needs no generated default.
//! - The `live_marker` VALUE sentinel recomputes on soft-delete, making
//!   `(tenant_id, path, live_marker)` enforce one live row per path with
//!   unlimited tombstones.
//! - `THROW` inside an event aborts the statement, freezing
//!   `file_version` rows at the engine.

use surql::schema::{
    datetime_field, event, index, int_field, object_field, record_field, string_field,
    table_schema, unique_index, FieldDefinition, TableDefinition, TableMode,
};

/// All tables in this cluster, in application order (`blob` before `file`
/// so record links always target an existing table definition).
pub fn tables() -> Vec<TableDefinition> {
    vec![blob_table(), file_table(), file_version_table()]
}

/// Shorthand: every schema field is built `build_unchecked` because the
/// merge module's reserved-word gate covers naming centrally.
fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

fn file_table() -> TableDefinition {
    table_schema("file")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(string_field("path").assertion("$value != ''")),
            built(
                string_field("state")
                    .assertion(
                        "$value INSIDE ['draft', 'uploading', 'scanning', 'ready', 'failed', \
                         'quarantined', 'deleted']",
                    )
                    .default("'draft'"),
            ),
            built(
                string_field("access")
                    .assertion("$value INSIDE ['public', 'private', 'tenant', 'grant']")
                    .default("'private'"),
            ),
            built(record_field("blob", Some("blob")).nullable(true)),
            built(record_field("current_version", Some("file_version")).nullable(true)),
            // Renditions are links, not edges: one source per derived file.
            built(record_field("derived_from", Some("file")).nullable(true)),
            built(string_field("rendition_kind").nullable(true)),
            built(string_field("rendition_params_digest").nullable(true)),
            built(string_field("content_type").default("'application/octet-stream'")),
            built(int_field("size_bytes").nullable(true)),
            built(string_field("digest").nullable(true)),
            built(string_field("idempotency_key").nullable(true)),
            built(object_field("metadata")),
            built(string_field("created_by")),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").value("time::now()")),
            built(datetime_field("expires_at").nullable(true)),
            built(datetime_field("deleted_at").nullable(true)),
            // Upload claim lease. Set on claim (expiry computed
            // server-side from time::now()), cleared on any transition
            // out of `uploading`. An expired lease is stealable and is
            // what the reaper sweeps to `failed`.
            built(string_field("upload_lease_owner").nullable(true)),
            built(datetime_field("upload_lease_expires_at").nullable(true)),
            // '' while live, the record id string once deleted. Probed:
            // VALUE recomputes on update, so the composite unique below
            // holds exactly one live row per (tenant, path).
            built(
                string_field("live_marker")
                    .value("IF deleted_at IS NONE THEN '' ELSE <string>id END"),
            ),
        ])
        .with_indexes([
            unique_index("uniq_file_live_path", ["tenant_id", "path", "live_marker"]),
            unique_index("uniq_file_idempotency", ["tenant_id", "idempotency_key"]),
            index("idx_file_listing", ["tenant_id", "state", "created_at"]),
            index("idx_file_renditions", ["derived_from", "rendition_kind"]),
            index("idx_file_expiry", ["expires_at"]),
            index("idx_file_blob", ["blob"]),
        ])
}

fn blob_table() -> TableDefinition {
    table_schema("blob")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            // Mirrors the record id (blob:<sha256>); kept as a column so
            // projections never need id string surgery.
            built(string_field("digest").assertion("string::len($value) = 64")),
            built(int_field("size_bytes")),
            // The store registry lands in a later cluster; until then the
            // backend is named by key so rows stay meaningful.
            built(string_field("store_key")),
            built(string_field("storage_path")),
            // Advisory cache. The authoritative reference count is
            // DERIVED by counting inbound `file.blob` links over
            // idx_file_blob (see repo::blob::recount_inbound_links):
            // an increment-based counter drifts upward when a crash
            // lands between sighting and link, and an undercount would
            // let GC delete live data. Sweeps refresh this column.
            built(int_field("refcount").default("0")),
            built(datetime_field("unreferenced_since").nullable(true)),
            built(
                string_field("lock_mode")
                    .nullable(true)
                    .assertion("$value INSIDE ['governance', 'compliance']"),
            ),
            built(datetime_field("lock_until").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([index("idx_blob_gc", ["refcount", "unreferenced_since"])])
}

fn file_version_table() -> TableDefinition {
    table_schema("file_version")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").readonly(true)),
            built(record_field("file", Some("file")).readonly(true)),
            built(int_field("number").assertion("$value >= 1").readonly(true)),
            built(record_field("blob", Some("blob")).readonly(true)),
            built(
                record_field("prior", Some("file_version"))
                    .nullable(true)
                    .readonly(true),
            ),
            built(string_field("content_type").readonly(true)),
            built(int_field("size_bytes").readonly(true)),
            built(string_field("digest").readonly(true)),
            built(object_field("metadata_snapshot").readonly(true)),
            built(string_field("created_by").readonly(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([unique_index("uniq_version_number", ["file", "number"])])
        .with_events([event(
            "file_version_frozen",
            "$event = 'UPDATE'",
            "THROW 'file_version records are immutable'",
        )])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> TableDefinition {
        tables()
            .into_iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("table {name} missing"))
    }

    #[test]
    fn optional_columns_render_option_types() {
        let ddl = surql::schema::generate_table_sql(&table("file"), false).join("\n");
        assert!(ddl.contains("DEFINE FIELD deleted_at ON TABLE file TYPE option<datetime>"));
        assert!(ddl.contains("DEFINE FIELD blob ON TABLE file TYPE option<record<blob>>"));
    }

    #[test]
    fn live_marker_sentinel_matches_probed_expression() {
        let ddl = surql::schema::generate_table_sql(&table("file"), false).join("\n");
        assert!(ddl.contains("VALUE IF deleted_at IS NONE THEN '' ELSE <string>id END"));
    }

    #[test]
    fn file_version_carries_the_freeze_event() {
        let ddl = surql::schema::generate_table_sql(&table("file_version"), false).join("\n");
        assert!(ddl.contains("THROW 'file_version records are immutable'"));
    }

    #[test]
    fn load_bearing_indexes_exist() {
        let file = table("file");
        let names: Vec<_> = file.indexes.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"uniq_file_live_path"));
        assert!(names.contains(&"uniq_file_idempotency"));
    }
}
