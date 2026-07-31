//! Delivery cluster: `access_grant`.
//!
//! A grant is a revocable, optionally use-limited capability to perform
//! one operation on one file, held as a bearer token whose secret the
//! store never sees in the clear. Creation is two-step: the row is
//! CREATEd inert (no `file` link, no expiry) and then ARMED in one
//! UPDATE that sets both, because record links and server-computed datetimes
//! are UPDATE-side constructs, and an un-armed grant fails every
//! redemption guard, so a crash between the steps leaves nothing
//! usable.

use surql::schema::{
    datetime_field, index, int_field, record_field, string_field, table_schema, FieldDefinition,
    TableDefinition, TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![access_grant_table()]
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

fn access_grant_table() -> TableDefinition {
    table_schema("access_grant")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            // Armed by the post-create UPDATE; nullable so the inert
            // row is schemafull-valid.
            built(record_field("file", Some("file")).nullable(true)),
            // What the capability authorizes: reading the file's
            // bytes, or writing them once (a browser-direct upload).
            built(
                string_field("op")
                    .assertion("$value INSIDE ['get', 'put']")
                    .default("'get'"),
            ),
            // sha256 of the bearer secret; the secret itself never
            // reaches the store.
            built(string_field("secret_hash").assertion("string::len($value) = 64")),
            built(datetime_field("expires_at").nullable(true)),
            built(int_field("max_uses").nullable(true)),
            built(int_field("uses").default("0")),
            built(datetime_field("revoked_at").nullable(true)),
            built(string_field("created_by")),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([
            // Revoke-all-for-file and grant listings.
            index("idx_grant_file", ["file", "created_at"]),
            index("idx_grant_tenant", ["tenant_id", "created_at"]),
        ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_ddl_renders_armable_shape() {
        let ddl = surql::schema::generate_table_sql(&access_grant_table(), false).join("\n");
        assert!(ddl.contains("DEFINE FIELD file ON TABLE access_grant TYPE option<record<file>>"));
        assert!(ddl.contains("DEFINE FIELD expires_at ON TABLE access_grant TYPE option<datetime>"));
        assert!(ddl.contains("$value INSIDE ['get']"));
    }
}
