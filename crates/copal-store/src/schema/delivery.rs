//! Delivery cluster: `access_grant` and `short_link`.
//!
//! A grant is a revocable, optionally use-limited capability to perform
//! one operation on one file, held as a bearer token whose secret the
//! store never sees in the clear. Creation is two-step: the row is
//! CREATEd inert (no `file` link, no expiry) and then ARMED in one
//! UPDATE that sets both, because record links and server-computed datetimes
//! are UPDATE-side constructs, and an un-armed grant fails every
//! redemption guard, so a crash between the steps leaves nothing
//! usable.
//!
//! A short link is a typeable name for one render grant, keyed by the
//! link itself so the engine enforces that no two links collide.

use surql::schema::{
    datetime_field, index, int_field, record_field, string_field, table_schema, FieldDefinition,
    TableDefinition, TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![access_grant_table(), short_link_table()]
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
            // bytes, writing them once (a browser-direct upload), or
            // displaying them on the sandboxed render listener.
            built(
                string_field("op")
                    .assertion("$value INSIDE ['get', 'put', 'render']")
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

fn short_link_table() -> TableDefinition {
    table_schema("short_link")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            // The render grant the link opens. A plain id, not a record
            // link, because the grant row is fetched by id at redemption
            // and repointing replaces it wholesale.
            built(string_field("grant_id").assertion("$value != ''")),
            built(string_field("created_by")),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").default("time::now()")),
        ])
        .with_indexes([index("idx_short_link_tenant", ["tenant_id", "created_at"])])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_ddl_renders_armable_shape() {
        let ddl = surql::schema::generate_table_sql(&access_grant_table(), false).join("\n");
        assert!(ddl.contains("DEFINE FIELD file ON TABLE access_grant TYPE option<record<file>>"));
        assert!(ddl.contains("DEFINE FIELD expires_at ON TABLE access_grant TYPE option<datetime>"));
        // The whole capability vocabulary: reads, the single-use
        // writes browser uploads redeem, and sandboxed renders.
        assert!(ddl.contains("$value INSIDE ['get', 'put', 'render']"));
    }

    #[test]
    fn short_link_ddl_names_its_tenant_and_grant() {
        let ddl = surql::schema::generate_table_sql(&short_link_table(), false).join(
            "
",
        );
        assert!(ddl.contains("DEFINE TABLE short_link SCHEMAFULL"));
        assert!(ddl.contains("DEFINE FIELD grant_id ON TABLE short_link TYPE string"));
        assert!(ddl.contains("DEFINE INDEX idx_short_link_tenant ON TABLE short_link"));
    }
}
