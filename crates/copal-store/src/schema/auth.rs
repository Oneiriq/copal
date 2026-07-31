//! Auth cluster: `api_key` — tenant bearer credentials.
//!
//! A key row is the stateful half of a `ck1.<id>.<secret>` bearer
//! token: only `sha256(secret)` is stored, verification is a record
//! fetch plus a constant-time hash compare, and revocation is one
//! UPDATE. Same capability design as grants — no signing key exists
//! anywhere.

use surql::schema::{
    datetime_field, string_field, table_schema, unique_index, FieldDefinition, TableDefinition,
    TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![api_key_table()]
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

fn api_key_table() -> TableDefinition {
    table_schema("api_key")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            // Operator-facing label; unique per tenant so rotation
            // reads as replace-by-name.
            built(string_field("name").assertion("$value != ''")),
            built(string_field("key_hash").assertion("$value != ''")),
            built(datetime_field("revoked_at").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([unique_index("uniq_key_name", ["tenant_id", "name"])])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ddl_carries_the_per_tenant_name_uniqueness() {
        let ddl = surql::schema::generate_table_sql(&api_key_table(), false).join("\n");
        assert!(ddl.contains(
            "DEFINE INDEX uniq_key_name ON TABLE api_key COLUMNS tenant_id, name UNIQUE"
        ));
    }
}
