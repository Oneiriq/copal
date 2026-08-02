//! Copal schema as code.
//!
//! One module per cluster; this module merges them and gates every table
//! and field name through the reserved-word check at build time, so a
//! collision like `content` (the `CREATE ... CONTENT` clause) or
//! `namespace` fails a unit test here instead of parsing strangely in
//! production.
//!
//! The definitions in this module are the single migration source of
//! truth. `ensure_schema` renders and applies them idempotently for
//! development and tests; versioned migration files are generated from
//! the same definitions via the surql toolchain.

pub mod auth;
pub mod core;
pub mod delivery;
pub mod eventing;
pub mod flow;
pub mod s3;
pub mod text;
pub mod tus;

use std::collections::BTreeMap;

use surql::schema::{
    generate_table_sql, AccessDefinition, JwtConfig, RecordAccessConfig, TableDefinition,
};
use surql::types::reserved::check_reserved_word;

/// Columns the engine redacts for caller sessions unless the token
/// carries the admin claim. Hand-written today; a copal-server test
/// holds this list equal to the contract's guarded fields, so the two
/// enforcement layers cannot drift apart.
pub const ADMIN_GUARDED_COLUMNS: &[(&str, &str)] = &[("file_version", "created_by")];

/// Every table in the Copal control plane, in application order.
///
/// Each table leaves here carrying engine `PERMISSIONS`. The service
/// session is system-level and bypasses them; they exist for caller
/// sessions, where the engine becomes a second enforcement layer
/// under the application checks. The rule is mechanical so a future
/// table cannot dodge it: tables with a `tenant_id` column admit only
/// rows whose tenant matches the token, and tables without one are
/// closed to caller sessions entirely.
pub fn tables() -> Vec<TableDefinition> {
    let mut tables = core::tables();
    tables.extend(delivery::tables());
    tables.extend(flow::tables());
    tables.extend(auth::tables());
    tables.extend(tus::tables());
    tables.extend(eventing::tables());
    tables.extend(s3::tables());
    tables.extend(text::tables());
    tables.into_iter().map(with_engine_permissions).collect()
}

fn with_engine_permissions(mut table: TableDefinition) -> TableDefinition {
    let tenant_scoped = table.fields.iter().any(|field| field.name == "tenant_id");
    let rule = if tenant_scoped {
        "tenant_id = $token.tn"
    } else {
        "false"
    };
    for field in &mut table.fields {
        if ADMIN_GUARDED_COLUMNS.contains(&(table.name.as_str(), field.name.as_str())) {
            field.permissions = Some(BTreeMap::from([(
                "select".to_owned(),
                "$token.adm = true".to_owned(),
            )]));
        }
    }
    table.with_permissions([("select, create, update, delete", rule)])
}

/// The record access method caller tokens authenticate against.
fn caller_access(key: &str) -> AccessDefinition {
    AccessDefinition::record(
        "caller",
        RecordAccessConfig::new().with_jwt(JwtConfig::hs256(key)),
    )
    // No engine-side session expiry: a session's lifetime IS its
    // work's lifetime, ended by drop. Copal already bounds every
    // session it mints: tokens expire in seconds and gate opening,
    // request sessions drop with their request, and streams end on
    // their ceilings. An engine clock beside those is a second clock
    // whose only contribution was racing the first, killing live
    // queries silently near the subscription ceiling.
    .with_session("NONE")
}

/// The code side of the schema diff: every table (with the vector
/// index folded in when a dimension is configured) and the analyzer.
///
/// The database side comes from live introspection, so the
/// comparison is against what the engine actually holds rather than
/// any record of what was once applied.
pub fn code_snapshot(embedding_dimension: Option<u32>) -> surql::migration::diff::SchemaSnapshot {
    let mut tables = tables();
    if let Some(dimension) = embedding_dimension {
        for table in &mut tables {
            if table.name == "text_chunk" {
                table.indexes.push(text::vector_index(dimension));
            }
        }
    }
    surql::migration::diff::SchemaSnapshot {
        tables,
        edges: Vec::new(),
        buckets: Vec::new(),
        analyzers: text::analyzers(),
    }
}

/// The caller access method's `OVERWRITE` form. Applied whenever the
/// key is configured rather than diffed: the engine redacts keys in
/// its echo, so an access definition can never compare equal, and
/// one idempotent statement per boot costs less than pretending it
/// could.
pub fn access_overwrite(key: &str) -> copal_core::Result<String> {
    caller_access(key)
        .to_surql_overwrite()
        .map_err(|e| copal_core::CopalError::Store(format!("access ddl: {e}")))
}

/// Render the idempotent DDL statements for the full schema, in order.
///
/// Analyzers come first: a full-text index names one, so the index
/// definition cannot apply before the analyzer exists.
pub fn schema_statements() -> Vec<String> {
    let mut statements: Vec<String> = text::analyzers()
        .iter()
        .map(|analyzer| analyzer.to_surql_with_options(true))
        .collect();
    statements.extend(
        tables()
            .iter()
            .flat_map(|table| generate_table_sql(table, true)),
    );
    statements
}

/// Names that collide with SurrealQL keywords, as (table, name) pairs.
///
/// Empty on a healthy schema; asserted empty by a unit test so a future
/// column cannot reintroduce the `content`-style trap.
pub fn reserved_name_violations() -> Vec<(String, String)> {
    let mut violations = Vec::new();
    for table in tables() {
        if check_reserved_word(&table.name, false).is_some() {
            violations.push((table.name.clone(), table.name.clone()));
        }
        for field in &table.fields {
            // Dot-notation nested fields validate per segment.
            for segment in field.name.split('.') {
                if check_reserved_word(segment, false).is_some() {
                    violations.push((table.name.clone(), field.name.clone()));
                }
            }
        }
    }
    violations
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_reserved_words_anywhere() {
        assert_eq!(reserved_name_violations(), Vec::new());
    }

    #[test]
    fn every_table_carries_engine_permissions() {
        for table in tables() {
            let ddl = table.to_surql();
            assert!(
                ddl.contains("PERMISSIONS FOR"),
                "{} has no engine permissions: {ddl}",
                table.name
            );
            let tenant_scoped = table.fields.iter().any(|f| f.name == "tenant_id");
            if tenant_scoped {
                assert!(
                    ddl.contains("tenant_id = $token.tn"),
                    "{}: {ddl}",
                    table.name
                );
            } else {
                assert!(ddl.contains("WHERE false"), "{}: {ddl}", table.name);
            }
        }
    }

    #[test]
    fn guarded_columns_exist_in_the_schema() {
        for (table_name, column) in ADMIN_GUARDED_COLUMNS {
            let table = tables()
                .into_iter()
                .find(|t| t.name == *table_name)
                .unwrap_or_else(|| panic!("guarded table {table_name} missing"));
            let field = table
                .fields
                .iter()
                .find(|f| f.name == *column)
                .unwrap_or_else(|| panic!("guarded column {table_name}.{column} missing"));
            let permissions = field.permissions.as_ref().expect("guard applied");
            assert_eq!(permissions.get("select").unwrap(), "$token.adm = true");
        }
    }

    #[test]
    fn code_snapshot_carries_the_whole_schema() {
        let snapshot = code_snapshot(None);
        assert_eq!(snapshot.tables.len(), tables().len());
        assert_eq!(snapshot.analyzers.len(), 1);
        assert!(snapshot.tables.iter().all(|t| t.permissions.is_some()));
    }

    #[test]
    fn code_snapshot_folds_the_vector_index_at_a_width() {
        let without = code_snapshot(None);
        let with = code_snapshot(Some(384));
        let count = |s: &surql::migration::diff::SchemaSnapshot| {
            s.tables
                .iter()
                .find(|t| t.name == "text_chunk")
                .map(|t| t.indexes.len())
                .unwrap_or_default()
        };
        assert_eq!(count(&with), count(&without) + 1);
    }

    #[test]
    fn access_renders_one_replacing_statement() {
        let statement = access_overwrite("k1").unwrap();
        assert!(statement.starts_with("DEFINE ACCESS OVERWRITE caller "));
        assert!(statement.contains("FOR SESSION NONE"));
    }

    #[test]
    fn every_table_renders_ddl() {
        let statements = schema_statements();
        let table_defines = statements
            .iter()
            .filter(|s| s.starts_with("DEFINE TABLE"))
            .count();
        assert_eq!(table_defines, tables().len());
        // Idempotency: applying twice must be safe.
        assert!(statements.iter().all(|s| s.contains("IF NOT EXISTS")));
        // An analyzer must precede the index that names it.
        let analyzer_at = statements
            .iter()
            .position(|s| s.starts_with("DEFINE ANALYZER"))
            .expect("the text analyzer is defined");
        let index_at = statements
            .iter()
            .position(|s| s.contains("ANALYZER copal_text"))
            .expect("the text index names it");
        assert!(analyzer_at < index_at);
    }
}
