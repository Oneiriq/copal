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
pub mod flow;
pub mod tus;

use surql::schema::{generate_table_sql, TableDefinition};
use surql::types::reserved::check_reserved_word;

/// Every table in the Copal control plane, in application order.
pub fn tables() -> Vec<TableDefinition> {
    let mut tables = core::tables();
    tables.extend(delivery::tables());
    tables.extend(flow::tables());
    tables.extend(auth::tables());
    tables.extend(tus::tables());
    tables
}

/// Render the idempotent DDL statements for the full schema, in order.
pub fn schema_statements() -> Vec<String> {
    tables()
        .iter()
        .flat_map(|table| generate_table_sql(table, true))
        .collect()
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
    fn every_table_renders_ddl() {
        let statements = schema_statements();
        let table_defines = statements
            .iter()
            .filter(|s| s.starts_with("DEFINE TABLE"))
            .count();
        assert_eq!(table_defines, tables().len());
        // Idempotency: applying twice must be safe.
        assert!(statements.iter().all(|s| s.contains("IF NOT EXISTS")));
    }
}
