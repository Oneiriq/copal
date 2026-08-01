//! Auth cluster: `api_key`, the tenant bearer credentials.
//!
//! A key row is the stateful half of a `ck1.<id>.<secret>` bearer
//! token: only `sha256(secret)` is stored, verification is a record
//! fetch plus a constant-time hash compare, and revocation is one
//! UPDATE. Same capability design as grants: no signing key exists
//! anywhere.

use surql::schema::{
    datetime_field, event, index, int_field, object_field, string_field, table_schema,
    unique_index, FieldDefinition, TableDefinition, TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![
        api_key_table(),
        rate_window_table(),
        s3_credential_table(),
        edge_key_table(),
        audit_event_table(),
    ]
}

/// cg2 edge signing keys. Same custody rule as S3 credentials: the
/// server signs with the secret, so it stores it sealed under the
/// blob master key; the operator installs the same secret at their
/// CDN edge. Revoking the key ends every token it signed.
fn edge_key_table() -> TableDefinition {
    table_schema("edge_key")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(string_field("secret_sealed").assertion("$value != ''")),
            built(datetime_field("revoked_at").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([index("idx_edgekey_tenant", ["tenant_id", "created_at"])])
}

/// S3 gateway credentials. SigV4 verification derives an HMAC chain
/// from the shared secret, so the server must be able to read it back:
/// the secret is stored SEALED under the blob master key, never as a
/// hash and never in the clear. These are minted separately and never
/// reuse `ck1` material.
fn s3_credential_table() -> TableDefinition {
    table_schema("s3_credential")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(string_field("secret_sealed").assertion("$value != ''")),
            built(datetime_field("revoked_at").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([index("idx_s3cred_tenant", ["tenant_id", "created_at"])])
}

/// The audit trail: custody and lifecycle actions, append-only.
///
/// Immutability is engine-enforced with the same THROW-event pattern
/// that freezes version rows: an UPDATE or DELETE against an audit
/// event aborts inside SurrealDB itself, not in application code.
fn audit_event_table() -> TableDefinition {
    table_schema("audit_event")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            // Who acted: `admin` for operator-token actions, the
            // tenant id for tenant-authenticated ones.
            built(string_field("actor").assertion("$value != ''")),
            // Dotted verb, e.g. `key.minted`, `grant.issued`.
            built(string_field("action").assertion("$value != ''")),
            // What it acted on (key id, grant id, file id).
            built(string_field("subject")),
            // Forwarded origin (proxy-provided), when the deployment
            // passes one; forensic value only, never authorization.
            built(string_field("origin").nullable(true)),
            built(object_field("detail").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([index("idx_audit_tenant", ["tenant_id", "created_at"])])
        .with_events([event(
            "audit_immutable",
            "$event = 'UPDATE' OR $event = 'DELETE'",
            "THROW 'audit events are immutable'",
        )])
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

/// One minute of one caller's consumption, shared by every replica.
/// The record id is `bucket|minute`, so the check-and-increment is a
/// single guarded statement on one row.
fn rate_window_table() -> TableDefinition {
    table_schema("rate_window")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(int_field("used").default("0")),
            built(int_field("minute")),
        ])
        .with_indexes([index("idx_rate_minute", ["minute"])])
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
            // Comma-joined scope names; empty means unscoped, which is
            // what every key minted before scoping existed reads as,
            // so an upgrade tightens nothing by surprise.
            built(string_field("scopes").default("''")),
            built(datetime_field("expires_at").nullable(true)),
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
