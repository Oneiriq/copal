//! Eventing cluster: the file-event outbox and webhook delivery.
//!
//! Events are born INSIDE the engine: a DEFINE EVENT on `file` (see the
//! core cluster) writes a `file_event` row in the same transaction as
//! the state change, so every face that moves a file produces events
//! without carrying eventing code. The dispatcher fans events out to
//! `webhook_delivery` rows and delivers them with the claim discipline
//! used everywhere else; LIVE SELECT is the wake signal, never the
//! source of truth, which is what makes delivery survive restarts.

use surql::schema::{
    bool_field, datetime_field, index, int_field, object_field, record_field, string_field,
    table_schema, unique_index, FieldDefinition, TableDefinition, TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![
        webhook_endpoint_table(),
        file_event_table(),
        webhook_delivery_table(),
    ]
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

/// A tenant's webhook endpoint. The signing secret is stored sealed
/// under the blob master key (delivery signs with it, so it must read
/// back), same custody rule as S3 credentials.
fn webhook_endpoint_table() -> TableDefinition {
    table_schema("webhook_endpoint")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(string_field("target_url").assertion("$value != ''")),
            // Comma-joined event filter; empty means every event.
            built(string_field("events").default("''")),
            built(string_field("secret_sealed").assertion("$value != ''")),
            built(bool_field("active").default("true")),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").value("time::now()")),
        ])
        .with_indexes([index("idx_webhook_tenant", ["tenant_id", "created_at"])])
}

/// The outbox. Rows are written by the engine event on `file`; the
/// dispatcher marks them dispatched after fanning out deliveries.
fn file_event_table() -> TableDefinition {
    table_schema("file_event")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(record_field("file", Some("file")).nullable(true)),
            // Dotted verb, e.g. `file.ready`; named like the audit
            // column because `event` is a reserved word.
            built(string_field("action").assertion("$value != ''")),
            built(object_field("payload")),
            built(bool_field("dispatched").default("false")),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
        ])
        .with_indexes([
            index("idx_event_tenant", ["tenant_id", "created_at"]),
            index("idx_event_dispatch", ["dispatched", "created_at"]),
            // Narrowing a feed to one verb, on both the list face and
            // the subscription, which share a filter vocabulary.
            index("idx_event_action", ["tenant_id", "action", "created_at"]),
        ])
}

/// One event bound for one endpoint. The unique pair makes fan-out
/// idempotent: a dispatcher crash between creating deliveries and
/// marking the event dispatched retries into conflicts, never
/// duplicates.
fn webhook_delivery_table() -> TableDefinition {
    table_schema("webhook_delivery")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            built(record_field("event_row", Some("file_event")).nullable(true)),
            built(record_field("endpoint", Some("webhook_endpoint")).nullable(true)),
            built(
                string_field("state")
                    .assertion("$value INSIDE ['pending', 'delivered', 'failed']")
                    .default("'pending'"),
            ),
            built(int_field("attempts").default("0")),
            built(datetime_field("next_attempt_at").nullable(true)),
            built(int_field("last_status").nullable(true)),
            // Single-deliverer exclusion, the shared claim discipline.
            built(string_field("claim_owner").nullable(true)),
            built(datetime_field("claim_expires_at").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").value("time::now()")),
        ])
        .with_indexes([
            unique_index("uniq_delivery_pair", ["event_row", "endpoint"]),
            index("idx_delivery_due", ["state", "next_attempt_at"]),
            // One endpoint's delivery history, newest first: the
            // sub-collection hanging off a webhook.
            index("idx_delivery_endpoint", ["endpoint", "created_at"]),
        ])
}
