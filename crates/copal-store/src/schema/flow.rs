//! Flow cluster: `workflow_run` and `workflow_step`, the durable journal.
//!
//! A run is a claimable unit of work over a code-registered workflow; a
//! step row is the exactly-once record of one activity attempt. The
//! design premise: at-least-once execution plus the unique
//! `(run, step_key, attempt)` constraint equals exactly-once RECORDING,
//! which is what makes replay after a crash deterministic: completed
//! steps are skipped by consulting the journal, not by trusting memory.

use surql::schema::{
    datetime_field, index, int_field, object_field, record_field, string_field, table_schema,
    unique_index, FieldDefinition, TableDefinition, TableMode,
};

/// All tables in this cluster.
pub fn tables() -> Vec<TableDefinition> {
    vec![
        workflow_run_table(),
        workflow_step_table(),
        service_lease_table(),
    ]
}

/// Named coordination leases (record id = lease name). One holder at a
/// time via CAS; expiry makes crashes self-healing. The sweep loop
/// uses `sweeps` so replicas stop duplicating maintenance work.
fn service_lease_table() -> TableDefinition {
    table_schema("service_lease")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("holder").assertion("$value != ''")),
            built(datetime_field("expires_at").nullable(true)),
        ])
}

fn built(builder: surql::schema::FieldBuilder) -> FieldDefinition {
    builder
        .build_unchecked()
        .expect("static schema field definitions are valid by construction")
}

fn workflow_run_table() -> TableDefinition {
    table_schema("workflow_run")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(string_field("tenant_id").assertion("$value != ''")),
            // Names a workflow registered in code; the versioned
            // definitions-as-data table supersedes this later.
            built(string_field("workflow_key").assertion("$value != ''")),
            built(
                string_field("status")
                    .assertion(
                        "$value INSIDE ['pending', 'running', 'completed', 'failed', 'cancelled']",
                    )
                    .default("'pending'"),
            ),
            built(record_field("file", Some("file")).nullable(true)),
            built(object_field("input")),
            built(object_field("output").nullable(true)),
            built(string_field("run_error").nullable(true)),
            built(string_field("idempotency_key").nullable(true)),
            // Same lease discipline as uploads: claims expire, expired
            // claims are reaped back to pending, and the claim CAS is
            // the only path into `running`.
            built(string_field("lease_owner").nullable(true)),
            built(datetime_field("lease_expires_at").nullable(true)),
            built(datetime_field("started_at").nullable(true)),
            built(datetime_field("ended_at").nullable(true)),
            built(
                datetime_field("created_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("updated_at").value("time::now()")),
        ])
        .with_indexes([
            unique_index("uniq_run_idempotency", ["tenant_id", "idempotency_key"]),
            // Dispatch scan: pending runs, oldest first.
            index("idx_run_dispatch", ["status", "created_at"]),
            // Reaper scan: running runs whose lease aged out.
            index("idx_run_lease", ["status", "lease_expires_at"]),
            // Ops: a tenant's failures in the last hour is one range.
            index("idx_run_ops", ["tenant_id", "status", "created_at"]),
        ])
}

fn workflow_step_table() -> TableDefinition {
    table_schema("workflow_step")
        .with_mode(TableMode::Schemafull)
        .with_fields([
            built(record_field("run", Some("workflow_run")).nullable(true)),
            // Raw run id kept as a plain column so step lookups never
            // need record-literal predicates on the hot path.
            built(string_field("run_key").assertion("$value != ''")),
            built(string_field("step_key").assertion("$value != ''")),
            built(int_field("attempt").assertion("$value >= 1")),
            built(
                string_field("status")
                    .assertion("$value INSIDE ['running', 'completed', 'failed']")
                    .default("'running'"),
            ),
            built(object_field("output").nullable(true)),
            built(string_field("step_error").nullable(true)),
            built(
                datetime_field("started_at")
                    .default("time::now()")
                    .readonly(true),
            ),
            built(datetime_field("ended_at").nullable(true)),
        ])
        .with_indexes([
            // THE load-bearing index: at-least-once execution + this
            // uniqueness = exactly-once recording.
            unique_index("uniq_step_attempt", ["run_key", "step_key", "attempt"]),
            index("idx_step_run", ["run_key", "started_at"]),
        ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_ddl_carries_the_exactly_once_index() {
        let ddl = surql::schema::generate_table_sql(&workflow_step_table(), false).join("\n");
        assert!(ddl.contains(
            "DEFINE INDEX uniq_step_attempt ON TABLE workflow_step \
             COLUMNS run_key, step_key, attempt UNIQUE"
        ));
    }
}
