//! The wire mapper: domain records rendered in the CONTRACT's shape.
//!
//! Every read surface (REST handlers and GraphQL resolvers alike)
//! serializes files through this one function, so the wire cannot
//! drift from `docs/openapi.json` per-handler. Internal columns
//! (tenant scoping, audit, lease bookkeeping) stay off the wire; the
//! contract is the allowlist.

use copal_core::FileRecord;
use copal_store::repo::flow::RunRow;

/// A file in contract shape.
pub fn wire_file(record: &FileRecord) -> serde_json::Value {
    let mut value = serde_json::to_value(record).expect("FileRecord serializes");
    let map = value.as_object_mut().expect("FileRecord is an object");
    if let Some(size) = map.remove("size_bytes") {
        map.insert("size".into(), size);
    }
    map.remove("tenant_id");
    map.remove("created_by");
    map.remove("upload_lease_owner");
    map.remove("upload_lease_expires_at");
    value
}

/// A workflow run in contract shape.
pub fn wire_run(run: &RunRow) -> serde_json::Value {
    serde_json::json!({
        "id": run.run_id(),
        "workflow": run.workflow_key,
        "status": run.status,
        "output": run.output,
        "error": run.run_error,
        "created_at": run.created_at,
        "ended_at": run.ended_at,
    })
}

/// One outbox event as both faces render it. The `action` column
/// keeps its name on the wire: `event` is reserved in SurrealDB v3,
/// and the contract's name gate refuses overrides that collide there.
pub fn wire_event(row: &copal_store::repo::eventing::EventRow) -> serde_json::Value {
    serde_json::json!({
        "id": row.event_id(),
        "action": row.action,
        "payload": row.payload,
        "created_at": row.created_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_shape_matches_the_contract_allowlist() {
        let record: FileRecord = serde_json::from_value(serde_json::json!({
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "tenant_id": "acme",
            "path": "a.txt",
            "state": "ready",
            "access": "private",
            "content_type": "text/plain",
            "size_bytes": 3,
            "digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "metadata": {},
            "created_by": "api",
            "created_at": "2026-07-30T00:00:00Z",
            "updated_at": "2026-07-30T00:00:00Z",
            "version_count": 1,
            "upload_lease_owner": "instance-1",
        }))
        .unwrap();
        let wire = wire_file(&record);
        let map = wire.as_object().unwrap();
        assert_eq!(map["size"], 3);
        for absent in [
            "size_bytes",
            "tenant_id",
            "created_by",
            "upload_lease_owner",
        ] {
            assert!(!map.contains_key(absent), "{absent} leaked onto the wire");
        }
        for present in [
            "id",
            "path",
            "state",
            "access",
            "content_type",
            "digest",
            "version_count",
            "metadata",
            "created_at",
            "updated_at",
        ] {
            assert!(map.contains_key(present), "{present} missing from the wire");
        }
    }
}
