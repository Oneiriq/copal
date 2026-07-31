//! The standard post-upload pipeline.
//!
//! Three activities over the flow engine — this is the durable-journal
//! replacement for the predecessor's blob-created orchestration, using
//! nothing but Copal's own planes:
//!
//! 1. `sniff_type`: read the first bytes by digest and sniff the real
//!    content type; record declared vs sniffed.
//! 2. `extension_policy`: check the path against the blocked-extension
//!    denylist; record the verdict.
//! 3. `finalize_upload`: one CAS moves `scanning -> ready` (clean) or
//!    `scanning -> quarantined` (blocked), annotating
//!    `metadata.processing` atomically with the transition. Quarantine
//!    is terminal-until-delete: content and grants refuse via the
//!    servability rule.
//!
//! Activities are idempotent by construction (sniffing and policy are
//! pure over immutable content; the finalize CAS loses harmlessly on
//! replay), so journal replay semantics hold.

use futures::StreamExt as _;
use serde_json::{json, Value};

use copal_blob::BlobStore;
use copal_core::{
    sniff_content_type, ContentDigest, CopalError, ExtensionPolicy, FileId, FileState, TenantId,
};
use copal_flow::FlowRegistry;
use copal_store::repo::file as file_repo;
use copal_store::Store;

/// The workflow key the server enqueues after uploads when configured.
pub const UPLOAD_WORKFLOW: &str = "post_upload";

/// Deterministic idempotency key for a file's post-upload run: recovery
/// re-enqueues dedupe instead of double-processing.
pub fn upload_run_key(file: &FileId, digest: &ContentDigest) -> String {
    format!("post:{file}:{digest}")
}

/// Build the standard registry over the given planes.
///
/// `enforce_type_match`: when set, a file whose sniffed content type
/// contradicts its declared type is QUARANTINED instead of merely
/// annotated — the declared-type lie becomes a blocking verdict.
/// Unsniffable content never blocks (unverifiable is not a lie).
pub fn standard_registry<B: BlobStore>(
    store: Store,
    blobs: B,
    policy: ExtensionPolicy,
    enforce_type_match: bool,
) -> FlowRegistry {
    let sniff_blobs = blobs.clone();
    let finalize_store = store;

    FlowRegistry::new()
        .activity("sniff_type", move |input: Value| {
            let blobs = sniff_blobs.clone();
            async move {
                let digest = ContentDigest::parse(input["digest"].as_str().unwrap_or_default())?;
                let (_, mut stream) = blobs.open_read(&digest).await?;
                let head = match stream.next().await {
                    Some(chunk) => chunk?,
                    None => bytes::Bytes::new(),
                };
                let sniffed = sniff_content_type(&head);
                let declared = input["declared_type"].as_str().unwrap_or_default();
                let mut out = input.clone();
                out["sniffed_type"] = match sniffed {
                    Some(mime) => json!(mime),
                    None => Value::Null,
                };
                out["type_matches"] = json!(match sniffed {
                    Some(mime) => mime == declared,
                    // Unverifiable is not a lie.
                    None => true,
                });
                Ok(out)
            }
        })
        .activity("extension_policy", move |input: Value| {
            let policy = policy.clone();
            async move {
                let path = input["path"].as_str().unwrap_or_default();
                let mut out = input.clone();
                match policy.blocks(path) {
                    Some(extension) => {
                        out["verdict"] = json!("blocked");
                        out["verdict_reason"] = json!(format!("blocked extension .{extension}"));
                    }
                    None if enforce_type_match && out["type_matches"] == json!(false) => {
                        out["verdict"] = json!("blocked");
                        out["verdict_reason"] = json!(format!(
                            "declared {} but content is {}",
                            out["declared_type"].as_str().unwrap_or("unknown"),
                            out["sniffed_type"].as_str().unwrap_or("unknown"),
                        ));
                    }
                    None => {
                        out["verdict"] = json!("clean");
                    }
                }
                Ok(out)
            }
        })
        .activity("finalize_upload", move |input: Value| {
            let store = finalize_store.clone();
            async move {
                let tenant = TenantId::parse(input["tenant"].as_str().unwrap_or_default())?;
                let file = FileId::parse(input["file"].as_str().unwrap_or_default())?;
                let blocked = input["verdict"].as_str() == Some("blocked");
                let target = if blocked {
                    FileState::Quarantined
                } else {
                    FileState::Ready
                };
                let annotations = json!({
                    "sniffed_type": input["sniffed_type"],
                    "type_matches": input["type_matches"],
                    "verdict": input["verdict"],
                    "verdict_reason": input["verdict_reason"],
                });
                let result = file_repo::transition(
                    &store,
                    &tenant,
                    &file,
                    FileState::Scanning,
                    target,
                    file_repo::TransitionSets {
                        set_json: vec![("metadata.processing".to_owned(), annotations)],
                        ..Default::default()
                    },
                )
                .await;
                match result {
                    Ok(record) => Ok(json!({
                        "file": input["file"],
                        "state": record.state.as_str(),
                        "verdict": input["verdict"],
                    })),
                    // Replay after a prior finalize: the file already
                    // moved; losing the CAS is the idempotent outcome,
                    // not an error.
                    Err(CopalError::Conflict(_)) => Ok(json!({
                        "file": input["file"],
                        "state": "already-finalized",
                        "verdict": input["verdict"],
                    })),
                    Err(other) => Err(other),
                }
            }
        })
        .workflow(
            UPLOAD_WORKFLOW,
            &["sniff_type", "extension_policy", "finalize_upload"],
            3,
        )
}
