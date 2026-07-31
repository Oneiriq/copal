//! The standard post-upload pipeline.
//!
//! Three activities over the flow engine; this is the durable-journal
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
//!
//! The registry also carries `render_rendition`, the derivatives
//! activity: decode an image source by digest, resize, encode, and
//! finish the pre-created derived file through the standard claim and
//! complete path. Refusals (not an image, too large) fail the derived
//! record and complete the run; only infrastructure errors retry.

use futures::StreamExt as _;
use serde_json::{json, Value};

use copal_blob::BlobStore;
use copal_core::{
    sniff_content_type, ContentDigest, CopalError, ExtensionPolicy, FileId, FileState, TenantId,
};
use copal_flow::FlowRegistry;
use copal_store::repo::{blob as blob_repo, file as file_repo};
use copal_store::Store;

/// The derivatives workflow key.
pub const DERIVE_WORKFLOW: &str = "derive";

/// Sources past this size are refused for decoding; renditions target
/// interactive latency, and a decoder is an amplification surface.
pub const MAX_DERIVE_SOURCE_BYTES: u64 = 32 * 1024 * 1024;

/// Deterministic idempotency key for a rendition run: one render per
/// derived record per source content and parameter set.
pub fn derive_run_key(derived: &FileId, source_digest: &ContentDigest, params: &str) -> String {
    format!("derive:{derived}:{source_digest}:{params}")
}

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
/// annotated; the declared-type lie becomes a blocking verdict.
/// Unsniffable content never blocks (unverifiable is not a lie).
pub fn standard_registry<B: BlobStore>(
    store: Store,
    blobs: B,
    policy: ExtensionPolicy,
    enforce_type_match: bool,
) -> FlowRegistry {
    let sniff_blobs = blobs.clone();
    let derive_store = store.clone();
    let derive_blobs = blobs;
    let finalize_store = store;

    FlowRegistry::new()
        .activity("render_rendition", move |input: Value| {
            let store = derive_store.clone();
            let blobs = derive_blobs.clone();
            async move { render_rendition(&store, &blobs, input).await }
        })
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
        .workflow(DERIVE_WORKFLOW, &["render_rendition"], 3)
}

/// The derivatives activity body. The derived record was created (and
/// linked) by the API before the run; this renders the bytes and
/// finishes it through claim and complete, so a rendition is a real
/// file with a digest, versions, and every serving rule intact.
async fn render_rendition<B: BlobStore>(
    store: &Store,
    blobs: &B,
    input: Value,
) -> copal_core::Result<Value> {
    let tenant = TenantId::parse(input["tenant"].as_str().unwrap_or_default())?;
    let derived = FileId::parse(input["derived_file"].as_str().unwrap_or_default())?;
    let source_digest = ContentDigest::parse(input["source_digest"].as_str().unwrap_or_default())?;
    let width = input["width"].as_u64().unwrap_or(256) as u32;
    let height = input["height"].as_u64().unwrap_or(256) as u32;
    let format = input["format"].as_str().unwrap_or("jpeg").to_owned();

    // Replay after success: the derived file already carries content.
    if let Some(record) = file_repo::get_file(store, &tenant, &derived).await? {
        if record.state == FileState::Ready && record.digest.is_some() {
            return Ok(json!({
                "file": derived.as_str(),
                "outcome": "already-rendered",
            }));
        }
    }

    let declared_size = input["source_size"].as_u64().unwrap_or(0);
    if declared_size > MAX_DERIVE_SOURCE_BYTES {
        let reason = format!(
            "source is {declared_size} bytes; the decode ceiling is {MAX_DERIVE_SOURCE_BYTES}",
        );
        return refuse_rendition(store, &tenant, &derived, reason).await;
    }

    let source = blobs.read(&source_digest).await?;
    if source.len() as u64 > MAX_DERIVE_SOURCE_BYTES {
        let reason = "source exceeds the decode ceiling".to_owned();
        return refuse_rendition(store, &tenant, &derived, reason).await;
    }
    let decoded = match image::load_from_memory(&source) {
        Ok(decoded) => decoded,
        Err(err) => {
            let reason = format!("source does not decode as an image: {err}");
            return refuse_rendition(store, &tenant, &derived, reason).await;
        }
    };
    let resized = decoded.thumbnail(width, height);
    let mut encoded = std::io::Cursor::new(Vec::new());
    let target = match format.as_str() {
        "png" => image::ImageFormat::Png,
        _ => image::ImageFormat::Jpeg,
    };
    // JPEG has no alpha; flatten before encoding.
    let writable = if target == image::ImageFormat::Jpeg {
        image::DynamicImage::ImageRgb8(resized.to_rgb8())
    } else {
        resized
    };
    if let Err(err) = writable.write_to(&mut encoded, target) {
        let reason = format!("encoding failed: {err}");
        return refuse_rendition(store, &tenant, &derived, reason).await;
    }
    let bytes = encoded.into_inner();

    let body = futures::stream::iter(vec![Ok::<_, String>(bytes::Bytes::from(bytes))]);
    let stored = blobs.put_streamed(body).await?;
    blob_repo::record_sighting(
        store,
        &stored.digest,
        stored.size_bytes,
        "local",
        &stored.storage_path,
    )
    .await?;
    file_repo::claim_upload(store, &tenant, &derived, "derive-worker", 900).await?;
    let record = file_repo::complete_upload(
        store,
        &tenant,
        &derived,
        &stored.digest,
        stored.size_bytes,
        "derive",
        FileState::Ready,
    )
    .await?;
    Ok(json!({
        "file": derived.as_str(),
        "outcome": "rendered",
        "digest": stored.digest.as_str(),
        "size_bytes": stored.size_bytes,
        "state": record.state.as_str(),
    }))
}

/// A business refusal fails the derived record and completes the run.
/// The record walks the legal path (a claim, then a failed attempt);
/// a lost CAS means another path already settled it.
async fn refuse_rendition(
    store: &Store,
    tenant: &TenantId,
    derived: &FileId,
    reason: String,
) -> copal_core::Result<Value> {
    let _ = file_repo::claim_upload(store, tenant, derived, "derive-worker", 60).await;
    match file_repo::transition(
        store,
        tenant,
        derived,
        FileState::Uploading,
        FileState::Failed,
        Default::default(),
    )
    .await
    {
        Ok(_) | Err(CopalError::Conflict(_)) => {}
        Err(other) => return Err(other),
    }
    Ok(json!({
        "file": derived.as_str(),
        "outcome": "refused",
        "reason": reason,
    }))
}
