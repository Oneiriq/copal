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
use copal_store::repo::{blob as blob_repo, completion as completion_repo, file as file_repo};
use copal_store::Store;

// The URL-ingestion activity, split out by size alone; its policy
// and run key stay part of this module's public face.
mod fetch;

pub use fetch::{fetch_run_key, FetchPolicy};

use fetch::fetch_source;

/// The derivatives workflow key.
pub const DERIVE_WORKFLOW: &str = "derive";

/// The external-transform workflow key.
pub const TRANSFORM_WORKFLOW: &str = "transform";

/// The URL-ingestion workflow key.
pub const FETCH_WORKFLOW: &str = "fetch";

/// Default source ceiling for external transforms. Individual
/// transformers can raise or lower it in their configuration.
pub const MAX_TRANSFORM_SOURCE_BYTES: u64 = 64 * 1024 * 1024;

/// Deterministic idempotency key for a transform run: one derivation
/// per derived record per source content, transformer, and parameter
/// set.
pub fn transform_run_key(
    derived: &FileId,
    source_digest: &ContentDigest,
    transformer: &str,
    params: &str,
) -> String {
    format!("transform:{derived}:{source_digest}:{transformer}:{params}")
}

/// Sources past this size are refused for decoding; renditions target
/// interactive latency, and a decoder is an amplification surface.
pub const MAX_DERIVE_SOURCE_BYTES: u64 = 32 * 1024 * 1024;

/// Ceiling on decoded pixel buffers. Compressed size bounds nothing on
/// its own: a few megabytes of PNG can describe a gigapixel canvas, so
/// the decoder gets an explicit allocation limit and refuses rather
/// than exhausting the host.
pub const MAX_DECODE_BYTES: u64 = 256 * 1024 * 1024;

/// Ceiling on stored extracted text. Retrieval wants documents, not
/// disk images; past this the extraction truncates and says so.
pub const MAX_TEXT_CHARS: usize = 1_000_000;

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
#[allow(clippy::too_many_arguments)]
pub fn standard_registry<B: BlobStore>(
    store: Store,
    residencies: crate::app::Residencies<B>,
    policy: ExtensionPolicy,
    enforce_type_match: bool,
    clamav_addr: Option<String>,
    extractor_addr: Option<String>,
    embedding: Option<(String, String)>,
    transformers: std::collections::HashMap<String, crate::config::TransformerConfig>,
    fetch: FetchPolicy,
) -> FlowRegistry {
    let embed_store = store.clone();
    let embed_config = embedding;
    let extract_store = store.clone();
    let extract_residencies = residencies.clone();
    let extract_addr = extractor_addr;
    let scan_residencies = residencies.clone();
    let scan_addr = clamav_addr.clone();
    let sniff_residencies = residencies.clone();
    let derive_store = store.clone();
    let derive_residencies = residencies.clone();
    let transform_store = store.clone();
    let transform_residencies = residencies.clone();
    let transform_map = transformers;
    let fetch_store = store.clone();
    let fetch_residencies = residencies;
    let fetch_policy = fetch;
    let finalize_store = store;

    FlowRegistry::new()
        .activity("embed_text", move |input: Value| {
            let store = embed_store.clone();
            let config = embed_config.clone();
            async move {
                let mut out = input.clone();
                // Nothing to embed without a service, and nothing to
                // embed when extraction found no text.
                let Some((addr, model)) = config else {
                    out["embedded"] = json!(false);
                    return Ok(out);
                };
                if out["extracted"] != json!(true) {
                    out["embedded"] = json!(false);
                    return Ok(out);
                }
                let file = FileId::parse(input["file"].as_str().unwrap_or_default())?;
                let digest = input["digest"].as_str().unwrap_or_default();
                // Only passages still lacking a vector are embedded,
                // so a retried run resumes rather than paying for the
                // whole document again.
                let pending =
                    copal_store::repo::text::chunks_without_embedding(&store, &file).await?;
                let mut embedded = 0usize;
                for chunk in &pending {
                    // An unreachable service is an ERROR: silently
                    // skipping would leave passages out of semantic
                    // search with nothing recording why.
                    let vector = crate::embed::embed(&addr, &model, &chunk.body).await?;
                    if copal_store::repo::text::put_embedding(
                        &store,
                        &chunk.chunk_id(),
                        digest,
                        &vector,
                        &model,
                    )
                    .await?
                    {
                        embedded += 1;
                    }
                }
                out["embedded"] = json!(embedded > 0);
                out["embedded_passages"] = json!(embedded);
                Ok(out)
            }
        })
        .activity("extract_text", move |input: Value| {
            let store = extract_store.clone();
            let residencies = extract_residencies.clone();
            let addr = extract_addr.clone();
            async move { extract_text(&store, &residencies, addr.as_deref(), input).await }
        })
        .activity("scan_malware", move |input: Value| {
            let residencies = scan_residencies.clone();
            let addr = scan_addr.clone();
            async move {
                // Configured or not, the step is registered; without an
                // address it records that no scanner ran rather than
                // implying a clean verdict.
                let mut out = input.clone();
                let Some(addr) = addr else {
                    out["scanned"] = json!(false);
                    return Ok(out);
                };
                // A verdict already blocked upstream stands; the scan
                // adds a reason, it never clears one.
                if out["verdict"] == json!("blocked") {
                    out["scanned"] = json!(false);
                    return Ok(out);
                }
                let digest = ContentDigest::parse(input["digest"].as_str().unwrap_or_default())?;
                let residency = input["residency"].as_str().unwrap_or("local");
                let blobs = residencies.get(residency)?;
                let content = blobs.read(&digest).await?;
                // A scanner that cannot be reached is an ERROR, not a
                // pass: the run retries, and the file never reaches
                // ready on an unscanned body.
                match crate::clamav::scan(&addr, &content).await? {
                    crate::clamav::Verdict::Clean => {
                        out["scanned"] = json!(true);
                        // WHICH content was cleared, not merely that a
                        // scan happened: serving compares this against
                        // the record's current digest, so bytes that
                        // replaced it are not covered by its verdict.
                        out["scanned_digest"] = json!(digest.as_str());
                    }
                    crate::clamav::Verdict::Infected(signature) => {
                        out["scanned"] = json!(true);
                        out["scanned_digest"] = json!(digest.as_str());
                        out["verdict"] = json!("blocked");
                        out["verdict_reason"] = json!(format!("malware detected: {signature}"));
                    }
                }
                Ok(out)
            }
        })
        .activity("render_rendition", move |input: Value| {
            let store = derive_store.clone();
            let residencies = derive_residencies.clone();
            async move { render_rendition(&store, &residencies, input).await }
        })
        .activity("sniff_type", move |input: Value| {
            let residencies = sniff_residencies.clone();
            async move {
                let digest = ContentDigest::parse(input["digest"].as_str().unwrap_or_default())?;
                let residency = input["residency"].as_str().unwrap_or("local");
                let blobs = residencies.get(residency)?;
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
                    "scanned": input["scanned"],
                    "scanned_digest": input["scanned_digest"],
                    "extracted": input["extracted"],
                    "extract_chars": input["extract_chars"],
                    "extract_truncated": input["extract_truncated"],
                    "passages": input["passages"],
                    // The marker verdict: how many spans resolved,
                    // which declarations could not be located (each
                    // of those restricted the whole file), and how
                    // many passages inherited a level. This is where
                    // the uploader learns a typo'd anchor
                    // over-withheld, and re-uploads corrected.
                    "marker_spans": input["marker_spans"],
                    "markers_unresolved": input["markers_unresolved"],
                    "passages_restricted": input["passages_restricted"],
                    "embedded": input["embedded"],
                    "embedded_passages": input["embedded_passages"],
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
        // The scan sits between the cheap checks and the transition
        // that would make content servable, so a blocked verdict
        // reaches finalize as quarantine rather than ready.
        // Extraction runs AFTER the scan and BEFORE finalize: text is
        // pulled from content a scanner has already judged, and the
        // file becomes readable and searchable in the same step.
        .workflow(
            UPLOAD_WORKFLOW,
            &[
                "sniff_type",
                "extension_policy",
                "scan_malware",
                "extract_text",
                "embed_text",
                "finalize_upload",
            ],
            3,
        )
        .workflow(DERIVE_WORKFLOW, &["render_rendition"], 3)
        .activity("transform_external", move |input: Value| {
            let store = transform_store.clone();
            let residencies = transform_residencies.clone();
            let transformers = transform_map.clone();
            async move { transform_external(&store, &residencies, &transformers, input).await }
        })
        .workflow(TRANSFORM_WORKFLOW, &["transform_external"], 3)
        .activity("fetch_source", move |input: Value| {
            let store = fetch_store.clone();
            let residencies = fetch_residencies.clone();
            let policy = fetch_policy.clone();
            async move { fetch_source(&store, &residencies, &policy, input).await }
        })
        .workflow(FETCH_WORKFLOW, &["fetch_source"], 3)
}

/// The external-transform activity body. The derived record exists
/// before the run, like a rendition's; this ships the source bytes to
/// the configured service and finishes the derived file with whatever
/// comes back. A 4xx answer is the service refusing the input (the
/// derived record fails, the run completes); a 5xx or transport error
/// is infrastructure and retries.
async fn transform_external<B: BlobStore>(
    store: &Store,
    residencies: &crate::app::Residencies<B>,
    transformers: &std::collections::HashMap<String, crate::config::TransformerConfig>,
    input: Value,
) -> copal_core::Result<Value> {
    let tenant = TenantId::parse(input["tenant"].as_str().unwrap_or_default())?;
    let derived = FileId::parse(input["derived_file"].as_str().unwrap_or_default())?;
    let source_digest = ContentDigest::parse(input["source_digest"].as_str().unwrap_or_default())?;
    let name = input["transformer"].as_str().unwrap_or_default().to_owned();

    // Replay after success: the derived file already carries content.
    // The check reads the digest rather than a single state, because a
    // finished transform hands its output to the post-upload pipeline
    // and so passes through `scanning` on its way to `ready`.
    if let Some(record) = file_repo::get_file(store, &tenant, &derived).await? {
        if record.digest.is_some()
            && record.state != FileState::Draft
            && record.state != FileState::Failed
        {
            return Ok(json!({
                "file": derived.as_str(),
                "outcome": "already-transformed",
            }));
        }
    }

    // The map is read at run time, so a transformer dropped from the
    // configuration between enqueue and execution refuses instead of
    // retrying forever.
    let Some(config) = transformers.get(&name) else {
        let reason = format!("transformer {name} is not configured");
        return refuse_derived(store, &tenant, &derived, reason).await;
    };
    let ceiling = config
        .max_source_bytes
        .unwrap_or(MAX_TRANSFORM_SOURCE_BYTES);
    let declared_size = input["source_size"].as_u64().unwrap_or(0);
    if declared_size > ceiling {
        let reason = format!("source is {declared_size} bytes; the transform ceiling is {ceiling}");
        return refuse_derived(store, &tenant, &derived, reason).await;
    }
    let source_backend = residencies.get(input["source_residency"].as_str().unwrap_or("local"))?;
    let source = source_backend.read(&source_digest).await?;
    if source.len() as u64 > ceiling {
        let reason = "source exceeds the transform ceiling".to_owned();
        return refuse_derived(store, &tenant, &derived, reason).await;
    }

    let timeout = std::time::Duration::from_secs(config.timeout_secs.unwrap_or(60).clamp(1, 600));
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| CopalError::Blob(format!("transform client: {e}")))?;
    let params_json = serde_json::to_string(input.get("params").unwrap_or(&Value::Null))
        .unwrap_or_else(|_| "null".to_owned());
    let mut request = client
        .post(&config.url)
        .query(&[("params", params_json.as_str())])
        .header(
            "content-type",
            input["source_content_type"]
                .as_str()
                .unwrap_or("application/octet-stream"),
        )
        .header("x-copal-tenant", tenant.as_str())
        .header(
            "x-copal-source-file",
            input["source_file"].as_str().unwrap_or_default(),
        )
        .header("x-copal-source-digest", source_digest.as_str())
        .body(source.to_vec());
    if let Some(secret) = &config.secret {
        request = request.header("x-copal-transform-secret", secret);
    }
    let response = crate::trace::inject(request)
        .send()
        .await
        .map_err(|e| CopalError::Blob(format!("transformer {name}: {e}")))?;
    let status = response.status();
    if status.is_client_error() {
        let reason: String = response
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(200)
            .collect();
        crate::metrics::incr("copal_transform_refusals_total");
        let reason = format!("transformer {name} refused ({status}): {reason}");
        return refuse_derived(store, &tenant, &derived, reason).await;
    }
    if !status.is_success() {
        return Err(CopalError::Blob(format!(
            "transformer {name} answered {status}"
        )));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| CopalError::Blob(format!("transformer {name} body: {e}")))?;
    if bytes.is_empty() {
        let reason = format!("transformer {name} returned no bytes");
        return refuse_derived(store, &tenant, &derived, reason).await;
    }

    // Derived content lands in the tenant's CURRENT residency, like
    // any other new content.
    let residency = copal_store::repo::tenant::get_residency(store, &tenant).await?;
    let target = residencies.get(&residency)?;
    let size = bytes.len();
    let body = futures::stream::iter(vec![Ok::<_, String>(bytes::Bytes::from(bytes.to_vec()))]);
    let stored = target.put_streamed(body).await?;
    blob_repo::record_sighting(
        store,
        &stored.digest,
        stored.size_bytes,
        &residency,
        &stored.storage_path,
    )
    .await?;
    file_repo::claim_upload(store, &tenant, &derived, "transform-worker", 900).await?;
    // These bytes came from another process, so they get the same
    // scrutiny an upload gets: sniffed, checked against policy,
    // scanned, and extracted. Extraction is the point for a
    // transformer that produces text, since a transcript nobody
    // indexed is a transcript nobody can find.
    let record = completion_repo::complete_upload(
        store,
        &tenant,
        &derived,
        &residency,
        &stored.digest,
        stored.size_bytes,
        "transform",
        FileState::Scanning,
        None,
    )
    .await?;
    let post_input = json!({
        "tenant": tenant.as_str(),
        "file": derived.as_str(),
        "residency": residency,
        "digest": stored.digest.as_str(),
        "declared_type": record.content_type,
        "path": record.path,
    });
    copal_store::repo::flow::enqueue(
        store,
        &tenant,
        UPLOAD_WORKFLOW,
        post_input,
        Some(&derived),
        Some(&upload_run_key(&derived, &stored.digest)),
    )
    .await?;
    crate::metrics::incr("copal_transforms_total");
    Ok(json!({
        "file": derived.as_str(),
        "outcome": "transformed",
        "digest": stored.digest.as_str(),
        "bytes": size,
    }))
}

/// The derivatives activity body. The derived record was created (and
/// linked) by the API before the run; this renders the bytes and
/// finishes it through claim and complete, so a rendition is a real
/// file with a digest, versions, and every serving rule intact.
async fn render_rendition<B: BlobStore>(
    store: &Store,
    residencies: &crate::app::Residencies<B>,
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
        return refuse_derived(store, &tenant, &derived, reason).await;
    }

    let source_backend = residencies.get(input["source_residency"].as_str().unwrap_or("local"))?;
    let source = source_backend.read(&source_digest).await?;
    if source.len() as u64 > MAX_DERIVE_SOURCE_BYTES {
        let reason = "source exceeds the decode ceiling".to_owned();
        return refuse_derived(store, &tenant, &derived, reason).await;
    }
    let bytes = match render_image(&source, width, height, &format) {
        Ok(bytes) => bytes,
        Err(reason) => return refuse_derived(store, &tenant, &derived, reason).await,
    };

    // Renditions land in the tenant's CURRENT residency, resolved at
    // render time like any other new content.
    let residency = copal_store::repo::tenant::get_residency(store, &tenant).await?;
    let target = residencies.get(&residency)?;
    let body = futures::stream::iter(vec![Ok::<_, String>(bytes::Bytes::from(bytes))]);
    let stored = target.put_streamed(body).await?;
    blob_repo::record_sighting(
        store,
        &stored.digest,
        stored.size_bytes,
        &residency,
        &stored.storage_path,
    )
    .await?;
    file_repo::claim_upload(store, &tenant, &derived, "derive-worker", 900).await?;
    let record = completion_repo::complete_upload(
        store,
        &tenant,
        &derived,
        &residency,
        &stored.digest,
        stored.size_bytes,
        "derive",
        FileState::Ready,
        None,
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

/// Decode, resize, and encode one rendition. `Err` carries a refusal
/// reason (the source is not a workable image), never infrastructure
/// trouble; both faces of the derivatives surface share this body.
pub(crate) fn render_image(
    source: &[u8],
    width: u32,
    height: u32,
    format: &str,
) -> Result<Vec<u8>, String> {
    let decoded = decode_bounded(source)
        .map_err(|err| format!("source does not decode as an image: {err}"))?;
    let resized = decoded.thumbnail(width, height);
    let mut encoded = std::io::Cursor::new(Vec::new());
    let target = match format {
        "png" => image::ImageFormat::Png,
        _ => image::ImageFormat::Jpeg,
    };
    // JPEG has no alpha; flatten before encoding.
    let writable = if target == image::ImageFormat::Jpeg {
        image::DynamicImage::ImageRgb8(resized.to_rgb8())
    } else {
        resized
    };
    writable
        .write_to(&mut encoded, target)
        .map_err(|err| format!("encoding failed: {err}"))?;
    Ok(encoded.into_inner())
}

/// Decode an image under an explicit allocation ceiling, so a small
/// compressed file cannot claim a huge pixel buffer.
fn decode_bounded(source: &[u8]) -> Result<image::DynamicImage, image::ImageError> {
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_DECODE_BYTES);
    let mut reader = image::ImageReader::new(std::io::Cursor::new(source))
        .with_guessed_format()
        .map_err(image::ImageError::IoError)?;
    reader.limits(limits);
    reader.decode()
}

/// Pull searchable text out of content.
///
/// Text and JSON decode here; anything else needs a real parser, and
/// Copal does not carry one. An optional extractor service (Apache
/// Tika speaks this shape) handles the rest, the same seam clamd
/// uses: configured, it participates; absent, the record says no
/// extraction happened rather than implying the document was empty.
async fn extract_text<B: BlobStore>(
    store: &Store,
    residencies: &crate::app::Residencies<B>,
    extractor: Option<&str>,
    input: Value,
) -> copal_core::Result<Value> {
    let mut out = input.clone();
    // Blocked content is never opened for text: a quarantined body is
    // exactly what should not be parsed further.
    if out["verdict"] == json!("blocked") {
        out["extracted"] = json!(false);
        return Ok(out);
    }
    let tenant = TenantId::parse(input["tenant"].as_str().unwrap_or_default())?;
    let file = FileId::parse(input["file"].as_str().unwrap_or_default())?;
    let digest = ContentDigest::parse(input["digest"].as_str().unwrap_or_default())?;
    let residency = input["residency"].as_str().unwrap_or("local");
    let declared = input["declared_type"].as_str().unwrap_or_default();
    let sniffed = input["sniffed_type"].as_str().unwrap_or_default();

    let blobs = residencies.get(residency)?;
    let content = blobs.read(&digest).await?;

    let native = declared.starts_with("text/")
        || sniffed.starts_with("text/")
        || declared == "application/json"
        || sniffed == "application/json";

    let (body, extractor_name) = if native {
        match std::str::from_utf8(&content) {
            Ok(text) => (text.to_owned(), "native".to_owned()),
            // Declared text that is not UTF-8 is not text we can index.
            Err(_) => {
                out["extracted"] = json!(false);
                return Ok(out);
            }
        }
    } else if let Some(addr) = extractor {
        // A configured extractor that cannot be reached is an ERROR,
        // so the run retries rather than recording an empty document.
        (
            crate::extract::fetch(addr, &content).await?,
            "external".to_owned(),
        )
    } else {
        out["extracted"] = json!(false);
        return Ok(out);
    };

    let trimmed = body.trim();
    if trimmed.is_empty() {
        out["extracted"] = json!(false);
        return Ok(out);
    }
    let truncated = trimmed.chars().count() > MAX_TEXT_CHARS;
    let stored: String = if truncated {
        trimmed.chars().take(MAX_TEXT_CHARS).collect()
    } else {
        trimmed.to_owned()
    };

    // Markers resolve HERE, at chunking time, because this is the
    // only moment the extracted text and the declaration are both in
    // hand. The declaration is read back from the version row rather
    // than trusted from the run's input, so a retry or a
    // re-extraction under an upgraded extractor resolves the same
    // declaration the uploader made against the same digest. An
    // unresolvable marker restricts the WHOLE file; the reasons land
    // in the annotations below, under `metadata.processing`, where
    // the uploader can see the miss and re-upload a corrected
    // declaration. Mapping failure costs availability, never
    // confidentiality.
    let declared =
        copal_store::repo::version::markers_for(store, &tenant, &file, digest.as_str()).await?;
    let context = copal_core::marker::ResolveContext {
        text: &stored,
        native: extractor_name == "native",
        lead_trim: body.chars().count() - body.trim_start().chars().count(),
        source_chars: body.chars().count(),
        truncated,
    };
    let resolution = crate::markers::resolve_declaration(declared.as_ref(), &context);

    // The spans land in the same statement as the body, and each
    // chunk's level lands in the same CREATE as its body, so no
    // crash between writes can leave a restricted passage readable:
    // there is no unmarked window.
    let withheld = crate::markers::withheld_spans(&resolution.spans);
    copal_store::repo::text::put_text(
        store,
        &tenant,
        &file,
        digest.as_str(),
        &stored,
        &extractor_name,
        &withheld,
    )
    .await?;
    // Passages are the retrieval unit, so they are written with the
    // text rather than lazily: a document that extracted but never
    // chunked would be readable and unfindable.
    let passages = copal_core::split_spans(&stored);
    let chunks = crate::markers::leveled_chunks(&passages, &resolution.spans);
    let restricted = chunks.iter().filter(|chunk| chunk.access.is_some()).count();
    copal_store::repo::text::put_chunks(store, &tenant, &file, digest.as_str(), &chunks).await?;

    out["extracted"] = json!(true);
    out["extract_chars"] = json!(stored.chars().count());
    out["extract_truncated"] = json!(truncated);
    out["passages"] = json!(chunks.len());
    if declared.is_some() {
        out["marker_spans"] = json!(resolution.spans.len());
        out["markers_unresolved"] = json!(resolution.unresolved);
        out["passages_restricted"] = json!(restricted);
    }
    Ok(out)
}

/// A business refusal fails the derived record and completes the run.
/// The record walks the legal path (a claim, then a failed attempt);
/// a lost CAS means another path already settled it.
pub(crate) async fn refuse_derived(
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
