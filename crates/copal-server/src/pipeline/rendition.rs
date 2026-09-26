//! The derivatives activity: decode an image source by digest,
//! resize, encode, and finish the pre-created derived file. Split out
//! of the pipeline module by size alone.

use serde_json::{json, Value};

use copal_blob::BlobStore;
use copal_core::{ContentDigest, FileId, FileState, TenantId};
use copal_store::repo::{blob as blob_repo, completion as completion_repo, file as file_repo};
use copal_store::Store;

use super::{content_cleared, refuse_derived, MAX_DECODE_BYTES, MAX_DERIVE_SOURCE_BYTES};

/// The derivatives activity body. The derived record was created (and
/// linked) by the API before the run; this renders the bytes and
/// finishes it through claim and complete, so a rendition is a real
/// file with a digest, versions, and every serving rule intact.
pub(super) async fn render_rendition<B: BlobStore>(
    store: &Store,
    residencies: &crate::app::Residencies<B>,
    topology: &crate::tiering::Topology,
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
        // A crash between completion and the ready mark left the
        // bytes in place; finish the mark instead of rendering again.
        if let (FileState::Scanning, Some(digest)) = (record.state, record.digest.as_ref()) {
            let cleared = rendition_source_cleared(store, &tenant, &input, &source_digest).await?;
            mark_rendition_ready(store, &tenant, &derived, digest, &source_digest, cleared).await?;
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

    let source_backend = crate::recall::pipeline_source(
        store,
        residencies,
        topology,
        &tenant,
        input["source_residency"].as_str().unwrap_or("local"),
        &source_digest,
    )
    .await?;
    let source = source_backend.read(&source_digest).await?;
    // A derive reading its source is a byte read, same as transform.
    crate::tiering::note_blob_read(
        store,
        input["source_residency"].as_str().unwrap_or("local"),
        &source_digest,
    );
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
    let cleared = rendition_source_cleared(store, &tenant, &input, &source_digest).await?;
    let record = complete_rendition(
        store,
        &tenant,
        &derived,
        &residency,
        &stored,
        &source_digest,
        cleared,
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

/// Whether the source a rendition run reads is content a scan cleared:
/// the source file still serves the digest the run renders, and its
/// processing cleared that digest.
async fn rendition_source_cleared(
    store: &Store,
    tenant: &TenantId,
    input: &Value,
    source_digest: &ContentDigest,
) -> copal_core::Result<bool> {
    let Ok(source) = FileId::parse(input["source_file"].as_str().unwrap_or_default()) else {
        return Ok(false);
    };
    Ok(file_repo::get_file(store, tenant, &source)
        .await?
        .is_some_and(|record| {
            record.digest.as_ref() == Some(source_digest) && content_cleared(&record)
        }))
}

/// Finish a rendition: complete its upload, then mark it ready with
/// the processing note the scan gate reads.
///
/// A rendition is Copal's own decode and re-encode of an image, and no
/// byte of the source's container survives the encoder. When a scan
/// cleared the source, that clearance covers the rendition, and the
/// note records it against the rendition's own digest. `scanned` stays
/// false because no scanner read these bytes. A source no scan cleared
/// passes nothing on, so the gate withholds the rendition exactly as
/// it withholds the source.
pub(crate) async fn complete_rendition(
    store: &Store,
    tenant: &TenantId,
    derived: &FileId,
    residency: &str,
    stored: &copal_blob::StoredBlob,
    source_digest: &ContentDigest,
    source_cleared: bool,
) -> copal_core::Result<copal_core::FileRecord> {
    completion_repo::complete_upload(
        store,
        tenant,
        derived,
        residency,
        &stored.digest,
        stored.size_bytes,
        "derive",
        FileState::Scanning,
        None,
    )
    .await?;
    mark_rendition_ready(
        store,
        tenant,
        derived,
        &stored.digest,
        source_digest,
        source_cleared,
    )
    .await
}

/// The `scanning -> ready` step of [`complete_rendition`], with the
/// note in the same CAS.
async fn mark_rendition_ready(
    store: &Store,
    tenant: &TenantId,
    derived: &FileId,
    digest: &ContentDigest,
    source_digest: &ContentDigest,
    source_cleared: bool,
) -> copal_core::Result<copal_core::FileRecord> {
    let note = json!({
        "scanned": false,
        "scanned_digest": if source_cleared { json!(digest.as_str()) } else { Value::Null },
        "derived_from": source_digest.as_str(),
    });
    file_repo::transition(
        store,
        tenant,
        derived,
        FileState::Scanning,
        FileState::Ready,
        file_repo::TransitionSets {
            set_json: vec![("metadata.processing".to_owned(), note)],
            ..Default::default()
        },
    )
    .await
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
