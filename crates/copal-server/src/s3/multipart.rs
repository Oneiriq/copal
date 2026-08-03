//! Multipart upload for the S3 gateway.
//!
//! The aws CLI switches to multipart above 8 MiB without asking, so a
//! gateway without it fails ordinary large-file copies. Parts stage
//! individually under the session prefix (they arrive in any order,
//! in parallel, and may be re-sent), and completion streams them in
//! part order through the same put, finalize, and pipeline path every
//! other upload face uses.
//!
//! Staging always lives on the LOCAL backend, like resumable
//! sessions: parts need many small writes, and the completed object
//! streams into the tenant's residency once.

use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt as _;

use copal_blob::{BlobStore, StoredBlob};
use copal_core::{AccessLevel, FileSpec, FileState, TenantId};
use copal_store::repo::{file as file_repo, multipart as mpu_repo};

use crate::app::finalize_new_content;

use super::{authorize_bucket, copal_to_s3, xml_escape, xml_response, S3Gateway};

/// Parts stage under this key shape; the number pads so the listing
/// order and the numeric order agree.
fn part_key(prefix: &str, part_number: i64) -> String {
    format!("{prefix}/{part_number:05}")
}

/// POST /{bucket}/{key}?uploads
pub async fn create_multipart<B: BlobStore>(
    gateway: &S3Gateway<B>,
    tenant: &TenantId,
    bucket: &str,
    key: &str,
    content_type: &str,
) -> Response {
    let row = match mpu_repo::create_upload(&gateway.app.store, tenant, key, content_type).await {
        Ok(row) => row,
        Err(err) => return copal_to_s3(err),
    };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(&row.upload_id()),
    );
    xml_response(StatusCode::OK, body)
}

/// PUT /{bucket}/{key}?partNumber=N&uploadId=X
pub async fn upload_part<B: BlobStore>(
    gateway: &S3Gateway<B>,
    tenant: &TenantId,
    upload_id: &str,
    part_number: i64,
    headers: &axum::http::HeaderMap,
    body: axum::body::Body,
) -> Response {
    if !(1..=10_000).contains(&part_number) {
        return super::xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidPart",
            "partNumber must be within 1..=10000",
        );
    }
    let session = match mpu_repo::fetch_upload(&gateway.app.store, tenant, upload_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return super::xml_error(StatusCode::NOT_FOUND, "NoSuchUpload", "unknown upload id")
        }
        Err(err) => return copal_to_s3(err),
    };

    // Parts stage on the local backend; a re-sent part number
    // overwrites its key, so the last write wins in bytes as well as
    // in metadata.
    let key = part_key(&session.staging_prefix, part_number);
    let blobs = &gateway.app.blobs;
    if let Err(err) = blobs.discard_staged(&key).await {
        return copal_to_s3(err);
    }
    let max = gateway.app.limits.max_upload_bytes as u64;
    let mut running = 0u64;
    let mut hasher = copal_core::DigestBuilder::new();
    let raw = body.into_data_stream().map(|chunk| match chunk {
        Ok(bytes) => Ok(bytes),
        Err(err) => Err(format!("body: {err}")),
    });
    // Parts arrive with the same streaming-signature framing as
    // whole objects; storing it verbatim assembles framing into the
    // object, which is corruption. Decode BEFORE counting and
    // hashing, so the limit and the ETag speak about content bytes.
    let decoded: futures::stream::BoxStream<'static, Result<Bytes, String>> =
        if super::is_aws_chunked(headers) {
            Box::pin(super::decode_aws_chunked(raw))
        } else {
            Box::pin(raw)
        };
    let stream = decoded.map(move |chunk| match chunk {
        Ok(bytes) => {
            running += bytes.len() as u64;
            if running > max {
                Err("length limit exceeded".to_owned())
            } else {
                Ok(bytes)
            }
        }
        Err(err) => Err(err),
    });
    // Hash while staging so the part ETag is its content digest.
    let hashing = stream.map(move |chunk: Result<Bytes, String>| {
        if let Ok(bytes) = &chunk {
            hasher.update(bytes);
        }
        chunk
    });
    let size = match blobs.append_staged(&key, hashing).await {
        Ok(size) => size,
        Err(err) => return copal_to_s3(err),
    };
    // The digest comes from a second pass over the staged bytes: the
    // hashing closure above owns its builder inside the stream, and a
    // staged part is small enough that one re-read is cheaper than
    // threading state out of the writer.
    let digest = match hash_staged(blobs, &key).await {
        Ok(digest) => digest,
        Err(err) => return copal_to_s3(err),
    };
    if let Err(err) = mpu_repo::put_part(
        &gateway.app.store,
        upload_id,
        part_number,
        size,
        &digest,
        &key,
    )
    .await
    {
        return copal_to_s3(err);
    }
    (StatusCode::OK, [(header::ETAG, format!("\"{digest}\""))]).into_response()
}

/// Digest of a staged object, streamed.
async fn hash_staged<B: BlobStore>(blobs: &B, key: &str) -> copal_core::Result<String> {
    let (_, mut stream) = blobs.open_staged(key).await?;
    let mut hasher = copal_core::DigestBuilder::new();
    while let Some(chunk) = stream.next().await {
        hasher.update(&chunk?);
    }
    let (digest, _) = hasher.finish();
    Ok(digest.as_str().to_owned())
}

/// POST /{bucket}/{key}?uploadId=X with the completion manifest.
pub async fn complete_multipart<B: BlobStore>(
    gateway: &S3Gateway<B>,
    tenant: &TenantId,
    bucket: &str,
    upload_id: &str,
    manifest: &str,
    headers: &axum::http::HeaderMap,
    principal: Option<&str>,
) -> Response {
    let session = match mpu_repo::fetch_upload(&gateway.app.store, tenant, upload_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return super::xml_error(StatusCode::NOT_FOUND, "NoSuchUpload", "unknown upload id")
        }
        Err(err) => return copal_to_s3(err),
    };
    let parts = match mpu_repo::list_parts(&gateway.app.store, upload_id).await {
        Ok(parts) => parts,
        Err(err) => return copal_to_s3(err),
    };
    if parts.is_empty() {
        return super::xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "no parts were uploaded",
        );
    }

    // The manifest names the parts the client believes it sent. Any
    // number it names that we never stored, or whose ETag disagrees,
    // fails the completion rather than silently assembling something
    // the client did not describe.
    let claimed = parse_manifest(manifest);
    if !claimed.is_empty() {
        for (number, etag) in &claimed {
            match parts.iter().find(|p| p.part_number == *number) {
                Some(part) if etag.is_empty() || etag.trim_matches('"') == part.digest => {}
                Some(_) => {
                    return super::xml_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidPart",
                        &format!("part {number} etag does not match the stored part"),
                    )
                }
                None => {
                    return super::xml_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidPart",
                        &format!("part {number} was never uploaded"),
                    )
                }
            }
        }
    }

    // Every part but the last must meet the floor. The parts are
    // ordered, so the last one is exempt by position, and a
    // single-part upload is all last part.
    let floor = gateway.app.limits.min_multipart_part_bytes;
    if let Some((_, leading)) = parts.split_last() {
        if let Some(undersized) = leading.iter().find(|p| p.size_bytes < floor) {
            return super::xml_error(
                StatusCode::BAD_REQUEST,
                "EntityTooSmall",
                &format!(
                    "part {} is {} bytes; every part but the last must be at least {floor}",
                    undersized.part_number, undersized.size_bytes,
                ),
            );
        }
    }

    let state = &gateway.app;
    let total: u64 = parts.iter().map(|p| p.size_bytes.max(0) as u64).sum();
    if let Err(err) = state.quota_headroom(tenant, Some(total)).await {
        return copal_to_s3(err.0);
    }

    // Find or create the destination record, then claim it, exactly
    // as a single PutObject does.
    let record = match file_repo::find_by_path(&state.store, tenant, &session.object_key).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            let spec = FileSpec {
                path: session.object_key.clone(),
                content_type: session.content_type.clone(),
                access: AccessLevel::Private,
                metadata: serde_json::Value::Null,
                idempotency_key: None,
            };
            match file_repo::create_file(&state.store, tenant, &spec, "s3").await {
                Ok(created) => created.record,
                Err(err) => return copal_to_s3(err),
            }
        }
        Err(err) => return copal_to_s3(err),
    };
    let id = record.id.clone();
    let (residency, backend) = match state.residency_for(tenant).await {
        Ok(resolved) => resolved,
        Err(err) => return copal_to_s3(err.0),
    };
    let precondition = match super::write_precondition(headers) {
        Ok(precondition) => precondition,
        Err(message) => {
            return super::xml_error(StatusCode::BAD_REQUEST, "InvalidRequest", &message)
        }
    };
    let claim = precondition
        .as_ref()
        .map(super::WritePrecondition::as_claim)
        .unwrap_or_default();
    if let Err(err) = file_repo::claim_upload_if(
        &state.store,
        tenant,
        &id,
        &state.instance_id,
        state.limits.upload_lease_secs,
        claim,
    )
    .await
    {
        return match &precondition {
            Some(condition) => super::conditional_refusal(state, tenant, &id, condition, err).await,
            None => copal_to_s3(err),
        };
    }

    // Assemble: the parts stream in part order into one put, so the
    // final object is content-addressed like every other upload.
    let local = state.blobs.clone();
    let keys: Vec<String> = parts.iter().map(|p| p.staging_key.clone()).collect();
    let assembled = futures::stream::iter(keys.clone())
        .then(move |key| {
            let local = local.clone();
            async move { local.open_staged(&key).await }
        })
        .map(|opened| match opened {
            Ok((_, stream)) => stream.map(|chunk| chunk.map_err(|e| e.to_string())).boxed(),
            Err(err) => futures::stream::once(async move { Err(err.to_string()) }).boxed(),
        })
        .flatten();

    let stored = match backend.put_streamed(Box::pin(assembled)).await {
        Ok(stored) => stored,
        Err(err) => {
            state.abandon_reservation(tenant, Some(total)).await;
            let _ = file_repo::transition(
                &state.store,
                tenant,
                &id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
            return copal_to_s3(err);
        }
    };
    let StoredBlob {
        digest,
        size_bytes,
        storage_path,
    } = stored;

    state
        .settle_reservation(tenant, Some(total), size_bytes)
        .await;
    if let Err(err) = finalize_new_content(
        state,
        tenant,
        &id,
        &residency,
        &digest,
        size_bytes,
        &storage_path,
        principal.unwrap_or("s3"),
    )
    .await
    {
        return copal_to_s3(err.0);
    }

    // The session and its staged parts go last: a crash before this
    // leaves only garbage the sweep collects.
    for key in &keys {
        let _ = state.blobs.discard_staged(key).await;
    }
    let _ = mpu_repo::delete_upload(&state.store, upload_id).await;

    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><ETag>&quot;{}&quot;</ETag></CompleteMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(&session.object_key),
        xml_escape(digest.as_str()),
    );
    xml_response(StatusCode::OK, body)
}

/// DELETE /{bucket}/{key}?uploadId=X
pub async fn abort_multipart<B: BlobStore>(
    gateway: &S3Gateway<B>,
    tenant: &TenantId,
    upload_id: &str,
) -> Response {
    let session = match mpu_repo::fetch_upload(&gateway.app.store, tenant, upload_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return super::xml_error(StatusCode::NOT_FOUND, "NoSuchUpload", "unknown upload id")
        }
        Err(err) => return copal_to_s3(err),
    };
    discard_session(gateway, &session).await;
    StatusCode::NO_CONTENT.into_response()
}

/// GET /{bucket}/{key}?uploadId=X
pub async fn list_parts<B: BlobStore>(
    gateway: &S3Gateway<B>,
    tenant: &TenantId,
    bucket: &str,
    upload_id: &str,
) -> Response {
    let session = match mpu_repo::fetch_upload(&gateway.app.store, tenant, upload_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return super::xml_error(StatusCode::NOT_FOUND, "NoSuchUpload", "unknown upload id")
        }
        Err(err) => return copal_to_s3(err),
    };
    let parts = match mpu_repo::list_parts(&gateway.app.store, upload_id).await {
        Ok(parts) => parts,
        Err(err) => return copal_to_s3(err),
    };
    let entries: String = parts
        .iter()
        .map(|part| {
            format!(
                "<Part><PartNumber>{}</PartNumber><ETag>&quot;{}&quot;</ETag><Size>{}</Size></Part>",
                part.part_number,
                xml_escape(&part.digest),
                part.size_bytes,
            )
        })
        .collect();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId>{}</ListPartsResult>",
        xml_escape(bucket),
        xml_escape(&session.object_key),
        xml_escape(upload_id),
        entries,
    );
    xml_response(StatusCode::OK, body)
}

/// GET /{bucket}?uploads
pub async fn list_uploads<B: BlobStore>(
    gateway: &S3Gateway<B>,
    tenant: &TenantId,
    bucket: &str,
) -> Response {
    let sessions = match mpu_repo::list_uploads(&gateway.app.store, tenant, 1_000).await {
        Ok(sessions) => sessions,
        Err(err) => return copal_to_s3(err),
    };
    let entries: String = sessions
        .iter()
        .map(|session| {
            format!(
                "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated></Upload>",
                xml_escape(&session.object_key),
                xml_escape(&session.upload_id()),
                xml_escape(&session.created_at),
            )
        })
        .collect();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><IsTruncated>false</IsTruncated>{}</ListMultipartUploadsResult>",
        xml_escape(bucket),
        entries,
    );
    xml_response(StatusCode::OK, body)
}

/// Drop a session's staged parts and rows.
async fn discard_session<B: BlobStore>(gateway: &S3Gateway<B>, session: &mpu_repo::MultipartRow) {
    if let Ok(parts) = mpu_repo::list_parts(&gateway.app.store, &session.upload_id()).await {
        for part in parts {
            let _ = gateway.app.blobs.discard_staged(&part.staging_key).await;
        }
    }
    let _ = mpu_repo::delete_upload(&gateway.app.store, &session.upload_id()).await;
}

/// Whether a request's query claims a multipart operation. Checked
/// before the body is consumed, so ordinary object requests pass
/// through untouched.
pub fn claims_request(method: &axum::http::Method, uri: &axum::http::Uri) -> bool {
    let query = super::parse_query(uri.query().unwrap_or_default());
    (method == axum::http::Method::POST && query.contains_key("uploads"))
        || query.contains_key("uploadId")
}

/// Handle a request [`claims_request`] accepted.
pub async fn dispatch<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    Path((bucket, key)): Path<(String, String)>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let query = super::parse_query(parts.uri.query().unwrap_or_default());
    let is_create = parts.method == axum::http::Method::POST && query.contains_key("uploads");

    let caller = match authorize_bucket(
        &gateway,
        &parts.method,
        &parts.uri,
        &parts.headers,
        &bucket,
    )
    .await
    {
        Ok(caller) => caller,
        Err(response) => return response,
    };
    let tenant = &caller.tenant;

    if is_create {
        let content_type = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        return create_multipart(&gateway, tenant, &bucket, &key, content_type).await;
    }

    let upload_id = query.get("uploadId").cloned().unwrap_or_default();
    match parts.method {
        axum::http::Method::PUT => {
            let part_number = query
                .get("partNumber")
                .and_then(|raw| raw.parse::<i64>().ok())
                .unwrap_or(0);
            upload_part(
                &gateway,
                tenant,
                &upload_id,
                part_number,
                &parts.headers,
                body,
            )
            .await
        }
        axum::http::Method::POST => {
            let manifest = match axum::body::to_bytes(body, 1 << 20).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(_) => String::new(),
            };
            complete_multipart(
                &gateway,
                tenant,
                &bucket,
                &upload_id,
                &manifest,
                &parts.headers,
                caller.principal.as_deref(),
            )
            .await
        }
        axum::http::Method::DELETE => abort_multipart(&gateway, tenant, &upload_id).await,
        axum::http::Method::GET => list_parts(&gateway, tenant, &bucket, &upload_id).await,
        _ => super::xml_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "unsupported multipart method",
        ),
    }
}

/// Sweep abandoned sessions with their staged parts.
pub async fn sweep_expired<B: BlobStore>(
    store: &copal_store::Store,
    blobs: &B,
    ttl_secs: u64,
) -> copal_core::Result<u64> {
    let expired = mpu_repo::list_expired(store, ttl_secs, 500).await?;
    let mut swept = 0u64;
    for session in expired {
        let upload_id = session.upload_id();
        if let Ok(parts) = mpu_repo::list_parts(store, &upload_id).await {
            for part in parts {
                let _ = blobs.discard_staged(&part.staging_key).await;
            }
        }
        mpu_repo::delete_upload(store, &upload_id).await?;
        swept += 1;
    }
    Ok(swept)
}

/// Pull `(part number, etag)` pairs out of a CompleteMultipartUpload
/// body. Hand-parsed like the rest of the gateway's XML; a manifest
/// we cannot read is treated as absent, and the stored parts stand.
fn parse_manifest(body: &str) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for chunk in body.split("<Part>").skip(1) {
        let number = between(chunk, "<PartNumber>", "</PartNumber>")
            .and_then(|raw| raw.trim().parse::<i64>().ok());
        let etag = between(chunk, "<ETag>", "</ETag>")
            .map(|raw| {
                raw.trim()
                    .replace("&quot;", "\"")
                    .trim_matches('"')
                    .to_owned()
            })
            .unwrap_or_default();
        if let Some(number) = number {
            out.push((number, etag));
        }
    }
    out
}

fn between<'a>(haystack: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = haystack.find(open)? + open.len();
    let end = haystack[start..].find(close)? + start;
    Some(&haystack[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifests_parse_numbers_and_etags() {
        let body = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;abc&quot;</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"def\"</ETag></Part></CompleteMultipartUpload>";
        assert_eq!(
            parse_manifest(body),
            vec![(1, "abc".to_owned()), (2, "def".to_owned())],
        );
        assert!(parse_manifest("<nothing/>").is_empty());
    }

    #[test]
    fn part_keys_sort_with_their_numbers() {
        assert_eq!(part_key("mpu/x", 1), "mpu/x/00001");
        assert!(part_key("mpu/x", 2) > part_key("mpu/x", 1));
        assert!(part_key("mpu/x", 10) > part_key("mpu/x", 9));
    }
}
