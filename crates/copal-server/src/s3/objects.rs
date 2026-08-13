//! CopyObject and batch DeleteObjects: the two operations migration
//! tooling leans on. `mc mirror`, `rclone sync`, and `aws s3 sync`
//! all issue them, so a gateway without them cannot receive an
//! existing bucket.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures::StreamExt as _;

use copal_blob::BlobStore;
use copal_core::{AccessLevel, FileSpec, FileState, TenantId};
use copal_store::repo::{blob as blob_repo, file as file_repo};

use crate::app::{finalize_new_content, forwarded_origin, remove_file_core, AppState};

use super::{
    authorize_bucket, copal_to_s3, decode_aws_chunked, is_aws_chunked, xml_error, xml_escape,
    xml_response, S3Gateway,
};

/// The most keys one batch delete may carry, matching S3's own limit.
const MAX_DELETE_KEYS: usize = 1_000;

/// Ceiling on the batch-delete request body. A thousand keys of
/// maximum S3 key length fit comfortably; anything larger is not a
/// delete document.
const MAX_DELETE_BODY_BYTES: usize = 2 * 1024 * 1024;

/// CopyObject: `PUT /{bucket}/{key}` with `x-amz-copy-source`.
///
/// Storage is content-addressed, so a copy moves no bytes: the
/// destination becomes a new file record over the same blob, and
/// `record_sighting` inside the shared finalize path keeps the
/// refcount honest. The destination runs the standard post-upload
/// pipeline like any other write, because extraction and embeddings
/// are per-file even when content is shared.
pub(crate) async fn copy_object<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    Path((bucket, key)): Path<(String, String)>,
    request: axum::extract::Request,
) -> Response {
    let (parts, _body) = request.into_parts();
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
    let state = &gateway.app;

    let raw_source = match parts
        .headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
    {
        Some(raw) => raw,
        None => {
            return xml_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "x-amz-copy-source is required",
            )
        }
    };
    if raw_source.contains("?versionId=") {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "copying a specific version is not supported",
        );
    }
    let decoded = percent_decode(raw_source);
    let trimmed = decoded.trim_start_matches('/');
    let Some((source_bucket, source_key)) = trimmed.split_once('/') else {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "x-amz-copy-source must name a bucket and key",
        );
    };
    if source_bucket != tenant.as_str() {
        return xml_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "copy source must live in this credential's bucket",
        );
    }
    if source_key == key {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "copying a key onto itself",
        );
    }

    // The source must be servable, under the same discipline the read
    // path applies: no bytes, quarantined, and grant-only content all
    // refuse. A copy is a read followed by a write.
    let store = match caller.store(state).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let source = match super::lookup_servable(&store, tenant, source_key).await {
        Ok(record) => record,
        Err(response) => return response,
    };
    let Some(digest) = source.digest.clone() else {
        return xml_error(StatusCode::NOT_FOUND, "NoSuchKey", "no served content");
    };
    let Some(size_bytes) = source.size_bytes else {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "source record carries no size",
        );
    };
    let residency = source
        .blob_residency
        .as_deref()
        .unwrap_or("local")
        .to_owned();
    let store_key = match blob_repo::get_location(&state.store, &residency, &digest).await {
        Ok(Some((store_key, _))) => store_key,
        Ok(None) => {
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "source content location is missing",
            )
        }
        Err(err) => return copal_to_s3(err),
    };

    // Destination record, exactly as PutObject would create it.
    let record = match file_repo::find_by_path(&state.store, tenant, &key).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            let spec = FileSpec {
                path: key.clone(),
                content_type: source.content_type.clone(),
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

    // The copy's size is known up front, so the whole reservation
    // happens here; a same-content copy still spends logical quota,
    // because usage is per file even when blobs dedupe.
    let declared = Some(size_bytes);
    if let Err(err) = state.quota_headroom(tenant, declared).await {
        return copal_to_s3(err.0);
    }
    if let Err(err) = file_repo::claim_upload(
        &state.store,
        tenant,
        &id,
        &state.instance_id,
        state.limits.upload_lease_secs,
    )
    .await
    {
        state.abandon_reservation(tenant, declared).await;
        return super::claim_refusal(state, tenant, &id, err).await;
    }

    state.settle_reservation(tenant, declared, size_bytes).await;
    match finalize_new_content(
        state,
        tenant,
        &id,
        &residency,
        &digest,
        size_bytes,
        &store_key,
        caller.principal.as_deref().unwrap_or("s3"),
        // No markers on the S3 face.
        None,
    )
    .await
    {
        Ok(finished) => {
            let body = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <CopyObjectResult><ETag>&quot;{digest}&quot;</ETag>\
                 <LastModified>{}</LastModified></CopyObjectResult>",
                xml_escape(&finished.updated_at),
            );
            xml_response(StatusCode::OK, body)
        }
        Err(err) => {
            state.abandon_reservation(tenant, declared).await;
            let _ = file_repo::transition(
                &state.store,
                tenant,
                &id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
            copal_to_s3(err.0)
        }
    }
}

/// Batch DeleteObjects: `POST /{bucket}?delete` with an XML body of
/// keys. Deleting an absent key reports success, matching S3: the
/// caller asked for the key to not exist, and it does not.
pub(crate) async fn delete_objects<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    Path(bucket): Path<String>,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();
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

    let raw_stream = body.into_data_stream().map(|chunk| match chunk {
        Ok(bytes) => Ok(bytes),
        Err(err) => Err(format!("body: {err}")),
    });
    let decoded: futures::stream::BoxStream<'static, Result<Bytes, String>> =
        if is_aws_chunked(&parts.headers) {
            Box::pin(decode_aws_chunked(raw_stream))
        } else {
            Box::pin(raw_stream)
        };
    let document = match read_bounded(decoded, MAX_DELETE_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };
    let text = String::from_utf8_lossy(&document);

    let quiet = text.contains("<Quiet>true</Quiet>");
    let keys = extract_keys(&text);
    if keys.is_empty() {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "the delete document names no keys",
        );
    }
    if keys.len() > MAX_DELETE_KEYS {
        return xml_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "a delete carries at most 1000 keys",
        );
    }

    let origin = forwarded_origin(&parts.headers);
    let mut deleted = Vec::new();
    let mut failed = Vec::new();
    for key in keys {
        match delete_one(&gateway.app, tenant, &key, origin.as_deref()).await {
            Ok(()) => deleted.push(key),
            Err(err) => failed.push((key, err)),
        }
    }

    let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult>");
    if !quiet {
        for key in &deleted {
            body.push_str(&format!(
                "<Deleted><Key>{}</Key></Deleted>",
                xml_escape(key)
            ));
        }
    }
    for (key, err) in &failed {
        body.push_str(&format!(
            "<Error><Key>{}</Key><Code>InternalError</Code><Message>{}</Message></Error>",
            xml_escape(key),
            xml_escape(&err.to_string()),
        ));
    }
    body.push_str("</DeleteResult>");
    xml_response(StatusCode::OK, body)
}

/// Soft-delete one key; an absent key is already deleted.
async fn delete_one<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
    key: &str,
    origin: Option<&str>,
) -> Result<(), copal_core::CopalError> {
    match file_repo::find_by_path(&state.store, tenant, key).await? {
        Some(record) => remove_file_core(&state.store, tenant, &record.id, origin)
            .await
            .map_err(|e| e.0),
        None => Ok(()),
    }
}

/// Collect a body stream up to `cap` bytes; past it, refuse rather
/// than buffer.
async fn read_bounded(
    mut stream: futures::stream::BoxStream<'static, Result<Bytes, String>>,
    cap: usize,
) -> Result<Vec<u8>, Response> {
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        let bytes =
            chunk.map_err(|err| xml_error(StatusCode::BAD_REQUEST, "IncompleteBody", &err))?;
        if collected.len() + bytes.len() > cap {
            return Err(xml_error(
                StatusCode::BAD_REQUEST,
                "MalformedXML",
                "delete document too large",
            ));
        }
        collected.extend_from_slice(&bytes);
    }
    Ok(collected)
}

/// Every `<Key>` element's text, XML-unescaped.
fn extract_keys(document: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut rest = document;
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + 5..];
        let Some(end) = after.find("</Key>") else {
            break;
        };
        keys.push(xml_unescape(&after[..end]));
        rest = &after[end + 6..];
    }
    keys
}

/// The inverse of `xml_escape`, plus numeric character references,
/// which S3 clients emit for control characters in key names.
fn xml_unescape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(start) = rest.find('&') {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        let Some(end) = after.find(';') else {
            out.push_str(after);
            return out;
        };
        let entity = &after[1..end];
        match entity {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ => {
                let decoded = entity
                    .strip_prefix("#x")
                    .or_else(|| entity.strip_prefix("#X"))
                    .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                    .or_else(|| entity.strip_prefix('#').and_then(|dec| dec.parse().ok()))
                    .and_then(char::from_u32);
                match decoded {
                    Some(c) => out.push(c),
                    // An unknown entity passes through unchanged; a
                    // key is better served verbatim than dropped.
                    None => out.push_str(&after[..=end]),
                }
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

/// Percent-decode a copy-source header value. Keys arrive URL-encoded
/// there even though the request path itself is already decoded.
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(value) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(value);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_extract_and_unescape() {
        let document = "<Delete><Object><Key>plain.txt</Key></Object>\
                        <Object><Key>a&amp;b &lt;c&gt;.txt</Key></Object>\
                        <Object><Key>tab&#x9;name</Key></Object></Delete>";
        assert_eq!(
            extract_keys(document),
            vec!["plain.txt", "a&b <c>.txt", "tab\tname"],
        );
        assert!(extract_keys("<Delete></Delete>").is_empty());
    }

    #[test]
    fn copy_sources_percent_decode() {
        assert_eq!(percent_decode("/acme/a%20b.txt"), "/acme/a b.txt");
        assert_eq!(percent_decode("plain"), "plain");
        // A stray percent passes through rather than corrupting.
        assert_eq!(percent_decode("50%"), "50%");
    }
}
