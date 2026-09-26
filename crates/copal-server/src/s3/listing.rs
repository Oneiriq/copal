//! ListObjects for the S3 gateway, in both of S3's dialects.
//!
//! Both walk the live-path index: lexicographic by key,
//! prefix-filtered, keyset on the last key returned. They differ in
//! how a client names where to resume. V2 (`list-type=2`) resumes from
//! an opaque `continuation-token`, or from `start-after` on a first
//! page. V1 resumes from `marker`, a plain key, and a truncated V1
//! page names the next one in `NextMarker`.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use base64::Engine as _;

use copal_blob::BlobStore;
use copal_store::repo::file as file_repo;

use super::{
    authorize_bucket, copal_to_s3, multipart, not_implemented, parse_query,
    unsupported_subresource, xml_error, xml_escape, xml_response, S3Gateway,
};

/// GET on a bucket: the object listing, or one of the bucket-level
/// queries that share its route.
pub(crate) async fn list_objects<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(bucket): Path<String>,
) -> Response {
    let caller = match authorize_bucket(&gateway, &method, &uri, &headers, &bucket).await {
        Ok(caller) => caller,
        Err(response) => return response,
    };
    let tenant = &caller.tenant;
    let params = parse_query(uri.query().unwrap_or_default());
    // GetBucketLocation: minio-go asks before its first operation and
    // treats a missing answer as a missing bucket. Signing accepts
    // any region, so one region is as true as another.
    if params.contains_key("location") {
        return xml_response(
            StatusCode::OK,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">us-east-1</LocationConstraint>"
                .to_owned(),
        );
    }
    // `?uploads` on the bucket asks for open multipart sessions, not
    // for objects.
    if params.contains_key("uploads") {
        return multipart::list_uploads(&gateway, tenant, &bucket).await;
    }
    // GetBucketVersioning has a true answer, so it gets one. The S3
    // face exposes no versionIds, which is what an empty configuration
    // states; copal's own version history lives on the REST face. Some
    // clients probe this before their first transfer and treat a
    // failure as a reason to stop.
    if params.contains_key("versioning") {
        return xml_response(
            StatusCode::OK,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\" />"
                .to_owned(),
        );
    }
    if let Some(subresource) = unsupported_subresource(uri.query()) {
        return not_implemented(subresource);
    }
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().unwrap_or_default();
    let max_keys = params
        .get("max-keys")
        .and_then(|raw| raw.parse::<i64>().ok())
        .unwrap_or(1_000)
        .clamp(1, 1_000);
    let v2 = params.get("list-type").map(String::as_str) == Some("2");
    // Where the walk resumes. V2 prefers its token and falls back to
    // start-after, which only a first page carries. V1 names a key.
    let after = if v2 {
        match params.get("continuation-token") {
            Some(token) => match base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(token)
                .ok()
                .and_then(|raw| String::from_utf8(raw).ok())
            {
                Some(path) => Some(path),
                None => {
                    return xml_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "malformed continuation-token",
                    )
                }
            },
            None => params.get("start-after").cloned(),
        }
    } else {
        params.get("marker").cloned()
    }
    .filter(|key| !key.is_empty());

    let store = match caller.store(&gateway.app).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    let rows =
        match file_repo::list_by_path_prefix(&store, tenant, &prefix, after.as_deref(), max_keys)
            .await
        {
            Ok(rows) => rows,
            Err(err) => return copal_to_s3(err),
        };
    let truncated = rows.len() as i64 == max_keys;
    let last_key = if truncated {
        rows.last().map(|record| record.path.clone())
    } else {
        None
    };

    // With a delimiter, keys sharing a segment collapse into
    // CommonPrefixes; rows arrive in path order, so a last-seen check
    // dedupes the group.
    let mut contents = String::new();
    let mut common = String::new();
    let mut last_common: Option<String> = None;
    let mut key_count = 0usize;
    for record in &rows {
        // An upload that died before finalize leaves a claim row with
        // no content. GetObject answers NoSuchKey for it, so the
        // listing must not name it either: a mirror that sees a
        // zero-byte entry treats the key as present and wrong, and
        // refuses to resume over it.
        if !record.servable_content() {
            continue;
        }
        let Some(remainder) = record.path.strip_prefix(prefix.as_str()) else {
            continue;
        };
        if !delimiter.is_empty() {
            if let Some(idx) = remainder.find(delimiter.as_str()) {
                let group = format!("{}{}", prefix, &remainder[..idx + delimiter.len()]);
                if last_common.as_deref() != Some(group.as_str()) {
                    common.push_str(&format!(
                        "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                        xml_escape(&group),
                    ));
                    last_common = Some(group);
                    key_count += 1;
                }
                continue;
            }
        }
        let etag = record
            .digest
            .as_ref()
            .map(|digest| format!("&quot;{digest}&quot;"))
            .unwrap_or_default();
        contents.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(&record.path),
            xml_escape(&record.updated_at),
            etag,
            record.size_bytes.unwrap_or_default(),
        ));
        key_count += 1;
    }

    // The dialects name the resume point differently: V2 hands back
    // an opaque token, V1 the last key it walked.
    let (position, resume) = if v2 {
        let continuation = last_key
            .map(|key| {
                format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.as_bytes()),
                )
            })
            .unwrap_or_default();
        (format!("<KeyCount>{key_count}</KeyCount>"), continuation)
    } else {
        let next = last_key
            .map(|key| format!("<NextMarker>{}</NextMarker>", xml_escape(&key)))
            .unwrap_or_default();
        let marker = params.get("marker").map(String::as_str).unwrap_or_default();
        (format!("<Marker>{}</Marker>", xml_escape(marker)), next)
    };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{}</MaxKeys>{}<IsTruncated>{}</IsTruncated>{}{}{}</ListBucketResult>",
        xml_escape(&bucket),
        xml_escape(&prefix),
        max_keys,
        position,
        truncated,
        resume,
        contents,
        common,
    );
    xml_response(StatusCode::OK, body)
}
