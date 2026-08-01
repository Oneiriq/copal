//! The S3-compatible ingest gateway.
//!
//! A bucket is a tenant; a key is a file path. The gateway serves
//! path-style requests (`/{bucket}/{key}`) on its own listener so
//! stock S3 tooling works with an endpoint URL and nothing else.
//! Requests authenticate with SigV4 against credentials minted on the
//! admin surface; the shared secrets those signatures derive from are
//! stored sealed under the blob master key, which is why the gateway
//! refuses to start without one.
//!
//! Coverage is the object plane: PutObject, GetObject, HeadObject,
//! DeleteObject, ListObjectsV2 (prefix, delimiter, continuation),
//! ListBuckets, HeadBucket. Uploads land in the same claim, finalize,
//! and pipeline path as every other face, so scanning, dedupe, and
//! versioning apply to S3 writes unchanged.

pub mod multipart;
pub mod sigv4;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use base64::Engine as _;
use futures::StreamExt as _;

use copal_blob::{BlobStore, StoredBlob};
use copal_core::{AccessLevel, CopalError, FileSpec, FileState, TenantId};
use copal_store::repo::{file as file_repo, s3 as s3_repo};

use crate::app::{finalize_new_content, forwarded_origin, remove_file_core, AppState};
use crate::serve::{serve_blob, CacheClass, ServeSpec};

/// Gateway state: the application plus the master cipher that seals
/// and opens credential secrets.
#[derive(Clone)]
pub struct S3Gateway<B: BlobStore> {
    pub app: AppState<B>,
}

/// The gateway router, mounted at the root of its own listener.
pub fn s3_router<B: BlobStore + 'static>(app: AppState<B>) -> Router {
    let gateway = S3Gateway { app };
    Router::new()
        .route("/", get(list_buckets::<B>))
        .route("/{bucket}", get(list_objects::<B>).head(head_bucket::<B>))
        .route(
            "/{bucket}/{*key}",
            get(object_route::<B>)
                .head(head_object::<B>)
                .put(object_route::<B>)
                .post(object_route::<B>)
                .delete(object_route::<B>),
        )
        .with_state(gateway)
}

/// Credential management for the admin surface. Merged into the admin
/// listener only when the gateway is configured, because minting needs
/// the cipher.
pub fn s3_admin_router<B: BlobStore + 'static>(app: AppState<B>) -> Router {
    let gateway = S3Gateway { app };
    Router::new()
        .route(
            "/v1/admin/tenants/{tenant}/s3-credentials",
            axum::routing::post(mint_credential::<B>).get(list_credentials::<B>),
        )
        .route(
            "/v1/admin/tenants/{tenant}/s3-credentials/{access_key_id}",
            axum::routing::delete(revoke_credential::<B>),
        )
        .with_state(gateway)
}

/// Key-route entry point. Multipart requests carry `uploads` or
/// `uploadId` in the query and are claimed first; everything else
/// falls through to the ordinary object handlers.
async fn object_route<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    Path((bucket, key)): Path<(String, String)>,
    request: axum::extract::Request,
) -> Response {
    if multipart::claims_request(request.method(), request.uri()) {
        return multipart::dispatch(State(gateway), Path((bucket, key)), request).await;
    }
    let method = request.method().clone();
    match method {
        Method::PUT => put_object(State(gateway), Path((bucket, key)), request).await,
        Method::GET | Method::DELETE => {
            let (parts, _) = request.into_parts();
            if parts.method == Method::GET {
                get_object(
                    State(gateway),
                    parts.method,
                    parts.uri,
                    parts.headers,
                    Path((bucket, key)),
                )
                .await
            } else {
                delete_object(
                    State(gateway),
                    parts.method,
                    parts.uri,
                    parts.headers,
                    Path((bucket, key)),
                )
                .await
            }
        }
        _ => xml_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "unsupported method for this route",
        ),
    }
}

/// One S3 XML error response.
pub(crate) fn xml_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>",
        xml_escape(code),
        xml_escape(message),
    );
    xml_response(status, body)
}

pub(crate) fn xml_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

/// Map an internal error onto the S3 error envelope.
pub(crate) fn copal_to_s3(err: CopalError) -> Response {
    match err {
        CopalError::NotFound(msg) => xml_error(StatusCode::NOT_FOUND, "NoSuchKey", &msg),
        CopalError::Unauthorized(msg) => xml_error(StatusCode::FORBIDDEN, "AccessDenied", &msg),
        CopalError::Validation(msg) => xml_error(StatusCode::BAD_REQUEST, "InvalidRequest", &msg),
        CopalError::Conflict(msg) => xml_error(StatusCode::CONFLICT, "OperationAborted", &msg),
        CopalError::PayloadTooLarge(msg) => {
            xml_error(StatusCode::BAD_REQUEST, "EntityTooLarge", &msg)
        }
        other => xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &other.to_string(),
        ),
    }
}

pub(crate) fn xml_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Authenticate a request: parse the SigV4 header, fetch and open the
/// credential, verify the signature. Returns the credential's tenant;
/// the caller still checks it against the bucket.
async fn authenticate<B: BlobStore>(
    gateway: &S3Gateway<B>,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<TenantId, Response> {
    let auth = sigv4::parse_authorization(headers)
        .map_err(|e| xml_error(StatusCode::FORBIDDEN, "AccessDenied", &e.to_string()))?;
    let row = s3_repo::fetch_credential(&gateway.app.store, &auth.access_key_id)
        .await
        .map_err(copal_to_s3)?;
    let Some(row) = row else {
        return Err(xml_error(
            StatusCode::FORBIDDEN,
            "InvalidAccessKeyId",
            "unknown access key",
        ));
    };
    if row.revoked_at.is_some() {
        return Err(xml_error(
            StatusCode::FORBIDDEN,
            "InvalidAccessKeyId",
            "credential revoked",
        ));
    }
    let sealed = base64::engine::general_purpose::STANDARD
        .decode(&row.secret_sealed)
        .map_err(|_| {
            xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "credential storage corrupt",
            )
        })?;
    let cipher = match gateway.app.require_cipher() {
        Ok(cipher) => cipher,
        Err(err) => return Err(copal_to_s3(err.0)),
    };
    let secret_bytes = cipher.open(&sealed).map_err(|_| {
        xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "credential does not open under the configured key",
        )
    })?;
    let secret = String::from_utf8(secret_bytes).map_err(|_| {
        xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "credential storage corrupt",
        )
    })?;
    sigv4::verify(
        &auth,
        &secret,
        method,
        uri.path(),
        uri.query().unwrap_or_default(),
        headers,
    )
    .map_err(|e| {
        xml_error(
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch",
            &e.to_string(),
        )
    })?;
    TenantId::parse(&row.tenant_id)
        .map_err(|e| xml_error(StatusCode::FORBIDDEN, "AccessDenied", &e.to_string()))
}

/// Authenticate and pin the bucket to the credential's tenant.
pub(crate) async fn authorize_bucket<B: BlobStore>(
    gateway: &S3Gateway<B>,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    bucket: &str,
) -> Result<TenantId, Response> {
    let tenant = authenticate(gateway, method, uri, headers).await?;
    if tenant.as_str() != bucket {
        return Err(xml_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "bucket does not belong to this credential",
        ));
    }
    Ok(tenant)
}

/// ListBuckets: exactly one bucket, the credential's tenant.
async fn list_buckets<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let tenant = match authenticate(&gateway, &method, &uri, &headers).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>{id}</ID><DisplayName>{id}</DisplayName></Owner><Buckets><Bucket><Name>{id}</Name><CreationDate>1970-01-01T00:00:00.000Z</CreationDate></Bucket></Buckets></ListAllMyBucketsResult>",
        id = xml_escape(tenant.as_str()),
    );
    xml_response(StatusCode::OK, body)
}

/// HeadBucket: authorization is the whole answer.
async fn head_bucket<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(bucket): Path<String>,
) -> Response {
    match authorize_bucket(&gateway, &method, &uri, &headers, &bucket).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(response) => response,
    }
}

/// ListObjectsV2 over the live-path index: lexicographic by key,
/// prefix-filtered, keyset continuation on the last key returned.
async fn list_objects<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(bucket): Path<String>,
) -> Response {
    let tenant = match authorize_bucket(&gateway, &method, &uri, &headers, &bucket).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let params = parse_query(uri.query().unwrap_or_default());
    // `?uploads` on the bucket asks for open multipart sessions, not
    // for objects.
    if params.contains_key("uploads") {
        return multipart::list_uploads(&gateway, &tenant, &bucket).await;
    }
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().unwrap_or_default();
    let max_keys = params
        .get("max-keys")
        .and_then(|raw| raw.parse::<i64>().ok())
        .unwrap_or(1_000)
        .clamp(1, 1_000);
    let after = match params.get("continuation-token") {
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
        None => None,
    };

    let rows = match file_repo::list_by_path_prefix(
        &gateway.app.store,
        &tenant,
        &prefix,
        after.as_deref(),
        max_keys,
    )
    .await
    {
        Ok(rows) => rows,
        Err(err) => return copal_to_s3(err),
    };
    let truncated = rows.len() as i64 == max_keys;
    let next_token = if truncated {
        rows.last().map(|record| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(record.path.as_bytes())
        })
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

    let continuation = next_token
        .map(|token| format!("<NextContinuationToken>{token}</NextContinuationToken>"))
        .unwrap_or_default();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{}</MaxKeys><KeyCount>{}</KeyCount><IsTruncated>{}</IsTruncated>{}{}{}</ListBucketResult>",
        xml_escape(&bucket),
        xml_escape(&prefix),
        max_keys,
        key_count,
        truncated,
        continuation,
        contents,
        common,
    );
    xml_response(StatusCode::OK, body)
}

/// PutObject: create or re-upload the file at the key, stream the
/// bytes through the standard claim and finalize path.
async fn put_object<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    Path((bucket, key)): Path<(String, String)>,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let tenant = match authorize_bucket(
        &gateway,
        &parts.method,
        &parts.uri,
        &parts.headers,
        &bucket,
    )
    .await
    {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let state = &gateway.app;

    // The signed payload hash doubles as an integrity assertion when
    // it is a literal digest (both sides are SHA-256 of the content).
    let declared_sha256 = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .filter(|raw| raw.len() == 64 && raw.bytes().all(|b| b.is_ascii_hexdigit()))
        .map(str::to_ascii_lowercase);

    let record = match file_repo::find_by_path(&state.store, &tenant, &key).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            let spec = FileSpec {
                path: key.clone(),
                content_type: parts
                    .headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("application/octet-stream")
                    .to_owned(),
                access: AccessLevel::Private,
                metadata: serde_json::Value::Null,
                idempotency_key: None,
            };
            match file_repo::create_file(&state.store, &tenant, &spec, "s3").await {
                Ok(created) => created.record,
                Err(err) => return copal_to_s3(err),
            }
        }
        Err(err) => return copal_to_s3(err),
    };
    let id = record.id.clone();

    let declared_len = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.parse::<u64>().ok());
    let headroom = match state.quota_headroom(&tenant, declared_len).await {
        Ok(headroom) => headroom,
        Err(err) => return copal_to_s3(err.0),
    };

    let (residency, backend) = match state.residency_for(&tenant).await {
        Ok(resolved) => resolved,
        Err(err) => return copal_to_s3(err.0),
    };

    if let Err(err) = file_repo::claim_upload(
        &state.store,
        &tenant,
        &id,
        &state.instance_id,
        state.limits.upload_lease_secs,
    )
    .await
    {
        return copal_to_s3(err);
    }

    // Same in-stream ceiling as the REST upload; aws-chunked framing
    // is decoded first when the client signed a streaming payload.
    let max = match headroom {
        Some(remaining) => (state.limits.max_upload_bytes as u64).min(remaining),
        None => state.limits.max_upload_bytes as u64,
    };
    let mut running_total = 0u64;
    let raw_stream = body.into_data_stream().map(|chunk| match chunk {
        Ok(bytes) => Ok(bytes),
        Err(err) => Err(format!("body: {err}")),
    });
    let chunked = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|raw| raw.starts_with("STREAMING-"))
        || parts
            .headers
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|raw| raw.contains("aws-chunked"));
    let decoded: futures::stream::BoxStream<'static, Result<Bytes, String>> = if chunked {
        Box::pin(decode_aws_chunked(raw_stream))
    } else {
        Box::pin(raw_stream)
    };
    let counted = decoded.map(move |chunk| match chunk {
        Ok(bytes) => {
            running_total += bytes.len() as u64;
            if running_total > max {
                Err("length limit exceeded".to_owned())
            } else {
                Ok(bytes)
            }
        }
        Err(err) => Err(err),
    });

    let stored = match backend.put_streamed(counted).await {
        Ok(stored) => stored,
        Err(err) => {
            state.abandon_reservation(&tenant, declared_len).await;
            let _ = file_repo::transition(
                &state.store,
                &tenant,
                &id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
            let text = err.to_string();
            if text.contains("length limit") {
                return xml_error(
                    StatusCode::BAD_REQUEST,
                    "EntityTooLarge",
                    &format!("upload exceeds {} bytes", state.limits.max_upload_bytes),
                );
            }
            return copal_to_s3(err);
        }
    };
    let StoredBlob {
        digest,
        size_bytes,
        storage_path,
    } = stored;

    if let Some(declared) = declared_sha256 {
        if declared != digest.as_str() {
            state.abandon_reservation(&tenant, declared_len).await;
            let _ = file_repo::transition(
                &state.store,
                &tenant,
                &id,
                FileState::Uploading,
                FileState::Failed,
                Default::default(),
            )
            .await;
            return xml_error(
                StatusCode::BAD_REQUEST,
                "XAmzContentSHA256Mismatch",
                "payload hash does not match the signed x-amz-content-sha256",
            );
        }
    }

    state
        .settle_reservation(&tenant, declared_len, size_bytes)
        .await;
    match finalize_new_content(
        state,
        &tenant,
        &id,
        &residency,
        &digest,
        size_bytes,
        &storage_path,
    )
    .await
    {
        Ok(_) => (StatusCode::OK, [(header::ETAG, format!("\"{digest}\""))]).into_response(),
        Err(err) => copal_to_s3(err.0),
    }
}

/// GetObject through the standard serving discipline (ETag, Range,
/// conditional requests). Grant-access files refuse here exactly as
/// they do on the REST face: their bytes flow only through grants.
async fn get_object<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let tenant = match authorize_bucket(&gateway, &method, &uri, &headers, &bucket).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let record = match lookup_servable(&gateway, &tenant, &key).await {
        Ok(record) => record,
        Err(response) => return response,
    };
    let digest = record.digest.as_ref().expect("servable implies digest");
    let backend = match gateway.app.backend_for_record(&record) {
        Ok(backend) => backend,
        Err(err) => return copal_to_s3(err.0),
    };
    match serve_blob(
        &backend,
        &headers,
        ServeSpec {
            content_type: &record.content_type,
            digest,
            path: &record.path,
            cache: CacheClass::Private,
        },
    )
    .await
    {
        Ok(response) => response,
        Err(err) => copal_to_s3(err.0),
    }
}

/// HeadObject: the metadata headers without the body.
async fn head_object<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let tenant = match authorize_bucket(&gateway, &method, &uri, &headers, &bucket).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let record = match lookup_servable(&gateway, &tenant, &key).await {
        Ok(record) => record,
        Err(response) => return response,
    };
    let digest = record.digest.as_ref().expect("servable implies digest");
    (
        StatusCode::OK,
        [
            (header::ETAG, format!("\"{digest}\"")),
            (header::CONTENT_TYPE, record.content_type.clone()),
            (
                header::CONTENT_LENGTH,
                record.size_bytes.unwrap_or_default().to_string(),
            ),
            (header::ACCEPT_RANGES, "bytes".to_owned()),
        ],
        Body::empty(),
    )
        .into_response()
}

/// Shared read-path lookup: live file at the key, servable, and not
/// grant-gated.
async fn lookup_servable<B: BlobStore>(
    gateway: &S3Gateway<B>,
    tenant: &TenantId,
    key: &str,
) -> Result<copal_core::FileRecord, Response> {
    let record = file_repo::find_by_path(&gateway.app.store, tenant, key)
        .await
        .map_err(copal_to_s3)?
        .ok_or_else(|| xml_error(StatusCode::NOT_FOUND, "NoSuchKey", "no such key"))?;
    if !record.servable_content() {
        return Err(xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "no served content at this key",
        ));
    }
    if record.access == AccessLevel::Grant {
        return Err(xml_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "grant-access content is served through grants only",
        ));
    }
    Ok(record)
}

/// DeleteObject: soft delete, idempotent. A missing key is already
/// deleted as far as S3 semantics care.
async fn delete_object<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let tenant = match authorize_bucket(&gateway, &method, &uri, &headers, &bucket).await {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    match file_repo::find_by_path(&gateway.app.store, &tenant, &key).await {
        Ok(Some(record)) => {
            match remove_file_core(
                &gateway.app,
                &tenant,
                &record.id,
                forwarded_origin(&headers).as_deref(),
            )
            .await
            {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(err) => copal_to_s3(err.0),
            }
        }
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => copal_to_s3(err),
    }
}

/// Decode `aws-chunked` framing: `<hex-size>[;ext]\r\n<bytes>\r\n`
/// repeated, terminated by a zero-size chunk (whose trailers are
/// dropped). Per-chunk signatures are not re-verified; the seed
/// signature authenticated the request and the content hash is
/// recomputed downstream regardless.
fn decode_aws_chunked<S>(
    inner: S,
) -> impl futures::Stream<Item = Result<Bytes, String>> + Send + 'static
where
    S: futures::Stream<Item = Result<Bytes, String>> + Send + Unpin + 'static,
{
    enum Mode {
        Header,
        Data(u64),
        Crlf,
        Done,
    }
    struct Decode<S> {
        inner: S,
        buf: Vec<u8>,
        mode: Mode,
    }
    futures::stream::unfold(
        Decode {
            inner,
            buf: Vec::new(),
            mode: Mode::Header,
        },
        |mut st| async move {
            loop {
                match st.mode {
                    Mode::Done => return None,
                    Mode::Header => {
                        if let Some(idx) = find_crlf(&st.buf) {
                            let line = String::from_utf8_lossy(&st.buf[..idx]).into_owned();
                            st.buf.drain(..idx + 2);
                            let size_hex = line.split(';').next().unwrap_or_default().trim();
                            let Ok(size) = u64::from_str_radix(size_hex, 16) else {
                                st.mode = Mode::Done;
                                return Some((
                                    Err("malformed aws-chunked size line".to_owned()),
                                    st,
                                ));
                            };
                            if size == 0 {
                                return None;
                            }
                            st.mode = Mode::Data(size);
                            continue;
                        }
                    }
                    Mode::Data(remaining) => {
                        if !st.buf.is_empty() {
                            let take = (remaining as usize).min(st.buf.len());
                            let bytes = Bytes::from(st.buf.drain(..take).collect::<Vec<u8>>());
                            let left = remaining - take as u64;
                            st.mode = if left == 0 {
                                Mode::Crlf
                            } else {
                                Mode::Data(left)
                            };
                            return Some((Ok(bytes), st));
                        }
                    }
                    Mode::Crlf => {
                        if st.buf.len() >= 2 {
                            if &st.buf[..2] != b"\r\n" {
                                st.mode = Mode::Done;
                                return Some((
                                    Err("malformed aws-chunked delimiter".to_owned()),
                                    st,
                                ));
                            }
                            st.buf.drain(..2);
                            st.mode = Mode::Header;
                            continue;
                        }
                    }
                }
                match st.inner.next().await {
                    Some(Ok(bytes)) => st.buf.extend_from_slice(&bytes),
                    Some(Err(err)) => {
                        st.mode = Mode::Done;
                        return Some((Err(err), st));
                    }
                    None => {
                        st.mode = Mode::Done;
                        return Some((Err("truncated aws-chunked body".to_owned()), st));
                    }
                }
            }
        },
    )
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|pair| pair == b"\r\n")
}

pub(crate) fn parse_query(raw: &str) -> std::collections::HashMap<String, String> {
    raw.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// Decode percent-encoding and plus-as-space in query values.
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes.get(i + 1..i + 3).and_then(|pair| {
                    std::str::from_utf8(pair)
                        .ok()
                        .and_then(|s| u8::from_str_radix(s, 16).ok())
                });
                match hex {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Mint a credential pair. The secret appears exactly once, here; the
/// store keeps only its sealed form.
async fn mint_credential<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<(StatusCode, axum::Json<serde_json::Value>), crate::error::ApiError> {
    crate::auth::require_admin(&gateway.app, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let token = copal_sign::ApiKeyToken::mint();
    let sealed = gateway
        .app
        .require_cipher()?
        .seal(token.secret.as_bytes())?;
    let sealed_b64 = base64::engine::general_purpose::STANDARD.encode(sealed);
    let row =
        s3_repo::create_credential(&gateway.app.store, &tenant, &token.key_id, &sealed_b64).await?;
    copal_store::repo::auth::record_audit(
        &gateway.app.store,
        &tenant,
        "admin",
        "s3credential.minted",
        &row.access_key_id(),
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        axum::Json(serde_json::json!({
            "access_key_id": row.access_key_id(),
            "secret_access_key": token.secret,
            "created_at": row.created_at,
        })),
    ))
}

/// List a tenant's credentials (never their secrets).
async fn list_credentials<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    headers: HeaderMap,
    Path(tenant): Path<String>,
) -> Result<axum::Json<serde_json::Value>, crate::error::ApiError> {
    crate::auth::require_admin(&gateway.app, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    let rows = s3_repo::list_credentials(&gateway.app.store, &tenant).await?;
    Ok(axum::Json(serde_json::json!({ "items": rows })))
}

/// Revoke a credential; unknown and already-revoked both read as 404.
async fn revoke_credential<B: BlobStore>(
    State(gateway): State<S3Gateway<B>>,
    headers: HeaderMap,
    Path((tenant, access_key_id)): Path<(String, String)>,
) -> Result<StatusCode, crate::error::ApiError> {
    crate::auth::require_admin(&gateway.app, &headers)?;
    let tenant = TenantId::parse(&tenant)?;
    if !s3_repo::revoke_credential(&gateway.app.store, &tenant, &access_key_id).await? {
        return Err(CopalError::not_found(format!("credential {access_key_id}")).into());
    }
    copal_store::repo::auth::record_audit(
        &gateway.app.store,
        &tenant,
        "admin",
        "s3credential.revoked",
        &access_key_id,
        forwarded_origin(&headers).as_deref(),
        None,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn aws_chunked_framing_decodes_and_refuses_truncation() {
        let body = b"5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=end\r\n\r\n";
        let source = futures::stream::iter(
            body.chunks(7)
                .map(|c| Ok::<_, String>(Bytes::copy_from_slice(c)))
                .collect::<Vec<_>>(),
        );
        let decoded: Vec<u8> = decode_aws_chunked(source)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .concat();
        assert_eq!(&decoded, b"hello world");

        let cut = &body[..20];
        let source = futures::stream::iter(vec![Ok::<_, String>(Bytes::copy_from_slice(cut))]);
        let items: Vec<_> = decode_aws_chunked(source).collect().await;
        assert!(items.last().unwrap().is_err(), "truncation surfaces");
    }

    #[test]
    fn query_parsing_decodes_percent_and_plus() {
        let params = parse_query("prefix=a%2Fb+c&list-type=2");
        assert_eq!(params["prefix"], "a/b c");
        assert_eq!(params["list-type"], "2");
    }

    #[test]
    fn xml_escaping_covers_the_five() {
        assert_eq!(xml_escape("a&<>\"'z"), "a&amp;&lt;&gt;&quot;&apos;z");
    }
}
