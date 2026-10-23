//! Content serving: one helper, every byte-serving route.
//!
//! Every response that carries stored bytes goes through
//! [`serve_blob`], so the security and caching floor cannot vary by
//! route:
//!
//! - `X-Content-Type-Options: nosniff` always; declared types are
//!   caller data, and browsers must not second-guess them upward.
//! - `Content-Disposition` always, with a sanitized filename;
//!   script-capable types (HTML, SVG, XML) are forced to `attachment`
//!   so hostile uploads cannot execute under the service origin.
//! - `Cache-Control` per class: tenant-authed and grant responses are
//!   `no-store` (a shared cache must never retain a one-time grant's
//!   bytes); public content, immutable by digest, caches hard.
//! - `ETag`/`If-None-Match` conditional GETs and single-range
//!   `Range`/`If-Range` requests, both natural gifts of
//!   content-addressed storage.
//!
//! The one exception to forced downloads is the render listener
//! (`COPAL_RENDER_BIND`), a separate origin that serves nothing but
//! render links. There [`render_sandbox`] displays content inline
//! under a `sandbox` content security policy: the page gets an opaque
//! origin, so its scripts run without reaching any cookie, storage, or
//! credential, and it may not open network connections of its own.

use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use copal_blob::BlobStore;
use copal_core::ContentDigest;

use crate::error::ApiError;

/// Cache posture for a served blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheClass {
    /// Tenant-authed or grant-redeemed: never cached by shared caches.
    Private,
    /// Public access level: immutable by digest, cache hard.
    Public,
}

impl CacheClass {
    fn header_value(self) -> &'static str {
        match self {
            Self::Private => "no-store",
            Self::Public => "public, max-age=31536000, immutable",
        }
    }
}

/// What to serve.
pub struct ServeSpec<'a> {
    pub content_type: &'a str,
    pub digest: &'a ContentDigest,
    /// The file's path label; its basename becomes the filename.
    pub path: &'a str,
    pub cache: CacheClass,
}

/// Types a browser will execute or script against if rendered inline
/// under our origin; always forced to download.
fn script_capable(content_type: &str) -> bool {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "text/html" | "application/xhtml+xml" | "image/svg+xml" | "text/xml" | "application/xml"
    )
}

/// Basename of the path label, reduced to a header-safe alphabet.
/// Quotes, control bytes, and separators cannot survive into the
/// `Content-Disposition` header.
fn safe_filename(path: &str) -> String {
    let base = path.rsplit(['/', '\\']).next().unwrap_or_default();
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('.');
    if trimmed.is_empty() {
        "download".to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// One parsed `Range: bytes=...` request over a known length: a single
/// satisfiable range, unsatisfiable, or absent/ignored (multi-range
/// and malformed values are legally served as a full 200).
enum RangeOutcome {
    Full,
    Partial { start: u64, end: u64 },
    Unsatisfiable,
}

fn parse_range(raw: Option<&str>, len: u64) -> RangeOutcome {
    let Some(raw) = raw else {
        return RangeOutcome::Full;
    };
    let Some(spec) = raw.trim().strip_prefix("bytes=") else {
        return RangeOutcome::Full;
    };
    if spec.contains(',') {
        // Multi-range: legal to ignore; serve the whole object.
        return RangeOutcome::Full;
    }
    let Some((from, to)) = spec.split_once('-') else {
        return RangeOutcome::Full;
    };
    let (from, to) = (from.trim(), to.trim());
    let (start, end_inclusive) = match (from.is_empty(), to.is_empty()) {
        // bytes=-N : the final N bytes.
        (true, false) => {
            let Ok(suffix) = to.parse::<u64>() else {
                return RangeOutcome::Full;
            };
            if suffix == 0 || len == 0 {
                return RangeOutcome::Unsatisfiable;
            }
            (len.saturating_sub(suffix), len - 1)
        }
        // bytes=N- : from N to the end.
        (false, true) => {
            let Ok(start) = from.parse::<u64>() else {
                return RangeOutcome::Full;
            };
            if start >= len {
                return RangeOutcome::Unsatisfiable;
            }
            (start, len - 1)
        }
        // bytes=A-B inclusive.
        (false, false) => {
            let (Ok(start), Ok(end)) = (from.parse::<u64>(), to.parse::<u64>()) else {
                return RangeOutcome::Full;
            };
            if start > end || start >= len {
                return RangeOutcome::Unsatisfiable;
            }
            (start, end.min(len.saturating_sub(1)))
        }
        (true, true) => return RangeOutcome::Full,
    };
    RangeOutcome::Partial {
        start,
        end: end_inclusive,
    }
}

fn base_headers(spec: &ServeSpec<'_>, etag: &str) -> [(header::HeaderName, String); 5] {
    let disposition_kind = if script_capable(spec.content_type) {
        "attachment"
    } else {
        "inline"
    };
    [
        (header::ETAG, etag.to_owned()),
        (header::ACCEPT_RANGES, "bytes".to_owned()),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_owned()),
        (header::CACHE_CONTROL, spec.cache.header_value().to_owned()),
        (
            header::CONTENT_DISPOSITION,
            format!(
                "{disposition_kind}; filename=\"{}\"",
                safe_filename(spec.path),
            ),
        ),
    ]
}

/// Whether the request's `If-None-Match` matches `etag`. Weak
/// comparison per RFC 9110: a `W/` prefix on a candidate is ignored
/// (our ETags are strong, the digest itself, so the octets decide).
/// `If-Range` is excluded on purpose: the RFC requires the strong
/// comparison there, so its exact match elsewhere is correct.
pub fn if_none_match_hits(request_headers: &HeaderMap, etag: &str) -> bool {
    request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|candidates| {
            candidates == "*"
                || candidates.split(',').any(|c| {
                    let c = c.trim();
                    c.strip_prefix("W/").unwrap_or(c) == etag
                })
        })
}

/// The content security policy every rendered response carries.
///
/// `sandbox` without `allow-same-origin` gives the page an opaque
/// origin: scripts run, but `document.cookie`, storage, and any
/// credentialed request to another origin are out of reach, so hostile
/// content cannot act as its viewer. `connect-src 'none'` stops the
/// page from calling out. The fetch allowances match what hosted
/// artifacts conventionally load: inline script and style, Google
/// Fonts, and pinned libraries from the three common script CDNs.
/// Images and media must be inline (`data:` or `blob:`).
pub const RENDER_CSP: &str = "sandbox allow-scripts allow-popups \
     allow-popups-to-escape-sandbox; default-src 'none'; script-src 'unsafe-inline' \
     https://cdnjs.cloudflare.com https://cdn.jsdelivr.net https://unpkg.com; style-src \
     'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com \
     data:; img-src data: blob:; media-src data: blob:; connect-src 'none'; frame-src \
     'none'; worker-src blob:; form-action 'none'; base-uri 'none'; frame-ancestors \
     'none'";

/// Turn a served response into a rendered one: displayed inline under
/// [`RENDER_CSP`], never framed, never referring, and denied the
/// device features a page could otherwise prompt for.
///
/// Only the render listener calls this. The service's own origin keeps
/// forcing script-capable types to download, so an upload can never
/// execute where tenant credentials live.
pub fn render_sandbox(mut response: Response) -> Response {
    let headers = response.headers_mut();
    // The served disposition already carries the sanitized filename;
    // only its kind changes.
    let inline = headers
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("attachment"))
        .and_then(|rest| header::HeaderValue::from_str(&format!("inline{rest}")).ok());
    if let Some(value) = inline {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    let fixed: [(header::HeaderName, &'static str); 6] = [
        (header::CONTENT_SECURITY_POLICY, RENDER_CSP),
        (header::REFERRER_POLICY, "no-referrer"),
        (header::X_FRAME_OPTIONS, "DENY"),
        (
            header::HeaderName::from_static("cross-origin-resource-policy"),
            "same-origin",
        ),
        (
            header::HeaderName::from_static("permissions-policy"),
            "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
        ),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
    ];
    for (name, value) in fixed {
        headers.insert(name, header::HeaderValue::from_static(value));
    }
    response
}

/// A 304 with the caching headers but no disposition and no body.
pub fn not_modified_response(spec: &ServeSpec<'_>) -> Response {
    let etag = format!("\"{}\"", spec.digest);
    let mut response = (
        StatusCode::NOT_MODIFIED,
        base_headers(spec, &etag),
        Body::empty(),
    )
        .into_response();
    response.headers_mut().remove(header::CONTENT_DISPOSITION);
    response
}

/// Serve a blob with the full header discipline, honoring
/// `If-None-Match`, `Range`, and `If-Range`.
pub async fn serve_blob<B: BlobStore>(
    blobs: &B,
    request_headers: &HeaderMap,
    spec: ServeSpec<'_>,
) -> Result<Response, ApiError> {
    let etag = format!("\"{}\"", spec.digest);

    // Conditional GET: the ETag is the digest, so a hit is exact.
    if if_none_match_hits(request_headers, &etag) {
        return Ok(not_modified_response(&spec));
    }

    // If-Range: a stale validator downgrades the range request to the
    // full object; resuming across a re-upload must not splice bytes
    // from two different contents.
    let range_header = request_headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok());
    let range_applies = match request_headers
        .get(header::IF_RANGE)
        .and_then(|v| v.to_str().ok())
    {
        Some(validator) => validator.trim() == etag,
        None => true,
    };

    // Length first (from the store's stat via open calls below); the
    // range math needs it, so open the full stream lazily.
    let (len, stream) = blobs.open_read(spec.digest).await?;

    let outcome = if range_applies {
        parse_range(range_header, len)
    } else {
        RangeOutcome::Full
    };

    match outcome {
        RangeOutcome::Full => {
            let response = (
                base_headers(&spec, &etag),
                [
                    (header::CONTENT_TYPE, spec.content_type.to_owned()),
                    (header::CONTENT_LENGTH, len.to_string()),
                ],
                Body::from_stream(stream),
            );
            Ok(response.into_response())
        }
        RangeOutcome::Partial { start, end } => {
            drop(stream);
            let (total, ranged) = blobs.open_range(spec.digest, start, end + 1).await?;
            let response = (
                StatusCode::PARTIAL_CONTENT,
                base_headers(&spec, &etag),
                [
                    (header::CONTENT_TYPE, spec.content_type.to_owned()),
                    (header::CONTENT_LENGTH, (end - start + 1).to_string()),
                    (
                        header::CONTENT_RANGE,
                        format!("bytes {start}-{end}/{total}"),
                    ),
                ],
                Body::from_stream(ranged),
            );
            Ok(response.into_response())
        }
        RangeOutcome::Unsatisfiable => {
            drop(stream);
            let response = (
                StatusCode::RANGE_NOT_SATISFIABLE,
                base_headers(&spec, &etag),
                [(header::CONTENT_RANGE, format!("bytes */{len}"))],
                Body::empty(),
            );
            Ok(response.into_response())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filenames_cannot_smuggle_header_syntax() {
        assert_eq!(safe_filename("plans/a-101.pdf"), "a-101.pdf");
        assert_eq!(safe_filename("x\"; rm -rf\r\n.txt"), "x___rm_-rf__.txt");
        assert_eq!(safe_filename("......"), "download");
        assert_eq!(safe_filename(""), "download");
    }

    #[test]
    fn script_capable_types_force_download() {
        for t in [
            "text/html",
            "TEXT/HTML; charset=utf-8",
            "image/svg+xml",
            "application/xml",
        ] {
            assert!(script_capable(t), "{t}");
        }
        for t in ["application/pdf", "text/plain", "image/png"] {
            assert!(!script_capable(t), "{t}");
        }
    }

    #[test]
    fn render_sandbox_displays_inline_inside_an_opaque_origin() {
        let served = (
            [(
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"page.html\"",
            )],
            Body::empty(),
        )
            .into_response();
        let rendered = render_sandbox(served);
        let get = |name: &str| rendered.headers().get(name).unwrap().to_str().unwrap();
        assert_eq!(get("content-disposition"), "inline; filename=\"page.html\"");
        let csp = get("content-security-policy");
        assert!(csp.starts_with("sandbox allow-scripts"), "{csp}");
        // The opaque origin is the whole point: same-origin must never
        // be granted back.
        assert!(!csp.contains("allow-same-origin"), "{csp}");
        assert!(csp.contains("connect-src 'none'"), "{csp}");
        assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
        assert_eq!(get("referrer-policy"), "no-referrer");
        assert_eq!(get("x-frame-options"), "DENY");
        assert_eq!(get("x-content-type-options"), "nosniff");
    }

    #[test]
    fn render_sandbox_adds_no_disposition_to_a_304() {
        let not_modified = (StatusCode::NOT_MODIFIED, Body::empty()).into_response();
        let rendered = render_sandbox(not_modified);
        assert!(rendered.headers().get("content-disposition").is_none());
        assert!(rendered.headers().get("content-security-policy").is_some());
    }

    #[test]
    fn range_parsing_covers_the_grammar() {
        let partial = |raw| match parse_range(Some(raw), 100) {
            RangeOutcome::Partial { start, end } => Some((start, end)),
            _ => None,
        };
        assert_eq!(partial("bytes=0-9"), Some((0, 9)));
        assert_eq!(partial("bytes=90-"), Some((90, 99)));
        assert_eq!(partial("bytes=-10"), Some((90, 99)));
        assert_eq!(partial("bytes=50-200"), Some((50, 99)), "end clamps");
        assert!(matches!(
            parse_range(Some("bytes=100-"), 100),
            RangeOutcome::Unsatisfiable
        ));
        assert!(matches!(
            parse_range(Some("bytes=5-2"), 100),
            RangeOutcome::Unsatisfiable
        ));
        // Multi-range and malformed values legally serve the full body.
        assert!(matches!(
            parse_range(Some("bytes=0-1,5-9"), 100),
            RangeOutcome::Full
        ));
        assert!(matches!(
            parse_range(Some("items=0-1"), 100),
            RangeOutcome::Full
        ));
        assert!(matches!(parse_range(None, 100), RangeOutcome::Full));
    }
}
