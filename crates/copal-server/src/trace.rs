//! Request spans and W3C trace context.
//!
//! One span per served request, joined to the caller's `traceparent`
//! when one arrives, and the same context injected into every
//! outbound call copal makes on a caller's behalf (webhooks,
//! transformers, fetches), so a distributed trace shows the cause
//! and its effects as one tree. Export is opt-in: without
//! `COPAL_OTLP_ENDPOINT` the spans feed the log subscriber and
//! nothing leaves the process.

use axum::extract::MatchedPath;
use axum::http::HeaderMap;
use tracing::Instrument as _;

struct HeaderCarrier<'a>(&'a HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// One span per request. The route template (never the raw path, so
/// ids stay out of span names) and the method identify it; the
/// status lands when the response exists.
pub async fn middleware(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let span = tracing::info_span!(
        "request",
        otel.name = %format!("{method} {route}"),
        http.request.method = %method,
        http.route = %route,
        http.response.status_code = tracing::field::Empty,
    );
    let parent = opentelemetry::global::get_text_map_propagator(|p| {
        p.extract(&HeaderCarrier(request.headers()))
    });
    // Err means no OTel layer is installed (export off): the span
    // stays local and the request proceeds identically.
    let _ = tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(&span, parent);
    let response = next.run(request).instrument(span.clone()).await;
    span.record("http.response.status_code", response.status().as_u16());
    response
}

struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

impl opentelemetry::propagation::Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let name = reqwest::header::HeaderName::from_bytes(key.as_bytes());
        let val = reqwest::header::HeaderValue::from_str(&value);
        if let (Ok(name), Ok(val)) = (name, val) {
            self.0.insert(name, val);
        }
    }
}

/// Attach the current trace context to an outbound request, so a
/// webhook delivery, transformer call, or fetch shows up as a child
/// of the request that caused it. A no-op when no propagator is
/// installed.
pub fn inject(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let context = tracing_opentelemetry::OpenTelemetrySpanExt::context(&tracing::Span::current());
    let mut headers = reqwest::header::HeaderMap::new();
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&context, &mut HeaderInjector(&mut headers))
    });
    request.headers(headers)
}
