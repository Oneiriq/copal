//! HTTP mapping for the workspace error taxonomy.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use copal_core::CopalError;

/// Wrapper making `CopalError` an axum response.
#[derive(Debug)]
pub struct ApiError(pub CopalError);

impl From<CopalError> for ApiError {
    fn from(err: CopalError) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // One vocabulary across faces: these kinds match the GraphQL
        // error extension codes exactly (payload_too_large excepted;
        // GraphQL never sees a 413).
        let (status, kind) = match &self.0 {
            CopalError::Validation(_) => (StatusCode::BAD_REQUEST, "bad_request"),
            CopalError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
            CopalError::Forbidden(_) => (StatusCode::FORBIDDEN, "forbidden"),
            CopalError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            CopalError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            CopalError::PreconditionFailed(_) => {
                (StatusCode::PRECONDITION_FAILED, "precondition_failed")
            }
            CopalError::PayloadTooLarge(_) => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            CopalError::TooManyRequests(_) => (StatusCode::TOO_MANY_REQUESTS, "too_many_requests"),
            CopalError::Store(_) | CopalError::Blob(_) => {
                // Infrastructure detail stays out of responses; the full
                // error goes to the log.
                tracing::error!(error = %self.0, "internal failure");
                let body = json!({"error": {"kind": "internal", "message": "internal error"}});
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
            }
        };
        let body = json!({"error": {"kind": kind, "message": self.0.to_string()}});
        (status, Json(body)).into_response()
    }
}
