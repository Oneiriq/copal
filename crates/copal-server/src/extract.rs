//! The external text-extractor seam.
//!
//! Copal decodes text and JSON itself and carries no document
//! parsers: PDF, Office, and OCR are large, fast-moving, and
//! historically a rich source of memory-safety bugs, which is a poor
//! trade inside a service holding other people's files. An external
//! extractor gets the untrusted bytes instead.
//!
//! The contract is deliberately plain, so anything can serve it:
//!
//! ```text
//! PUT /tika        (Apache Tika's own endpoint)
//! Accept: text/plain
//! <raw bytes>
//! -> 200 with the extracted text as the body
//! ```

use copal_core::CopalError;

/// Send content to the extractor and return its text.
///
/// Every failure is an error rather than empty text: a document that
/// could not be parsed must not be recorded as a document with
/// nothing in it.
pub async fn fetch(addr: &str, content: &[u8]) -> copal_core::Result<String> {
    let url = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_owned()
    } else {
        format!("http://{addr}/tika")
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| CopalError::Store(format!("extractor client: {e}")))?;
    let response = client
        .put(&url)
        .header("accept", "text/plain")
        .header("content-type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .map_err(|e| CopalError::Store(format!("extractor request: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(CopalError::Store(format!("extractor answered {status}")));
    }
    response
        .text()
        .await
        .map_err(|e| CopalError::Store(format!("extractor body: {e}")))
}
