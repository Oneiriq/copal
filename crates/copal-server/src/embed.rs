//! The embedding-service seam.
//!
//! Copal does not run models. Inference means model weights, a
//! runtime, and hardware assumptions that have no business inside a
//! storage service, and the deployments that want semantic search
//! already run something that serves embeddings.
//!
//! The contract is the OpenAI embeddings shape, which is what Ollama,
//! llama.cpp's server, text-embeddings-inference, LocalAI, vLLM, and
//! OpenAI itself all speak. One seam, many providers, all
//! self-hostable:
//!
//! ```text
//! POST /v1/embeddings
//! { "model": "...", "input": "..." }
//! -> { "data": [ { "embedding": [0.1, ...] } ] }
//! ```

use serde::Deserialize;

use copal_core::CopalError;

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingDatum>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingDatum {
    embedding: Vec<f64>,
}

/// One backfill pass: re-embed a batch of chunks whose vector is
/// absent or was produced by a different model. Runs beside the
/// sweeps when an embedding service is configured, so a model change
/// drains the old geometry without an operator remembering to; the
/// vector index itself already rebuilds through the schema diff when
/// the dimension changes.
pub async fn backfill_pass(
    store: &copal_store::Store,
    addr: &str,
    model: &str,
    batch: i64,
) -> copal_core::Result<usize> {
    let stale = copal_store::repo::text::stale_chunks(store, model, batch).await?;
    let mut refreshed = 0usize;
    for chunk in &stale {
        let vector = embed_fitting(addr, model, &chunk.body).await?;
        if copal_store::repo::text::put_embedding(
            store,
            &chunk.chunk_id(),
            &chunk.digest,
            &vector,
            model,
        )
        .await?
        {
            refreshed += 1;
        }
    }
    if refreshed > 0 {
        crate::metrics::add("copal_backfill_refreshed_total", refreshed as u64);
    }
    Ok(refreshed)
}

/// Most pieces a passage is split into when the service refuses it.
const MAX_PIECES: usize = 8;

/// Embed a stored passage, splitting it when the service refuses it
/// whole.
///
/// Passages are cut by characters, models count tokens, and dense text
/// (tables of figures, identifiers) holds far more tokens per character
/// than prose, so a passage can exceed a small model's window. A
/// refusal answered with an HTTP status re-embeds the passage as 2, 4,
/// then 8 pieces split at whitespace and returns their mean, normalized,
/// which keeps the passage findable instead of failing the whole file.
/// A transport failure is not retried this way: a service that cannot
/// be reached would refuse every piece too.
pub async fn embed_fitting(addr: &str, model: &str, text: &str) -> copal_core::Result<Vec<f64>> {
    let mut pieces = 1;
    loop {
        let mut vectors = Vec::with_capacity(pieces);
        let mut refused = None;
        for piece in split_evenly(text, pieces) {
            match request(addr, model, piece).await? {
                Ok(vector) => vectors.push(vector),
                Err(status) => {
                    refused = Some(status);
                    break;
                }
            }
        }
        match refused {
            None => return Ok(mean_normalized(&vectors)),
            Some(status) if pieces >= MAX_PIECES || text.len() < 2 * pieces => {
                return Err(CopalError::Store(format!(
                    "embedding service answered {status}",
                )));
            }
            Some(_) => pieces *= 2,
        }
    }
}

/// Split text into `pieces` parts of about equal length, each cut at
/// whitespace when a whitespace is near, and never inside a character.
fn split_evenly(text: &str, pieces: usize) -> Vec<&str> {
    let mut parts = Vec::with_capacity(pieces);
    let mut start = 0;
    for done in 1..pieces {
        let target = text.len() * done / pieces;
        let mut cut = target.max(start);
        while cut < text.len() && !text.is_char_boundary(cut) {
            cut += 1;
        }
        // Prefer the nearest whitespace before the target, within a
        // tenth of the text, so words stay whole.
        let window = text.len() / 10;
        if let Some(at) = text[start..cut].rfind(char::is_whitespace) {
            if cut - (start + at) <= window {
                cut = start + at;
            }
        }
        parts.push(&text[start..cut]);
        start = cut;
    }
    parts.push(&text[start..]);
    parts.retain(|part| !part.trim().is_empty());
    parts
}

/// The element-wise mean of equal-length vectors, scaled to unit
/// length, the shape embedding services answer with.
fn mean_normalized(vectors: &[Vec<f64>]) -> Vec<f64> {
    let len = vectors.first().map_or(0, Vec::len);
    let mut mean = vec![0.0; len];
    for vector in vectors {
        for (sum, value) in mean.iter_mut().zip(vector) {
            *sum += value;
        }
    }
    let norm = mean.iter().map(|v| v * v).sum::<f64>().sqrt();
    if norm > 0.0 {
        for value in &mut mean {
            *value /= norm;
        }
    }
    mean
}

/// Embed one text, returning the vector.
///
/// Failures are errors rather than empty vectors: a document recorded
/// with no embedding would silently drop out of semantic search.
pub async fn embed(addr: &str, model: &str, text: &str) -> copal_core::Result<Vec<f64>> {
    request(addr, model, text)
        .await?
        .map_err(|status| CopalError::Store(format!("embedding service answered {status}")))
}

/// One embeddings call. The outer error is a failure to talk to the
/// service at all; the inner one is the status of a refusal.
async fn request(
    addr: &str,
    model: &str,
    text: &str,
) -> copal_core::Result<Result<Vec<f64>, reqwest::StatusCode>> {
    let url = if addr.starts_with("http://") || addr.starts_with("https://") {
        format!("{}/v1/embeddings", addr.trim_end_matches('/'))
    } else {
        format!("http://{addr}/v1/embeddings")
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| CopalError::Store(format!("embedding client: {e}")))?;
    let response = client
        .post(&url)
        .json(&serde_json::json!({ "model": model, "input": text }))
        .send()
        .await
        .map_err(|e| CopalError::Store(format!("embedding request: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        return Ok(Err(status));
    }
    let body: EmbeddingResponse = response
        .json()
        .await
        .map_err(|e| CopalError::Store(format!("embedding response: {e}")))?;
    body.data
        .into_iter()
        .next()
        .map(|datum| datum.embedding)
        .filter(|vector| !vector.is_empty())
        .map(Ok)
        .ok_or_else(|| CopalError::Store("embedding service returned no vector".into()))
}

/// Fuse ranked lists by reciprocal rank.
///
/// Lexical and semantic retrieval disagree usefully: one matches
/// words, the other meaning. RRF combines them without needing their
/// scores to be comparable, which matters here because this engine
/// reports no lexical score at all. A document near the top of either
/// list ranks well; one near the top of both ranks best.
pub fn reciprocal_rank_fusion(lists: &[Vec<String>], k: f64) -> Vec<String> {
    let mut scored: std::collections::HashMap<&str, f64> = std::collections::HashMap::new();
    for list in lists {
        for (rank, id) in list.iter().enumerate() {
            *scored.entry(id.as_str()).or_insert(0.0) += 1.0 / (k + (rank as f64) + 1.0);
        }
    }
    let mut ordered: Vec<(&str, f64)> = scored.into_iter().collect();
    // Ties break on the id so a fused ranking is deterministic.
    ordered.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    ordered.into_iter().map(|(id, _)| id.to_owned()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in embedding service that refuses any input longer than
    /// `limit` characters with a 500, the way llama.cpp refuses input
    /// past its batch, and otherwise answers a vector made from the
    /// input's length.
    async fn small_window_service(limit: usize) -> String {
        use axum::{http::StatusCode, routing::post, Json, Router};
        let app = Router::new().route(
            "/v1/embeddings",
            post(move |Json(body): Json<serde_json::Value>| async move {
                let input = body["input"].as_str().unwrap_or_default();
                if input.chars().count() > limit {
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
                Ok(Json(serde_json::json!({
                    "data": [{ "embedding": [input.len() as f64, 1.0] }]
                })))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    #[tokio::test]
    async fn a_passage_too_large_for_the_model_is_embedded_in_pieces() {
        let addr = small_window_service(40).await;
        let passage = "dense figures 1947 65.2 1948 64.7 1949 64.0 1950 62.9 1951 62.6";
        assert!(passage.len() > 40);

        // Sent whole, the service refuses it.
        assert!(embed(&addr, "m", passage).await.is_err());
        // Fitted, it comes back as the unit-length mean of its pieces.
        let vector = embed_fitting(&addr, "m", passage).await.unwrap();
        let norm = vector.iter().map(|v| v * v).sum::<f64>().sqrt();
        assert!((norm - 1.0).abs() < 1e-9, "{vector:?}");
        // Text that fits is embedded in one call, exactly as before.
        let fits = embed_fitting(&addr, "m", "short text").await.unwrap();
        let direct = mean_normalized(&[embed(&addr, "m", "short text").await.unwrap()]);
        assert_eq!(fits, direct);
    }

    #[tokio::test]
    async fn a_service_that_refuses_everything_still_fails() {
        let addr = small_window_service(0).await;
        let err = embed_fitting(&addr, "m", &"word ".repeat(200)).await;
        assert!(err.is_err(), "a refusal at every size must not pass");
        // An unreachable service fails at once rather than splitting.
        assert!(embed_fitting("127.0.0.1:1", "m", "anything").await.is_err());
    }

    #[test]
    fn splits_are_even_whole_and_lose_nothing() {
        let text = "alpha beta gamma delta epsilon zeta eta theta";
        for pieces in [1, 2, 4, 8] {
            let parts = split_evenly(text, pieces);
            assert!(parts.len() <= pieces);
            assert_eq!(parts.concat(), text, "{pieces} pieces lost text");
        }
        // Never cuts inside a character.
        let wide = "ééééééééé";
        assert_eq!(split_evenly(wide, 4).concat(), wide);
    }

    #[test]
    fn fusion_rewards_agreement_between_lists() {
        let lexical = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let semantic = vec!["c".to_owned(), "b".to_owned(), "d".to_owned()];
        let fused = reciprocal_rank_fusion(&[lexical, semantic], 60.0);

        // The property fusion exists for: documents BOTH retrievals
        // found outrank documents only one found. b and c appear in
        // each list; a and d appear in one apiece.
        let position = |id: &str| fused.iter().position(|f| f == id).unwrap();
        assert!(position("b") < position("a"), "{fused:?}");
        assert!(position("b") < position("d"), "{fused:?}");
        assert!(position("c") < position("a"), "{fused:?}");
        assert!(position("c") < position("d"), "{fused:?}");
        // Everything either list found is present.
        assert_eq!(fused.len(), 4);
    }

    #[test]
    fn fusion_of_one_list_preserves_its_order() {
        let only = vec!["x".to_owned(), "y".to_owned(), "z".to_owned()];
        assert_eq!(
            reciprocal_rank_fusion(std::slice::from_ref(&only), 60.0),
            only
        );
        assert!(reciprocal_rank_fusion(&[], 60.0).is_empty());
    }
}
