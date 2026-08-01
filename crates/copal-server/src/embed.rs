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

/// Embed one text, returning the vector.
///
/// Failures are errors rather than empty vectors: a document recorded
/// with no embedding would silently drop out of semantic search.
pub async fn embed(addr: &str, model: &str, text: &str) -> copal_core::Result<Vec<f64>> {
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
        return Err(CopalError::Store(format!(
            "embedding service answered {status}",
        )));
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
