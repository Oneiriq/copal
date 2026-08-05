//! The reranking-service seam.
//!
//! Retrieval and ranking answer a cheap question over the whole
//! corpus: which passages contain these words, and which of them look
//! most like the query by term statistics or by vector distance.
//! Neither reads the passage against the question. A reranker does,
//! one pair at a time, which is why it runs over a shortlist instead
//! of a corpus.
//!
//! Copal does not run models, for the reason the embedding seam gives:
//! inference means weights, a runtime, and hardware assumptions with
//! no business inside a storage service. So this is a contract an
//! operator points at something that already serves one.
//!
//! Two response shapes are accepted, because the self-hostable
//! implementations split between them:
//!
//! ```text
//! POST {addr}
//! { "query": "...", "documents": ["passage", ...] }
//!
//! -> { "results": [ { "index": 0, "relevance_score": 0.91 }, ... ] }   Cohere, Jina, Voyage
//! -> [ { "index": 0, "score": 0.91 }, ... ]                            text-embeddings-inference
//! ```
//!
//! An index the answer does not mention keeps its incoming order below
//! everything the answer ranked, so a service that returns a `top_n`
//! shortlist is as usable as one that scores every document.

use serde::{Deserialize, Serialize};

use copal_core::CopalError;

/// Where the reranker is and what it wants, carried on the app state
/// so a search does not re-read the environment.
#[derive(Debug, Clone)]
pub struct Reranker {
    pub addr: String,
    pub model: Option<String>,
    pub token: Option<String>,
    /// How many fused candidates get read against the query.
    pub depth: usize,
}

/// How long to wait. A reranker reads every pair with a model, so it
/// is slower than retrieval by design, and a search that hangs on it
/// is worse than a search that ranks by fusion alone.
const TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Serialize)]
struct RerankRequest<'a> {
    query: &'a str,
    documents: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct Scored {
    index: usize,
    /// Cohere and Jina name it `relevance_score`; text-embeddings-
    /// inference names it `score`. Either is the same number.
    #[serde(alias = "score")]
    relevance_score: f64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RerankResponse {
    Wrapped { results: Vec<Scored> },
    Bare(Vec<Scored>),
}

impl RerankResponse {
    fn into_scores(self) -> Vec<Scored> {
        match self {
            RerankResponse::Wrapped { results } => results,
            RerankResponse::Bare(scores) => scores,
        }
    }
}

/// Ask the service to order `documents` against `query`.
///
/// Answers with the input positions, best first. Positions the service
/// did not score follow in their incoming order, so a `top_n` reply
/// still yields a total order over the shortlist.
pub async fn rerank(
    addr: &str,
    model: Option<&str>,
    token: Option<&str>,
    query: &str,
    documents: &[String],
) -> copal_core::Result<Vec<usize>> {
    if documents.is_empty() {
        return Ok(Vec::new());
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(TIMEOUT_SECS))
        .build()
        .map_err(|e| CopalError::Store(format!("rerank client: {e}")))?;

    let mut request = client.post(addr).json(&RerankRequest {
        query,
        documents,
        model,
    });
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|e| CopalError::Store(format!("rerank service unreachable: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(CopalError::Store(format!(
            "rerank service returned {status}"
        )));
    }
    let scored = response
        .json::<RerankResponse>()
        .await
        .map_err(|e| CopalError::Store(format!("rerank service answered unusably: {e}")))?
        .into_scores();

    Ok(order_from(scored, documents.len()))
}

/// Turn scores into a total order over `count` positions.
///
/// Kept separate from the call so the ordering rules are testable
/// without a service: an out-of-range index is dropped rather than
/// panicking a search, a repeated index is honoured once, and whatever
/// went unscored trails in its original order.
fn order_from(mut scored: Vec<Scored>, count: usize) -> Vec<usize> {
    scored.retain(|s| s.index < count);
    // Highest score first; equal scores keep the service's own order.
    scored.sort_by(|a, b| {
        b.relevance_score
            .partial_cmp(&a.relevance_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut order = Vec::with_capacity(count);
    let mut placed = vec![false; count];
    for entry in scored {
        if !placed[entry.index] {
            placed[entry.index] = true;
            order.push(entry.index);
        }
    }
    for (index, done) in placed.iter().enumerate() {
        if !done {
            order.push(index);
        }
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scored(pairs: &[(usize, f64)]) -> Vec<Scored> {
        pairs
            .iter()
            .map(|(index, relevance_score)| Scored {
                index: *index,
                relevance_score: *relevance_score,
            })
            .collect()
    }

    #[test]
    fn the_best_score_leads() {
        assert_eq!(
            order_from(scored(&[(0, 0.1), (1, 0.9), (2, 0.5)]), 3),
            vec![1, 2, 0],
        );
    }

    /// A service answering `top_n` scores a shortlist. What it left
    /// out still has to come back, below what it ranked and in the
    /// order it arrived, or a page would lose documents.
    #[test]
    fn unscored_positions_trail_in_their_own_order() {
        assert_eq!(
            order_from(scored(&[(3, 0.9), (1, 0.8)]), 5),
            vec![3, 1, 0, 2, 4]
        );
    }

    /// A malformed answer must not panic a search or drop a document.
    #[test]
    fn nonsense_indexes_are_ignored_rather_than_fatal() {
        let order = order_from(scored(&[(9, 0.9), (1, 0.5), (1, 0.4)]), 3);
        assert_eq!(
            order,
            vec![1, 0, 2],
            "out of range dropped, repeat honoured once"
        );
        assert_eq!(order.len(), 3, "every position comes back exactly once");
    }

    #[test]
    fn an_empty_answer_keeps_the_incoming_order() {
        assert_eq!(order_from(Vec::new(), 3), vec![0, 1, 2]);
    }

    /// Both shapes the self-hostable services return.
    #[test]
    fn either_response_shape_parses() {
        let wrapped: RerankResponse =
            serde_json::from_str(r#"{"results":[{"index":1,"relevance_score":0.9}]}"#).unwrap();
        assert_eq!(wrapped.into_scores()[0].index, 1);

        let bare: RerankResponse = serde_json::from_str(r#"[{"index":2,"score":0.7}]"#).unwrap();
        let bare = bare.into_scores();
        assert_eq!(bare[0].index, 2);
        assert!((bare[0].relevance_score - 0.7).abs() < f64::EPSILON);
    }
}
