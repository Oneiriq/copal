# A reranking service

Copal runs no models. Retrieval decides which passages contain the
query's words or sit near its vector, and neither reads a passage
against the question. A reranker does, one pair at a time, which is
why Copal hands it a shortlist rather than a corpus.

## The contract

```
POST {COPAL_RERANK_ADDR}
Authorization: Bearer {COPAL_RERANK_TOKEN}      (only when set)
{"query": "...", "documents": ["passage", ...], "model": "..."}
```

The answer:

```
200 {"results": [{"index": 0, "relevance_score": 0.91}, ...]}
```

Copal also accepts the bare array text-embeddings-inference returns:

```
200 [{"index": 0, "score": 0.91}, ...]
```

so an existing TEI deployment needs no adapter. `model` is sent only
when `COPAL_RERANK_MODEL` is set, since some services require one and
others reject it.

Scoring a subset is allowed. Whatever the answer leaves out keeps its
incoming order below everything it ranked, so a `top_n` reply is as
usable as a full one and no document is ever lost from a page.

## Running the reference

```
RERANK_TOKEN=shared python rerank.py

COPAL_RERANK_ADDR=http://127.0.0.1:9300/rerank \
COPAL_RERANK_TOKEN=shared \
COPAL_RERANK_DEPTH=50 \
copal-server
```

A search then carries how far the reranking reached:

```
GET /v1/search?q=vessel+inspection
{"mode": "lexical", "reranked": 2, "items": [...]}
```

`reranked` is `0` when a service is configured and did not answer. It
is absent when none is configured, and also when fewer than two
documents matched, since there is no order to change and the service
is not called.

The reference scores by term overlap and how tightly the matched words
sit together. It is not a real reranker. It exists so the seam can be
run and tested before anyone provisions a GPU, and `score()` is the
only part a real deployment replaces. It listens on `RERANK_PORT`,
default 9300.

## Real implementations

Anything speaking one of the two shapes works unchanged:
text-embeddings-inference with a cross-encoder (bge-reranker,
mxbai-rerank), Infinity, Jina, Cohere, Voyage.

## What a failure costs

Relevance, and nothing else. A reranker improves an answer that
already exists, so an unreachable or refusing service leaves the fused
ranking standing, logs a warning naming the cause, and reports
`reranked: 0`. Semantic retrieval degrades to lexical on the same
reasoning.
