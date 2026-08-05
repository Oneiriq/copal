#!/usr/bin/env python3
"""A reranking service, in the shape Copal asks for.

Copal runs no models. Retrieval decides which passages contain the
words or sit near the query vector; neither reads a passage against
the question. A reranker does, one pair at a time, which is why Copal
hands it a shortlist rather than a corpus. The contract is one
request:

    POST /rerank
    Authorization: Bearer {token}      (only if COPAL_RERANK_TOKEN is set)
    {"query": "...", "documents": ["passage", ...], "model": "..."}
    -> 200 {"results": [{"index": 0, "relevance_score": 0.91}, ...]}

Copal also accepts the bare array text-embeddings-inference returns,
`[{"index": 0, "score": 0.91}, ...]`, so an existing TEI deployment
needs no adapter at all. Scoring a subset is allowed: whatever the
answer leaves out keeps its incoming order below what it ranked.

This reference scores by term overlap, which is not what a reranker is
for and is deliberately not pretending to be. It exists so the seam
can be run, tested, and pointed at before anyone provisions a GPU.
`score()` at the bottom is the whole of what a real deployment
replaces; everything above it is the contract.

    RERANK_TOKEN=shared python rerank.py

    COPAL_RERANK_ADDR=http://127.0.0.1:9300/rerank \\
    COPAL_RERANK_TOKEN=shared copal-server

Production alternatives that speak one of the two shapes already:
text-embeddings-inference with a cross-encoder (bge-reranker,
mxbai-rerank), Infinity, Jina, Cohere, Voyage.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re

TOKEN = os.environ.get("RERANK_TOKEN")
PORT = int(os.environ.get("RERANK_PORT", "9300"))


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.rstrip("/") != "/rerank":
            return self.fail(404, "no such path")
        if TOKEN and self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self.fail(403, "bearer token does not match")

        try:
            length = int(self.headers.get("Content-Length", "0"))
            ask = json.loads(self.rfile.read(length) or b"{}")
            query = ask["query"]
            documents = ask["documents"]
        except (ValueError, KeyError, TypeError) as error:
            return self.fail(400, f"expected query and documents: {error}")

        results = [
            {"index": index, "relevance_score": score(query, document)}
            for index, document in enumerate(documents)
        ]
        self.send(200, {"results": results})

    def fail(self, status, message):
        self.send(status, {"error": message})

    def send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def score(query: str, document: str) -> float:
    """Replace this. Everything above it is the contract.

    A real reranker runs a cross-encoder over the pair and returns
    what the model says. This counts how many of the query's words the
    passage carries and how close together they sit, which beats
    nothing and loses to any actual model.
    """
    words = [w for w in re.findall(r"\w+", query.lower()) if len(w) > 1]
    if not words:
        return 0.0
    tokens = re.findall(r"\w+", document.lower())
    if not tokens:
        return 0.0

    present = {word for word in words if word in tokens}
    coverage = len(present) / len(words)
    if len(present) < 2:
        return round(coverage, 6)

    # Closest window holding every matched word: a passage arguing the
    # point says the words together, and one merely mentioning them
    # says them pages apart.
    positions = [i for i, token in enumerate(tokens) if token in present]
    tightest = len(tokens)
    for start in range(len(positions)):
        seen = set()
        for end in range(start, len(positions)):
            seen.add(tokens[positions[end]])
            if len(seen) == len(present):
                tightest = min(tightest, positions[end] - positions[start] + 1)
                break
    proximity = len(present) / tightest
    return round(coverage + 0.25 * proximity, 6)


if __name__ == "__main__":
    print(f"reranking on :{PORT}{' (token required)' if TOKEN else ''}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
