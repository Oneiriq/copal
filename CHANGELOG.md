# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Copal has not cut a release yet. Everything below is the road to 0.1.0.

## [Unreleased]

### Retrieval

- **Subscriptions over the event outbox.** A client watches file events as the
  engine writes them instead of polling. Served as graphql-sse on the existing
  `POST /graphql` under `Accept: text/event-stream`, so one route and one
  authenticator cover every operation. `action` became filterable on the
  listing and the subscription alike, and both the tenant scope and the action
  filter are conditions the engine applies before delivering a row.
- **Retrieval over passages.** Documents split into overlapping passages, each
  embedded and indexed on its own, so a hit names the part that answers the
  question rather than the file that contains it.
- **The vector index stores F32.** Embedding models emit single precision at
  best, so F64 doubled index memory for digits that never existed. F16 and
  DiskANN wait upstream: the newest published `surrealdb` crate parses
  neither.
- **Embeddings and hybrid retrieval.** Per-passage vectors from any service
  speaking the OpenAI embeddings shape, HNSW-indexed, fused with the lexical
  ranking by reciprocal rank fusion. A relevance floor lets a semantic query
  about nothing stored return nothing.
- **Text extraction and full-text search.** Native for text and JSON, an
  external extractor seam for everything else, BM25-indexed in the same
  database that holds the records.

### Sub-collections

- **A file's versions and an endpoint's delivery attempts** are declared in the
  contract and reach both faces and all four generated clients:
  `GET /v1/files/{id}/versions`, `GET /v1/webhooks/{id}/deliveries`, and a
  field on the parent's GraphQL type. Deliveries gained a `state` filter and
  the index to serve it.

### Ingest and serving

- **S3-compatible ingest gateway** with SigV4 verification, the object plane,
  `ListObjectsV2` with delimiter collapse, multipart upload, and sealed
  per-tenant credentials. `CopyObject` and batch `DeleteObjects` followed,
  which is what `mc mirror`, `rclone sync`, and `aws s3 sync` need; copy moves
  no bytes, since content-addressed storage makes it a new record over the
  same blob.
- **Resumable uploads** over the tus 1.0.0 protocol.
- **Signed upload URLs**: single-use write capabilities in the `cg1` family,
  op-scoped so a read token cannot write.
- **`cg2` edge tokens**: stateless capabilities under sealed per-tenant edge
  keys, verifiable at a CDN worker with no database hop.
- **Image renditions** on the flow engine, with deterministic paths and
  idempotent repeats.
- **Serving correctness**: range requests, conditional requests, and weak
  validator matching.

### Safety and processing

- **Malware scanning** through a clamd seam, with content withheld until the
  digest that was scanned matches the digest being served.
- **The durable execution journal** (`copal-flow`) and the standard post-upload
  pipeline: sniff, extension policy, scan, extract, embed, finalize.
- **The engine outbox and signed webhooks.** Events are born in the same
  transaction as the state change, so every face produces them without
  carrying eventing code.

### Storage and tenancy

- **Per-tenant storage residencies**: named OpenDAL backends with per-row
  backend resolution, so reassignment never strands content.
- **Encryption at rest** with chunked AEAD and plaintext digests, then
  **per-residency keys**, so a tenant needing key separation takes its own
  residency.
- **Per-tenant quotas and usage accounting**, with maintained counters that
  also closed the concurrent-upload over-commit gap.
- **Versioning, soft delete, and the maintenance sweep loop**, with a garbage
  collector that refuses to collect a resurrected blob.

### Interfaces

- **One contract, every face.** Files, events, webhooks, and runs are declared
  once and served over REST and GraphQL, with four generated clients and an
  OpenAPI document that cannot drift from either. Usage stays REST-only: it
  reports a number rather than a collection of rows.
- **API-key authentication**, with the trusted header demoted to a dev mode.
- **A Prometheus endpoint** over process counters.

### Deployment

- Request and transfer timeouts, a CORS allowlist that is absent when
  unconfigured, a multi-stage container image with a non-root runtime user,
  graceful shutdown, `/healthz` and `/readyz`, and a dependency audit in CI.
- Admin token rotation without a restart, and forwarded origins recorded on
  audit rows.

### Governance

- **Keys narrow.** A `ck1` key can be minted with scopes (`read`, `write`,
  `admin`) and an expiry the engine's clock enforces; an expired key refuses
  with the same uniform 401 as a wrong one. An unscoped key holds every scope,
  so nothing existing tightens by surprise. In key mode the scopes ride the
  GraphQL request as its principal, ready for the contract's scope
  declarations.
- **The contract declares its own ceilings.** The GraphQL depth and
  complexity limits were hand-wired at the router, invisible to the
  artifacts and the differ. They now live in the contract, the served
  schema applies them from it, the OpenAPI document carries them as
  `x-limits`, and tightening one is a breaking change the differ names.
  An oversized payload also surfaces as 413 `payload_too_large` on the
  GraphQL face; it was downgraded to a generic 400 because the error
  vocabulary had no such refusal.

### Fixed

- **Lexical search ranks by relevance.** The full-text index decides which
  passages match but returns them in insertion order, and `search::score`
  reports 0 for every row. Search trusted that order, so it answered with the
  OLDEST matching passages rather than the best ones, a bounded search kept the
  wrong ones, and hybrid retrieval fused a meaningless lexical rank against a
  real semantic one. BM25 is now scored in Copal over a bounded window of
  matches, using the same English Snowball stemmer the index's analyzer uses so
  a stemmed match is scored rather than buried. The engine behaviour that forced
  this is pinned by tests, so an engine that learns to rank fails them loudly.
- **Semantic search reaches its index.** The KNN operator's second operand
  decides the plan: an integer is the HNSW search effort, a metric name makes
  the engine compare every row. The query rendered the metric form, so vector
  search was a full table scan.
- **The scan gate asks about content, not lifecycle.** Withholding on state
  meant a re-upload hid the previous, already-scanned version.
- **Quota reservations are released on every path that ends an upload.** Four
  of the five acquisition sites leaked headroom until the sweep reconciled.
- **Keyset pagination no longer repeats a row.** A strict range on a prefix of
  a composite key returns the boundary row on SurrealDB 3.x; a residual
  inequality lands the predicate in the filter stage.
- Grant uses burn only on actual reads; weak validators match correctly.

### Changed

- **The versions listing uses the contract's page envelope.** It answered
  `next_before` and took a `before` query parameter, which no other listing
  did. It is now `next_cursor` and `cursor`, like everything else.
