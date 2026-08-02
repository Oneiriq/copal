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

- **A MinIO migration guide** (`docs/migration.md`): the one-run move with
  stock tools, what the content gains on arrival, and the boundaries stated
  plainly.
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

- **The gateway survives MinIO's own tooling, proven by a recorded
  mirror.** A live `mc mirror` run against the gateway surfaced four
  defects that in-process tests never could, each now fixed and
  pinned: bucket-level requests with the trailing slash minio-go
  always sends were routed as unknown paths, `GetBucketLocation` was
  unimplemented (mc reads both refusals as a missing bucket and
  transfers nothing), served objects carried no `Last-Modified`
  header (mc refuses reads without one), multipart parts stored their
  streaming-signature framing verbatim (assembling framing into the
  object, caught by a round-trip digest), and listings named
  unfinalized uploads as zero-byte entries that blocked mirror
  resume. The recorded run in the migration guide shows the full
  mirror, a no-op second pass, an empty `mc diff`, and the 80 MiB
  multipart object round-tripping byte-identical.

- **One engine session per process, on purpose.** Against a real
  `ws://` engine, most REST requests failed with `Session not found`:
  the SDK mints an engine session per handle clone and announces each
  clone and drop over a side channel the remote router can lose under
  concurrency, and an axum state extraction clones per request.
  surql-rs now shares one session across client clones, so the
  service holds one session and mints new ones only explicitly, which
  is also the architecture the `PERMISSIONS` pushdown assumes.

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

- **Caller sessions reach the request path, behind a flag.**
  `COPAL_ENGINE_SESSIONS=on` makes the files resource's handlers run
  repository calls through engine sessions minted per request from
  the authenticated key's identity and scopes, so table and field
  `PERMISSIONS` filter live traffic under the application checks.
  Boot refuses the flag without `COPAL_ENGINE_ACCESS_KEY`. The
  guarded attribution column now tolerates engine redaction end to
  end: a non-admin key lists version history with `created_by`
  removed by the engine before projection, an admin key sees it, and
  wire shapes match the service-session face exactly. Off by
  default. The sweep then reached every handler with direct
  tenant-scoped store access: search, text, usage, renditions and
  run listings, version downloads, and the tus trio. Content
  uploads, the public download path, and the flow engine stay on the
  service store by architecture, with the reasons on the roadmap.
  The helper-based handlers (grants, webhooks, edge) and every
  GraphQL resolver followed: the execute path seeds the caller
  session into the typed context, resolvers read it back, and
  subscriptions carry the session for their whole lifetime, with the
  access method's session duration raised to an hour so re-auth,
  never session expiry, is what ends a stream.

- **The engine is now a second enforcement layer.** Every table
  carries `PERMISSIONS` by a mechanical rule: tables with a
  `tenant_id` column admit only rows matching the caller token's
  tenant, tables without one are closed to caller sessions, and
  `file_version.created_by` is redacted unless the token carries the
  admin claim, held equal to the contract's field guards by a parity
  test. With `COPAL_ENGINE_ACCESS_KEY` set the store defines a record
  access method (rotating by key replacement), copal-server mints
  short-lived caller tokens mirroring the scope model, and
  `Store::caller` opens engine-filtered sessions over the same
  connection. The service session bypasses all of it, so nothing
  changes for the request path until handlers adopt caller stores;
  the proof that matters already holds: a repository call with no
  tenant filter at all cannot cross tenants through a caller session.

- **The pushdown disposition was wrong, and three pinned tests say
  why.** `engine_sessions.rs` proves the engine CAN tell Copal's
  callers apart: a cloned handle is its own session over the same
  connection, a record access JWT binds a caller identity to it, and
  table and field `PERMISSIONS` then filter engine side while the
  root handle keeps full authority. Also pinned: enforcement follows
  the actor, so a record session is filtered even on an engine
  without credentials, where only the anonymous session acts as
  owner; refused writes return empty rows with no error. `PERMISSIONS` pushdown returns to the
  roadmap as an implementable project.

- **Streams end, and that is the re-auth.** A subscription lives at most
  `COPAL_SUBSCRIPTION_MAX_SECS` (default 900), then completes normally;
  re-subscribing runs full authentication, which is how revocation and
  expiry reach streams already running. One caller holds at most eight
  subscriptions, declared in the contract and enforced by the dispatcher,
  with the slot freed the moment a stream drops.
- **One budget for the whole fleet.** `COPAL_RATE_LEDGER=store` keeps the
  consumption windows in the shared database, with the check and the
  increment as one guarded statement so racing replicas serialize instead
  of overspending. The in-memory ledger stays the single-node default,
  since the shared one costs a store round trip per operation. Old windows
  are swept with everything else.
- **Persisted operations.** `COPAL_PERSISTED_OPERATIONS` names a JSON file of
  sha256 hash to GraphQL document; set, only listed operations run, named by
  hash in the Apollo shape or sent whole, and anything else refuses before
  parsing costs anything. The file is verified at startup, so an allowlist
  that lies about a hash refuses to boot instead of serving the wrong
  operation later.

- **The first field guard.** A version's `created_by` is audit data:
  `admin`-scoped keys and header mode see it, everyone else lists history
  without it. Both faces redact from the one declaration, REST by omitting
  the key through the shared projection API and GraphQL by the dispatcher's
  projection, proven identical by test. The field reads as nullable on every
  generated surface and carries `x-guard` in OpenAPI.
- **The declarations release.** The contract now declares what every
  operation demands, and both faces enforce it from that one declaration:
  reads require the `read` scope, mutations `write`, webhook registration
  `admin`. Refusals are identical across faces, 403 naming the scope.
  Consumption is metered against one shared ledger (reads 6000 units a
  minute per caller, mutations 600; a listing costs its row limit), so
  switching protocols never dodges a budget; exhaustion is 429
  `too_many_requests` everywhere. OpenAPI operations carry
  `x-requires-scopes`. Header mode holds every scope, since it is full
  trust.

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
