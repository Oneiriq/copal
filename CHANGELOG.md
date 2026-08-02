# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Copal has not cut a release yet. Everything below is the road to 0.1.0.

## [Unreleased]

### Retrieval

- **Search and extracted text answer to the contract.** The
  retrieval surface was REST-only: absent from GraphQL, invisible to
  the differ, and outside the declaration set governing everything
  else, on a product whose thesis is governed retrieval. Both are now
  contract queries, a shape Janus grew for reads that answer a
  question rather than paging a collection. They declare their
  parameters, their read scope, and their rate class; they render
  into the OpenAPI document and the GraphQL schema; and both faces
  call one core, proven by a test that asserts the two answers are
  the same value and that a key without the read scope is refused on
  each.

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

- **Schema evolution: boot reconciles the database against the
  code.** `IF NOT EXISTS` created and then never updated, so a
  database from an older release silently lacked every later
  definition, engine `PERMISSIONS` included, exactly where an
  operator turning caller sessions on needed them. The store now
  introspects the live database at both `INFO` levels, diffs it
  against the release's schema through the surql-rs diff engine, and
  applies only the differences as `OVERWRITE` forms, engine-pinned to
  create absent objects and replace present definitions without
  touching rows. A matching database runs no DDL; an older one heals
  on first boot, proven by a test that rebuilds the pre-permissions
  shape, seeds rows, applies, and diffs to zero with the rows intact.
  Removals are reported, never executed. Getting the diff to zero
  fixed six surql-rs defects (grouped-permission equality, the
  engine's `none | T` echo for optional fields, a `$value`/`VALUE`
  keyword collision, field names ending in `type` breaking the type
  parser, synthetic `field.*` children read as removals, and
  add-table diffs that re-defined their own table), each carrying
  the engine echo that exposed it as a test fixture.

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

- **The conformance table is green, and the failures were the
  harness.** The two checks that shipped red came from reusing a
  running stack between runs: rebuilding the image recreates the
  Copal container with an empty blob root while the engine container
  keeps its in-memory metadata, so records survive pointing at
  content the backend no longer holds. That reads exactly like a
  product defect. The harness now clears the stack before standing
  it up, and every client passes every check across repeated runs.
  The run also captures the server's file listing, its blob
  inventory, and its logs, because attributing a failure needs both
  sides.

- **A busy key answers SlowDown.** Re-uploading a key whose previous
  upload is still finishing refused with a terminal 409, which broke
  ordinary mirrors: rclone re-uploads whenever its comparison
  disagrees, and every such write failed the sync. S3 has no notion
  of a key being busy, so the answer is the retryable `SlowDown` with
  `Retry-After`, which stock clients already carry. Found by the
  conformance harness on its first honest run.

- **The conformance harness.** `conformance/run.sh` is one command
  and one table: the compose stack builds Copal from the repository,
  pins the engine, seeds MinIO, and runs a named-check scenario per
  client (mc, aws CLI, rclone), with the five defects the first live
  migration surfaced as permanent assertions. Results commit per
  release under `conformance/results/` and the migration guide links
  them; a prospect reproduces the table with the same command, which
  is what makes the compatibility claim self-verifiable. A manual
  workflow runs it on demand.

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

- **A performance envelope.** `bench/run.sh` stands the stack up
  twice, with caller-bound engine sessions off and on, and takes the
  same measurements through the S3 gateway both times, so the cost of
  the second enforcement layer is a column rather than a guess.
  Timings are the best of three with the spread beside them, because
  on a single node the two configurations differ by less than the
  machine's own noise, and the table says so rather than implying a
  precision it does not have. Results commit per release under
  `bench/results/`.

- **A backup and restore procedure.** Two stores need an order, and
  the order follows from the storage model: content is written to a
  staging name and renamed onto its digest, so blobs are append-only
  and a blob without a record is harmless while a record without a
  blob is a broken file. Back up blobs first, metadata second.
  [backup.md](docs/backup.md) carries the commands, what a restore
  yields (derived counters rebuild, stale claims reap through the
  path a crash already uses, journaled runs resume), what it does not
  cover (point-in-time recovery between exports), and the
  verification drill, because an untested backup is a claim rather
  than a procedure.

- Request and transfer timeouts, a CORS allowlist that is absent when
  unconfigured, a multi-stage container image with a non-root runtime user,
  graceful shutdown, `/healthz` and `/readyz`, and a dependency audit in CI.
- Admin token rotation without a restart, and forwarded origins recorded on
  audit rows.

### Governance

- **The retention admin surface, with the WORM authority line.**
  Retention and holds are set on the admin listener, and compliance
  mode refuses shortening, clearing, and downgrading until its clock
  expires, for admins too. The refusal is enforced in the update's
  own WHERE clause rather than a read-then-write two admins could
  race. Holds require a stated reason in both directions, and every
  change writes an audit event carrying it.

- **Retention holds bytes at the eraser.** Versions carry
  `retain_until`, `retention_mode`, and `legal_hold`, and the GC's
  reference recount treats a non-erasable version as a live link: a
  hold or an unexpired clock carries content through its file's
  deletion, and the blob never reaches the mark step. The freeze
  event now names the frozen set (links and the armed flag) rather
  than refusing every update, so retention state moves on armed rows
  while the artifact stays immutable and tampering still throws.
  Tests prove the round trip at zero grace: held bytes survive an
  aggressive sweep, release collects them on the next pass, and the
  clock alone decides.

- **Principals within tenants, designed.** The API key is the
  smallest identity Copal has, so audit answers "which credential"
  rather than "who", and two keys with the same scopes are
  indistinguishable to every layer.
  [principals.md](docs/principals.md) makes keys credentials
  belonging to named actors: scopes resolve as the intersection of
  key and principal, the token gains a `pr` claim, and ownership
  becomes expressible in compiled `PERMISSIONS`, which is what lets
  the engine layer say what the application layer says. Rows written
  before principals existed count as nobody's, because treating
  unknown authorship as ownership would silently widen access on
  upgrade.

- **Caller sessions are reused across requests.** Opening one costs
  two engine round trips, which a caller making many small reads paid
  every time. Sessions are now held by the identity that minted them:
  tenant, caller, and scopes, so a key whose scopes change mints a
  different session rather than riding an old one. The cache sits
  behind authentication, so a revoked key never reaches it. Bounds
  are configuration (`COPAL_SESSION_CACHE_SECS`,
  `COPAL_SESSION_CACHE_SIZE`), and either at zero pays the open every
  time; sessions carry no engine expiry, so the lifetime is Copal
  policy alone. Both faces and the S3 gateway share one cache.

- **Engine policy is compiled from the contract.** The hand-written
  guarded-column list is gone: field guards and read-scope conjuncts
  derive from the same declarations the application layer enforces,
  thread into the schema through the store config, and a guard the
  contract declares without an engine clause refuses the boot. Table
  select clauses on contract-backed tables now also require the
  declared read scope (`$token.sc CONTAINS 'read'`) beside the
  tenancy rule, writes keep tenancy alone, and the generator tests
  hold the rendered DDL to the contract. One declaration set, two
  enforcement layers, no list to keep in sync.

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
  subscriptions carry the session for their whole lifetime, and the
  access method declares no session expiry at all: a session's
  lifetime is its work's lifetime, ended by drop, with Copal's token
  TTL, request scope, and stream ceilings as the lifetime authority.
  An engine expiry clock beside those had nothing to add and raced
  the stream ceiling, killing live queries silently.

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
