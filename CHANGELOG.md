# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Copal has not cut a release yet. Everything below is the road to 0.1.0.

## [Unreleased]

### Retrieval

- **Search narrows and pages.** The contract query gained `prefix`,
  `content_type`, and `cursor`: filters apply at the engine on both
  retrieval legs through the chunk's file link, so a filtered search
  never ranks passages it must discard, and the cursor continues a
  ranking page by page (best-effort by nature, since rankings shift
  as content changes).
- **Embeddings heal after a model change.** Chunks record the model
  that embedded them; a background pass re-embeds any passage whose
  vector is absent or carries old geometry, so a swapped model
  drains the stale set without an operator remembering to. The
  index itself already rebuilds through the schema diff on a
  dimension change.

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
- **Renditions serve straight from their URL.** `GET
  /v1/files/{id}/renditions/{kind}-{w}x{h}.{format}` serves an
  existing rendition under its own access level and derives on a
  miss, inline, before answering. Deriving is a write, so it needs
  the write scope and the owning tenant; anonymous callers read what
  exists. Both faces of the derivatives surface share one render
  body and one path formula, so the URL face and the request action
  can never disagree about where a rendition lives.
- **Ingestion from a URL.** `POST /v1/files/fetch` creates the
  record and the server pulls the bytes itself, under the outbound
  policy (tenant-supplied URLs refuse private address space by
  default) and the upload ceiling, enforced mid-stream against lying
  Content-Length headers. Fetched content walks the standard sniff,
  scan, and finalize pipeline, and the source's served content type
  stands when the caller declares none. Declared in the contract, so
  agents get `file_fetch` as a tool.
- **The external transformer seam.** Any operator-run HTTP service
  becomes a derivation step: the transform action ships source bytes
  out and lands whatever comes back through the same claim and
  complete path uploads use, so outputs are real files with digests,
  versions, and serving rules intact. A 4xx answer fails the derived
  record with the service's reason; 5xx and transport trouble retry
  on the flow engine's budget. Declared in the contract, so the
  action rides REST, GraphQL, and the MCP manifest alike, and
  derivations list beside renditions.
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

- **Google Cloud Storage and Azure Blob Storage back residencies.**
  Two new residency schemes, `gcs` and `azblob`, join `fs` and `s3`
  behind the same blob port. Everything a residency already carries
  applies unchanged: per-residency encryption keys, key rotation
  with `previous_encryption_key`, the re-seal sweep, dedupe scoping,
  and tenant assignment, because the store speaks OpenDAL and the
  scheme is connection detail. GCS credentials may be inline
  (base64 service-account JSON), a path, or absent for the ambient
  chain, so workload identity works with an empty credential block.

- **The blob master key rotates without downtime.** Move the old key
  to `COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS`, set the new one, restart:
  reads fall back to the retiring key, database secrets re-seal at
  boot, and a background sweep re-writes sealed objects under the
  current key, in place, because digests cover plaintext and the
  address never moves. `copal_resealed_total` going quiet is the
  signal to drop the old key. Residencies sealing under their own
  keys rotate independently through `previous_encryption_key`. The
  old instruction to re-mint credentials after a rotation is gone;
  the boot pass carries them over.

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

- **The four clients package.** `sdks/` holds one skeleton per
  language and one `VERSION`; `sdks/build.sh` assembles crate
  `oneiriq-copal`, Python `oneiriq-copal`, npm `@oneiriq/copal`, and
  `github.com/Oneiriq/copal-go` from the generated clients and
  validates each (`cargo check`, `py_compile` + import, `tsc
  --noEmit`, `go build` + `go vet`). CI runs the same script on
  every push, and a manually dispatched publish workflow dry-runs
  the release motion until registry tokens exist. The pipeline's
  first run caught a real bug: the generated Python had an
  IndentationError in every sub-collection method (janus #13),
  because nothing had ever compiled the file.

- **The agent face joined the evidence.** A curl-only MCP runner is
  the conformance harness's fourth client: handshake, generated
  manifest, a listing over the mirrored objects, and a search that
  finds extracted text, in the committed table beside mc, the aws
  CLI, and rclone. The bench gained the retrieval thesis's numbers:
  search and an MCP tool call, measured through the API face in both
  session configurations. The newest claims now carry the same
  runnable proof as the oldest.

- **Ingestion joined the contract.** Creation was a hand-written
  REST handler no artifact documented, no differ governed, and no
  generated client or agent tool reached; the byte paths were
  likewise invisible. `files` now declares a `create` action and
  content faces: `POST /v1/files` and the content `PUT`/`GET` appear
  in the OpenAPI document, `fileCreate` lands on GraphQL, and
  `file_create` becomes an MCP tool, all dispatching through the
  same chain. The proof is the agent loop the audit found
  impossible: create, obtain an upload grant, deliver bytes, read
  the work back ready, all through tools plus one grant URL.

- **The MCP face.** `POST /mcp` serves agents: `tools/list` is the
  manifest Janus generates from the contract (every resource's list
  and get, every action, every query, with scopes and rate classes
  as annotations), committed as `docs/mcp-tools.json` under the same
  drift gate as every artifact. `tools/call` dispatches through the
  same chain as GraphQL, so scopes, budgets, field guards, and
  caller-bound engine sessions bind agents exactly as they bind
  everyone, proven by a test where a read-only key's remove call is
  refused with the scope named. A parity test holds the tool router
  and the generated manifest to the same names.

- **The change feed replays.** The events listing gained keyset
  pagination over `(created_at, id)`: ascending from a cursor
  replays forward, so a down indexer resumes from a saved cursor
  instead of re-listing the world, and the default newest-first
  order pages backward through history. One core serves both faces,
  `GET /v1/events` joined the REST router (the OpenAPI document
  promised it; now a handler answers), and the subscription remains
  the live half of the same surface. The cursor is opaque because
  outbox rows carry engine-assigned ids that hold no time order, a
  fact the first cut of this feature learned the hard way.

- **Conditional writes.** `If-None-Match: *` creates and never
  replaces; `If-Match: <digest>` replaces exactly the content the
  caller believes is current; both answer 412 through every face
  (REST body path, S3 PutObject, S3 CompleteMultipartUpload). The
  condition rides the upload claim's own compare-and-set, so two
  racing writers resolve at the engine rather than in a
  check-then-claim window, and a busy key still answers SlowDown
  rather than 412 when the condition itself holds. The ETag is the
  content digest, so If-Match is a digest compare-and-swap, which is
  what agents coordinating on shared keys actually need. Two named
  conformance checks pin the semantics through the stock aws CLI.

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

- **Traces export over OTLP.** `COPAL_OTLP_ENDPOINT` turns every
  served request into a span named by its route template, joined to
  the caller's `traceparent` and propagated outward on webhook
  deliveries, transformer calls, and fetches, so one tree shows the
  request and everything it caused across services. Export is
  http/protobuf; unset, spans feed the logs and nothing leaves the
  process.

- **The embedded tier.** The default build carries the metadata
  engine in the binary: `COPAL_DB_URL=surrealkv://./data/db` runs
  copal as one process with the database on disk beside the blobs,
  and `mem://` runs it ephemeral for demos. Same engine, so schema
  reconciliation, PERMISSIONS, caller sessions, and audit
  immutability hold identically; the two-instance topology stays on
  `ws://`, and moving up is pointing a SurrealDB server at the same
  directory. Small installs stop operating a database.

- **Counters on the newer surfaces.** The `/metrics` scrape gains
  MCP calls and errors, embedding backfill refreshes, change-feed
  reads, compliance-mode retention refusals, and conditional-write
  refusals on both faces, so the operator watching the newest
  behavior has the same instrument as the oldest.

- **Two instances, proven.** `COPAL_HA=1 ./conformance/run.sh`
  stands up two Copal processes against one engine behind
  round-robin nginx and passes every conformance check for every
  client through the proxy: claims and leases resolve at the engine,
  multipart completions see parts whichever instance staged them
  through the shared blob root, and conditional writes hold across
  hops. The topology requirements are documented (shared blob root,
  same keys, one engine); session cache, sweeps, and the embedding
  backfill stay per-instance by design.

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

- **The audit trail exports to a SIEM.** One admin endpoint serves
  the whole deployment's audit events as NDJSON, ascending, keyset
  cursored over `(created_at, id)` with the checkpoint riding the
  `x-copal-next-cursor` header, so the body stays pure events and a
  partial page still advances the checkpoint. A collector is a curl
  loop and a checkpoint file; the operations doc ships one. Optional
  `tenant=` narrows the stream. Engine immutability means an
  exported line never goes stale.

- **Compliance actions land in the change feed.** Retention set and
  cleared, holds applied and released, and version pruning emit
  outbox events beside the audit trail, so the feed and the webhook
  dispatcher carry exactly the events compliance watches instead of
  only file lifecycle. Pruning names how many versions went and the
  depth that ruled.

- **S3 credentials answer to principals.** The last credential type
  joins the actor model: minting takes an optional `principal`
  (refused when the ceiling cannot carry the gateway's read and
  write grant), uploads through SigV4 record the actor's handle in
  `created_by`, the caller token carries `pr` so engine-side
  ownership holds on the gateway too, and disabling a principal
  refuses its S3 credentials live. An agent fleet speaking S3 now
  has per-agent attribution and one-switch revocation, exactly as it
  does with keys.

- **Ownership reaches both enforcement layers.** Field guards see
  the row now, so `created_by` is visible to its author, to admins,
  and to nobody else, decided per row within one listing on both
  faces; a caller who only partially sees the column cannot filter
  by it. The engine says the same thing: caller tokens carry the
  principal handle as `pr`, and the compiled clause on the column is
  `$token.adm = true OR created_by = $token.pr`. Uploads through the
  REST body path record the actor's handle; rows written before
  principals existed, and by principal-less keys, match no handle
  and read as nobody's, because treating unknown authorship as
  ownership would widen access on upgrade. Rate buckets key on the
  principal, so an agent's spend is the agent's across every
  credential it holds.

- **Principals: named actors that keys belong to.** The first two
  slices of the design. A `principal` carries a handle, a kind, a
  scope ceiling, and a disabled switch; keys mint under one and
  answer to it. Scopes beyond the ceiling refuse at mint time,
  effective scopes are the intersection of key and principal computed
  at every authentication (so narrowing a principal narrows its keys
  live), and disabling a principal refuses every key it owns at once,
  which is the operation an incident actually needs. Field guards now
  compare actors: a key under a principal answers as its handle. A
  key without a principal behaves exactly as before, which is what
  makes this adoptable.

- **Retention completes: policy, pruning, the engine clause, and the
  ledger line.** A tenant default stamps every new version at
  creation, computed once and never recomputed, so a policy change
  cannot shorten what exists. `keep_last` prunes erasable history
  beyond a depth, and only erasable history: holds and unexpired
  clocks survive any setting. The compiled `PERMISSIONS` delete
  clause on versions refuses while anything binds the row, so a
  request-path bug meets a second refusal. Retained bytes ride
  beside usage, so a full tenant can see how much of the total is
  bound.

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
