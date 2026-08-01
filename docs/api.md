# API guide

Copal serves two faces from one contract: REST under `/v1` and GraphQL at
`/graphql`. Both dispatch into the same repositories and render rows through
the same wire mapper. The generated artifacts are the reference documents:

| Artifact | Path |
| --- | --- |
| OpenAPI 3.1 | `docs/openapi.json` |
| GraphQL SDL | `docs/schema.graphql` (also served at `GET /graphql`) |
| Rust client | `clients/client.rs` |
| TypeScript client | `clients/client.ts` |
| Python client | `clients/client.py` |
| Go client | `clients/client.go` |

This guide covers what the generated documents cannot: authentication, the
byte routes, error shapes, and the behaviors shared across faces.

## Authentication

Two modes, selected by `COPAL_AUTH_MODE`.

`keys` is the deployment mode. Requests carry `Authorization: Bearer
ck1.<key id>.<secret>`. The tenant comes out of the key row, so a caller
cannot name one. Every failure (absent, malformed, unknown, wrong secret,
revoked) returns the same 401 with the same timing.

`header` is the development mode and the default until 1.0. The
`x-copal-tenant` header is trusted as the tenant identity. The server logs a
warning at startup. See [operations.md](operations.md) for key custody.

Two routes skip tenant authentication by design: grant redemption (the token
is the authorization) and public-file content (anonymous by access level).

## Files

```
POST   /v1/files                    create a record (state: draft)
GET    /v1/files                    list (keyset pagination)
GET    /v1/files/{id}               fetch metadata
DELETE /v1/files/{id}               soft-delete
PUT    /v1/files/{id}/content       stream bytes in
GET    /v1/files/{id}/content       stream bytes out
GET    /v1/files/{id}/versions      version history (paginated)
GET    /v1/files/{id}/versions/{n}/content
POST   /v1/files/{id}/url           issue a signed URL
```

Create accepts `path`, `content_type`, `access`, `metadata`, and an optional
`idempotency_key`. A replayed key returns the original record with status 200
instead of 201. The `metadata.processing` namespace is server-owned; anything
a caller supplies there is stripped, because the pipeline writes its verdicts
under that key.

When malware scanning is configured, content is withheld until a scan
clears that content: reads and signed URLs answer 409 for bytes no
scan has covered, and a detection quarantines the file. A re-upload
keeps serving the previous version until the new one clears. See
[operations.md](operations.md).

Upload is a single PUT of raw bytes. The server streams to staging while the
digest accumulates, then finalizes with a rename. The size ceiling
(`COPAL_MAX_UPLOAD_BYTES`) is enforced inside the stream and returns 413.
With a processing pipeline configured the record lands in `scanning` and a
run is enqueued; without one it lands in `ready`.

### Listing and cursors

List endpoints take `limit`, `cursor`, a filter, and `sort`. Filters and sorts
are contract-validated: every filterable column rides an index and every sort
is reachable through an index prefix. The response envelope is
`{ "items": [...], "next_cursor": "..." | null }`.

Cursors are opaque and embed their sort direction. Replaying a cursor under a
different sort returns 400, because the alternative is silently wrong pages.
A cursor minted on one face works on the other; both use the same codec.

### Access levels

The `access` field is enforced at the byte boundary.

| Level | Content behavior |
| --- | --- |
| `public` | Anonymous. Served with `Cache-Control: public, max-age=31536000, immutable`. |
| `private`, `tenant` | Owning tenant only. Served with `no-store`. The two levels coincide until principals within a tenant exist. |
| `grant` | Direct download refuses with 403 for everyone, owner included. Bytes flow through issued URLs only. |

Metadata, listing, and version routes require tenant authentication for every
level. Cross-tenant reads answer 404, so ids leak nothing.

### Serving behavior

Every byte route sends `ETag` (the content digest), `Accept-Ranges: bytes`,
`X-Content-Type-Options: nosniff`, and a sanitized `Content-Disposition`.
Script-capable types (HTML, SVG, XML) are forced to `attachment` so a hostile
upload cannot execute under the service origin.

`If-None-Match` answers 304 (weak comparison per RFC 9110). Single-range
`Range` requests answer 206 with `Content-Range`; unsatisfiable ranges answer
416; multi-range requests are served whole. `If-Range` with a stale validator
downgrades to the full object, so a resume across a re-upload cannot splice
two contents.

## Resumable uploads

`/v1/tus` speaks the tus 1.0.0 core protocol with the creation and
termination extensions, tenant-authenticated like every management
route. `Upload-Metadata` must carry `path` (base64 per the spec);
`content_type` and `access` are optional keys.

```
OPTIONS /v1/tus          capabilities
POST    /v1/tus          create a session (Upload-Length, Upload-Metadata)
HEAD    /v1/tus/{id}     current offset, for resuming
PATCH   /v1/tus/{id}     append at Upload-Offset (409 on a stale offset)
DELETE  /v1/tus/{id}     terminate: staged bytes go, the file fails retryably
```

The session claims its file for the whole upload, so a rival single-PUT
loses its compare-and-swap instead of interleaving. One append runs at
a time per session (a second concurrent PATCH answers 409). When the
offset reaches the declared length, the staged bytes promote to their
content address (hashed, and sealed when encryption is on) and the
file finishes through the same finalize path a single PUT uses: same
digest, same pipeline, same dedupe behavior. Abandoned sessions sweep
after `COPAL_TUS_SESSION_TTL_SECS` with their staged bytes.

## S3 gateway

With `COPAL_S3_BIND` set, a second listener speaks the S3 API in
path-style form: the bucket is the tenant id, the key is the file path.
Point any S3 client at the endpoint URL and it works against Copal
storage; uploads land in the same claim and finalize path as the REST
face, so scanning, dedupe, and versioning apply unchanged.

```
GET     /                      ListBuckets (the credential's tenant)
HEAD    /{bucket}              HeadBucket
GET     /{bucket}?list-type=2  ListObjectsV2 (prefix, delimiter,
                               max-keys, continuation-token)
PUT     /{bucket}/{key}        PutObject (create or re-upload at the path)
GET     /{bucket}/{key}        GetObject (ETag, Range, conditional requests)
HEAD    /{bucket}/{key}        HeadObject
DELETE  /{bucket}/{key}        DeleteObject (idempotent soft delete)
```

Multipart upload is supported, which matters because the aws CLI
switches to it above 8 MiB without asking:

```
POST   /{bucket}/{key}?uploads                        CreateMultipartUpload
PUT    /{bucket}/{key}?partNumber=N&uploadId=X        UploadPart
GET    /{bucket}/{key}?uploadId=X                     ListParts
POST   /{bucket}/{key}?uploadId=X                     CompleteMultipartUpload
DELETE /{bucket}/{key}?uploadId=X                     AbortMultipartUpload
```

Parts stage individually, so they may arrive in any order, in
parallel, and a re-sent part number replaces its predecessor. Each
part's ETag is its own content digest, and a completion manifest
naming a part that was never uploaded or whose ETag disagrees fails
the completion. Completion streams the parts in order through the
same put and finalize path as every other upload, so the assembled
object is content-addressed, scanned, versioned, and quota-checked
identically. No file record exists until completion, which is what
lets S3's concurrent uploads to one key coexist with Copal's one live
file per path. Abandoned sessions sweep with their parts on the
resumable-session TTL.

Requests authenticate with SigV4. Credentials are minted on the admin
surface (`POST /v1/admin/tenants/{tenant}/s3-credentials`) and are
separate from `ck1` API keys: SigV4 derives its signing key from the
shared secret, so the server must read the secret back, and Copal
stores it sealed under `COPAL_BLOB_ENCRYPTION_KEY` rather than hashed.
That is why the gateway refuses to start without the encryption key.
Any region in the credential scope is accepted; the clock-skew window
is fifteen minutes.

A signed `x-amz-content-sha256` that is a literal digest doubles as an
integrity assertion (both sides are SHA-256 of the content); a mismatch
fails the upload after hashing. Streaming signatures
(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, the aws CLI default over plain
HTTP) are accepted: the chunk framing is decoded and the seed signature
authenticates the request, though per-chunk signatures are not
re-verified. The ETag is the SHA-256 digest, quoted; clients that
compare ETags to MD5 will see every object as changed, which affects
`aws s3 sync` change detection and nothing else.

Grant-access files refuse GetObject exactly as they refuse direct REST
download: their bytes flow only through issued grants. Listings walk
the live-path index in key order; `delimiter` collapses shared segments
into `CommonPrefixes`.

## Search

```
GET /v1/search?q=terms&mode=hybrid&limit=20   search a tenant's documents
GET /v1/files/{id}/text                        one file's extracted text
```

`mode` is `lexical` (words), `semantic` (meaning), or `hybrid` (both,
fused; the default). Semantic modes need an embedding service; without
one they answer lexically and the response's `mode` field says which
retrieval actually ran, so a client can tell. An unrecognised mode is
a request error rather than a silent default.

Uploads run through a text-extraction step, and what it produces is
indexed for full-text search in the same database that holds the file
records. Text and JSON extract natively; other formats need an
extractor service to be configured (see
[operations.md](operations.md)), and a file with no extraction
answers 404 on its text rather than an empty document.

Hybrid fuses the two rankings by reciprocal rank rather than by
score. Lexical and semantic relevance are not on a comparable scale,
and this engine reports no lexical score at all, so fusing positions
is both simpler and more honest: a document near the top of either
ranking scores well, one near the top of both scores best.

Embeddings are per document, not per passage. That answers "which
documents are about this" and does not yet answer "which paragraph
says it"; passage-level chunking is the next increment.

Hits carry the file id, the character count, and an excerpt bounded
at 400 characters, in the engine's relevance order. There is no score
field: SurrealDB 3.x does not report per-row BM25 values through the
full-text scan, so a score column would be a constant dressed as
relevance. The analyzer lowercases, folds accents, and stems English,
so `inspect` finds `inspection`.

Search is tenant-scoped in the query itself rather than filtered
afterward, empty terms refuse, a re-upload replaces what matches, and
deleting a file removes it from the index so results never name
content nobody can fetch.

## Renditions

`POST /v1/files/{id}/renditions` derives an image rendition on the
flow engine. The body takes `kind` (default `thumb`), `width` and
`height` (16 to 4096, default 256), and `format` (`jpeg` or `png`).
The response is 202 with the derived record in `draft` plus the run
id; the render finishes it to `ready` through the standard claim and
complete path, so a rendition is a real file with a digest, versions,
grants, and every serving rule intact. `GET /v1/files/{id}/renditions`
lists them.

Renditions live at a deterministic path,
`{source_path}@{kind}-{w}x{h}.{format}`, and repeating a request
returns the existing record with 200 instead of a duplicate. The
derived record links its source (`derived_from`) and inherits the
source's access level. Sources must be servable images; a source past
32 MiB, one whose decoded pixels would exceed 256 MiB, or one that
fails to decode at all fails the derived record with the run
completed, and the reason lands in the run output. Re-uploading a
source does not touch existing renditions; request again to render
from the new content.

## Events and webhooks

Terminal state transitions (`file.ready`, `file.quarantined`,
`file.failed`, `file.deleted`) write outbox rows inside the database
engine, in the same transaction as the state change. Every face
produces events this way: REST, resumable sessions, the S3 gateway,
the processing pipeline, and the sweeps, none of which carry eventing
code. `GET /v1/events` lists a tenant's recent events.

```
POST   /v1/webhooks               register: { "url": ..., "events": [...] }
GET    /v1/webhooks               list endpoints (never secrets)
DELETE /v1/webhooks/{id}          deactivate
GET    /v1/webhooks/deliveries    delivery attempts and outcomes
```

The register response carries the signing secret exactly once. Every
delivery is an HTTP POST with `x-copal-event`, `x-copal-delivery`, and
`x-copal-signature: sha256=<hex>`, the HMAC of the exact body bytes
under that secret; verify it before trusting the payload. The `events`
filter takes dotted actions; empty means everything.

Endpoint URLs must resolve to public addresses; loopback, private,
link-local, and cloud metadata destinations refuse at registration and
again at delivery, and redirects are not followed. Deployments with
internal receivers opt in through configuration.

Delivery is at-least-once: dedupe on the event `id` in the body. A
non-2xx answer retries on exponential backoff (30 seconds doubling,
capped at one hour) up to eight attempts, then the delivery reads
`failed` in the deliveries listing. The dispatcher wakes on a live
query over the outbox, so delivery latency is normally milliseconds.

Webhooks require `COPAL_BLOB_ENCRYPTION_KEY`: signing needs the secret
back, and Copal stores such secrets sealed or not at all, the same
custody rule as S3 gateway credentials.

## Grants

`POST /v1/files/{id}/url` issues a capability for a servable file. The body
takes `ttl_secs` (default 900, capped at one year) and `max_uses`. The
response contains the bearer token exactly once; the store keeps its hash.

```
POST   /v1/files/{id}/upload-url   issue a write capability
GET    /v1/grants/{token}          redeem: serve the bytes, no tenant header
PUT    /v1/grants/{token}          redeem: upload bytes, no tenant header
DELETE /v1/grants/{id}             revoke, tenant-authenticated by grant id
```

A capability authorizes exactly one operation. `POST /v1/files/{id}/url`
mints a read token; `POST /v1/files/{id}/upload-url` mints a write
token for a record awaiting content, so a browser can upload straight
to Copal without holding a tenant key. Your backend creates the record
(deciding path, access level, and metadata) and hands the browser only
the URL. A read token refuses `PUT` and a write token refuses `GET`,
both with the same 404 every other refusal uses.

Upload tokens are single-use by construction and capped at a
24-hour TTL, because a write capability in an untrusted client is a
different risk from a read one. The use burns before any byte lands,
so a replay during an in-flight upload is not also authorized; a
failed upload leaves the record retryable through a freshly issued
URL. Uploads through a grant obey the same size ceiling, quota, and
processing pipeline as every other face.

Redemption refuses uniformly: malformed token, unknown grant, wrong secret,
revoked, expired, exhausted, and missing file all answer the same 404. A use
is consumed only when bytes actually serve. Refused redemptions and 304
revalidations leave the counter alone, and a revoked or expired grant stops
revalidating as well. Range requests consume per request, so media seeking
should use TTL-bounded grants without `max_uses`.

## Usage and quotas

`GET /v1/usage` reports the tenant's live files and logical bytes (the
sum of current file sizes; version history and dedupe play no part)
plus the quota ceiling when one is set. Quotas are assigned on the
admin surface. Enforcement happens before bytes move: an upload whose
declared length would cross the ceiling refuses with 409, a resumable
session past the headroom refuses at creation, and undeclared streams
are clamped to the remaining headroom in flight. Deleting files
releases their usage immediately.

## Edge tokens

`POST /v1/files/{id}/edge-url` issues a `cg2` token: a stateless HMAC
capability signed by the tenant's newest active edge key. The body
takes `ttl_secs` (default 300, ceiling 86400). `GET /v1/edge/{token}`
redeems anonymously, verifying the signature, the expiry, and the key
against the token's own claims; refusals are the same uniform 404 the
grant family uses.

The point of `cg2` is verification WITHOUT a database hop: a CDN
worker or reverse proxy holding the same edge secret validates the
signature and expiry locally and serves its cache, never touching the
origin for authorization. Edge keys are minted on the admin surface
(`POST /v1/admin/tenants/{tenant}/edge-keys`), stored sealed under the
blob master key, and the secret appears once; install that value at
the edge. Statelessness trades away per-token revocation: a token
lives until it expires or its whole key is revoked, so keep TTLs
short and use `cg1` grants where revocation or use counting matters.

## Runs

```
POST   /v1/runs               start a workflow run
GET    /v1/runs               list (status filter, keyset pagination)
GET    /v1/runs/{id}          status plus the step journal
POST   /v1/runs/{id}/retry    retry a failed run
```

Start takes `workflow`, `input`, an optional subject `file`, an optional
`idempotency_key`, and `mode`. Async mode (the default) answers 202 with the
run id. Sync mode executes in-request and answers 200 with the output, or 202
with `status: "pending"` when a worker claimed the run first. A pending
answer means poll the run; it is distinct from a completed run whose output
is null.

Retry applies to failed runs only. The journal survives, completed steps
replay from their recorded output, and a failed subject file returns to
`scanning` first so the pipeline's finalize step applies. A run superseded by
a newer upload of different content refuses with 409. See
[processing.md](processing.md) for the underlying semantics.

## GraphQL

`POST /graphql` executes queries and mutations; `GET /graphql` serves the SDL.
The schema is built at startup from the contract, so it matches
`docs/schema.graphql` exactly.

Queries follow the contract vocabulary: `files(limit, cursor, state, sort)`,
`file(id)`, `events(limit, cursor, sort)`, `event(id)`,
`runs(limit, cursor, status, sort)`, `run(id)`. Mutations map the
contract actions: `fileIssueUrl`, `fileIssueUploadUrl`,
`fileRequestRendition`, `fileRemove`, `runStart`, `runRetry`.

Field names stay as the contract declares them (`created_at`, not
`createdAt`), because the generated SDL, the OpenAPI document, and
the four clients all render from the same field list. The outbox
exposes its column as `action`: `event` is reserved in SurrealDB v3,
and the contract refuses renames that would collide there.

Depth is capped at 10 and complexity at 500, which rejects alias
amplification before any resolver runs. Auth failures surface as GraphQL
errors with `extensions.code`, keeping transport status 200.

## Errors

REST errors share one envelope:

```json
{ "error": { "kind": "bad_request", "message": "..." } }
```

GraphQL errors carry the same vocabulary in `extensions.code`. The kinds are
`bad_request`, `unauthorized`, `forbidden`, `not_found`, `conflict`,
`payload_too_large` (REST only), and `internal`. Internal failures log the
cause server-side and return no detail.

## Admin surface

Key custody and the audit trail live under `/v1/admin`, guarded by
`x-copal-admin-token` and excluded from the contract. With
`COPAL_ADMIN_BIND` set they exist only on that listener. See
[operations.md](operations.md).
