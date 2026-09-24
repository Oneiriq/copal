# API guide

Copal serves the same files on several faces:

| Face | Where | Section |
| --- | --- | --- |
| Hand-written REST | `/v1` on the main listener | most of this guide |
| Contract-generated REST | `/v1c` | [The contract-first REST face](#the-contract-first-rest-face) |
| GraphQL | `POST /graphql` (schema at `GET /graphql`) | [GraphQL](#graphql) |
| MCP for agents | `POST /mcp` | [The MCP face](#the-mcp-face) |
| tus resumable uploads | `/v1/tus` | [Resumable uploads](#resumable-uploads) |
| S3-compatible gateway | its own listener, `COPAL_S3_BIND` | [S3 gateway](#s3-gateway) |
| Operator console | `/admin/console` on the admin surface | [operations.md](operations.md) |

All of them reach the same repositories and enforce the same access
rules. The generated artifacts are the reference documents:

| Artifact | Path |
| --- | --- |
| OpenAPI 3.1 | `docs/openapi.json` |
| GraphQL SDL | `docs/schema.graphql` (also served at `GET /graphql`) |
| MCP tool manifest | `docs/mcp-tools.json` (also served by `tools/list`) |
| Rust client | `clients/client.rs` |
| TypeScript client | `clients/client.ts` |
| Python client | `clients/client.py` |
| Go client | `clients/client.go` |

The generated clients have two limits you should know first. The
contract declares header authentication, so they send only the
`x-copal-tenant` header: they work against a server in
`COPAL_AUTH_MODE=header` and cannot send API keys. They also cover the
JSON routes only, with no methods for uploading or downloading bytes.
None of them is published to a package registry yet. See
[sdks.md](sdks.md).

This guide covers what the generated documents cannot: authentication,
the byte routes, error shapes, and the behaviors shared across faces.

## The root

`GET /` names the service, its version, and the surfaces that answer
on this listener. A request asking for `text/html` gets a page with
links; anything else gets a JSON document. The console is listed only
when an admin token is configured, because without one it does not
exist.

## Route map

The tenant-facing families on the main listener. Each has its own
section below.

| Family | Routes |
| --- | --- |
| Files | `POST /v1/files`, `GET /v1/files`, `GET /v1/files/{id}`, `DELETE /v1/files/{id}` |
| Content | `PUT /v1/files/{id}/content`, `GET /v1/files/{id}/content` |
| Versions | `GET /v1/files/{id}/versions`, `GET /v1/files/{id}/versions/{n}/content` |
| Signed read URLs (`cg1` grants) | `POST /v1/files/{id}/url`, redeemed at `GET /v1/grants/{token}`, revoked at `DELETE /v1/grants/{id}` |
| Upload URLs | `POST /v1/files/{id}/upload-url`, redeemed at `PUT /v1/grants/{token}` |
| Renditions | `POST /v1/files/{id}/renditions`, `GET /v1/files/{id}/renditions`, `GET /v1/files/{id}/renditions/{kind}-{w}x{h}.{format}` |
| Transform | `POST /v1/files/{id}/transform` |
| Fetch from a URL | `POST /v1/files/fetch` |
| Extracted text | `GET /v1/files/{id}/text` |
| Search | `GET /v1/search?q=...&mode=lexical\|semantic\|hybrid` |
| Usage | `GET /v1/usage` |
| Events | `GET /v1/events`, `GET /v1/events/{id}` |
| Runs | `POST /v1/runs`, `GET /v1/runs`, `GET /v1/runs/{id}`, `POST /v1/runs/{id}/retry` |
| Resumable uploads | `/v1/tus`, `/v1/tus/{id}` |
| Webhooks | `POST /v1/webhooks`, `GET /v1/webhooks`, `DELETE /v1/webhooks/{id}`, `GET /v1/webhooks/deliveries`, `GET /v1/webhooks/{id}/deliveries` |
| Edge URLs (`cg2`) | `POST /v1/files/{id}/edge-url`, redeemed at `GET /v1/edge/{token}` |
| Health | `GET /healthz`, `GET /readyz` |

Webhooks and edge URLs exist only when `COPAL_BLOB_ENCRYPTION_KEY` is
set, because both store secrets sealed under it. Without the key those
routes answer 404. The admin surface, `/metrics`, and the console are
covered in [operations.md](operations.md).

## Authentication

Two modes, selected by `COPAL_AUTH_MODE`.

`keys` is the deployment mode. Requests carry `Authorization: Bearer
ck1.<key id>.<secret>`. The tenant comes out of the key row, so a caller
cannot name one. Every failure (absent, malformed, unknown, wrong secret,
revoked, expired) returns the same 401 with the same timing.

A key can be narrowed at minting: `scopes` from the vocabulary `read`,
`write`, `admin`, and `ttl_secs` for an expiry the engine's clock
enforces. An unscoped key holds every scope, which is what every key
minted before scoping existed does, so upgrading tightens nothing by
surprise.

The contract declares what each operation demands, and REST, GraphQL,
and MCP all enforce it: reads (listings, gets, sub-collections, search,
text, watching) need `read`; mutations need `write`; registering or
removing webhooks needs `admin`, because a webhook endpoint receives
every future event. A missing scope refuses 403 `forbidden`, naming
the scope, identically on every face. OpenAPI operations carry
`x-requires-scopes`. Header mode holds every scope, since header mode
is full trust.

Consumption is metered against one ledger on every face. The contract
declares two rate classes: `reads` at 6000 units a minute and
`mutations` at 600, where a listing costs its row limit and everything
else costs one. Exhaustion is 429 `too_many_requests`, retryable the
next minute. Switching protocols does not dodge a budget, because
every face charges the same ledger. The budget belongs to the caller:
a key's principal when it has one, otherwise the key itself. In header
mode every caller shares one bucket per class, whatever tenant it
names, because header mode has no key to tell callers apart. The
ledger is per process unless `COPAL_RATE_LEDGER=store` shares it
across a fleet.

One field is guarded: a version's `created_by` is audit data, visible
to `admin`-scoped keys and to header mode, and absent for everyone
else. REST omits the key and GraphQL renders it null, from the same
declaration, which is why the field reads as nullable in the schema
and carries `x-guard` in the OpenAPI document.

`header` is the development mode and the default until 1.0. The
`x-copal-tenant` header is trusted as the tenant identity. The server
logs a warning at startup. Any value of `COPAL_AUTH_MODE` other than
`keys` (a typo included) also selects header mode. See
[operations.md](operations.md) for key custody.

Some routes skip tenant authentication by design, because the token or
the access level is the authorization: grant redemption (`GET` and
`PUT /v1/grants/{token}`), edge-token redemption
(`GET /v1/edge/{token}`), and the content and existing renditions of
`public` files.

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
The server registers the upload pipeline, so the record lands in
`scanning` and a `post_upload` run is enqueued; the run finalizes it to
`ready` or `quarantined` (see [processing.md](processing.md)). An
application that embeds the server library without the pipeline gets
`ready` directly.

The PUT accepts an `x-copal-markers` header carrying confidentiality
markers for exactly these bytes: a JSON array of spans, each an
`access` level with a character `range` (`{"start", "end"}`, native
text only) or a text anchor (`"from"`, optional `"until"`, quoting
the document at itself). Marked passages answer retrieval only at
their own level; see the search section below. A marker can only
narrow access: a marker looser than the file's level is a 400 before
any byte moves. Markers describe content, so they ride the content
calls (this header, `file_fetch`'s `markers` field, tus
`Upload-Metadata`) and are not accepted on `POST /v1/files`. They last
as long as the version they describe: a re-upload without markers is
unmarked content.

### Listing and cursors

List endpoints take `limit`, `cursor`, a filter, and `sort`. Filters and sorts
are contract-validated: every filterable column rides an index and every sort
is reachable through an index prefix. The REST response envelope is
`{ "items": [...], "next_cursor": "..." | null }`; GraphQL names the same
field `nextCursor`.

Cursors are opaque and embed their sort direction. Replaying a cursor under a
different sort returns 400, because the alternative is silently wrong pages.
A cursor minted on one face works on the others; they share one codec.

### Access levels

The `access` field is enforced at the byte boundary.

| Level | Content behavior |
| --- | --- |
| `public` | Anonymous. Served with `Cache-Control: public, max-age=31536000, immutable`. |
| `private`, `tenant` | Owning tenant only. Served with `no-store`. Principals within a tenant exist, but the code still enforces these two levels identically. |
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
`content_type`, `access`, and `markers` (the same JSON the content
PUT's `x-copal-markers` header carries, validated at creation) are
optional keys.

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
PUT     /{bucket}/{key}        CopyObject, with x-amz-copy-source
GET     /{bucket}/{key}        GetObject (ETag, Range, conditional requests)
HEAD    /{bucket}/{key}        HeadObject
DELETE  /{bucket}/{key}        DeleteObject (idempotent soft delete)
POST    /{bucket}?delete       DeleteObjects (up to 1000 keys, Quiet mode)
```

Copy moves no bytes: storage is content-addressed, so the destination
becomes a new file record over the same blob and runs the standard
post-upload pipeline. The source must be servable and live in the
credential's own bucket; copying a specific version is refused.
Batch delete reports an absent key as deleted, matching S3. Together
with multipart these are what `mc mirror`, `rclone sync`, and
`aws s3 sync` need, so an existing bucket can migrate in with stock
tooling.

Multipart upload is supported, which matters because the aws CLI
switches to it above 8 MiB without asking:

```
POST   /{bucket}/{key}?uploads                        CreateMultipartUpload
PUT    /{bucket}/{key}?partNumber=N&uploadId=X        UploadPart
GET    /{bucket}/{key}?uploadId=X                     ListParts
POST   /{bucket}/{key}?uploadId=X                     CompleteMultipartUpload
DELETE /{bucket}/{key}?uploadId=X                     AbortMultipartUpload
GET    /{bucket}?uploads                              ListMultipartUploads
```

Every part but the last must be at least 5 MiB, as S3 requires;
completion refuses with `EntityTooSmall` otherwise, because a client
written against S3 expects that rejection. A single-part upload is
all last part and carries no minimum.

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

### Where the compatibility stops

The gateway covers the object plane so stock tooling can move bytes in
and out. It is a door onto Copal storage, and the operations above are
the whole of it. Everything else an S3 client might reach for is
absent, and the reason differs by case.

Four are absent because Copal already answers the question somewhere
else, and a second answer on the S3 face could disagree with the
first:

| S3 feature | Where Copal answers it |
| --- | --- |
| Versioning API (`?versions`, `versionId`) | A file's version list: `GET /v1/files/{id}/versions`. The gateway serves the live version of a key and refuses to copy a specific one. |
| Object Lock, legal hold | Retention policy and holds, tenant-wide and admin-set. See [retention.md](retention.md). |
| Bucket and object ACLs | The file's access level, and grants for anything narrower. A bucket is a tenant, so a bucket-wide ACL would be a tenant-wide one. |
| Lifecycle rules (`?lifecycle`) | Retention handles expiry. Moving cold bytes to cheaper storage is operator-set storage tiering on the admin surface; see the storage tiers section of [operations.md](operations.md). |

The rest are absent because nothing has needed them: object tagging,
`?cors`, `?policy`, `?encryption`, `?replication`, `?website`,
`?accelerate`, `?logging`, `?notification`, requester-pays, storage
classes beyond the default, presigned `POST` policy uploads, and
bucket creation or deletion (a bucket is a tenant, minted on the admin
surface).

Asking for any of them returns `501 NotImplemented` naming the
subresource. That refusal is load-bearing. A query key like `?tagging`
changes what a request means, and without the refusal these fall
through to the handler for the bare path: `GET /{bucket}?lifecycle`
would answer with an object listing under a `200`, and
`PUT /{bucket}/{key}?tagging` would write the tagging document into
the object as its content, destroying what the client had just
uploaded.

`GET /{bucket}?versioning` is the one exception, because it has a true
answer and clients probe it before transferring. It returns an empty
`VersioningConfiguration`: the S3 face exposes no versionIds, and
`versionId` on a request is refused like the rest.

The practical read: `mc`, `rclone`, and `aws s3` move data correctly,
including `sync` and `mirror`. A tool that manages bucket
configuration rather than objects is pointed at the wrong face, and
the admin surface is the right one.

## Ingestion, on the contract

Creation is a declared action: `POST /v1/files` in the OpenAPI
document, `fileCreate` on GraphQL, `file_create` as an MCP tool, all
from one declaration, all through the same dispatcher. The byte
paths are declared content faces, so `PUT` and `GET
/v1/files/{id}/content` appear in the OpenAPI document with the
differ governing their presence. An agent's ingest loop is three
calls: `file_create`, `file_issue_upload_url`, and an HTTP `PUT` of
the bytes to the grant URL.

When the bytes live behind a URL, one call replaces all three:
`POST /v1/files/fetch` (`file_fetch` on the other faces) takes `url`
and `path` plus the usual `content_type`, `access`, `metadata`,
`idempotency_key`, and optional `markers`, creates the record, and
the server pulls the bytes itself. Fetched content walks the standard
pipeline (sniff, policy, scan, extract, embed, finalize), so a fetched file is
indistinguishable from an uploaded one by the time it serves. The
URL is tenant-supplied, so the outbound policy applies (private
address space refuses unless `COPAL_FETCH_ALLOW_PRIVATE_TARGETS` is
set), the upload ceiling caps the body mid-stream, a 4xx from the
source fails the record with the reason, and a 5xx retries on the
flow engine's budget. When the caller declares no `content_type`,
the source's served type stands.

## The contract-first REST face

`/v1c` mirrors the JSON surface of `/v1` through Kayak's runtime
REST router. The route table derives from the contract, and every
request runs the same dispatcher chain the GraphQL and MCP faces
use, so `/v1c/files` exists because the declaration says so and
refuses the way every declared face refuses. The hand-written `/v1`
routes stay canonical. They carry REST-specific semantics (201 and
202 on creation and derivation, byte streaming, headers) that the
generated face does not restate, and `/v1c` has no byte routes. A
parity test holds the two faces to the same answers on listings,
gets, actions, and queries. The plan is for one face generated from
the declaration; `/v1c` is that face, running beside the original
until it can replace it.

## The MCP face

`POST /mcp` speaks MCP over JSON-RPC 2.0 with the same bearer keys
every other face takes. `tools/list` serves the manifest generated
from the contract ([mcp-tools.json](mcp-tools.json), a checked-in
artifact under the same drift gate as the OpenAPI document), and
`tools/call` dispatches through the same chain as GraphQL: scopes,
budgets, field guards, and caller-bound engine sessions enforce
identically whether the caller is a script or an agent's tool call.
Point an agent at `/mcp` with a key; what it may do is what the key
may do, and every tool carries its required scopes and rate class as
annotations the agent can read before calling.

## Search depth

Search takes `prefix` (keep results whose file path starts with it),
`content_type`, and a `cursor` from the previous page's
`next_cursor`. Filters apply at the engine on both retrieval legs,
so a filtered search never ranks passages it must discard. The
cursor continues a ranking and is best-effort: rankings shift as
content changes, so a page boundary can repeat or skip a result that
moved between requests. Depth caps at the over-fetch envelope both
legs already pay for.

Embeddings heal themselves: when the configured model changes, a
background pass re-embeds every passage carrying old geometry, batch
by batch, until nothing stale remains. The vector index itself
rebuilds through the schema diff when the dimension changes.

## The change feed

`GET /v1/events` and the GraphQL `events` listing are one cursor
surface over everything that happened to a tenant's files. The
default order is newest first for dashboards; `order=asc` with a
cursor replays forward, which is how an indexer that was down
resumes from where it stopped instead of re-listing the world:

```
GET /v1/events?order=asc&limit=100
GET /v1/events?order=asc&limit=100&cursor=<next_cursor from the last page>
```

Compliance actions ride the same feed: `version.retention_set`,
`version.retention_cleared`, `version.hold_applied`,
`version.hold_released`, and `version.pruned` appear beside the
lifecycle events, so an indexer or SIEM replaying the feed sees
exactly the events compliance watches. Webhooks fan them out under
the same action filters.

The cursor is opaque; hand back exactly what the previous page gave.
Pages never repeat an event, a saved cursor sees every later event
at least once, and `action=file.ready` narrows the feed to one verb
with the cursor still honored. The `events` subscription is the live
half of the same surface: catch up with the cursor, then watch.

## Conditional writes

Content writes take the standard preconditions on the REST and S3
faces, and the ETag is the content digest, so the conditions say
exactly what they mean:

- `If-None-Match: *` creates and never replaces: 412 when the key
  already holds content. Two agents racing to create the same key
  resolve at the engine; exactly one wins.
- `If-Match: <digest>` replaces only the content the caller believes
  is current: 412 on a stale belief. Read, decide, write-if-unchanged
  is a compare-and-swap.

`PUT /v1/files/{id}/content`, S3 `PutObject`, and S3
`CompleteMultipartUpload` all honor them. With neither header the
write is unconditional; both present is a 400, because the pair is
contradictory.

## Search

Search and extracted text are contract queries: declared parameters,
declared scopes, declared budget, rendered into `docs/openapi.json`,
`docs/schema.graphql`, and the MCP manifest, and served on every
contract face through one implementation. `GET /v1/search` and the GraphQL `search` field
answer the same value; the same holds for `GET /v1/files/{id}/text`
and `fileText`.

```
GET /v1/search?q=terms&mode=hybrid&limit=20   search a tenant's documents
GET /v1/files/{id}/text                        one file's extracted text
```

`mode` is `lexical` (words), `semantic` (meaning), or `hybrid` (both,
fused; the default). Lexical relevance is BM25, scored by Copal: the
database index decides which passages match and returns them in
insertion order, so ranking them is Copal's job. It scores a bounded
window of matches, which means a query matching more than a few
hundred passages ranks that window rather than every match. Semantic modes need an embedding service; without
one they answer lexically and the response's `mode` field says which
retrieval actually ran, so a client can tell. An unrecognized mode is
a request error rather than a silent default.

Uploads run through a text-extraction step, and what it produces is
indexed for full-text search in the same database that holds the file
records. Text and JSON extract natively; other formats need an
extractor service to be configured (see
[operations.md](operations.md)), and a file with no extraction
answers 404 on its text rather than an empty document.

Retrieval works over passages. Extraction splits
text at boundaries a reader would recognize (blank lines, then
sentence ends) into overlapping windows, and each passage is indexed
and embedded on its own. A hit therefore names the passage that
matched, and its excerpt is the window around the matching words
rather than the opening of a long document. `GET /v1/files/{id}/text`
still returns the whole text in one piece.

Retrieval withholds what the download path withholds. A grant-only
file serves its bytes exclusively through issued URLs, and an excerpt
of its text is those bytes, so it answers no search on either leg and
appears in no facet count. Quarantined records are withheld for the
same reason `servable_content` refuses them, and deleted records have
their text purged at deletion. The levels a read-scoped caller of the
owning tenant can download, which is `public`, `private`, and
`tenant`, are the levels that answer searches.

The same rule holds per passage. An upload's markers give individual
spans their own, narrower level; a chunk overlapping any marked span
inherits it, and a `grant`-marked chunk is never a search candidate:
no snippet, no rank, no facet contribution, no entry into the rerank
window, on either retrieval leg. A file whose only matching passages
are withheld does not surface at all, while its open passages keep
answering their own questions. `GET /v1/files/{id}/text` elides the
marked spans instead of refusing the document: the response's
`withheld` counts the elided regions, `chars` counts the served
text, and no positions are disclosed, because the length of a secret
is part of the secret. A marker that cannot be located (a typo'd
anchor, a range past the extraction ceiling or on extractor-produced
text) restricts the whole file, with the reason under
`metadata.processing`. All four levels are accepted on the wire.
`grant` is the operative restriction: principals exist, but the read
path does not yet tell them apart, so `public`, `private`, and
`tenant` all admit the same tenant-scoped, read-scoped callers.

Semantic retrieval applies a relevance floor, so a query about
something nobody stored returns nothing rather than the least-distant
passage in the corpus. The floor is a cosine distance
(`COPAL_MAX_SEMANTIC_DISTANCE`, default 0.65, where 0 is identical
and 1 is unrelated); raise it for looser recall, lower it for
stricter.

Hybrid fuses the two rankings by reciprocal rank, using positions
instead of scores. Lexical and semantic relevance are not on a
comparable scale, and this engine reports no lexical score at all. A
document near the top of either ranking scores well, and one near the
top of both scores best.

Hits carry the file id, the passage that matched, an excerpt bounded
at 400 characters, and `matches`, in the engine's relevance order.
There is no score field: SurrealDB 3.x does not report per-row BM25
values through the full-text scan, so a score column would be a
constant dressed as relevance. The analyzer lowercases, folds accents,
and stems English, so `inspect` finds `inspection`.

`matches` locates the query inside the excerpt: a list of
`[start, end)` pairs counted in characters of the excerpt string, so a
caller can mark them without searching the text again. Searching it
again is what a caller cannot reliably do, and the same reason governs
how the excerpt window is chosen. Both use the analyzer that decided
the match rather than the words the caller typed. Ask for
`inspecting`, match a passage that says `inspection`, and a literal
search finds nothing in it:

```json
{
  "mode": "hybrid",
  "items": [
    {
      "file": "file:01J...",
      "passage": 3,
      "excerpt": "...Routine inspection of the hull followed...",
      "matches": [[10, 20]]
    }
  ],
  "next_cursor": null
}
```

Where a passage has several matches, the window lands on the densest
cluster: a document that mentions a term once at the top and four
times together lower down is about the latter. A passage with nothing
to mark carries an empty list rather than omitting the field.

### Facets

`facets` counts the match set by one or more file fields, comma
separated. Two fields answer: `content_type` and `access`.

```
GET /v1/search?q=inspecting&facets=content_type,access
```

```json
"facets": {
  "content_type": [
    {"value": "text/plain", "files": 2},
    {"value": "text/markdown", "files": 1}
  ],
  "access": [{"value": "private", "files": 3}]
}
```

Counts are documents, and they are exact over the whole match set.
The engine counts rows, and a row is a passage, so a document matching
in six places would otherwise count six times; the count measures the
distinct set of files instead. The ranked page comes from a rescore
window bounded at 500 candidates, so a count taken from it would mean
"of the first five hundred". The facet query carries no limit.

Filters apply to the counts, so `prefix` and `content_type` narrow
what is counted the same way they narrow what is returned. Faceting on
`content_type` while also filtering by it therefore yields one bucket.

Asking for no facets runs no facet query and returns no `facets` key,
so a search that does not want counts does not pay for them. An
unsupported field is a `400` naming the fields that work, because an
empty list would read as "nothing matched".

### Reranking

Retrieval decides which passages carry the query's words or sit near
its vector. Neither reads a passage against the question. A reranker
does, one pair at a time, which is why it runs over a shortlist.

With `COPAL_RERANK_ADDR` set (see
[operations.md](operations.md) and
[examples/rerank](../examples/rerank/README.md)), search hands the top
`COPAL_RERANK_DEPTH` fused candidates to the service and orders them
by what comes back. The response says how far it reached:

```json
{"mode": "hybrid", "reranked": 50, "items": [...]}
```

`reranked` is absent when no service is configured, and also when
fewer than two documents matched, since there is no order to change.
Whatever sits below the depth keeps its fused order, so a deep page
reads as a reranked head followed by a fused remainder, and no
document is lost either way.

A reranker improves an answer that already exists, so losing one costs
relevance and leaves the search standing. An unreachable or refusing
service logs a warning naming the cause and reports `reranked: 0`.
Semantic retrieval degrades to lexical on the same reasoning.

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

A rendition also serves straight from its URL:
`GET /v1/files/{id}/renditions/{kind}-{width}x{height}.{format}`. An
existing rendition serves under its own access level, so public
thumbnails stay anonymous and hard-cacheable. A miss derives inline
before serving: that is a write, so it needs the write scope and the
owning tenant, and anonymous callers get 404 for renditions nobody
has derived. Racing first requests converge on one record through
the path's uniqueness.

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
code. `GET /v1/events` lists a tenant's recent events, and
`?action=file.ready` narrows the feed to one verb. To watch instead of
poll, subscribe over GraphQL (below).

```
POST   /v1/webhooks                    register: { "url": ..., "events": [...] }
GET    /v1/webhooks                    list endpoints (never secrets)
DELETE /v1/webhooks/{id}               deactivate
GET    /v1/webhooks/deliveries         delivery attempts and outcomes
GET    /v1/webhooks/{id}/deliveries    one endpoint's delivery attempts
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

Webhooks require `COPAL_BLOB_ENCRYPTION_KEY`. Signing needs the secret
back, and Copal stores such secrets only in sealed form, the same
custody rule as S3 gateway credentials. Without the key, the webhook
routes and the delivery dispatcher do not exist.

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

The point of `cg2` is verification with no database lookup. A CDN
worker or reverse proxy holding the same edge secret validates the
signature and expiry locally and serves its cache, and never asks the
origin for authorization. Edge keys are minted on the admin surface
(`POST /v1/admin/tenants/{tenant}/edge-keys`) and stored sealed under
the blob master key. The secret appears once; install that value at
the edge. Edge URLs and edge keys exist only when
`COPAL_BLOB_ENCRYPTION_KEY` is set.

Statelessness gives up per-token revocation: a token lives until it
expires or its whole key is revoked. Keep TTLs short, and use `cg1`
grants where revocation or use counting matters.

## Runs

```
POST   /v1/runs               start a workflow run
GET    /v1/runs               list (status filter, keyset pagination)
GET    /v1/runs/{id}          status plus the step journal
POST   /v1/runs/{id}/retry    retry a failed run
```

Start takes `workflow`, `input`, an optional subject `file`, an optional
`idempotency_key`, and `mode`. Async mode (the default) answers 202 with the
run id, or 200 with the existing run id when the idempotency key replays.
Sync mode executes in-request and answers 200 with the output, or 202
with `status: "pending"` when a worker claimed the run first. A pending
answer means poll the run; it is distinct from a completed run whose output
is null.

Retry applies to failed runs only. The journal survives, completed steps
replay from their recorded output, and a failed subject file returns to
`scanning` first so the pipeline's finalize step applies. A run superseded by
a newer upload of different content refuses with 409. See
[processing.md](processing.md) for the underlying semantics.

## Archive-cold content

Operators may place cold bytes on archive-class storage. When they
do, a byte read that finds its content archived answers 202 with a
body naming the recall run. Poll the run at `/v1/runs/{id}` and retry
the request when it completes; a `Retry-After` header suggests when.
The 202 itself started the recall, so retrying is harmless, nothing is
charged twice, and a counted grant is not consumed by it. On the S3
face the same state answers `403 InvalidObjectState`, and
`RestoreObject` starts the recall the way it does on AWS.

Metadata is never archived. Listings, file metadata, versions, and
events answer at full speed regardless of where bytes live, and an
archive-cold file remains fully searchable and its excerpts keep
serving; only following the hit to the bytes meets the 202.

## GraphQL

`POST /graphql` executes queries, mutations, and subscriptions;
`GET /graphql` serves the SDL.
The schema is built at startup from the contract, so it matches
`docs/schema.graphql` exactly.

A file carries its history and a webhook its delivery attempts, as fields
on the parent: `file(id) { versions(limit, cursor) { items { number } } }`
and `webhook(id) { deliveries(limit, cursor, state) { items { state } } }`.
Both are `GET /v1/files/{id}/versions` and
`GET /v1/webhooks/{id}/deliveries` on REST, and a method on each generated
client.

Queries follow the contract vocabulary: `files(limit, cursor, state, sort)`,
`file(id)`, `events(limit, cursor, action, sort)`, `event(id)`,
`webhooks(limit, cursor, sort)`, `webhook(id)`,
`runs(limit, cursor, status, sort)`, `run(id)`,
`search(q, mode, limit, prefix, contentType, cursor, facets)`, and
`fileText(id)`. Mutations map the contract actions: `fileCreate`,
`fileIssueUrl`, `fileIssueUploadUrl`, `fileIssueEdgeUrl`,
`fileRequestRendition`, `fileTransform`, `fileFetch`, `fileRemove`,
`webhookRegister`, `webhookRemove`, `runStart`, `runRetry`.

One subscription is served, `eventChanged(action)`, which delivers
outbox rows as the engine writes them. It takes the same `action`
filter the listing takes, and the engine applies both that filter and
the tenant scope before a row is delivered, so narrowing happens in the
database rather than in application code.

Subscriptions ride `POST /graphql` with `Accept: text/event-stream`,
answered as graphql-sse in distinct connections mode: one `next` event
per payload, then `complete`. That keeps one route and one
authenticator for every operation. A WebSocket transport would need a
second one, since a browser cannot set headers on a WebSocket
handshake and graphql-ws carries credentials in its own init payload.

A subscription is authorized when it opens and lives at most
`COPAL_SUBSCRIPTION_MAX_SECS` (default 900). The server then ends it
with a normal completion and the client re-subscribes, which runs the
full authentication path again: that is how key revocation and expiry
reach streams already running. One caller holds at most eight
subscriptions at once; over the ceiling refuses with
`too_many_requests` until one closes.

Usage and quotas stay REST-only: they report a number rather than a
collection of rows, which is not a shape this contract expresses.

Record fields keep the names the contract declares, so a file's
creation time is `created_at` on GraphQL as it is on REST. Arguments
and the page cursor follow GraphQL convention instead: arguments are
camelCase (`contentType`, `idempotencyKey`, `ttlSecs`), and a page's
cursor field is `nextCursor` where REST says `next_cursor`. The outbox
exposes its column as `action`: `event` is reserved in SurrealDB v3,
and the contract refuses renames that would collide there.

Depth is capped at 10 and complexity at 500, which rejects alias
amplification before any resolver runs. A deployment can lock the
face further with `COPAL_PERSISTED_OPERATIONS`, a JSON file of sha256
hash to document: only listed operations run, named by hash in the
Apollo `persistedQuery` shape or sent whole, and anything else
refuses before parsing. The file is verified at startup, so an
allowlist that lies about a hash refuses to boot. Auth failures surface as GraphQL
errors with `extensions.code`, keeping transport status 200.

## Errors

REST errors share one envelope:

```json
{ "error": { "kind": "bad_request", "message": "..." } }
```

GraphQL errors carry the same vocabulary in `extensions.code`. The kinds:

| Kind | REST status | Notes |
| --- | --- | --- |
| `bad_request` | 400 | |
| `unauthorized` | 401 | |
| `forbidden` | 403 | |
| `not_found` | 404 | |
| `conflict` | 409 | |
| `precondition_failed` | 412 | A failed `If-Match` or `If-None-Match`. GraphQL reports it as `conflict`. |
| `payload_too_large` | 413 | REST only. |
| `too_many_requests` | 429 | A rate class is exhausted, or a caller holds too many subscriptions. |
| `internal` | 500 | The cause is logged server-side; the response carries no detail. |

The S3 gateway answers in S3's own XML error vocabulary instead.

## Admin surface

The operator surface lives under `/v1/admin`, guarded by
`x-copal-admin-token` and outside the contract: API keys, principals,
S3 credentials, edge keys, quotas, storage residencies, retention and
holds, tiering, and the audit trail. `/metrics` and the console ride
the same surface. With `COPAL_ADMIN_BIND` set, all of it exists only on
that listener. See [operations.md](operations.md).
