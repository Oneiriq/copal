# Operations

## Configuration

Every value comes from the environment. Defaults target local development.

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_BIND` | `127.0.0.1:8080` | Tenant-facing listener. |
| `COPAL_ADMIN_BIND` | unset | Separate listener for the admin surface. When set, admin routes exist only there. |
| `COPAL_S3_BIND` | unset | Listener for the S3-compatible gateway (path-style bucket routes at its root). Requires `COPAL_BLOB_ENCRYPTION_KEY`; startup refuses otherwise. |
| `COPAL_DB_URL` | `ws://127.0.0.1:8000` | SurrealDB endpoint. |
| `COPAL_DB_NS` / `COPAL_DB_NAME` | `copal` / `copal` | Namespace and database. |
| `COPAL_DB_USER` / `COPAL_DB_PASS` | `root` / `root` | Database credentials. Use a scoped user in deployments. |
| `COPAL_BLOB_ROOT` | `./data/blobs` | Filesystem blob store root. |
| `COPAL_BLOB_ENCRYPTION_KEY` | unset | 64-hex master key enabling encryption at rest. New objects seal (chunked AES-256-GCM, per-object derived keys); existing plaintext objects keep serving. Digests stay plaintext digests, so addressing and dedupe are unchanged. |
| `COPAL_MAX_UPLOAD_BYTES` | `1073741824` | Upload ceiling, enforced in-stream (413 past it). |
| `COPAL_UPLOAD_LEASE_SECS` | `900` | Upload claim lease. Expired claims are stealable and reaped. |
| `COPAL_AUTH_MODE` | `header` | `header` (development) or `keys`. The default flips to `keys` at 1.0. |
| `COPAL_ADMIN_TOKEN` | unset | Operator token. Unset disables the admin surface entirely. |
| `COPAL_ADMIN_TOKEN_PREVIOUS` | unset | Outgoing operator token during a rotation window; accepted beside the current one, then unset. |
| `COPAL_BLOCKED_EXTENSIONS` | built-in list | Comma-separated denylist for the upload pipeline. |
| `COPAL_ENFORCE_TYPE_MATCH` | `false` | Quarantine declared-type lies instead of annotating them. |
| `COPAL_SWEEP_INTERVAL_SECS` | `60` | Maintenance cadence. |
| `COPAL_STAGING_TTL_SECS` | `86400` | Staging entries older than this are deleted. |
| `COPAL_GC_GRACE_SECS` | `86400` | A blob must stay unreferenced this long before its bytes go. |
| `COPAL_GC_BATCH` | `1000` | Blob rows per GC batch query. The pass loops batches until the population is covered. |
| `COPAL_SCAN_STALE_SECS` | `3600` | Files stuck in `scanning` with no live run are failed after this age. |
| `COPAL_REQUEST_TIMEOUT_SECS` | `30` | Deadline for ordinary requests (408 past it). |
| `COPAL_TRANSFER_TIMEOUT_SECS` | `3600` | Deadline for the byte routes; ends slow-drip connections. |
| `COPAL_TUS_SESSION_TTL_SECS` | `86400` | Resumable-upload and S3 multipart sessions older than this are swept with their staged bytes. |
| `COPAL_CORS_ORIGINS` | unset | Comma-separated browser-origin allowlist. Unset attaches no CORS layer at all. |
| `COPAL_CLAMAV_ADDR` | unset | `host:port` of a clamd instance. Set adds a malware scan to the upload pipeline AND withholds content until a scan clears it. |
| `COPAL_EXTRACTOR_ADDR` | unset | `host:port` (or full URL) of a text extractor speaking Apache Tika's shape: `PUT /tika` with the bytes, `Accept: text/plain`, text in the response body. Text and JSON extract natively without it. |
| `COPAL_EMBEDDING_ADDR` | unset | Embedding service speaking the OpenAI `/v1/embeddings` shape (Ollama, llama.cpp, text-embeddings-inference, LocalAI, vLLM, OpenAI). Set enables semantic and hybrid search. |
| `COPAL_EMBEDDING_MODEL` | `nomic-embed-text` | Model name passed to that service. |
| `COPAL_EMBEDDING_DIMENSION` | `768` | Width the model emits. The vector index is defined at this dimension at startup, so it must match the model. |
| `COPAL_SUBSCRIPTION_MAX_SECS` | 900 | Lifetime of one subscription; re-subscribing re-authenticates. |
| `COPAL_RATE_LEDGER` | memory | `store` shares one consumption budget across a fleet. |
| `COPAL_ENGINE_ACCESS_KEY` | unset | HS256 key for the engine's caller access method. Set, the store defines record access, every table's `PERMISSIONS` become enforceable per caller, and `Store::caller` sessions filter engine side. Rotation replaces the method on next boot. |
| `COPAL_ENGINE_SESSIONS` | `off` | `on` runs the adopted request handlers' repository calls through caller-bound engine sessions, so `PERMISSIONS` filter live traffic. Requires the access key; boot refuses otherwise. Two engine round trips per request to open the session. |
| `COPAL_SESSION_CACHE_SECS` | `60` | How long an open caller session may be reused. Zero pays the two-round-trip open on every request. |
| `COPAL_SESSION_CACHE_SIZE` | `256` | How many caller sessions to hold. Zero disables reuse. |

| `COPAL_PERSISTED_OPERATIONS` | unset | JSON file of sha256 to document; set, GraphQL runs listed operations only. |
| `COPAL_MAX_SEMANTIC_DISTANCE` | `0.65` | Cosine distance beyond which a passage is not a semantic match (0 identical, 1 unrelated). Without a floor, nearest-neighbour search answers every query with its nearest results however far away they are. |
| `COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS` | `false` | Permit webhook endpoints resolving to private, loopback, or link-local addresses. Off by default: tenant-supplied URLs pointing inside the deployment are server-side request forgery. Turn on only when receivers are genuinely internal and tenants are trusted. |
| `COPAL_RESIDENCIES` | unset | JSON map of named storage residencies beyond `local`, e.g. `{"eu": {"scheme": "s3", "bucket": "...", "endpoint": "...", "region": "...", "access_key_id": "...", "secret_access_key": "...", "encryption_key": "<64 hex>"}}`. Filesystem residencies use `{"scheme": "fs", "root": "...", "encryption_key": "<64 hex>"}`. `encryption_key` is optional and seals that residency's objects under its own key instead of the master. Names are lowercase alphanumeric. |


## Upgrades and the schema

Boot reconciles the database against the code. The store introspects
what the engine actually holds (`INFO FOR DB` for modes, permissions,
analyzers, and access methods; `INFO FOR TABLE` per table for fields,
indexes, and events), diffs it against the release's schema, and
applies only the differences as `OVERWRITE` statements, which create
absent objects and replace present definitions while leaving rows
untouched. A database that matches runs no DDL at all. A database
created by an older release receives every later definition,
`PERMISSIONS` included, on its first boot under newer code.

Definitions the database holds that the code no longer declares are
logged and left alone: removal is an operator decision, made with
`REMOVE` statements when the definition is truly retired. Concurrent
replica boots race benignly, since identical `OVERWRITE` statements
are idempotent.

## Key custody

Keys are minted, listed, and revoked through the admin surface, guarded by
`x-copal-admin-token` compared in constant time.

```
POST   /v1/admin/tenants/{tenant}/keys            body: { "name": "ci" }
GET    /v1/admin/tenants/{tenant}/keys
DELETE /v1/admin/tenants/{tenant}/keys/{key_id}
GET    /v1/admin/tenants/{tenant}/audit
```

The mint response carries the bearer token once. Store it; the server keeps
the hash. Key names are unique per tenant, so rotation reads as revoke old,
mint new under the next name. Listings never include hash material.

S3 gateway credentials live beside the keys and follow a different
custody rule, forced by the protocol: SigV4 verification derives its
signing key from the shared secret, so the server must read the secret
back. The secret is stored sealed under `COPAL_BLOB_ENCRYPTION_KEY`
(never hashed, never clear) and appears once in the mint response.
These credentials never reuse `ck1` material and revoke independently.

```
POST   /v1/admin/tenants/{tenant}/s3-credentials
GET    /v1/admin/tenants/{tenant}/s3-credentials
DELETE /v1/admin/tenants/{tenant}/s3-credentials/{access_key_id}
```

Webhook endpoint secrets follow the same rule: stored sealed under the
master key, returned once at registration, read back only to sign
deliveries. The webhook surface and its dispatcher exist only when the
key is configured.

Rotating the blob master key invalidates sealed credentials, webhook
secrets, and edge keys (they open under the key that sealed them);
re-mint them as part of any master-key rotation.

Edge keys (`cg2` signing secrets) complete the sealed-secret set:
minted per tenant, sealed under the master key, returned once for the
operator to install at the CDN edge, revocable as a whole (which ends
every token the key signed).

Custody and lifecycle actions land in the audit trail: `key.minted`,
`key.revoked`, `s3credential.minted`, `s3credential.revoked`,
`edgekey.minted`, `edgekey.revoked`, `edge.issued`,
`webhook.registered`, `webhook.removed`, `grant.issued`,
`grant.upload_issued`, `grant.revoked`, `tenant.quota_set`,
`tenant.quota_cleared`, `tenant.storage_assigned`, `file.removed`. When
the proxy forwards a client origin (`x-forwarded-for`), the first hop
is recorded on the row for forensics; it plays no part in
authorization. Rotate the operator token with zero downtime by moving
the old value to `COPAL_ADMIN_TOKEN_PREVIOUS`, deploying the new one,
and unsetting the previous once callers have moved. Audit rows
are immutable inside the engine; an UPDATE or DELETE against one aborts in
SurrealDB itself.

## Malware scanning

With `COPAL_CLAMAV_ADDR` set, the upload pipeline scans content
through clamd (the INSTREAM protocol, spoken directly) between the
cheap checks and the transition that would make bytes servable. A
detection quarantines the file with the matched signature recorded in
`metadata.processing.verdict_reason`; quarantine is terminal until
delete, and neither content nor grants serve from it.

Enabling a scanner also changes what serving means. Copal otherwise
serves on the digest alone, so bytes are readable while their
pipeline runs; with scanning on, content serves only once a scan has
cleared THAT content. The scan records which digest it cleared, and
serving compares it against the digest the record currently points
at, which is a question about content rather than lifecycle.

Reading the record's state instead would get two cases wrong. A run
that failed leaves unscanned bytes in place, which a "withhold only
while scanning" rule would serve. And a re-upload in flight still
points at the previous digest until it completes, which a
"`ready` only" rule would withhold even though that content passed
its own scan; Copal's documented behavior is that the previous
version keeps serving through a re-upload, and it still does.

A scanner that cannot be reached is an error, never a pass: the
activity fails, the run retries, and the file never reaches `ready`.
Without `COPAL_CLAMAV_ADDR` nothing changes, and records say
`metadata.processing.scanned: false` rather than implying a clean
verdict nobody rendered.

## Text extraction and search

The upload pipeline extracts text after the malware scan and before
the transition that publishes a file, so text is pulled from content
a scanner has already judged, and the file becomes readable and
searchable in one step. Extracted text lands in its own table with a
BM25 index, which is what makes stored files searchable without a
second datastore to keep in sync.

Copal decodes text and JSON itself and carries no document parsers.
PDF, Office formats, and OCR are large, fast-moving, and historically
a rich source of memory-safety bugs, which is a poor trade inside a
service holding other people's files; `COPAL_EXTRACTOR_ADDR` points
at something that does that work instead. A configured extractor that
cannot be reached is an error, so the run retries rather than
recording the document as empty. Extractions are capped at a million
characters and the record says when it truncated.

Extracted text follows its content: a re-upload replaces it, a delete
removes it, and each row records which digest it came from.

With an embedding service configured, the pipeline also embeds that
text and stores the vector beside it, indexed with HNSW at the
model's dimension. Copal runs no models: inference means weights, a
runtime, and hardware assumptions that have no business inside a
storage service, and anything speaking the OpenAI embeddings shape
can serve it. Changing models means changing
`COPAL_EMBEDDING_DIMENSION` to match and re-embedding existing
documents; a vector of the wrong width is refused by the index.

Semantic queries run against the HNSW index with an exploration
factor, which is the form that uses it: SurrealDB 3.x also accepts a
KNN operator naming a distance metric, but that one scans the whole
table and leaves the index unused.

Text is split into overlapping passages and each one is embedded
separately, so a long document is retrievable by whichever part
answers the question rather than by its average meaning. Passages are
capped per document, and a re-extraction replaces all of them at
once. Attaching an embedding is guarded on the digest, so a vector
computed for content that has since been replaced never attaches to
the new passages, and a retried run embeds only what still lacks a
vector rather than paying for the whole document again.

## Outbound request policy

Webhook delivery is the one place the server fetches a
tenant-supplied URL, which makes it the server-side request forgery
surface. Every endpoint URL resolves at registration and again at
delivery (DNS answers change in between), and any answer that is
loopback, private, link-local, carrier-grade NAT, or the cloud
metadata address refuses. The delivery client follows no redirects,
because a redirect reaches an address the guard never checked.
`COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS` disables the check for
deployments whose receivers are internal by design; the server logs a
warning at startup when it is on.

Rendition decoding is bounded twice: sources over 32 MiB are refused
before decoding, and the decoder itself carries a 256 MiB allocation
ceiling, so a small compressed file describing an enormous canvas
fails the derived record instead of exhausting the host.

## Storage residencies

A residency is a named backend (filesystem root or S3-compatible
bucket) configured through `COPAL_RESIDENCIES`. `local` always exists.
Pin a tenant's new content with the admin surface:

```
PUT /v1/admin/tenants/{tenant}/storage    body: { "residency": "eu" }
GET /v1/admin/tenants/{tenant}/storage
```

Assignment affects new uploads only. Blob rows record the residency
their content landed in, and every read path resolves the backend from
the row, so reassigning a tenant never strands existing content: old
files keep serving from where they live, new files land in the new
residency, and garbage collection removes bytes from the backend that
holds them. Deduplication is scoped per residency by design; the same
content pinned to two residencies stores twice, which is what
residency means. The blob master key, when configured, seals content
in every residency. Resumable-upload staging always lives on the
local backend; completed sessions stream into the tenant's residency
at promotion.

A residency may carry its own `encryption_key`, which seals its
objects instead of the master key and is what gives a tenant data
nobody else's key can open. Keys attach to residencies rather than to
tenants because a key boundary scopes deduplication exactly as a
backend boundary does, and residencies already carry that scope: a
tenant that needs its own key gets its own residency, with its own
bucket and credentials to match. Losing a residency key loses that
residency's objects; the master key cannot open them.

Every instance that runs sweeps must configure the residencies whose
rows it may collect; a row whose residency is unknown to the instance
is collected in the database and its bytes logged as unreachable.

## Quotas

```
PUT    /v1/admin/tenants/{tenant}/quota    body: { "max_bytes": 10737418240 }
GET    /v1/admin/tenants/{tenant}/quota    ceiling plus current usage
DELETE /v1/admin/tenants/{tenant}/quota    return to unlimited
```

The ceiling compares against logical usage: the sum of live files'
current sizes, which is also what `GET /v1/usage` shows the tenant.
Physical storage can be lower (dedupe) or higher (version history
until GC); the accounting follows what tenants can see and delete.
The audit trail records `tenant.quota_set` and `tenant.quota_cleared`.

Reservations are released on every path that can end an upload:
completion settles against the real size, failures and digest
mismatches give the whole reservation back, terminating a resumable
session or aborting a multipart upload releases immediately, and the
sweep releases what a client abandoned without saying so. The usage
recount remains the backstop, not the mechanism.

Usage is a maintained counter rather than an aggregate per upload,
because summing a tenant's files stops being cheap once there are
many. Uploads whose length is declared reserve their bytes in the
same statement that checks the ceiling, so concurrent uploads cannot
each read the same headroom and collectively overshoot; the
reservation settles to the real size when the body lands and is
released when it fails. The counter is a cache, not the truth: every
sweep recomputes it from the file rows, the same way blob reference
counts are derived rather than trusted, so any drift a crash leaves
behind corrects within one interval.

## Retention and legal holds

Retention attaches to versions, and the admin surface is where policy
is stated:

```
PUT    /v1/admin/tenants/{tenant}/files/{id}/versions/{n}/retention   body: { "seconds": 31536000, "mode": "compliance" }
DELETE /v1/admin/tenants/{tenant}/files/{id}/versions/{n}/retention
PUT    /v1/admin/tenants/{tenant}/files/{id}/versions/{n}/hold        body: { "reason": "case 2026-cv-1138" }
DELETE /v1/admin/tenants/{tenant}/files/{id}/versions/{n}/hold        body: { "reason": "case closed" }
```

A tenant default stamps every new version at creation and prunes
history:

```
PUT    /v1/admin/tenants/{tenant}/retention   body: { "seconds": 31536000, "mode": "governance", "keep_last": 10 }
GET    /v1/admin/tenants/{tenant}/retention
DELETE /v1/admin/tenants/{tenant}/retention
```

The stamped value is computed at creation and never recomputed, so a
policy change cannot shorten what already exists. `keep_last` prunes
only erasable history: holds and unexpired clocks survive any depth.
Retained bytes appear beside usage on `/v1/usage` and the admin quota
view, so a full tenant can see how much of the total is bound.

`mode` is `governance` (the default) or `compliance`. A compliance
clock only extends: shortening, clearing, or downgrading it refuses
with 409 until it expires, for admins too, which is the authority
line WORM names. Holds require a stated reason in both directions,
and every change writes an audit event carrying it. A held or
retained version keeps its bytes through file deletion; soft delete
still hides the file, because hiding is not erasing.

## Maintenance sweeps

One interval loop runs five failure-isolated passes: expired upload claims to
`failed`, expired run claims back to `pending`, stale scans to `failed`,
staging deletion past TTL, and blob garbage collection (mark, grace, fresh
recount, collect).

Replicas elect a sweep leader per pass through a database lease. Losers skip
the pass. A crashed leader's lease expires and any replica takes over. Worker
loops need no election; run claims are already CAS-guarded.

GC deletes the database row first, then the object bytes, with a final row
existence check between the two: identical content re-registered inside that
window aborts the collection and the fresh bytes stay.

## Runbook: files stuck in scanning

`scanning` is transient. If files sit there:

1. `GET /v1/runs?status=failed` and look for `post_upload` runs naming the
   file. A terminal pipeline failure moves the file to `failed` on its own;
   a file still in `scanning` usually means the run is pending or running.
2. A failed run retries with `POST /v1/runs/{id}/retry` once the cause (for
   example, unreadable blob storage) is fixed. The file returns to
   `scanning`, a worker finishes the journaled steps, and the record
   finalizes.
3. Orphans with no run at all (a crash between upload completion and
   enqueue) are failed automatically by the stale-scan sweep. A failed file
   with a digest keeps serving its previous content and accepts a re-upload.

## Backup and restore

Back up the blob plane first, then the metadata plane. That order
makes every metadata reference in the backup point at bytes the
backup already holds; anything uploaded between the two passes is
simply absent from both. The reverse order can capture records whose
bytes are missing.

The GC grace period (`COPAL_GC_GRACE_SECS`, default one day) covers
the other direction: a blob unreferenced at backup time still has its
bytes for the full grace window, so a restore inside that window
never resurrects a record whose content is gone.

Restore in the same order: bytes into the blob root, then the
SurrealDB import, then start the server (schema application is
idempotent). Run one maintenance pass afterward; derived refcounts
recompute on their own.

## Metrics

`GET /metrics` on the admin surface serves Prometheus text, guarded by
the admin token: request volumes and error rates are operator data.
Deployments that split `COPAL_ADMIN_BIND` keep the scrape off the
tenant-facing network along with key custody.

| Series | Meaning |
| --- | --- |
| `copal_http_responses_total{class}` | Responses by status class, counted at the outermost layer, so timeouts and refusals are included. |
| `copal_http_request_duration_seconds_sum` / `_count` | Total served time and request count; their ratio is the mean. Quantiles would require buckets the process does not compute, so none are claimed. |
| `copal_uploads_completed_total`, `copal_uploaded_bytes_total` | Content finished through the shared finalize path, whichever face carried it. |
| `copal_quota_refusals_total` | Uploads refused for exceeding a tenant ceiling. |
| `copal_webhook_deliveries_total{outcome}` | Delivery attempts by outcome: delivered, retry, failed. |
| `copal_blobs_collected_total`, `copal_reaped_uploads_total`, `copal_reaped_runs_total` | Sweep work, accumulated across passes. |

Counters are process-local and reset on restart, which is what
Prometheus expects; the database holds the durable truth for
everything they summarize.

## Health endpoints

`/healthz` is liveness: the process answers. `/readyz` is readiness:
one store round trip plus one blob-plane call must both succeed, so
orchestrators gate traffic on real dependencies. The container image
health check uses `/readyz`.

## Deployment security posture

Development defaults are open on purpose. An exposed deployment sets
`COPAL_AUTH_MODE=keys` with `COPAL_ADMIN_TOKEN`, sets `COPAL_ADMIN_BIND` so
custody lives off the tenant network, terminates TLS in front of the server,
scrubs grant URLs from proxy logs (tokens ride the path by design; TTLs bound
the exposure), runs the database as a scoped user, and rate-limits at the
proxy until built-in quotas land.

What the service enforces on its own: uniform 401s across the whole
credential path, timing included; server-owned scan verdicts; `nosniff` and
sanitized `Content-Disposition` with forced download for script-capable
types; `no-store` on private and grant bytes; access levels at the byte
boundary; the engine-immutable audit trail.
