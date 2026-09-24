# Operations

## Configuration

Every setting comes from an environment variable. There is no
configuration file, and neither the server nor `copalctl` loads `.env`
files: set the variables in your shell, service unit, or compose file.
Defaults target local development.

How values are read:

- Booleans accept only `true` or `false`. Any other value, `1`, `yes`,
  and `on` included, silently falls back to the default.
- A number that does not parse also falls back to the default.
- An unknown `COPAL_AUTH_MODE` falls back to `header`. The startup log
  then carries the header-mode warning.
- Invalid JSON in `COPAL_TRANSFORMERS`, `COPAL_RESIDENCIES`, or
  `COPAL_LOCAL_TIERS` is logged as an error and ignored, and the server
  starts without that configuration.
- Some mistakes stop the boot instead: `COPAL_ENGINE_SESSIONS` or
  `COPAL_RATE_LEDGER` outside their allowed values,
  `COPAL_ENGINE_SESSIONS=on` without `COPAL_ENGINE_ACCESS_KEY`,
  `COPAL_S3_BIND` without an encryption key, a
  `COPAL_PERSISTED_OPERATIONS` file that cannot be read or whose hashes
  do not match, an encryption key that is not 64 hex characters, an
  invalid residency or tier name, a tier that carries its own key, an
  archive-class tier on Azure, and key custody that cannot be reached.

### Listeners

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_BIND` | `127.0.0.1:8080` | Tenant-facing listener: REST, `/v1c`, GraphQL, MCP, tus, health. It also carries the admin surface when `COPAL_ADMIN_BIND` is unset. |
| `COPAL_ADMIN_BIND` | unset | Separate listener for the admin surface, `/metrics`, and the console. When set, those routes exist only there. |
| `COPAL_S3_BIND` | unset | Listener for the S3-compatible gateway (path-style bucket routes at its root). Requires an encryption key, from `COPAL_BLOB_ENCRYPTION_KEY` or key custody; startup refuses otherwise. The S3 credential admin routes exist only when this is set. |
| `COPAL_CORS_ORIGINS` | unset | Comma-separated browser-origin allowlist. Unset attaches no CORS layer at all. |

### Metadata database

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_DB_URL` | `ws://127.0.0.1:8000` | SurrealDB endpoint. `ws://` and `wss://` reach a SurrealDB server. The default build also accepts `surrealkv://<path>` (embedded, durable, one process) and `mem://` (embedded, lost on exit). See the embedded tier section. |
| `COPAL_DB_NS` / `COPAL_DB_NAME` | `copal` / `copal` | Namespace and database. |
| `COPAL_DB_USER` / `COPAL_DB_PASS` | `root` / `root` | Credentials Copal signs in with, embedded engines included. The `root` default applies only when both are unset; setting just one of them sends no credentials. Use a scoped user in deployments. |
| `COPAL_ENGINE_ACCESS_KEY` | unset | HS256 key for the engine's caller access method. Set, the store defines record access, every table's `PERMISSIONS` become enforceable per caller, and caller sessions filter on the engine side. Rotation replaces the method on the next boot. |
| `COPAL_ENGINE_SESSIONS` | `off` | `on` runs request handlers' repository calls through caller-bound engine sessions, so `PERMISSIONS` filter live traffic. Requires `COPAL_ENGINE_ACCESS_KEY`; boot refuses otherwise. Opening a session costs two engine round trips. Any value other than `off` or `on` refuses to boot. |
| `COPAL_SESSION_CACHE_SECS` | `60` | How long an open caller session may be reused. Zero pays the two-round-trip open on every request. |
| `COPAL_SESSION_CACHE_SIZE` | `256` | How many caller sessions to hold. Zero disables reuse. |

### Blob storage and encryption

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_BLOB_ROOT` | `./data/blobs` | Filesystem root of the `local` residency, which always exists. Staging, resumable-upload sessions, and S3 multipart parts also live here. |
| `COPAL_BLOB_ENCRYPTION_KEY` | unset | Master key for encryption at rest: 64 hex characters (32 bytes). New objects seal with chunked AES-256-GCM under per-object derived keys; existing plaintext objects keep serving. Digests stay plaintext digests, so addressing and dedupe are unchanged. Without this key (or key custody), webhooks, edge URLs and edge keys, S3 credentials, and the S3 gateway are disabled, because each stores secrets sealed under it. |
| `COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS` | unset | The retiring master key during a rotation. Reads fall back to it while the re-seal sweep moves objects and sealed secrets under the current key; nothing seals under it. Unset it once `copal_resealed_total` goes quiet. See the rotation section. |
| `COPAL_KMS_ADDR` | unset | External key custody. Set, the master key is fetched from here at boot and never read from the environment, and a deployment that cannot reach custody refuses to start. See master key custody. |
| `COPAL_KMS_KEY_ID` | `blob` | Which key custody should hand over. |
| `COPAL_KMS_TOKEN` | unset | Bearer token presented to custody. |
| `COPAL_RESIDENCIES` | unset | JSON map of named storage residencies beyond `local`: filesystem, S3-compatible, Google Cloud Storage, or Azure Blob Storage, each optionally with its own key and tiers. See storage residencies. |
| `COPAL_LOCAL_TIERS` | unset | Tiers of the `local` residency, as a JSON map of tier name to backend configuration with a `class`. See storage tiers. |
| `COPAL_TIER_MOVE_BATCH` | `100` | Tier moves started per mover pass. Each is a full read-write-read of the object, so this bounds a pass's IO; deferred moves resume next pass. |
| `COPAL_TIER_ERASE_GRACE_SECS` | `86400` | How long a displaced copy survives its placement flip before the mover erases it. Set it to at least your backup cadence, for the same reason the GC grace covers restores. |

### Authentication and the admin surface

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_AUTH_MODE` | `header` | `header` (development: the `x-copal-tenant` header is trusted) or `keys` (a `ck1` bearer key is required). Any other value falls back to `header`. The default changes to `keys` at 1.0. |
| `COPAL_ADMIN_TOKEN` | unset | Operator token. Unset disables the admin surface and the console entirely. |
| `COPAL_ADMIN_TOKEN_PREVIOUS` | unset | Outgoing operator token during a rotation window; accepted beside the current one, then unset. |
| `COPAL_OPERATOR_HEADER` | unset | Header naming the person an authenticating proxy let through, for example `x-forwarded-email`. Set it and every audited operator action carries that name, and the admin surface refuses a request that arrives without one. See operator identity. |
| `COPAL_CONSOLE_FLEET` | `false` | `true` adds the fleet view to the console: sibling namespaces on the shared engine, read-only (namespaces, databases, tables, row counts). Needs a remote engine and credentials with root reach; on an embedded engine the view says so and shows nothing. |

### Uploads, requests, and outbound calls

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_MAX_UPLOAD_BYTES` | `1073741824` (1 GiB) | Upload ceiling, enforced in the stream (413 past it). URL fetches use the same ceiling. |
| `COPAL_UPLOAD_LEASE_SECS` | `900` | Upload claim lease. Expired claims can be taken over and are reaped. |
| `COPAL_REQUEST_TIMEOUT_SECS` | `30` | Deadline for ordinary requests (408 past it). |
| `COPAL_TRANSFER_TIMEOUT_SECS` | `3600` | Deadline for the byte routes and tus; ends slow-drip connections. |
| `COPAL_TUS_SESSION_TTL_SECS` | `86400` | Resumable-upload and S3 multipart sessions older than this are swept with their staged bytes. |
| `COPAL_FETCH_ALLOW_PRIVATE_TARGETS` | `false` | Whether `POST /v1/files/fetch` may pull from private address space. Tenant-supplied URLs refuse private targets by default. |
| `COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS` | `false` | Permit webhook endpoints that resolve to private, loopback, or link-local addresses. Tenant-supplied URLs that point inside the deployment are server-side request forgery, so turn this on only when receivers are internal and tenants are trusted. The server logs a warning at startup when it is on. |

### Processing

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_BLOCKED_EXTENSIONS` | `exe,dll,bat,cmd,ps1,sh,php,asp,aspx,jsp,py,msi,scr,com,vbs,js` | Comma-separated extension denylist for the upload pipeline. Setting it replaces the built-in list. |
| `COPAL_ENFORCE_TYPE_MATCH` | `false` | `true` quarantines files whose sniffed type contradicts the declared type, instead of only recording the mismatch. |
| `COPAL_CLAMAV_ADDR` | unset | `host:port` of a clamd instance. Set, the upload pipeline scans content, and content is withheld until a scan clears it. |
| `COPAL_EXTRACTOR_ADDR` | unset | `host:port` or full URL of a text extractor speaking Apache Tika's shape: `PUT /tika` with the bytes, `Accept: text/plain`, text in the response body. A bare `host:port` becomes `http://host:port/tika`; a full URL is used exactly as written, so include the path (for example `http://tika:9998/tika`). Text and JSON extract natively without it. |
| `COPAL_TRANSFORMERS` | unset | JSON map of named external transformers, for example `{"ocr": {"url": "http://ocr:9100/run", "timeout_secs": 120, "secret": "...", "max_source_bytes": 33554432}}`. `timeout_secs` defaults to 60 (clamped to 1 to 600) and `max_source_bytes` to 64 MiB. See external transformers. |

### Search

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_EMBEDDING_ADDR` | unset | Embedding service speaking the OpenAI `/v1/embeddings` shape (Ollama, llama.cpp, text-embeddings-inference, LocalAI, vLLM, OpenAI). Set, semantic and hybrid search work. |
| `COPAL_EMBEDDING_MODEL` | `nomic-embed-text` | Model name passed to that service. |
| `COPAL_EMBEDDING_DIMENSION` | `768` | Width the model emits. The vector index is defined at this dimension at startup, so it must match the model. |
| `COPAL_MAX_SEMANTIC_DISTANCE` | `0.65` | Cosine distance beyond which a passage is not a semantic match (0 identical, 1 unrelated). Without a floor, nearest-neighbor search answers every query with its nearest results however far away they are. |
| `COPAL_RERANK_ADDR` | unset | Reranking service that reads each shortlisted passage against the query (text-embeddings-inference with a cross-encoder, Infinity, Jina, Cohere, Voyage). Unset, search ranks by fusion alone. |
| `COPAL_RERANK_MODEL` | unset | Model name, sent only when set, because some services require one and others reject it. |
| `COPAL_RERANK_TOKEN` | unset | Bearer token for that service. |
| `COPAL_RERANK_DEPTH` | `50` | How many fused candidates the reranker sees, clamped to 1 to 200. The reranker costs per pair, so this bounds the work. |

### GraphQL and rate limits

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_SUBSCRIPTION_MAX_SECS` | `900` | Lifetime of one subscription; re-subscribing re-authenticates. |
| `COPAL_PERSISTED_OPERATIONS` | unset | Path to a JSON file of sha256 hash to GraphQL document. Set, GraphQL runs listed operations only. |
| `COPAL_RATE_LEDGER` | `memory` | `memory` meters each process on its own; `store` keeps one consumption budget in the database for a whole fleet. Any other value refuses to boot. |

### Maintenance sweeps

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_SWEEP_INTERVAL_SECS` | `60` | Maintenance cadence. |
| `COPAL_STAGING_TTL_SECS` | `86400` | Staging entries older than this are deleted. |
| `COPAL_GC_GRACE_SECS` | `86400` | A blob must stay unreferenced this long before its bytes go. |
| `COPAL_GC_BATCH` | `1000` | Blob rows per GC batch query. The pass loops batches until it has covered every row. |
| `COPAL_SCAN_STALE_SECS` | `3600` | Files stuck in `scanning` with no live run are failed after this age. |

### Observability

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_OTLP_ENDPOINT` | unset | OTLP trace collector (http/protobuf), for example `http://otel-collector:4318/v1/traces`. Unset, spans feed the logs and nothing leaves the process. See traces. |
| `RUST_LOG` | `copal_server=info,copal_store=info` | Log filter, in the `tracing` env-filter syntax. |

### copalctl

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_URL` | `http://127.0.0.1:8080` | Server base URL. |
| `COPAL_TOKEN` | unset | A minted `ck1` key, sent as a bearer token. Takes precedence over `COPAL_TENANT`. |
| `COPAL_TENANT` | unset | Tenant for header mode, sent as `x-copal-tenant`. |
| `COPAL_ADMIN_TOKEN` | unset | Operator token for the `admin` commands. |

## The embedded tier

The default build carries the metadata engine inside the binary. Set

```
COPAL_DB_URL=surrealkv://./data/db
```

and Copal runs as one process: no SurrealDB server to operate, and the
database on disk beside the blob root. Copal still signs in with
`COPAL_DB_USER` and `COPAL_DB_PASS`, which default to `root`/`root`
when both are unset. Everything else is identical, because the engine
is the same engine: schema reconciliation, engine `PERMISSIONS`,
caller sessions, the audit trail's immutability event, and the
retrieval indexes all apply the way they do against a server. `mem://`
is the ephemeral variant for demos and tests.

The embedded engine serves one process, so the two-instance topology
and its shared engine stay on the `ws://` form. The upgrade path is
mechanical: stop Copal, start a SurrealDB server on the surrealkv
directory (the on-disk format is the engine's own), point
`COPAL_DB_URL` at it, and start two instances. Back up the embedded
tier by including the surrealkv directory in the same snapshot as the
blob root; the ordering rule in [backup.md](backup.md) applies
unchanged.

What the network hop costs, measured by `bench/scale.sh` with the same
engine version on both sides (in-process `mem://` against a same-host
`ws://` server, both in memory, so the connection is the only
variable; p50 with p95 beside it, release build):

| repository call | embedded | ws:// same host | the connection's share |
| --- | --- | --- | --- |
| `get_file` point read | 109 us (183) | 606 us (844) | ~0.5 ms |
| 100-row listing | 1.11 ms (1.44) | 2.68 ms (4.10) | ~1.6 ms |
| claim CAS (up to two attempts) | 356 us (508) | 1.23 ms (1.63) | ~0.9 ms |
| transition CAS | 380 us (481) | 737 us (1.02 ms) | ~0.4 ms |

Half a millisecond to a millisecond and a half per repository call on
one host, before any real network distance. That is what the embedded
tier saves per call, multiplied by however many calls a request makes.

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
replica boots race without harm, since identical `OVERWRITE`
statements are idempotent.

## API keys, credentials, and the audit trail

Keys are minted, listed, and revoked through the admin surface,
guarded by `x-copal-admin-token` compared in constant time.

```
POST   /v1/admin/tenants/{tenant}/keys            body: { "name": "ci" }
GET    /v1/admin/tenants/{tenant}/keys
DELETE /v1/admin/tenants/{tenant}/keys/{key_id}
GET    /v1/admin/tenants/{tenant}/audit
```

The mint body also takes optional `scopes` (from `read`, `write`,
`admin`; empty means all three), `ttl_secs`, and `principal`. The mint
response carries the bearer token once. Store it; the server keeps
only the hash. Key names are unique per tenant, so rotation reads as
revoke the old key and mint a new one under the next name. Listings
never include hash material.

S3 gateway credentials live beside the keys and follow a different
custody rule, forced by the protocol: SigV4 verification derives its
signing key from the shared secret, so the server must read the
secret back. The secret is stored sealed under the master key (never
hashed, never in the clear) and appears once in the mint response.
These credentials never reuse `ck1` material and revoke independently.
The routes exist only when `COPAL_S3_BIND` is set.

```
POST   /v1/admin/tenants/{tenant}/s3-credentials
GET    /v1/admin/tenants/{tenant}/s3-credentials
DELETE /v1/admin/tenants/{tenant}/s3-credentials/{access_key_id}
```

Webhook endpoint secrets follow the same rule: stored sealed under the
master key, returned once at registration, read back only to sign
deliveries. Edge keys (`cg2` signing secrets) complete the
sealed-secret set: minted per tenant, sealed under the master key,
returned once for the operator to install at the CDN edge, and
revocable as a whole, which ends every token the key signed.

```
POST   /v1/admin/tenants/{tenant}/edge-keys
GET    /v1/admin/tenants/{tenant}/edge-keys
DELETE /v1/admin/tenants/{tenant}/edge-keys/{key_id}
```

The webhook surface, its dispatcher, and the edge routes exist only
when a master key is configured. Sealed credentials, webhook secrets,
and edge keys survive a master key rotation without re-minting: the
boot pass re-seals them under the current key (see the rotation
section below).

Custody and lifecycle actions land in the audit trail:

- Keys and credentials: `key.minted`, `key.revoked`,
  `s3credential.minted`, `s3credential.revoked`, `edgekey.minted`,
  `edgekey.revoked`, `principal.created`, `principal.disabled`.
- Capabilities: `grant.issued`, `grant.upload_issued`,
  `grant.revoked`, `edge.issued`.
- Webhooks and files: `webhook.registered`, `webhook.removed`,
  `file.removed`.
- Tenant policy: `tenant.quota_set`, `tenant.quota_cleared`,
  `tenant.storage_assigned`, `tenant.retention_policy_set`,
  `tenant.retention_policy_cleared`, `tenant.tiering_policy_set`,
  `tenant.tiering_policy_cleared`.
- Versions and placement: `version.retention_set`,
  `version.retention_cleared`, `version.hold_applied`,
  `version.hold_released`, `file.tier_pinned`,
  `file.tier_pin_released`.

When the proxy forwards a client origin (`x-forwarded-for`), the first
hop is recorded on the row for forensics; it plays no part in
authorization. Audit rows are immutable inside the engine: an UPDATE
or DELETE against one aborts in SurrealDB itself.

Rotate the operator token with no downtime: move the old value to
`COPAL_ADMIN_TOKEN_PREVIOUS`, deploy the new one as
`COPAL_ADMIN_TOKEN`, and unset the previous value once callers have
moved.

## Master key custody

`COPAL_BLOB_ENCRYPTION_KEY` puts the key that decides whether stored
bytes can be read at all into the process environment, where it sits
in shell history, in a compose file, and in whatever inspects a
running container. Point `COPAL_KMS_ADDR` at a key manager and the
key is fetched at boot instead, held in memory for the process
lifetime, and absent from the environment entirely.

The contract is one request, so a short adapter serves it:

```
GET {addr}/keys/{key_id}
Authorization: Bearer {token}

200 {"current": "<64 hex>", "previous": "<64 hex>" | null}
```

Both keys arrive together because a rotation is a state, and reading
them one at a time can show a half that never existed. A runnable
reference, plus the short adapters that reach Vault, AWS KMS, or an
HSM, lives in [`examples/kms`](../examples/kms).

A deployment configured for custody that cannot reach it does not
start. It does not fall back to the environment even when
`COPAL_BLOB_ENCRYPTION_KEY` is set, and it logs a warning that the
environment value is ignored. Coming up unable to open its own
content, or on a key an operator thought they had retired, are both
worse than not coming up. An answer that is not a key is refused at
the same point, where the cause is plain.

## Master key rotation

`COPAL_BLOB_ENCRYPTION_KEY` retires in three steps, with reads correct
throughout.

1. Set the new key as `COPAL_BLOB_ENCRYPTION_KEY`, move the old one to
   `COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS`, and restart. Under key
   custody the same change happens there, and Copal reads both halves
   from the one answer. New writes seal under the new key; reads try
   the current key first and fall back to the retiring one, so nothing
   sealed earlier becomes unreadable.
2. Let the sweep drain. Database secrets (S3 credentials, webhook
   endpoint secrets, edge keys) re-seal once at boot, before traffic.
   Objects re-seal in the background, a batch per residency and per
   tier every five minutes, counted by `copal_resealed_total`. Content
   addressing makes the rewrite safe in place: digests cover
   plaintext, so a re-sealed object keeps its address and its
   references.
3. When the counter goes quiet, unset
   `COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS` and restart. The old key is no
   longer needed and belongs out of custody.

A residency sealing under its own `encryption_key` rotates the same
way through `previous_encryption_key` in its residency JSON; the
deployment-level previous key never overrides a residency's own.
Objects written before encryption was enabled are plaintext, and the
sweep leaves them as they are; a rotation moves sealed bytes between
keys and does no first-time sealing. Uploading the content again seals
it.

The fallback order is strict: every open tries the current key first,
so a drained rotation costs nothing on the read path, and the sweep
re-seals exactly the objects whose first frame the current key fails
to open.

## Media and large files

The blob plane is content-agnostic: bytes go to a content address and
the type is metadata, so any format stores and serves without special
handling. Uploads stream, so size costs disk rather than memory, and
the ceiling (`COPAL_MAX_UPLOAD_BYTES`, 1 GiB by default) is enforced
mid-stream and surfaces as 413. Past a single request there is
resumable upload at `/v1/tus` and multipart on the S3 gateway.

Ranged reads work on sealed objects without decrypting the whole
file, because encryption frames at 64 KiB and a range touches only the
frames covering it. A video player scrubbing a large encrypted file
depends on this: a 1 MiB read from the far end of a 2 GiB sealed
object costs the same two milliseconds as one at its front (measured;
see the scale envelope below). Sealing costs a 20-byte header plus 16
bytes per frame, which is 0.024% on a 256 MiB file.

Type-specific behavior lives only in the layers above the bytes:

| Layer | Covers |
| --- | --- |
| Sniffing | Documents, images, archives, and the common audio and video containers, including formats that name themselves past their first bytes (RIFF at byte eight, ISO base media brands at byte eight). Anything unrecognized reads as unverifiable and never blocks. |
| Extraction | Text and JSON natively; other documents through `COPAL_EXTRACTOR_ADDR`. Media carries no transcription of its own, though a transformer that produces one lands searchable text. |
| Renditions | Images, to jpeg or png. |
| Transformers | Everything else, through the transform seam. |

One operational note for deployments holding media: a scanner has its
own ceiling. clamd's `StreamMaxLength` defaults to 25 MB, so large
files fail the scan step until that is raised to match
`COPAL_MAX_UPLOAD_BYTES`. It applies to derived content too, since
that walks the pipeline as well.

## The scale envelope

Where the large-file figures come from. Everything below was measured
by `bench/scale.sh` (ignored release-mode tests, one scenario per
process, requests driven straight into the router) on a 24-core
i9-12900KS with 64 GiB and NVMe under Windows 11: `mem://` metadata, a
filesystem blob root, and encryption at rest on. No network sits in
the frame on purpose;
these figures isolate what Copal itself costs per byte, and the
container bench in `bench/` watches the same paths from outside with
stock clients.

| measurement | 512 MiB | 1 GiB | 2 GiB |
| --- | --- | --- | --- |
| Streamed single PUT (REST face) | 1.6 s, 313 MiB/s | 3.4 s, 300 MiB/s | 6.4 s, 317 MiB/s |
| Multipart parts up (S3 face, 8 MiB parts) | 2.5 s, 205 MiB/s | 4.9 s, 207 MiB/s | 9.9 s, 207 MiB/s |
| Multipart completion (assemble, seal, hash) | 1.8 s, 291 MiB/s | 3.8 s, 267 MiB/s | 7.2 s, 286 MiB/s |
| Ranged 1 MiB read at start / middle / end | 2 / 2 / 1 ms | 2 / 3 / 2 ms | 2 / 2 / 2 ms |
| Full sequential read | 0.9 s, 585 MiB/s | 1.8 s, 565 MiB/s | 3.7 s, 549 MiB/s |
| Peak process memory, PUT and every read | 49 MiB | 49 MiB | 50 MiB |
| Peak process memory, whole multipart cycle | 64 MiB | 65 MiB | 65 MiB |

Up to 2 GiB there is no wall. Time scales linearly with size,
throughput holds flat, and process memory does not grow with the
object, because the streaming paths hold chunks and frames and never
whole objects. Sealing has a price on the byte paths: the same 1 GiB
PUT lands at 515 MiB/s plaintext against 300 MiB/s sealed, and a
plaintext sequential read runs at several GiB/s where decryption holds
sealed reads to the mid five-hundreds. Both sealed figures still
saturate a 2 Gb network link.

Two pipeline steps do grow with the object, because both read it
whole into memory: the malware scan and text extraction. The scan
buffers the object before speaking INSTREAM to clamd, and on a sealed
object that buffered read holds ciphertext and plaintext at once:

| object | peak process memory during the scan pass | buffered read | INSTREAM hand-off |
| --- | --- | --- | --- |
| 512 MiB | 1551 MiB | 0.7 s | 0.2 s |
| 1 GiB | 3088 MiB | 1.4 s | 0.4 s |
| 2 GiB | 6163 MiB | 2.9 s | 0.9 s |

The peak is three times the object, plus a few MiB of process.
Extraction makes the same fully buffered read, and it makes it for
every upload that was not blocked, scanner or not, including content
it then finds it cannot extract. Size the host running the pipeline
for about three times the largest object you accept. Uploads and
downloads themselves stream; on the request path, only an inline
rendition reads its source whole, and rendition sources are capped at
32 MiB.

The F16 vector index rebuild is the one a deployment performs when
upgrading past the half-precision change, and it is background work,
not a boot cost: `DEFINE INDEX ... CONCURRENTLY` returns at once, and
the timing below runs from the DEFINE to `INFO FOR INDEX` reporting
`status: ready`, on the embedded engine over a corpus of 100,000
passages at 768 dimensions.

| rebuild | time to ready |
| --- | --- |
| HNSW TYPE F16 (the current type) | 6.9 s |
| HNSW TYPE F32 (the previous type) | 6.7 s |

Seconds over a six-figure corpus, about the same either way, so the
type change is no reason to delay the upgrade. The memory saving
behind F16 is arithmetic (raw vectors at this corpus are 147 MiB half
precision against 293 MiB single), and the measured working set moved
the same way: the process sat 355 MiB lower after the F16 build than
after the F32 rebuild of the same corpus. Working-set deltas on a
process that does not return freed pages are a proxy, so the
arithmetic is the claim and the measurement corroborates it.

## Malware scanning

With `COPAL_CLAMAV_ADDR` set, the upload pipeline scans content
through clamd (the INSTREAM protocol, spoken directly) between the
cheap checks and the transition that would make bytes servable. A
detection quarantines the file with the matched signature recorded in
`metadata.processing.verdict_reason`. Quarantine lasts until the file
is deleted, and neither content nor grants serve from it.

Enabling a scanner also changes what serving means. Without one, Copal
serves on the digest alone, so bytes are readable while their pipeline
runs. With scanning on, content serves only once a scan has cleared
that exact content. The scan records which digest it cleared, and
serving compares it against the digest the record currently points
at, which is a question about content rather than lifecycle.

Reading the record's state instead would get two cases wrong. A run
that failed leaves unscanned bytes in place, which a "withhold only
while scanning" rule would serve. And a re-upload in flight still
points at the previous digest until it completes, which a "`ready`
only" rule would withhold even though that content passed its own
scan. The previous version keeps serving through a re-upload, as
documented, and scanning does not change that.

A scanner that cannot be reached is an error, never a pass: the
activity fails, the run retries, and the file never reaches `ready`.
Without `COPAL_CLAMAV_ADDR` nothing changes, and records say
`metadata.processing.scanned: false` so that no clean verdict is
implied.

The scan reads the whole object into memory before speaking
INSTREAM, and on a sealed object that read briefly holds about three
times the object's size (measured in the scale envelope above).
Deployments that raise clamd's ceiling to scan large files should size
the host for that multiple of the largest object they accept.

## Text extraction and search

The upload pipeline extracts text after the malware scan and before
the transition that publishes a file, so text is pulled from content
a scanner has already judged, and the file becomes readable and
searchable in one step. Extracted text lands in its own table with a
BM25 index, which makes stored files searchable without a second
datastore to keep in sync.

Copal decodes text and JSON itself and carries no document parsers.
PDF, Office formats, and OCR are large, fast-moving, and historically
a rich source of memory-safety bugs, which is a poor trade inside a
service holding other people's files; `COPAL_EXTRACTOR_ADDR` points
at a service that does that work. A configured extractor that cannot
be reached is an error, so the run retries and the document is not
recorded as empty. Extractions are capped at a million characters, and
the record says when it truncated.

Extracted text follows its content: a re-upload replaces it, a delete
removes it, and each row records which digest it came from.

With an embedding service configured, the pipeline also embeds that
text and stores the vector beside it, indexed with HNSW at the model's
dimension. Copal runs no models, since inference means weights, a
runtime, and hardware assumptions that do not belong inside a storage
service; anything speaking the OpenAI embeddings shape can serve it.
Changing models means changing `COPAL_EMBEDDING_DIMENSION` to match;
the vector index rebuilds at the new width, and a background pass
re-embeds existing passages batch by batch.

Semantic queries run against the HNSW index with an exploration
factor, which is the form that uses it. SurrealDB 3.x also accepts a
KNN operator naming a distance metric, but that form scans the whole
table and leaves the index unused.

Text is split into overlapping passages and each one is embedded
separately, so a long document is retrievable by whichever part
answers the question rather than by its average meaning. Passages are
capped per document, and a re-extraction replaces all of them at
once. Attaching an embedding is guarded on the digest, so a vector
computed for content that has since been replaced never attaches to
the new passages, and a retried run embeds only what still lacks a
vector.

## External transformers

Copal's built-in processing covers sniffing, scanning, extraction,
embedding, and image renditions. Everything else is an operator
concern, and the transform seam is how it plugs in without forking:
any HTTP service becomes a derivation step.

```
POST /v1/files/{id}/transform
{ "transformer": "ocr", "params": { "lang": "eng" }, "content_type": "application/pdf" }
```

The request creates a derived record (path `source@name-<params
digest>`, listed beside renditions) and enqueues a flow run. The
worker sends the source bytes to the configured URL and finishes the
derived file with whatever comes back, through the same claim and
complete path every upload uses, so the output is a real file with a
digest, versions, retention, and every serving rule intact.

The wire contract for the service:

- Request: `POST <url>?params=<JSON>` with the source bytes as the
  body, `content-type` set to the source's type, and headers
  `x-copal-tenant`, `x-copal-source-file`, `x-copal-source-digest`,
  plus `x-copal-transform-secret` when the configuration carries a
  secret.
- Answer `200` with the derived bytes as the body: the derivation
  lands and the derived file becomes ready.
- Answer any `4xx` to refuse the input: the derived record fails and
  the run completes. The first 200 characters of the response body are
  recorded as the reason.
- Answer `5xx`, time out, or refuse the connection for infrastructure
  trouble: the run retries within the flow engine's attempt budget.

Sources larger than the transformer's `max_source_bytes` (64 MiB by
default) are refused before any call. Repeating a transform request
returns the existing derived record; the run key covers source
content, transformer name, and parameters, so replays never derive
twice. The derived file serves as the `content_type` declared in the
request, because the operator knows the service's output and Copal
does not.

Transformer URLs are operator configuration, the same trust class as
`COPAL_CLAMAV_ADDR` and the extractor address, so the outbound policy
for tenant-supplied URLs does not apply to them.
`copal_transforms_total` counts derivations that landed and
`copal_transform_refusals_total` counts refused inputs.

Derived bytes came from another process, so they walk the same
pipeline an upload does: sniffed, checked against the extension
policy, scanned, and extracted, passing through `scanning` on the way
to `ready`. A transformer that produces text therefore produces
searchable text, which is what makes transcription and OCR worth
wiring up. Image renditions are generated in process from content that
already passed its own pipeline, so they land ready directly.

A worked example lives in
[`examples/transformers/ffmpeg`](../examples/transformers/ffmpeg): a
hundred-line service wrapping ffmpeg, with recipes for video
thumbnails, audio tracks, short previews, and `ffprobe` metadata. It
is also the shortest complete statement of the wire contract.

## Outbound request policy

Two features fetch URLs that tenants supply: webhook delivery and URL
ingestion (`POST /v1/files/fetch`). Both refuse destinations that
resolve to loopback, private, link-local, carrier-grade NAT, or cloud
metadata addresses.

Webhook URLs are checked at registration and again at every delivery,
because DNS answers can change in between. The delivery client
follows no redirects, because a redirect reaches an address the check
never saw. `COPAL_WEBHOOK_ALLOW_PRIVATE_TARGETS=true` turns the check
off for deployments whose receivers are internal by design; the
server logs a warning at startup when it is on.

URL ingestion checks the source URL when the request arrives and again
when the fetch run executes. `COPAL_FETCH_ALLOW_PRIVATE_TARGETS=true`
turns that check off.

Rendition decoding is bounded twice: sources over 32 MiB are refused
before decoding, and the decoder itself carries a 256 MiB allocation
ceiling, so a small compressed file describing an enormous canvas
fails the derived record and leaves the host alone.

## Running two instances

Two Copal processes against one engine is a supported topology, and
the conformance harness tests it: `COPAL_HA=1 ./conformance/run.sh`
stands up two instances behind round-robin nginx and passes every
check for every client through the proxy.

The requirements:

- **A shared blob root.** Multipart parts stage on the local
  filesystem root, and a completion must see parts whichever instance
  staged them, so both processes mount one root (a shared filesystem
  or the same device). Everything else about content addressing
  tolerates the sharing: writes land under a staging name and rename
  onto the digest.
- **The same keys**: blob encryption key, engine access key, admin
  token. During a master key rotation, both instances carry the same
  previous key too, and it stays configured until the sweep drains on
  every instance's residencies.
- **One engine.** Leases, claims, and every compare-and-set resolve
  there, which is why round-robin needs no sticky sessions.
- **One rate ledger**, if budgets should hold across the pair:
  `COPAL_RATE_LEDGER=store`. With the default `memory` ledger each
  instance meters on its own.

Each process keeps its own session cache, and that is fine. Sweeps
elect one leader per pass, and a crashed instance's leases expire so
another can take over. The embedding backfill runs on every instance,
and its batches are idempotent.

When probing readiness behind a balancer, ask each instance directly:
a balancer's 200 proves one backend, and the next request may land on
a cold one.

## Storage residencies

A residency is a named backend configured through
`COPAL_RESIDENCIES`. `local` always exists and is the filesystem root
at `COPAL_BLOB_ROOT`. Named residencies can be any of four backends:

```
{"eu":    {"scheme": "s3", "bucket": "...", "endpoint": "...", "region": "...",
           "access_key_id": "...", "secret_access_key": "..."},
 "gcp":   {"scheme": "gcs", "bucket": "...", "credential": "<base64 service-account JSON>"},
 "az":    {"scheme": "azblob", "container": "...",
           "endpoint": "https://{account}.blob.core.windows.net",
           "account_name": "...", "account_key": "..."},
 "vault": {"scheme": "fs", "root": "/data/vault"}}
```

- `s3` covers AWS and S3-compatible services. `endpoint` is for
  MinIO-style targets; unset means AWS. `root` optionally prefixes
  object keys, as it does for GCS and Azure.
- `gcs` falls back to `credential_path`, then to the ambient
  credential chain when `credential` is unset, so workload identity
  needs nothing in the configuration. `endpoint` is for
  fake-gcs-server and other compatible targets.
- Every scheme accepts an optional `encryption_key` (64 hex) that
  seals that residency's objects under its own key instead of the
  master, and `previous_encryption_key` for that key's rotation.
- Names are 1 to 32 lowercase letters and digits.
- A residency may nest a `tiers` block; see storage tiers.

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
holds them. Deduplication is scoped per residency: the same content
pinned to two residencies stores twice, which is what residency means.
The blob master key, when configured, seals content in every residency
that has no key of its own. Resumable-upload staging always lives on
the local backend; completed sessions stream into the tenant's
residency at promotion.

A residency with its own `encryption_key` gives a tenant data nobody
else's key can open. Keys attach to residencies and not to tenants
because a key boundary scopes deduplication exactly as a backend
boundary does, and residencies already carry that scope: a tenant that
needs its own key gets its own residency, with its own bucket and
credentials to match. Losing a residency key loses that residency's
objects; the master key cannot open them.

Every instance that runs sweeps must configure the residencies whose
rows it may collect. A row whose residency is unknown to the instance
is collected in the database, and its bytes are logged as unreachable.

## Storage tiers

A tier is a second named backend inside a residency, plus the rule for
when bytes belong there; the design is in
[design/lifecycle-tiering.md](design/lifecycle-tiering.md). Tiers are
configured in a residency's `tiers` block, or in `COPAL_LOCAL_TIERS`
for the `local` residency:

```
COPAL_RESIDENCIES='{"eu": {"scheme": "s3", "bucket": "acme-eu", ...,
  "tiers": {"cold": {"scheme": "s3", "bucket": "acme-eu-cold", ..., "class": "online"}}}}'
COPAL_LOCAL_TIERS='{"cold": {"scheme": "fs", "root": "/data/cold", "class": "online"}}'
```

`class` is `online` (a GET answers: S3 Standard-IA, Glacier Instant
Retrieval, GCS classes, Azure Cool) or `archive` (a GET cannot answer
until a restore). For an S3 archive tier, Copal drives `RestoreObject`
with the tier's own credentials, and `restore_days` (default 7) says
how long the restored copy stays readable. Filesystem and GCS tiers
need no restore call. Archive on Azure refuses at boot, because Copal
does not drive Azure rehydration yet. A tier never carries its own
encryption key; it seals under its residency's key.

Byte reads record day-coarse access recency on the blob row. Each
sweep pass, a classifier walks the corpus and reports what would move,
the mover acts on that, and recall answers for archive classes whose
bytes cannot answer a GET. Watch the report before setting an
aggressive policy: the measured would-be recall rate is the figure the
cost model assumes, and the report exists so it is checked against
real traffic.

```
PUT    /v1/admin/tenants/{tenant}/tiering
       body: { "tier": "cold", "after_seconds": 7776000,
               "basis": "accessed", "min_bytes": 131072 }
GET    /v1/admin/tenants/{tenant}/tiering
DELETE /v1/admin/tenants/{tenant}/tiering
PUT    /v1/admin/tenants/{tenant}/files/{id}/tier   body: { "pin": "hot" }
DELETE /v1/admin/tenants/{tenant}/files/{id}/tier
GET    /v1/admin/tiering/report
```

- `tier` must name a configured tier; anything else refuses at
  validation, so a policy can never point bytes at a backend that
  does not exist.
- `basis` is `accessed` (default; ages from the last byte read,
  falling back to creation age when no read was ever recorded) or
  `created` (no read tracking consulted at all).
- `min_bytes` sets a floor on object size: S3 Standard-IA bills
  128 KiB per object minimum, and small objects can cost more cold than
  hot.
- The pin is for the file that is old, cold by every measure, and
  needed in milliseconds anyway. Pins reach shared blobs: one tenant's
  pin holds every referent's copy hot, and the report says so.

The report lists candidates per tenant (a shared blob counts for every
referent; the totals count each blob once), the bytes involved, the
measured would-be recall count (blobs old enough to move whose recent
reads hold them, the measured stand-in for the recall-rate
assumption), and how many blobs each rule held back. A blob moves when
every referencing tenant's policy marks it cold, nothing pins it, all
policies agree on the target tier, and the blob's own residency
configures that tier. The most demanding reference always wins.

The mover rides the sweep leader lease and takes four steps per blob:

1. Copy the raw object, envelope and all, as opaque bytes. The cold
   backend needs no key material.
2. Verify by reading the copy back through the ordinary open path and
   comparing the digest to the row id.
3. Flip the row in one guarded UPDATE.
4. Erase the displaced copy after `COPAL_TIER_ERASE_GRACE_SECS`
   (default one day).

Set the erase grace to at least your backup cadence: a flip that lands
between a backend's backup and the metadata export must leave the
bytes findable in that backup. At no moment does the row name a
placement whose object is absent. Every crash window converges by
re-copy and re-verify, and a copy that fails verification is deleted
and retried. From the flip on, every read serves from the new
placement. Resolution is residency, then tier: one projected point
read on the byte path, skipped entirely for residencies without tiers.

Promotion is the same four steps in reverse, when eligibility lapses:
a pin lands, a policy tightens or is removed, or reads make the blob
ineligible. Reads never promote directly, so one monthly audit read
cannot pull a whole corpus hot; the pin is the explicit override.
`COPAL_TIER_MOVE_BATCH` (default 100) bounds moves started per pass,
since each is a full read-write-read of the object; what one pass
defers, the next resumes. Garbage collection erases from every tier
the residency configures, and a master key rotation's re-seal sweep
walks tier backends beside their residencies. The restore drill grows
one step: download and digest-check one object per tier per residency.

### Recall

An archive-class placement is probed, never assumed. An object still
readable in an archive tier (written before the bucket's lifecycle
moved it, or restored temporarily) serves directly, which is how S3
itself behaves. Only bytes that cannot answer meet the recall path,
and each face answers in its own dialect:

- REST byte routes (content, version content, ranges, grant and edge
  redemptions) answer 202 with a body naming the recall run and a
  `Retry-After`; the GET itself enqueues the recall, idempotently.
  The status is 202 because the request started durable work: a run
  exists, it is pollable at `/v1/runs/{id}`, and retrying the GET is
  harmless. A counted grant is not consumed by a 202, since no byte
  was read, and a TTL that expires mid-recall re-issues.
- The S3 face answers `403 InvalidObjectState` on GET (AWS's own
  vocabulary, which existing S3 clients already speak), accepts
  `RestoreObject` (202 when started, 200 when already in flight or
  already readable), and HEAD reports progress through `x-amz-restore`.
- Pipeline reads (a transform or derive whose source is archive-cold,
  a re-run scan or extraction) enqueue the recall and retry within the
  flow engine's budget. An inline rendition answers 202 and recalls
  its source.
- Search, facets, and extracted text are untouched: chunks and text
  live in the metadata plane, so an archive-cold file remains fully
  searchable and its excerpts keep serving. Only following the hit to
  the bytes meets the 202.

The recall run (workflow `tier.recall`) issues the backend's restore
(S3 `RestoreObject`, signed with the tier's credentials; nothing to
issue for filesystem and GCS tiers), polls readability, then runs the
promote steps: raw copy home, digest verify, and the compare-and-swap
flip, with the cold copy left for the mover's grace-delayed erase.
Requests share one run per blob per hour (the idempotency key is
hour-bucketed, so a failed run stalls at most the rest of its hour
before a fresh request starts a new one). One run's polling budget is
about two and a half hours. Glacier Flexible retrievals in the
Standard class can take longer; in that case the run fails, the 202
keeps answering with its state, and the next hour's request resumes
the wait against an object that is further along.
`copal_tiering_recalled_total` counts recalls that promoted bytes
home.

The flow worker runs one run at a time per instance, so a recall that
waits on a slow restore holds that instance's worker for the wait.

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
recount is the backstop.

Usage is a maintained counter, because summing a tenant's files stops
being cheap once there are many. Uploads whose length is declared
reserve their bytes in the same statement that checks the ceiling, so
concurrent uploads cannot each read the same headroom and together
overshoot; the reservation settles to the real size when the body
lands and is released when it fails. The counter is a cache: the
sweep recomputes it from the file rows, the same way blob reference
counts are derived, so any drift a crash leaves behind corrects within
one interval. The recount covers up to 500 tenants per pass.

## Principals

Named actors under a tenant that keys belong to. A key without one is
a tenant-level credential, exactly as every key was before principals
existed. The full model is in [principals.md](principals.md).

```
POST   /v1/admin/tenants/{tenant}/principals            body: { "handle": "agent-7", "kind": "agent", "scopes": ["read"] }
GET    /v1/admin/tenants/{tenant}/principals
DELETE /v1/admin/tenants/{tenant}/principals/{handle}   (disables; the audit trail keeps the name)
```

Key minting takes an optional `principal`. Scopes beyond the
principal's ceiling refuse at mint time, effective scopes are the
intersection of key and principal at every authentication, and
disabling a principal refuses all of its keys at once. `kind` is
`human`, `service`, or `agent`, carried for reporting; enforcement
never branches on it.

S3 credential minting takes the same optional `principal` in its
body. The mint refuses when the principal's ceiling cannot carry the
gateway's read and write grant, uploads through the credential record
the actor's handle, and disabling the principal refuses every one of
its S3 credentials immediately, mid-session included.

## Retention and legal holds

Retention attaches to versions, and the admin surface is where policy
is stated. The full model is in [retention.md](retention.md).

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

## Operator identity

The admin token says a deployment acted. It cannot say which person
did, which leaves an immutable audit trail recording `admin` for every
custody change, quota edit, and legal hold. Copal takes the missing
half from a proxy and does not implement sign-in itself: put
oauth2-proxy, Authelia, Authentik, Pomerium, or Cloudflare Access in
front of the admin surface, and name the header it sets.

```
COPAL_OPERATOR_HEADER=x-forwarded-email
```

Two things change. Audited actions record that person instead of
`admin`, and a request that arrives without the header is refused,
which is what stops a caller reaching the admin surface around the
proxy. The token check still runs first, so an identity header alone
grants nothing.

The proxy holds the admin token and injects it, so the people behind
it never handle the shared secret:

```
proxy_set_header x-copal-admin-token $copal_admin_token;
proxy_set_header x-forwarded-email   $authenticated_email;
```

Bind the admin surface where only the proxy reaches it
(`COPAL_ADMIN_BIND` on an interface the proxy shares), because a
header is a claim, and the network is what makes the claim
trustworthy. With the variable unset, operator actions are attributed
to the deployment.

## The console

`/admin/console` on the admin surface is the operator console.
Browsers cannot send `x-copal-admin-token`, so the console
authenticates with HTTP Basic: any username, the admin token as the
password, compared in constant time with the previous token honored
during a rotation. It exists only when an admin token is configured,
and because it rides the admin router, a separate `COPAL_ADMIN_BIND`
keeps it off the tenant-facing network.

The console names who is signed in when an identity header is
configured, and its authentication dialog says what to type. The
person it names is also the principal the dispatcher carries, so a
guard that reads the subject sees the operator.

The deployment home lists every tenant with files and bytes, and tails
the audit trail. With `COPAL_CONSOLE_FLEET=true` it also walks the
shared engine's other namespaces, read-only: one SurrealDB server
carrying several services shows each sibling's databases, tables, and
row counts on the same page. Every query is a self-addressing compound
on a private connection, so Copal's own session is never touched. The
walk needs a remote engine; on an embedded engine there are no
siblings, and the section says so.

Each tenant links into the contract pages. The same declaration that
renders the OpenAPI document, the GraphQL schema, and the MCP manifest
renders the console's listings, detail pages, action forms, and query
panels, and every read and submitted form runs the dispatcher chain
the API faces use, so the console shows and refuses exactly what an
all-scoped caller of that tenant would see.

The pages are server-rendered HTML that load no external assets: the
stylesheet and two small scripts (the theme switch and the reference
page's helpers) are inlined from Kayak. Without scripting the pages
still work, following the browser's light or dark preference. The
console works air-gapped and over the plainest tunnel.

## copalctl

`copalctl` (the `copal-cli` crate) is the service from a terminal.
Identity comes from the environment: `COPAL_URL`, then `COPAL_TOKEN`
(a minted key) or `COPAL_TENANT` (header-mode identity), and
`COPAL_ADMIN_TOKEN` for the admin commands. Output is always JSON, so
it composes with jq.

```
copalctl status
copalctl files create docs/spec.pdf --content-type application/pdf
copalctl files upload <id> ./spec.pdf
copalctl files list --state ready
copalctl files download <id> -o ./spec.pdf
copalctl search "signing key rotation" --limit 5
copalctl admin tenants
copalctl admin audit --limit 100
copalctl admin keys acme mint --name ci
```

`copalctl admin tenants` reads `GET /v1/admin/tenants`, the same
population query the console's deployment home uses.

`copalctl top` shows the deployment live: one screen with the figure
strip (tenants, files, bytes), the tenant population, and the audit
tail, polled on an interval (`--interval`, default 5 seconds). Press
`q` to leave. Rendering is a pure function over a snapshot, which is
what the golden tests draw.

## Audit export

The audit trail leaves the deployment through one admin endpoint,
shaped for SIEM collectors:

```
GET /v1/admin/audit/export?limit=500&cursor=<checkpoint>&tenant=<optional>
```

The body is NDJSON, one audit event per line, ascending over
`(created_at, id)` across every tenant (or one, with `tenant=`). The
next checkpoint rides the `x-copal-next-cursor` response header on
every page that carries rows; the body stays pure events, so a
collector appends it to its pipeline untouched. An empty body with no
header means the checkpoint is current. Store the header value and
present it as `cursor=` on the next poll. The walk is keyset-paged, so
a resume sees every later event at least once and re-reads nothing on
the normal path. `idx_audit_created` carries the scan.

A minimal collector is a loop:

```sh
CKPT=$(cat audit.ckpt 2>/dev/null)
curl -s -D /tmp/h "http://copal:8080/v1/admin/audit/export?limit=500${CKPT:+&cursor=$CKPT}" \
  -H "x-copal-admin-token: $COPAL_ADMIN_TOKEN" >> audit.ndjson
NEXT=$(tr -d '\r' < /tmp/h | awk 'tolower($1)=="x-copal-next-cursor:" {print $2}')
[ -n "$NEXT" ] && printf '%s' "$NEXT" > audit.ckpt
```

Run it from cron or a vector or fluent-bit exec source; the checkpoint
file is the only state. Rows are immutable inside the engine (an
UPDATE or DELETE against one aborts), so an exported line never goes
stale, and `copal_audit_exported_total` counts what has left through
this face.

## Maintenance sweeps

One interval loop (`COPAL_SWEEP_INTERVAL_SECS`) runs the maintenance
passes. Instances elect a sweep leader per pass through a database
lease named `sweeps`; the others skip the pass, and a crashed leader's
lease expires so another instance takes over. Each pass below is
failure-isolated: an error logs and the next pass still runs.

1. Reap expired upload claims to `failed` (retryable).
2. Return expired run claims to `pending`, so another worker replays
   them.
3. Fail stale scans: files in `scanning` longer than
   `COPAL_SCAN_STALE_SECS` with no pending or running run.
4. Delete rate-ledger windows older than two minutes (only the
   `store` ledger writes them).
5. Discard S3 multipart sessions older than
   `COPAL_TUS_SESSION_TTL_SECS`, with their parts.
6. Discard tus sessions older than `COPAL_TUS_SESSION_TTL_SECS`, with
   their staged bytes.
7. Delete staging entries on the local backend older than
   `COPAL_STAGING_TTL_SECS`.
8. Recompute tenant usage counters from the file rows, for up to 500
   tenants per pass.
9. Garbage-collect blobs: mark rows whose freshly derived reference
   count is zero, wait out `COPAL_GC_GRACE_SECS`, recount, then
   collect.

The same leader then runs the tiering classifier, which refreshes the
tiering gauges, and the tier mover, which starts up to
`COPAL_TIER_MOVE_BATCH` moves.

GC deletes the database row first, then the object bytes (from the
residency's primary backend and every tier it configures), with a
final row existence check between the two: identical content
re-registered inside that window aborts the collection, and the fresh
bytes stay.

Some background work runs outside the sweep loop, on every instance:
the flow worker, the webhook dispatcher (when a master key is set),
the embedding backfill every 60 seconds (when an embedding service is
set), and the master key re-seal sweep every five minutes during a
rotation.

## Runbook: files stuck in scanning

`scanning` is transient. If files sit there:

1. `GET /v1/runs?status=failed` and look for `post_upload` runs naming
   the file. A terminal pipeline failure moves the file to `failed` on
   its own; a file still in `scanning` usually means the run is
   pending or running.
2. A failed run retries with `POST /v1/runs/{id}/retry` once the cause
   (for example, unreadable blob storage) is fixed. The file returns
   to `scanning`, a worker finishes the journaled steps, and the
   record finalizes.
3. Orphans with no run at all (a crash between upload completion and
   enqueue) are failed automatically by the stale-scan sweep. A failed
   file with a digest keeps serving its previous content and accepts a
   re-upload.

## Backup and restore

Back up the blob plane first, then the metadata plane. That order
makes every metadata reference in the backup point at bytes the
backup already holds; anything uploaded between the two passes is
absent from both. The reverse order can capture records whose bytes
are missing.

The GC grace period (`COPAL_GC_GRACE_SECS`, default one day) covers
the other direction: a blob unreferenced at backup time still has its
bytes for the full grace window, so a restore inside that window never
resurrects a record whose content is gone.

Restore in the same order: bytes into the blob backends, then the
SurrealDB import, then start the server (schema application is
idempotent). Run one maintenance pass afterward; derived reference
counts recompute on their own. [backup.md](backup.md) has the full
procedure and the restore drill.

## Metrics

`GET /metrics` on the admin surface serves Prometheus text, guarded by
the admin token, because request volumes and error rates are operator
data. Deployments that set `COPAL_ADMIN_BIND` keep the scrape off the
tenant-facing network along with key custody.

| Series | Meaning |
| --- | --- |
| `copal_http_responses_total{class}` | Responses by status class, counted at the outermost layer, so timeouts and refusals are included. |
| `copal_http_request_duration_seconds_sum` / `_count` | Total served time and request count; their ratio is the mean. The process computes no buckets, so no quantiles are reported. |
| `copal_uploads_completed_total`, `copal_uploaded_bytes_total` | Content finished through the shared finalize path, whichever face carried it. |
| `copal_quota_refusals_total` | Uploads refused for exceeding a tenant ceiling. |
| `copal_precondition_refusals_total` | Conditional writes refused with 412, on REST and S3. |
| `copal_retention_refusals_total` | Retention changes refused because a compliance clock only extends. |
| `copal_webhook_deliveries_total{outcome}` | Delivery attempts by outcome: delivered, retry, failed. |
| `copal_feed_reads_total` | Reads of the `GET /v1/events` feed. |
| `copal_mcp_calls_total`, `copal_mcp_errors_total` | MCP requests, and those that answered with an error. |
| `copal_blobs_collected_total`, `copal_reaped_uploads_total`, `copal_reaped_runs_total` | Sweep work, accumulated across passes. |
| `copal_backfill_refreshed_total` | Passages re-embedded by the embedding backfill after a model change. |
| `copal_resealed_total`, `copal_secrets_resealed_total` | Rotation progress: objects and database secrets moved under the current master key. Quiet means the rotation has drained. |
| `copal_audit_exported_total` | Audit events served through the export face, accumulated across pages. |
| `copal_transforms_total`, `copal_transform_refusals_total` | External transform outcomes: derivations that landed, inputs the service refused. |
| `copal_fetches_total` | URL ingestions that landed content. |
| `copal_renditions_inline_total` | Renditions derived inline by the rendition URL route. |
| `copal_tiering_candidate_blobs`, `copal_tiering_candidate_bytes` | Gauges: what the tiering classifier would move, distinct blobs and their bytes, refreshed each sweep pass. `copal_tiering_candidate_bytes{tenant}` carries the per-tenant view. |
| `copal_tiering_would_recall_blobs` | Gauge: blobs old enough to move whose recent reads hold them, the measured stand-in for the recall-rate assumption in the tiering cost model. |
| `copal_tiering_demoted_total`, `copal_tiering_promoted_total` | Mover flips that landed, each after a digest-verified copy. |
| `copal_tiering_erased_total{copy}` | Displaced copies erased after the grace: `hot` after a demotion settles, `cold` after a promotion settles. |
| `copal_tiering_verify_failures_total` | Copies whose read-back digest disagreed with the row id. The bad copy is deleted and the move retries next pass; a non-zero rate here is a storage problem to investigate. |
| `copal_tiering_recalled_total` | Archive recalls that promoted bytes home. |

Counters are process-local and reset on restart, which is what
Prometheus expects; the database holds the durable truth for
everything they summarize.

## Traces

Set `COPAL_OTLP_ENDPOINT` and every served request becomes a span
(`{method} {route}`, using the route template so ids stay out of
names, with the status recorded on completion) exported over OTLP
http/protobuf under `service.name=copal`. A caller's `traceparent`
header joins its trace. The deliveries, transformer calls, and fetches
a request causes carry the context outward, so the collector shows
cause and effect as one tree across services.

Spans batch on the SDK's cadence, so a hard kill can lose the tail of
the last batch. The log subscriber runs either way, and without the
endpoint nothing leaves the process.

## Health endpoints

`/healthz` is liveness: the process answers. `/readyz` is readiness:
one store round trip plus one blob-plane call must both succeed, so
orchestrators gate traffic on real dependencies. The container image
health check uses `/readyz`.

## Deployment security posture

Development defaults are open on purpose. An exposed deployment:

- sets `COPAL_AUTH_MODE=keys` with `COPAL_ADMIN_TOKEN`;
- sets `COPAL_ADMIN_BIND` so custody lives off the tenant network;
- terminates TLS in front of the server;
- scrubs grant and edge URLs from proxy logs (tokens ride the path by
  design; TTLs bound the exposure);
- runs the database as a scoped user, with `COPAL_DB_USER` and
  `COPAL_DB_PASS` set;
- keeps a request-rate limit at the proxy.

Copal meters each authenticated caller against its built-in rate
classes (reads at 6000 units a minute, mutations at 600) and enforces
per-tenant storage quotas set on the admin surface. Those budgets are
charged after authentication and per caller, and in header mode every
caller shares one bucket. A proxy limit is still the right place to
absorb a flood of requests before it reaches the process.

What the service enforces on its own: uniform 401s across the whole
credential path, timing included; server-owned scan verdicts;
`nosniff` and sanitized `Content-Disposition` with forced download for
script-capable types; `no-store` on private and grant bytes; access
levels at the byte boundary; rate classes and quotas; the
engine-immutable audit trail.
