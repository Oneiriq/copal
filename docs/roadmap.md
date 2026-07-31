# Roadmap

Ordered by what matters: deployment hardening first, then the security
items that need design work, then the moat expansion the market
research points at, then platform depth. Items move to the top of
their tier when a deployment or a user needs them sooner.

## Tier 1: deployment hardening (gaps)

The items that gate running Copal anywhere real.

1. Request timeouts. A deadline on ordinary routes and a longer
   transfer deadline on the streaming routes, so slow-drip requests
   cannot hold claims and sockets open.
2. CORS. A strict allowlist layer driven by configuration, absent
   entirely when unconfigured, so browser applications can integrate
   without the server defaulting open.
3. A container image. Multi-stage build, locked dependencies,
   non-root runtime user, health check.
4. Graceful shutdown. Drain in-flight requests on SIGTERM; leases
   already make interrupted background work safe.
5. Readiness. `/readyz` probes the store and the blob plane;
   `/healthz` stays a liveness stub.
6. Client digest assertion. An `x-copal-digest` request header on
   upload verifies the stored content against what the client
   intended to send.
7. Version history completeness. Serve the recorded
   `metadata_snapshot` with each version.
8. Backup and restore documentation. Blob plane first, then the
   metadata export; the GC grace period covers the drift window.
9. Small closures: bounded admin listings, a fresh-attempt budget on
   run retry, sub-resource modeling in Janus (versions and grant
   revocation into the contract).

## Tier 2: security items needing design

1. Supply-chain gate: a dependency audit job in both CI pipelines.
2. Admin token rotation without restart: accept the previous token
   during a rotation window.
3. Audit forensics: record the forwarded origin on audit rows when
   the proxy provides one.
4. Per-tenant blob keys, building on the shipped encryption at rest
   (chunked AEAD, plaintext digests, non-destructive enablement).
   The data-residency story below depends on this.
5. Per-tenant byte quotas and usage accounting shipped (logical
   usage, pre-flight refusals, in-stream headroom clamps on every
   upload face). Request-rate limits remain at the proxy.

## Tier 3: moat expansion

Ordered by expected pull, from the July 2026 market research: MinIO's
community edition is archived, the S3-only alternatives have no
metadata brain, Supabase Storage is platform-bound, and SurrealDB 3.0
ships file primitives without a service layer. tus resumable uploads
shipped first from this tier, then the S3-compatible ingest gateway
(SigV4, the object plane, ListObjectsV2 with delimiter collapse,
sealed gateway credentials).

1. Eventing: the engine outbox and signed webhooks shipped (events
   born in the same transaction as the state change, LIVE SELECT as
   the dispatcher wake). Remaining: GraphQL subscriptions over the
   same outbox, which needs the Janus runtime subscription seam.
2. Derivatives: image renditions shipped on the flow engine
   (deterministic paths, idempotent repeats, refusals that fail the
   derived record with the run completed). Remaining: transcodes and
   documents, which need an external transformer seam.
3. Per-tenant storage residencies shipped: named OpenDAL backends
   (filesystem or S3-compatible bucket with its own keys), tenant
   pinning on the admin surface, per-row backend resolution so
   reassignment never strands content, residency-routed collection.
4. `cg2` HMAC edge tokens shipped: stateless capabilities under
   sealed tenant edge keys, verifiable at a CDN worker with no
   database hop, expiry-bounded with whole-key revocation. The tier
   is complete; what remains of it lives in the deferred seams above
   (GraphQL subscriptions, external transformers).

## Tier 5: post-moat audit items

From the July 2026 audit, in the order they matter:

1. Webhook destination policy and bounded image decoding (shipped).
2. S3 multipart upload (shipped): the aws CLI needs it above 8 MiB.
3. Signed upload URLs shipped: write capabilities in the `cg1`
   family, single-use, op-scoped so a read token cannot write.
4. Per-tenant blob keys (the sealed-object header names its key).
5. GraphQL parity for renditions, events, webhooks, usage, and edge
   tokens, plus subscriptions over the outbox.
6. Metrics and traces, then cached usage counters for large tenants.

## Tier 4: platform depth

1. Metrics and traces (`/metrics`, OTel spans).
2. Key scopes (read-only, upload-only) and key expiry.
3. Retention policies, legal hold, WORM for the compliance tier.
4. SDK publishing pipelines for the four generated clients.
5. Janus REST runtime router; contract-driven sub-resources.
6. Flip `COPAL_AUTH_MODE` default to `keys` at 1.0.
7. The batched `surql-rs` release once Copal and Janus stabilize.
8. S3, GCS, and Azure blob backends beyond the filesystem store;
   the SurrealDB bucket backend as a first-class single-binary mode.
