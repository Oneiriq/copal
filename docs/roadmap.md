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
   This unlocks the data-residency story below.
5. Per-tenant quotas and usage accounting (bytes stored, request
   rates), replacing the documented proxy-level interim.

## Tier 3: moat expansion

In leverage order, from the July 2026 market research: MinIO's
community edition is archived, the S3-only alternatives have no
metadata brain, Supabase Storage is platform-bound, and SurrealDB 3.0
ships file primitives without a service layer.

1. tus resumable uploads. The browser-ecosystem standard and the
   loudest feature gap against Supabase Storage.
2. S3-compatible ingest gateway. Existing S3 tooling points at Copal
   and gains metadata, pipelines, and contracts without a client
   rewrite; the window is open while SurrealDB's own S3 bucket API
   remains an open issue.
3. Eventing: LIVE SELECT drives webhooks and GraphQL subscriptions.
   Database-native live queries make real-time file events cheap in a
   way S3-family competitors cannot copy without changing databases.
4. Derivatives on the existing flow engine: thumbnails and transcodes
   as registered workflows, removing the recurring hosted-transform
   tax that drives the alternatives market.
5. Per-tenant BYO bucket over OpenDAL: each tenant's bytes in their
   own bucket and keys. The data-residency wedge.
6. `cg2` HMAC edge tokens for CDN-edge verification without a
   database hop, beside the stateful `cg1` family.

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
