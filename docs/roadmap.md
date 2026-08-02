# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md); this file carries only open items, so
an item disappearing from it means the item is done.

The three streams below are independent. They touch disjoint code, so
they proceed on parallel branches instead of queueing behind one
another's CI.

## Stream 1: the governance tier

From the July 2026 Janus review. The design: policies as named
references in the IR, enforced as dispatcher projection so REST,
GraphQL, and subscriptions inherit them identically, with the differ
treating any tightening as a named breaking change. The tier shipped:
error variants, contract-declared ceilings, principals and scopes,
rate classes metering at the dispatcher, field guards behind the
shared projection API, watch slots, stream lifetimes, the
fleet-shared ledger, and persisted operations, all live on both faces
from one contract. What remains:

1. **More guards as the data model earns them.** Per-field policy is
   in place; principals within tenants are what will make richer
   guards meaningful.

2. **`PERMISSIONS` pushdown, revived by probe.** An earlier note here
   dispositioned pushdown on the claim that the engine cannot tell
   Copal's callers apart. The conclusion was wrong.
   `engine_sessions.rs` pins the mechanism it missed: a cloned handle
   is its own engine session over the same connection, a record
   access JWT binds a caller identity to it, and table and field
   `PERMISSIONS` then filter engine side while the root handle beside
   it keeps full authority. The mechanism is in place end
   to end: `DatabaseClient::caller_session` opens the per-caller
   session and refuses tokens `PERMISSIONS` would never filter;
   every Copal table carries engine permissions by a mechanical rule
   (tenant tables admit only the token's tenant, tables without a
   tenant column are closed to caller sessions); the record access
   method applies with `COPAL_ENGINE_ACCESS_KEY`, rotating by
   replacement; token minting mirrors the scope model; and a parity
   test holds engine field guards equal to contract field guards.
   The request path started
   adopting: `COPAL_ENGINE_SESSIONS=on` routes the files resource's
   repository calls through caller sessions minted after
   authentication, proven on the REST face with the guarded column
   arriving engine-redacted. The sweep reached every handler
   with direct tenant-scoped store access: search, text, usage,
   renditions and run listings, version downloads, and the tus
   trio. Three surfaces stay on the service store by architecture:
   content uploads link blob rows, and blobs are deduplicated across
   tenants, so the blob table cannot be tenant-scoped; the public
   download path reads unscoped before it can know a tenant; and the
   flow engine holds its own store handle. The helper-based handlers
   (grants, webhooks, edge) and every GraphQL resolver followed: the
   execute path seeds the caller session into the typed context, the
   resolvers read it back, and subscriptions carry the session for
   their whole lifetime, with the access method's session duration
   raised above the longest subscription so re-auth, never expiry,
   ends a stream. What remains here: the S3 gateway, and a session
   cache once a deployment has watched the per-request cost. Facts that
   bound the design:
   enforcement follows the actor, so record sessions are filtered
   even on a credential-less engine, whose exposure is the anonymous
   session acting as owner; only record sessions are filtered, and
   plain `TYPE JWT` access lands database-level sessions that bypass
   table permissions; refused writes return empty rows with no error,
   so the application layer stays the face that explains refusals.

## Stream 2: the migration wedge

MinIO's community edition was archived in 2026, and its recommended
replacements are plain object stores. Copal can take those users only
if their existing tooling works, and the evidence now exists: the
migration guide carries a recorded `mc mirror` run end to end, with a
no-op second pass, an empty `mc diff`, and an 80 MiB multipart object
round-tripping byte-identical. Getting there surfaced and fixed four
gateway defects no in-process test had reached, which is the argument
for evidence runs as a practice. Remaining here:

1. **A compose file for the demo stack.** The recorded run hand-built
   its stack; a `docker compose up` that yields MinIO, Copal, and a
   seeded mirror would let a prospect replay it in minutes.

## Stream 3: retrieval cost

8. **F16 vectors and DiskANN, blocked upstream.** The server's 3.1
   release added both, and the newest published `surrealdb` crate
   (3.2.3, which is also the embedded engine tests run on) parses
   neither, along with the new distance metrics. Probed directly:
   every form refuses. The available half shipped: the HNSW index
   stores F32 instead of F64, since embedding models emit single
   precision at best. Revisit when the crate catches up to the
   server.

## Then

9. **Retention policies, legal hold, and WORM.** The compliance tier
   the enterprise buyers ask for. Retention interacts with the GC
   grace period and soft delete, so it needs design before code.
10. **External transformer seam** for transcodes and document
    renditions, following the pattern the extractor and embedding
    seams already set: pick a contract several self-hostable
    implementations speak, treat unconfigured as the feature being
    absent, and parse nothing in-process.
11. **On-the-fly rendition URLs and multi-source ingestion**, the
    axes the hosted services sell.
12. **More blob backends.** GCS and Azure beside the filesystem and
    S3-compatible stores, and the SurrealDB bucket backend as a
    first-class single-binary mode.
13. **Extracted text and search as contract shapes.** Text is a
    document rather than a collection, and search is a query rather
    than a listing, so neither fits the shapes the contract has.
    Either they gain shapes or the docs say plainly that they stay
    REST-only the way usage does. Search's answer interacts with
    stream 1, since field guards must project search results.

## Deferred, with reasons

14. **OTel spans.** Deferred twice. OTLP export means new
    dependencies and a cargo feature CI would not compile, which is
    untested code by construction. The `/metrics` endpoint set a
    dependency-free observability precedent this would break.
15. **SDK publishing pipelines** for the four generated clients.
    Generated and drift-gated already; publishing is packaging work
    that wants a release cadence to hang from.
16. **Janus REST runtime router.** Copal's REST handlers carry real
    behavior (streaming, ranges, conditionals) that a generic router
    has to earn the right to replace.
17. **The batched `surql-rs` release.** Shon cuts it. The branch
    carries the live-query `WHERE` clause, the session-scoped live
    query fix, index-backed KNN, and the scan-order correction.
