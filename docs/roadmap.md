# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md); this file carries only open items, so
an item disappearing from it means the item is done.

The three streams below are independent. They touch disjoint code, so
they proceed on parallel branches instead of queueing behind one
another's CI.

## Stream 1: the governance tier

From the July 2026 Janus review. Janus enforces shape; it does not yet
enforce identity or consumption. The error vocabulary has no 429 or
413, so the GraphQL face downgrades a payload refusal to 400. Depth
and complexity ceilings are hand-wired outside the contract, invisible
to the artifacts and the differ. `FieldExposure` is binary, with no
principal below the tenant. Nothing meters requests at the one layer
that can meter GraphQL accurately, the dispatcher, because one POST
can carry many aliased operations. Every seam the tier needs already
exists: middleware over dispatch, the typed context, the completeness
gate, the differ.

The design: policies as named references in the IR, enforced as
dispatcher projection so REST, GraphQL, and subscriptions inherit
them identically, with the differ treating any tightening as a named
breaking change. In order:

1. **Error variants and contract-declared limits.** `TooManyRequests`
   and `PayloadTooLarge` join the vocabulary, which fixes the live
   downgrade. Depth and complexity ceilings move into the contract,
   where the served schema applies them and the differ tracks them.
2. **Principal and scopes.** A principal below the tenant, carried in
   the context; resources and actions declare required scopes; the
   completeness gate refuses a declared scope nothing enforces. On
   the Copal side this is what turns a `ck1` key into (scopes,
   expiry, rate class) instead of all-or-nothing. Absorbs the
   key-scopes item and the `COPAL_AUTH_MODE` default flip.
3. **Rate classes and the dispatch limiter**, charging complexity
   units against a pluggable store, so a tenant's ceiling holds
   whether requests arrive as REST calls or as one POST of aliased
   GraphQL operations.
4. **Field guards as dispatcher projection**, applied to search
   results too. A guarded field renders nullable on every generated
   surface and is omitted when redacted; a caller who cannot see a
   column cannot filter on it. Search projection is the headline:
   permission-aware retrieval at the storage layer, which the
   two-layer object-store-plus-vector-database stack cannot offer
   without rebuilding authorization in glue code.
5. **The tail**: stream re-auth and per-principal watch caps,
   persisted operations, and the same declared policies compiled into
   SurrealDB `PERMISSIONS` as a second, independent refusal layer.

## Stream 2: the migration wedge

MinIO's community edition was archived in 2026, and its recommended
replacements are plain object stores. Copal can take those users only
if their existing tooling works, and today it does not.

6. **CopyObject and batch DeleteObjects** on the S3 gateway.
   `mc mirror`, `rclone sync`, and `aws s3 sync` need both. Copy is
   cheap here: content-addressed storage makes a server-side copy a
   new record over the same blob, with no byte movement.
7. **A documented migration path from MinIO**, once the tools run
   against the gateway end to end.

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
