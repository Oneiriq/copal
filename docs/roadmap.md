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
Slices 1 through 4 shipped in Janus, and the declarations release
turned scopes and rate metering on across both Copal faces against
one ledger. What remains of the stream:

The first guard shipped end to end: version attribution is visible to
admin-scoped keys and redacts identically on both faces through the
shared projection API. Stream lifetimes, watch ceilings, and the
fleet-shared consumption ledger shipped as well. Remaining:

2. **Persisted operations and PERMISSIONS pushdown**: an allowlist of
   known GraphQL documents, and the declared policies compiled into
   SurrealDB `PERMISSIONS` as a second, independent refusal layer.
3. **More guards as the data model earns them.** Per-field policy is
   in place; principals within tenants are what will make richer
   guards meaningful.

## Stream 2: the migration wedge

MinIO's community edition was archived in 2026, and its recommended
replacements are plain object stores. Copal can take those users only
if their existing tooling works, and today it does not.

The wedge shipped whole: CopyObject and batch DeleteObjects landed on
the gateway, and [migration.md](migration.md) documents the one-run
move with `mc mirror`, `rclone sync`, or `aws s3 sync`. What remains
of this stream is evidence rather than code: running the mirror
against a live MinIO deployment and recording the result.

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
