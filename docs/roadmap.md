# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md); this file carries only open items, so
an item disappearing from it means the item is done.

1. **Retention, legal hold, and WORM: built.** All five slices of
   [retention.md](retention.md) shipped: the erasable predicate at
   the GC, the admin surface with the compliance authority line,
   tenant policy stamped at creation with `keep_last` pruning, the
   engine-side delete clause, and retained bytes beside usage.

2. **Principals within tenants, designed.**
   [principals.md](principals.md) carries the design: keys become
   credentials belonging to named actors, the token gains a `pr`
   claim, and ownership becomes expressible in compiled
   `PERMISSIONS`, which is what lets the engine layer say what the
   application layer says. The first two slices are additive, so a
   deployment with no principals behaves exactly as it does today.

3. **Blob master key rotation.** The engine access key rotates by
   replacement; the content encryption key has no rotation path.
4. **Audit export.** `audit_event` rows exist and list; a SIEM
    export or streaming shape does not.
5. **External transformer seam** for transcodes and document
    renditions, following the extractor and embedding seams: a
    contract several self-hostable implementations speak,
    unconfigured means absent, nothing parsed in-process.
6. **On-the-fly rendition URLs and multi-source ingestion**, the
    axes the hosted services sell.
7. **More blob backends.** GCS and Azure beside the filesystem and
    S3-compatible stores.
8. **The embedded tier.** One process with the engine in it, as a
    positioned single-replica mode. Docker already answers deployment
    simplicity; what embedding buys is the wire's removal: every
    repository call stops being a round trip, caller sessions become
    near-free, and the `Session not found` failure class cannot
    exist. Parked until someone measures the round-trip cost and
    cares.
9. **F16 vectors and DiskANN, blocked upstream.** The server's 3.1
    release added both; the newest published `surrealdb` crate parses
    neither. Probed directly, every form refuses. The available half
    shipped (F32 HNSW). Revisit when the crate catches up.

## Deferred, with reasons

10. **OTel spans.** OTLP export means new dependencies and a cargo
    feature CI would not compile, which is untested code by
    construction. The `/metrics` endpoint set a dependency-free
    observability precedent this would break.
11. **SDK publishing pipelines** for the four generated clients.
    Generated and drift-gated already; publishing is packaging work
    that wants a release cadence to hang from.
12. **Janus REST runtime router.** Copal's REST handlers carry real
    behavior (streaming, ranges, conditionals) that a generic router
    has to earn the right to replace.
13. **The batched `surql-rs` release.** Shon cuts it. The branch
    carries the live-query `WHERE` clause, the session-scoped live
    query fix, index-backed KNN, the scan-order correction, caller
    sessions with the record-identity guard, the shared-session
    client model, `OVERWRITE` rendering, and the live-database
    reconciliation layer (analyzer parsing, echo-shape fixes, and
    apply-safe diffs) that schema evolution stands on.
