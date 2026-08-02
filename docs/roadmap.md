# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md); this file carries only open items, so
an item disappearing from it means the item is done.

1. **Retention, legal hold, and WORM, designed.**
   [retention.md](retention.md) carries the design: retention and
   holds attach to versions, one `erasable` predicate gates the three
   places that erase, and `compliance` mode is the whole of WORM
   because the difference from `governance` is authority rather than
   mechanism. The build order is in the document; the first slice
   (two columns, the hold flag, and the predicate consulted by the
   GC) makes retention real, because the GC is the only thing that
   erases today.

2. **The pushdown tail.** The S3 gateway still runs on the service
   session; adoption mirrors the REST and GraphQL faces. After it, a
   session cache amortizes the per-request open cost once a
   deployment has watched it; sessions carry no engine expiry, so
   cache lifetime is Copal policy alone.

3. **A backup and restore story.** Two stores (metadata plane, blob
   root) and no written procedure for a coherent snapshot or a
   restore drill. A storage product without a stated recovery
   procedure is not one yet.

4. **A performance envelope.** No published numbers exist: ingest
   throughput, retrieval latency, gateway baseline against MinIO,
   caller-session overhead. A bench harness turns "watch before you
   trust it" into something an operator can actually do.

5. **Principals within tenants.** The key is the smallest identity
   today. Per-human and per-agent principals unlock richer field
   guards, audit attribution, and the identity story agent
   deployments increasingly expect.

## Then

6. **Blob master key rotation.** The engine access key rotates by
   replacement; the content encryption key has no rotation path.
7. **Audit export.** `audit_event` rows exist and list; a SIEM
    export or streaming shape does not.
8. **External transformer seam** for transcodes and document
    renditions, following the extractor and embedding seams: a
    contract several self-hostable implementations speak,
    unconfigured means absent, nothing parsed in-process.
9. **On-the-fly rendition URLs and multi-source ingestion**, the
    axes the hosted services sell.
10. **More blob backends.** GCS and Azure beside the filesystem and
    S3-compatible stores.
11. **The embedded tier.** One process with the engine in it, as a
    positioned single-replica mode. Docker already answers deployment
    simplicity; what embedding buys is the wire's removal: every
    repository call stops being a round trip, caller sessions become
    near-free, and the `Session not found` failure class cannot
    exist. Parked until someone measures the round-trip cost and
    cares.
12. **F16 vectors and DiskANN, blocked upstream.** The server's 3.1
    release added both; the newest published `surrealdb` crate parses
    neither. Probed directly, every form refuses. The available half
    shipped (F32 HNSW). Revisit when the crate catches up.

## Deferred, with reasons

13. **OTel spans.** OTLP export means new dependencies and a cargo
    feature CI would not compile, which is untested code by
    construction. The `/metrics` endpoint set a dependency-free
    observability precedent this would break.
14. **SDK publishing pipelines** for the four generated clients.
    Generated and drift-gated already; publishing is packaging work
    that wants a release cadence to hang from.
15. **Janus REST runtime router.** Copal's REST handlers carry real
    behavior (streaming, ranges, conditionals) that a generic router
    has to earn the right to replace.
16. **The batched `surql-rs` release.** Shon cuts it. The branch
    carries the live-query `WHERE` clause, the session-scoped live
    query fix, index-backed KNN, the scan-order correction, caller
    sessions with the record-identity guard, the shared-session
    client model, `OVERWRITE` rendering, and the live-database
    reconciliation layer (analyzer parsing, echo-shape fixes, and
    apply-safe diffs) that schema evolution stands on.
