# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md); this file carries only open items, so
an item disappearing from it means the item is done.

The second audit's program and its tail are complete. Retention,
principals, blob master key rotation, audit export to a SIEM, the
external transformer seam, rendition URLs and URL ingestion, and the
GCS and Azure backends all shipped and left this file. What follows
came out of reading the service against what the market offers.

1. **Per-chunk authorization, if anything asks for it.** A chunk is
   withheld exactly when its file is, which is all-or-nothing: a
   document with one sensitive passage is readable or it is not.
   Splitting that finer needs something to decide which passages are
   sensitive, and nothing does. An earlier version of this file said
   the coarse behaviour was correct; it was not enforced at all until
   the CHANGELOG's entry on retrieval and access, and the fix is what
   makes it coarse rather than absent. What holds this open is the
   absence of a use case; the work itself is understood.

   A classifier seam is the shape that most resembles the rest of the
   service and is the one to resist. Every seam here fails safe when
   it is absent: no extractor means no text, no reranker means the
   fused order, no custody means the server refuses to boot. One
   deciding confidentiality would be the first whose absence leaks.

2. **Scale evidence above 256 MiB.** The largest object measured is
   256 MiB, in the encryption overhead figures in
   [operations.md](operations.md). Multipart assembly and ranged reads
   above that size are untimed, and so is the memory a scan holds at
   that scale. The ceiling is an absence of evidence; no limit has
   been found.

3. **Lifecycle and tiering.** Objects live at one cost forever. A
   policy that moves cold bytes to cheaper storage, and the recall
   path back, is the operating expense every hosted service competes
   on. Retention already carries the policy vocabulary this would
   extend.

4. **Multi-region replication.** Residencies place bytes; nothing
   copies them. A second region is currently a second deployment.

5. **The embedded tier.** One process with the engine in it, as a
   positioned single-replica mode. Docker already answers deployment
   simplicity; what embedding buys is the wire's removal: every
   repository call stops being a round trip, caller sessions become
   near-free, and the `Session not found` failure class cannot
   exist. Parked until someone measures the round-trip cost and
   cares.

6. **F16 vectors and DiskANN, blocked upstream.** The server's 3.1
   release added both; the newest published `surrealdb` crate parses
   neither. Probed directly, every form refuses. The available half
   shipped (F32 HNSW). Revisit when the crate catches up.

## Deferred, with reasons

7. **OTel spans.** OTLP export means new dependencies and a cargo
   feature CI would not compile, which is untested code by
   construction. The `/metrics` endpoint set a dependency-free
   observability precedent this would break.
8. **SDK publishing pipelines** for the four generated clients.
   Generated and drift-gated already; publishing is packaging work
   that wants a release cadence to hang from.
9. **Janus REST runtime router.** Copal's REST handlers carry real
   behavior (streaming, ranges, conditionals) that a generic router
   has to earn the right to replace.
10. **The batched `surql-rs` release.** Shon cuts it. The branch
    carries the live-query `WHERE` clause, the session-scoped live
    query fix, index-backed KNN, the scan-order correction, caller
    sessions with the record-identity guard, the shared-session
    client model, `OVERWRITE` rendering, and the live-database
    reconciliation layer (analyzer parsing, echo-shape fixes, and
    apply-safe diffs) that schema evolution stands on.
