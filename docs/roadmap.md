# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md); this file carries only open items, so
an item disappearing from it means the item is done.

The second audit's program and its tail are complete. Retention,
principals, blob master key rotation, audit export to a SIEM, the
external transformer seam, rendition URLs and URL ingestion, and the
GCS and Azure backends all shipped and left this file. Scale
evidence above 256 MiB retired too: `bench/scale.sh` measured the
streamed PUT, multipart assembly, ranged reads, and the scan pass at
512 MiB, 1 GiB, and 2 GiB and found no wall (throughput flat,
streaming memory flat, ranged reads offset-independent), with one
cost named rather than discovered later: the scan pass transiently
holds about three times the object in memory, which
[operations.md](operations.md) now states beside the tables. What
follows came out of reading the service against what the market
offers.

1. **Per-chunk authorization.** A chunk is withheld exactly when its
   file is, which is all-or-nothing: a document with one sensitive
   passage is readable or it is not. An earlier version of this file
   said the coarse behavior was correct; it was not enforced at all
   until the CHANGELOG's entry on retrieval and access, and the fix is
   what makes it coarse rather than absent.

   The demand is real. A contract with a public summary and
   confidential pricing, a record whose header is broadly readable and
   whose body is not, a manual serving several audiences, a passage
   carrying personal data that should not reach everyone cleared for
   the rest of the file: each of these is one document today and one
   access level today. The workaround is to split the document into
   several files, and it is weaker than it sounds here, because
   chunking is automatic. Someone who uploads one PDF gets passages
   they cannot govern individually, and pre-splitting by hand gives up
   what automatic extraction and embedding were for.

   What holds this open is a design decision about where the sensitivity
   comes from, since nothing in the upload path, the extractor, or the
   pipeline knows. Per-upload markers put the answer with the uploader,
   who does know, and they fail safe: no markers means today's
   behavior.

   A classifier seam is the shape that most resembles the rest of the
   service and is the one to resist. Every seam here fails safe when
   it is absent: no extractor means no text, no reranker means the
   fused order, no custody means the server refuses to boot. One
   deciding confidentiality would be the first whose absence leaks.

2. **Lifecycle and tiering.** Objects live at one cost forever. A
   policy that moves cold bytes to cheaper storage, and the recall
   path back, is the operating expense every hosted service competes
   on. Retention already carries the policy vocabulary this would
   extend.

3. **Multi-region replication.** Residencies place bytes; nothing
   copies them. A second region is currently a second deployment.

4. **The embedded tier.** One process with the engine in it, as a
   positioned single-replica mode. Docker already answers deployment
   simplicity; what embedding buys is the wire's removal: every
   repository call stops being a round trip, caller sessions become
   near-free, and the `Session not found` failure class cannot
   exist. The round-trip cost is measured now
   ([operations.md](operations.md) carries the table): on the same
   host, with the same engine version on both sides, the wire adds
   roughly 0.5 ms to a point read, 0.9 ms to a claim, and 1.6 ms to
   a 100-row listing, per repository call, before any real network
   distance. What remains parked is the other half of the sentence:
   whether anyone cares enough for a positioned mode.

## Deferred, with reasons

5. **OTel spans.** OTLP export means new dependencies and a cargo
   feature CI would not compile, which is untested code by
   construction. The `/metrics` endpoint set a dependency-free
   observability precedent this would break.
6. **SDK publishing pipelines** for the four generated clients.
   Generated and drift-gated already; publishing is packaging work
   that wants a release cadence to hang from.
7. **Janus REST runtime router.** Copal's REST handlers carry real
   behavior (streaming, ranges, conditionals) that a generic router
   has to earn the right to replace.
