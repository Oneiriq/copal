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

Per-chunk authorization retired next, through the design in
[design/per-chunk-authorization.md](design/per-chunk-authorization.md):
per-upload markers put the sensitivity answer with the uploader, the
only party who knows, and every fail direction lands on withholding
or on yesterday's behavior. The classifier seam this file warned
against stayed rejected; the CHANGELOG carries the details.

1. **Lifecycle and tiering: the mover, then recall.** The design
   ([design/lifecycle-tiering.md](design/lifecycle-tiering.md))
   ships in three stages and the first is done: tiers configure and
   validate inside their residency, the per-tenant policy row landed
   beside retention and storage, byte reads record day-coarse
   recency on the blob row, and the observe-only classifier reports
   candidates, bytes, and the measured would-be recall rate each
   sweep pass while touching nothing. What remains is stage two --
   the mover for online classes (copy, verify, flip, grace-erase,
   with the symmetric promote, the GC/backup/reseal legs, and the
   restore drill's per-tier check) -- and stage three, recall, which
   unlocks archive classes with the 202-and-run answer on REST and
   `InvalidObjectState` plus `RestoreObject` on the S3 face. The
   observe stage should run against real traffic long enough to
   check the cost model's recall assumption before stage two moves
   a byte.

2. **Multi-region replication.** Residencies place bytes; nothing
   copies them. A second region is currently a second deployment.

3. **The embedded tier.** One process with the engine in it, as a
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

4. **OTel spans.** OTLP export means new dependencies and a cargo
   feature CI would not compile, which is untested code by
   construction. The `/metrics` endpoint set a dependency-free
   observability precedent this would break.
5. **SDK publishing pipelines** for the four generated clients.
   Generated and drift-gated already; publishing is packaging work
   that wants a release cadence to hang from.
6. **Janus REST runtime router.** Copal's REST handlers carry real
   behavior (streaming, ranges, conditionals) that a generic router
   has to earn the right to replace.
