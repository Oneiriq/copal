# Performance envelope: Copal dev

Produced by `bench/run.sh`, measured through the S3 gateway with
stock tooling, because that is the path a migrating deployment
uses. Two passes: caller-bound engine sessions off, then on, so
the cost of the second enforcement layer is a column rather than
a guess.

Machine: Windows AMD64, single-node stack,
all containers on one host. Timings are the best of three samples
with the spread beside them; where the spread is wider than the
gap between the two columns, the honest reading is that this
machine cannot resolve the difference.

| measurement | sessions off | sessions on | unit |
| --- | --- | --- | --- |
| ingest_64mib_ms | 414 (+55) | 420 (+43) | ms |
| ingest_50_small_ms | 208 (+128) | 212 (+16) | ms |
| retrieve_64mib_ms | 191 (+28) | 194 (+15) | ms |
| retrieve_50_small_ms | 101 (+33) | 98 (+34) | ms |
| listing_ms | 22 (+6) | 19 (+3) | ms |
| ingest_throughput | 154 | 152 | MiB/s |
| ingest_small_objects | 240 | 235 | objects/s |
| retrieve_throughput | 335 | 329 | MiB/s |
| retrieve_small_objects | 495 | 510 | objects/s |
