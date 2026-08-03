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
| ingest_64mib_ms | 378 (+8) | 380 (+25) | ms |
| ingest_50_small_ms | 197 (+13) | 200 (+11) | ms |
| retrieve_64mib_ms | 169 (+11) | 169 (+15) | ms |
| retrieve_50_small_ms | 125 (+1) | 112 (+16) | ms |
| listing_ms | 18 (+7) | 18 (+5) | ms |
| search_ms | 3 | 2 (+1) | ms |
| mcp_files_list_ms | 4 | 4 | ms |
| ingest_throughput | 169 | 168 | MiB/s |
| ingest_small_objects | 253 | 250 | objects/s |
| retrieve_throughput | 378 | 378 | MiB/s |
| retrieve_small_objects | 400 | 446 | objects/s |
