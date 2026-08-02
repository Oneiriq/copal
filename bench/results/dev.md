# Performance envelope: Copal dev

Produced by `bench/run.sh` on the single-node stack, measured
through the S3 gateway with stock tooling, because that is the
path a migrating deployment uses. Two passes: caller-bound engine
sessions off, then on, so the cost of the second enforcement
layer is a column rather than a guess.

Machine: Windows AMD64, containers on one host.
These are laptop-class numbers from a single node. They say what
the shape is and how the two configurations compare; a tuned
deployment on real hardware is a different measurement.

| measurement | sessions off | sessions on | unit |
| --- | --- | --- | --- |
| ingest_64mib_ms | 423 | 469 | ms |
| ingest_throughput_mib_s | 151 | 136 | MiB/s |
| ingest_50_small_ms | 234 | 222 | ms |
| ingest_small_objects_s | 213 | 225 | objects/s |
| retrieve_64mib_ms | 242 | 190 | ms |
| retrieve_throughput_mib_s | 264 | 336 | MiB/s |
| retrieve_50_small_ms | 114 | 125 | ms |
| retrieve_small_objects_s | 438 | 400 | objects/s |
| listing_ms | 21 | 18 | ms |
