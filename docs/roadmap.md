# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md). The open list below carries only
unfinished items. The done list records items that used to be open
here, so a reader of an older copy can see where they went.

## Open

1. **Multi-region replication.** Residencies place bytes, but nothing
   copies them between regions. A second region is a second
   deployment today. The design is in
   [design/multi-region-replication.md](design/multi-region-replication.md),
   and none of it is implemented. Its observer-only applier is the
   first slice.

2. **Publishing the SDKs.** The four generated clients assemble into
   packages and validate in CI, and the publish workflow exists, but
   no package is on a registry yet: crates.io `oneiriq-copal`, PyPI
   `oneiriq-copal`, npm `@oneiriq/copal`, and the Go module
   `github.com/Oneiriq/copal-go` are all unpublished. Publishing is a
   manual dispatch of `.github/workflows/publish-sdks.yml` once
   registry tokens exist. See [sdks.md](sdks.md), which also lists
   what the clients can and cannot do.

## Deferred, with reasons

3. **Azure archive rehydration.** Archive-class tiers on Azure Blob
   Storage refuse at boot because Copal does not drive Azure's Set
   Blob Tier rehydration yet. Azure deployments can tier to online
   classes such as Cool. The recall machinery is backend-neutral, so
   this is one restore driver when someone needs it.

## Done

These were open items in earlier versions of this file.

- **The embedded tier.** The default build includes the `embedded`
  feature, so `COPAL_DB_URL=surrealkv://<path>` (durable) and `mem://`
  (ephemeral) run the engine inside the Copal process. See the
  embedded tier section of [operations.md](operations.md).
- **OpenTelemetry spans.** Set `COPAL_OTLP_ENDPOINT` and every request
  becomes a span exported over OTLP http/protobuf. See the traces
  section of [operations.md](operations.md).
- **SDK packaging pipeline.** `sdks/build.sh` assembles and validates
  the four packages, CI runs it, and `publish-sdks.yml` publishes on
  manual dispatch. The packages themselves are not published yet
  (open item 2).
- **Kayak REST runtime router.** `/v1c` serves the JSON surface from
  the contract through Kayak's runtime router, beside the hand-written
  `/v1`. See the contract-first REST face section of [api.md](api.md).
- **Lifecycle and tiering**, in the three stages its design declared:
  configuration and the observe-only classifier, the mover, and
  archive recall. See [design/lifecycle-tiering.md](design/lifecycle-tiering.md).
- **Per-chunk authorization** through per-upload markers. See
  [design/per-chunk-authorization.md](design/per-chunk-authorization.md).
- **Retention, principals, master key rotation, audit export, the
  external transformer seam, rendition URLs, URL ingestion, and the
  GCS and Azure backends.**
- **Scale evidence above 256 MiB.** `bench/scale.sh` measured the
  streamed PUT, multipart assembly, ranged reads, and the scan pass at
  512 MiB, 1 GiB, and 2 GiB. Throughput and streaming memory stayed
  flat, and ranged reads cost the same at any offset. The scan pass
  holds about three times the object in memory while it runs;
  [operations.md](operations.md) states this beside the tables.
