# Roadmap

What is left, ordered by what it blocks. Shipped work lives in
[CHANGELOG.md](../CHANGELOG.md); this file carries only open items, so
a tier disappearing from it means that tier is done.

Four tiers are complete and no longer listed: deployment hardening,
the security items that needed design, per-tenant residencies and
edge tokens, and the post-moat audit set. The AI-native turn is
complete through hybrid retrieval over passages.

## Now: the contract cannot express everything the service does

The project's claim is that one declaration serves every face. Two
surfaces break it, and both break it the same way.

1. **Sub-resources.** A file's versions and a webhook's deliveries are
   lists of rows belonging to one parent instance. Janus resources are
   top-level, so these stay REST-only and GraphQL and all four
   generated clients cannot reach them. Needs a sub-resource shape in
   the contract, validated against the parent key like any other
   filter, then `files.versions` and `webhooks.deliveries` declared
   through it.
2. **Extracted text and search.** `GET /v1/files/{id}/text` and
   `GET /v1/search` are REST-only. Text is a sub-resource once the
   above exists. Search is a query rather than a listing and needs its
   own shape, or an honest note that it stays REST-only the way usage
   does.

## Next: the capability model's weakest link

3. **Key scopes and expiry.** `ck1` API keys are all-or-nothing and
   never expire. Every other capability in the system is narrower:
   `cg1` grants are op-scoped and use-counted, `cg2` tokens are
   expiry-bounded, S3 credentials are per-tenant and revocable, upload
   grants are single-use. Keys should carry a scope (read, write,
   admin) and an optional expiry.
4. **Flip `COPAL_AUTH_MODE` to `keys` at 1.0.** The trusted header is
   a dev mode and should stop being the default. Worth doing after
   scopes, since flipping the default makes key granularity the thing
   every deployment lives with.

## Then: compliance and reach

5. **Retention policies, legal hold, and WORM.** The compliance tier
   the enterprise buyers ask for. Retention interacts with the GC
   grace period and soft delete, so it needs design before code.
6. **External transformer seam** for transcodes and document
   renditions, following the pattern the extractor and embedding
   seams already set: pick a contract several self-hostable
   implementations speak, treat unconfigured as the feature being
   absent, and parse nothing in-process.
7. **On-the-fly rendition URLs and multi-source ingestion**, the axes
   the hosted services sell.
8. **More blob backends.** GCS and Azure beside the filesystem and
   S3-compatible stores, and the SurrealDB bucket backend as a
   first-class single-binary mode.

## Deferred, with reasons

9. **OTel spans.** Deferred twice. OTLP export means new dependencies
   and a cargo feature CI would not compile, which is untested code by
   construction. The `/metrics` endpoint set a dependency-free
   observability precedent this would break. Revisit when someone
   decides the dependency is worth it.
10. **SDK publishing pipelines** for the four generated clients. The
    clients are generated and gated against drift already; publishing
    is packaging work that wants a release cadence to hang from.
11. **Janus REST runtime router.** Copal's REST handlers are
    hand-written over the same repositories the GraphQL resolvers
    call. A contract-driven REST router would remove that duplication,
    but the handlers carry real behavior (streaming, ranges,
    conditionals) that a generic router has to earn the right to
    replace.
12. **The batched `surql-rs` release.** Shon cuts it. The branch
    carries the live-query `WHERE` clause, the session-scoped live
    query fix, and index-backed KNN.
