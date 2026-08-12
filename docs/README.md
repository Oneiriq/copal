# Copal documentation

Copal is a self-hosted file service on SurrealDB. One database holds the
metadata, the access rules, the processing journal, and the audit trail. Bytes live
behind a content-addressed blob port. One contract object drives the REST
face, the GraphQL face, the OpenAPI document, the SDL, four generated
clients, and the breaking-change gate.

| Document | Covers |
| --- | --- |
| [architecture.md](architecture.md) | The two clocks (contract compiles, faces converge), the crates, deployment topologies, the file state machine, decision records. |
| [sequences.md](sequences.md) | Upload, the pipeline, an agent through MCP, signed URLs, the console, key rotation, the contract gate. |
| [api.md](api.md) | Authentication, routes, access levels, serving behavior, grants, runs, GraphQL, errors. |
| [processing.md](processing.md) | The journal model, the standard upload pipeline, failure and retry, writing workflows. |
| [operations.md](operations.md) | Configuration reference, key custody, sweeps, runbooks, deployment security posture. |
| [migration.md](migration.md) | Moving a MinIO bucket in with stock tools, and what the content gains. |
| [roadmap.md](roadmap.md) | What is left, ordered by what it blocks. Shipped work lives in the changelog. |
| [openapi.json](openapi.json) | Generated OpenAPI 3.1 document. Drift-gated by test. |
| [schema.graphql](schema.graphql) | Generated SDL, byte-identical to the served schema. Drift-gated by test. |
| [policy.json](policy.json) | Generated engine row-security clauses: read-scope conjuncts and field redactions the deployment folds into `PERMISSIONS`. Drift-gated by test. |

Generated client SDKs live in [`clients/`](../clients/): Rust, TypeScript,
Python, Go. Each is one self-contained file regenerated from the contract.

## Orientation in five minutes

A file is a record with a lifecycle (`draft`, `uploading`, `scanning`,
`ready`, `failed`, `quarantined`, `deleted`) and a content digest. Serving is
digest-based: bytes flow whenever verified content exists and the record is
not quarantined, so uploads and scans never blink existing readers.

Uploads stream through staging while the digest accumulates, then finalize
with a rename. With a pipeline configured, completion enqueues a journaled
workflow run that inspects the content and finalizes it to `ready` or
`quarantined`. Every stuck state has an automated exit.

Access is enforced at the byte boundary per file: `public` serves
anonymously and caches hard, `private` serves the owning tenant, `grant`
serves through issued URLs only. Grants and API keys are stateful
capabilities: rows holding secret hashes, revocable with one call, with no
signing key anywhere.
- [retention.md](retention.md): retention, legal hold, and WORM, designed ahead of the code.
- [principals.md](principals.md): principals within tenants, designed ahead of the code.
- [backup.md](backup.md): what to back up, in what order, and how to prove a restore worked.
- [../bench/README.md](../bench/README.md): the performance envelope, how it is measured, and what the numbers do not claim.
