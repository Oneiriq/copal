# Copal documentation

Copal is a self-hosted file service on SurrealDB. One database holds the
metadata, the access rules, the processing journal, and the audit
trail. Bytes live behind a content-addressed blob store: the local
filesystem by default, with S3, Google Cloud Storage, and Azure Blob
Storage available as named residencies.

The same files answer on several faces: a hand-written REST API
(`/v1`), a REST API generated from the contract (`/v1c`), GraphQL
(`/graphql`), MCP for agents (`/mcp`), tus resumable uploads
(`/v1/tus`), an S3-compatible gateway on its own listener, and a
server-rendered operator console (`/admin/console`). One contract
declaration drives the generated faces, the reference documents, four
generated clients, and the breaking-change gate.

## Start here

| Document | What it answers |
| --- | --- |
| [getting-started.md](getting-started.md) | A hands-on tour. Run Copal locally, mint a key, upload a file, search it, share a link, and try the other faces. |

## Guides

| Document | What it answers |
| --- | --- |
| [api.md](api.md) | How to call Copal: authentication, the route map, access levels, serving rules, grants, search, runs, GraphQL, MCP, the S3 gateway, and errors. |
| [operations.md](operations.md) | How to configure and run it: every environment variable, key custody, storage residencies and tiers, sweeps, metrics, runbooks, and the security posture of an exposed deployment. |
| [processing.md](processing.md) | What happens after an upload: the journaled workflow engine, the six-step upload pipeline, the other built-in workflows, failure and retry, and writing your own. |
| [principals.md](principals.md) | How to give people, services, and agents their own identity, scopes, and rate budget under a tenant. |
| [retention.md](retention.md) | How retention periods, legal holds, and compliance (WORM) mode keep versions from being erased. |
| [backup.md](backup.md) | What to back up, in what order, and how to prove a restore worked. |
| [migration.md](migration.md) | How to move a MinIO bucket into Copal with `mc`, `rclone`, or the aws CLI, and what the content gains. |
| [sdks.md](sdks.md) | What the generated clients cover, what they do not, and how they are packaged. |

## Reference

These files are generated from the contract and checked by a test, so
they match the running server.

| Document | What it answers |
| --- | --- |
| [openapi.json](openapi.json) | The OpenAPI 3.1 document for the JSON routes of the REST face. |
| [schema.graphql](schema.graphql) | The GraphQL schema, identical to what `GET /graphql` serves. |
| [mcp-tools.json](mcp-tools.json) | The MCP tool manifest that `tools/list` serves. |
| [policy.json](policy.json) | The engine row-security clauses (read-scope conjuncts and field redactions) that the deployment folds into `PERMISSIONS`. |

The generated clients live in [`clients/`](../clients/) (Rust,
TypeScript, Python, Go). Read [sdks.md](sdks.md) before you use them:
they authenticate with the development tenant header only and have no
byte upload or download methods.

## How it works

| Document | What it answers |
| --- | --- |
| [architecture.md](architecture.md) | How the pieces fit: the contract and the faces, the crates, deployment topologies, the file state machine, and the decision records. |
| [sequences.md](sequences.md) | Step-by-step diagrams of an upload, the pipeline, an agent call through MCP, a signed URL, the console, key rotation, and the contract gate. |
| [../bench/README.md](../bench/README.md) | The performance envelope, how it is measured, and what the numbers do not claim. |

## Design notes

| Document | What it answers |
| --- | --- |
| [design/per-chunk-authorization.md](design/per-chunk-authorization.md) | The design behind per-upload confidentiality markers. Shipped. |
| [design/lifecycle-tiering.md](design/lifecycle-tiering.md) | The design behind storage tiers, the mover, and archive recall. Shipped, apart from the items its status line names. |
| [design/multi-region-replication.md](design/multi-region-replication.md) | A proposed single-writer primary with read replicas over the changefeed. Design only; none of it is implemented. |

## Roadmap

| Document | What it answers |
| --- | --- |
| [roadmap.md](roadmap.md) | What is still open, ordered by what it blocks. Shipped work is in the [changelog](../CHANGELOG.md). |

## Orientation in five minutes

A file is a record with a lifecycle (`draft`, `uploading`, `scanning`,
`ready`, `failed`, `quarantined`, `deleted`) and a content digest.
Serving is digest-based: bytes flow whenever verified content exists
and the record is not quarantined, so uploads and scans do not
interrupt existing readers.

Uploads stream through staging while the digest accumulates, then
finalize with a rename. Completion enqueues a journaled workflow run
that inspects the content and finalizes it to `ready` or
`quarantined`. Every stuck state has an automated exit.

Access is enforced at the byte boundary per file. `public` serves
anonymously and caches hard. `private` and `tenant` serve the owning
tenant. `grant` serves through issued URLs only. Grants (`cg1`) and API
keys (`ck1`) are rows holding secret hashes, so revoking one is a
single call and no signing key exists for them. Edge tokens (`cg2`) are
the one signed form: an HMAC under a per-tenant edge key, for a CDN to
verify without calling back.
