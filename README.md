# Copal

Self-hosted file service on SurrealDB. Metadata, search, access control, and
the durable processing journal live in one transactional database. File
bytes live behind a pluggable content-addressed blob store (filesystem
today; any OpenDAL backend fits the same port). One contract object drives
the REST API, the GraphQL API, the OpenAPI document, the SDL, four generated
client SDKs, and the breaking-change gate.

What works today, end to end and under test: create, streamed upload with
digest verification and blob dedupe, journaled post-upload processing with
quarantine, versioned history, keyset listings on both faces, signed URLs
with use counts and revocation, per-file access levels enforced at the byte
boundary, API-key authentication, conditional and ranged serving, background
maintenance with leader election, and an engine-immutable audit trail.

## Documentation

The suite lives in [`docs/`](docs/README.md): architecture and decision
records, the API guide, the processing model, and the operations reference.
Generated artifacts (`docs/openapi.json`, `docs/schema.graphql`,
`clients/*`) are drift-gated by test against the contract.

## Layout

| Crate | Role |
| --- | --- |
| `copal-core` | Domain types: ids, digests, the file state machine. Pure, no IO. |
| `copal-store` | Metadata plane on SurrealDB via `surql-rs`: schema as code, repos as free functions. |
| `copal-blob` | Blob plane: content-addressed storage behind one port, OpenDAL backends. |
| `copal-sign` | Capability tokens for grants and API keys. Hashes in the store, secrets never. |
| `copal-flow` | Durable workflow execution over the journal. |
| `copal-server` | Axum HTTP layer: both API faces, sweeps, the worker loop. |

## Development

```
docker compose up -d          # SurrealDB v3 with the files capability
cargo test                    # unit + mem:// round-trip tests, no server needed
cargo run -p copal-server     # COPAL_DB_URL=ws://127.0.0.1:8000 by default
```

The whole service integration-tests against an in-memory SurrealDB engine.
No containers are involved in the test suite.

## Security posture

Development defaults are open on purpose. Before exposing a deployment, read
[docs/operations.md](docs/operations.md): API-key mode with an admin token,
a separate admin listener, TLS termination in front, a scoped database user,
and proxy-level rate limits. The service itself enforces uniform
credential-path 401s, server-owned scan verdicts, download-safe content
headers, access levels at the byte boundary, and an audit trail that refuses
rewrites inside the database engine.

## License

AGPL-3.0-only. Client SDKs are published separately under Apache-2.0.
