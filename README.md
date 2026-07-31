# Copal

Self-hosted file service on SurrealDB. Metadata, search, relationships, and
the durable processing journal live in one transactional database; file
bytes live behind a pluggable content-addressed blob store (filesystem, any
S3-compatible store, Azure Blob, GCS, or a SurrealDB bucket for
single-binary deployments).

Status: early. The vertical slice works end to end: create a file record,
stream bytes in, complete (digest verification, blob dedupe, state
transition), read metadata, stream bytes out.

## Layout

| Crate | Role |
| --- | --- |
| `copal-core` | Domain types: ids, digests, the file state machine. Pure, no IO. |
| `copal-store` | Metadata plane on SurrealDB via `surql-rs`: schema as code, repos as free functions. |
| `copal-blob` | Blob plane: content-addressed storage behind one port, OpenDAL backends. |
| `copal-server` | Axum HTTP API. |

## Development

```
docker compose up -d          # SurrealDB v3 with the files capability
cargo test                    # unit + mem:// round-trip tests, no server needed
cargo run -p copal-server     # COPAL_DB_URL=ws://127.0.0.1:8000 by default
```

## License

AGPL-3.0-only. Client SDKs are published separately under Apache-2.0.
