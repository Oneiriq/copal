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

## Security posture for deployments

Development defaults are open on purpose; an exposed deployment MUST:

- **Set `COPAL_AUTH_MODE=keys`** with `COPAL_ADMIN_TOKEN` configured.
  The default trusted-header mode (`x-copal-tenant`) lets anyone
  reaching the port act as any tenant; the server warns loudly at
  startup. The default flips to `keys` at 1.0. Keys are `ck1` bearer
  capabilities: the store holds only their hashes, revocation is one
  call, and there is no signing key to rotate or leak.
- **Set `COPAL_ADMIN_BIND`** so key custody and the audit trail live
  on a listener the tenant-facing network cannot reach.
- **Terminate TLS in front of the server.** Copal does not speak TLS
  itself; bearer keys and grant tokens must never cross a plaintext
  network.
- **Scrub grant URLs from proxy/access logs.** Grant tokens ride the
  path by design (`/v1/grants/<token>`); short TTLs bound the
  exposure, log hygiene removes it. Redemption responses are already
  `no-store`.
- **Run the database as a dedicated user**, not root: create a
  namespace/database-scoped SurrealDB user for the service and pass it
  via `COPAL_DB_USER`/`COPAL_DB_PASS`. All authorization is enforced
  by the service; the scoped DB user is defense in depth.
- **Rate-limit at the proxy.** The service enforces payload and page
  ceilings plus GraphQL depth/complexity limits, but request-rate and
  per-tenant quota policy belong to the deployment until built-in
  quotas land.

What the service already enforces everywhere: uniform 401s on the
whole credential path (timing included), server-owned scan verdicts
(`metadata.processing` cannot be caller-supplied), `nosniff` +
sanitized `Content-Disposition` with forced download for
script-capable types, `no-store` on private and grant bytes, access
levels (`public` / `private` / `tenant` / `grant`) at the byte
boundary, and an engine-immutable audit trail for key and grant
custody.

## License

AGPL-3.0-only. Client SDKs are published separately under Apache-2.0.
