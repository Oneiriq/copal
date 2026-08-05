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

## Run it

The default build carries the metadata engine, so a local instance needs
no database, no container, and no configuration file.

```sh
cargo build -p copal-server -p copal-cli

COPAL_BIND=127.0.0.1:8099 \
COPAL_AUTH_MODE=keys \
COPAL_ADMIN_TOKEN=local-admin-token \
COPAL_DB_URL="surrealkv://./data/local/db" \
COPAL_BLOB_ROOT=./data/local/blobs \
COPAL_BLOB_ENCRYPTION_KEY=$(printf 'a%.0s' {1..64}) \
  ./target/debug/copal-server
```

It reconciles the schema on a fresh database and listens. Mint a key,
store something, read it back:

```sh
export COPAL_URL=http://127.0.0.1:8099 COPAL_ADMIN_TOKEN=local-admin-token

TOKEN=$(curl -s -X POST $COPAL_URL/v1/admin/tenants/acme/keys \
  -H "x-copal-admin-token: $COPAL_ADMIN_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"name":"local","scopes":["read","write"]}' | jq -r .token)

ID=$(curl -s -X POST $COPAL_URL/v1/files \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"path":"runbooks/rotation.txt","content_type":"text/plain"}' | jq -r .id)

curl -s -X PUT $COPAL_URL/v1/files/$ID/content \
  -H "authorization: Bearer $TOKEN" --data-binary @some-file.txt

curl -s $COPAL_URL/v1/files/$ID/content -H "authorization: Bearer $TOKEN"
```

The upload lands in `scanning` and the pipeline finalizes it to `ready`
within a second, extracting text on the way, which is what makes
`/v1/search?q=...` answer. The bytes on disk open with the `CPE1` magic
because a key was configured; the plaintext never reaches the filesystem.

Every other face is already serving the same content:

```sh
curl -s $COPAL_URL/v1c/files -H "authorization: Bearer $TOKEN"        # generated REST
curl -s -X POST $COPAL_URL/graphql -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"query":"{ files(limit: 3) { items { id path state } } }"}'    # GraphQL
curl -s -X POST $COPAL_URL/mcp -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'     # MCP manifest

COPAL_TOKEN=$TOKEN ./target/debug/copalctl files list                 # the terminal
COPAL_TOKEN=$TOKEN ./target/debug/copalctl top                        # the live view
```

The console is at `http://127.0.0.1:8099/admin/console`; log in with any
username and the admin token as the password.

## Layout

| Crate | Role |
| --- | --- |
| `copal-core` | Domain types: ids, digests, the file state machine. Pure, no IO. |
| `copal-store` | Metadata plane on SurrealDB via `surql-rs`: schema as code, repos as free functions. |
| `copal-blob` | Blob plane: content-addressed storage behind one port, OpenDAL backends. |
| `copal-sign` | Capability tokens for grants and API keys. Hashes in the store, secrets never. |
| `copal-flow` | Durable workflow execution over the journal. |
| `copal-server` | Axum HTTP layer: every face (REST, generated REST, GraphQL, MCP, S3, console, admin), sweeps, the worker loop. |
| `copal-cli` | `copalctl` and the `copalctl top` live view. |

## Development

```
docker compose up -d          # SurrealDB v3 with the files capability
cargo test                    # unit + mem:// round-trip tests, no server needed
cargo run -p copal-server     # COPAL_DB_URL=ws://127.0.0.1:8000 by default
```

The container is for the server-backed topology. The embedded tier above
needs none of it.

The whole service integration-tests against an in-memory SurrealDB engine.
No containers are involved in the test suite.

### Continuous integration

CI runs on pull requests, on a daily schedule, and on demand. A merge
does not trigger its own build: it would re-verify a tree the pull
request already proved, and on one runner that duplicate is what the
next pull request waits behind. The scheduled run carries what a
merge build was actually worth, which is a signal for drift that
arrives without a commit (a new advisory has reddened this branch
that way) and a warm default-branch cache for pull requests to
restore.

The two jobs are independent, so runner capacity is the only thing
between them and running at once. `deploy/add-runner.sh` registers
another runner on the host, one per parallel job wanted.

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
