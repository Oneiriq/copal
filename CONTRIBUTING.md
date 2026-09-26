# Contributing to Copal

Thanks for your interest in Copal. This page covers how to build the
project, run the checks CI runs, and change the parts of the repository
that are generated.

## Prerequisites

- Rust 1.90 or newer, with `rustfmt` and `clippy`.
- A C compiler. A few dependencies build native code.
- Docker, only if you want a SurrealDB server or the conformance harness.

## Build and test

```sh
cargo build
cargo test --workspace
```

The test suite runs every service test against an in-memory SurrealDB
engine, so it needs no containers or network services.

Before you open a pull request, run what CI runs:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Running the server

For a local server with the embedded database, follow the
[quick start](README.md#quick-start). To develop against a SurrealDB
server instead:

```sh
docker compose up -d         # SurrealDB on 127.0.0.1:8000
cargo run -p copal-server    # connects to ws://127.0.0.1:8000 by default
```

## Generated files

The API contract in `crates/copal-server/src/contract/` is the source for
several checked-in files: `docs/openapi.json`, `docs/schema.graphql`,
`docs/mcp-tools.json`, `docs/policy.json`, and the four clients in
`clients/`. A test compares each one byte for byte with what the contract
generates, so a change to the contract fails the suite until you
regenerate them:

```sh
COPAL_BLESS=1 cargo test -p copal-server --test contract
```

Review the regenerated files in your diff before committing. The same
test also checks the contract against the database schema, so a filter or
sort that no index can serve fails here with a message naming it.

The contract is compiled by [Kayak](https://github.com/Oneiriq/kayak).

## Other checks

Run these locally when your change touches their area:

| Area | Command | Needs | In CI |
| --- | --- | --- | --- |
| Client SDK packaging | `sdks/build.sh` | Node 22, Go 1.22, Python 3.12 | Every run |
| Console layout | `console/run.sh` | Node and a Chromium build ([console/README.md](console/README.md)) | Every run |
| S3 and MCP conformance | `conformance/run.sh` | Docker ([conformance/README.md](conformance/README.md)) | On demand |
| Benchmarks | `bench/run.sh` | Docker ([bench/README.md](bench/README.md)) | No |

## Continuous integration

CI runs on pull requests, once a day, and on demand. It runs on a
self-hosted runner and does not run automatically for pull requests from
forks, so a maintainer will run the checks for you. Please run the
commands above before you ask for a review.

## Pull requests

- Keep each pull request to one change, and describe what it changes and
  why.
- Add or update tests for behavior you change.
- Update the docs in `docs/` when you change configuration, an endpoint,
  or anything an operator would notice.
- Add a line to `CHANGELOG.md` for user-visible changes.

## License

By contributing, you agree that your contributions are licensed under the
[AGPL-3.0-only](LICENSE) license that covers this repository.
