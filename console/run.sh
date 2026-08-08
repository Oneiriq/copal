#!/usr/bin/env bash
# Build the server, start it on a scratch database, ask the browser
# what the console looks like, stop it. Set CONSOLE_BASE to point at a
# server that is already running and nothing is started or stopped.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"

if [ ! -d "$here/node_modules" ]; then
  echo "console: install first, with 'npm --prefix console ci'" >&2
  exit 2
fi

started=""
cleanup() {
  if [ -n "$started" ]; then
    kill "$started" 2>/dev/null || true
    wait "$started" 2>/dev/null || true
  fi
  if [ -n "${work:-}" ]; then rm -rf "$work"; fi
}
trap cleanup EXIT

if [ -z "${CONSOLE_BASE:-}" ]; then
  cargo build --manifest-path "$root/Cargo.toml" -p copal-server --bin copal-server
  work="$(mktemp -d)"
  # Windows names it with an extension.
  server="$root/target/debug/copal-server"
  [ -x "$server" ] || server="$server.exe"
  # A port nobody else is on, so a developer's own server keeps running.
  port="${CONSOLE_PORT:-8137}"
  export CONSOLE_BASE="http://127.0.0.1:$port"
  export CONSOLE_ADMIN_TOKEN="console-checks"

  COPAL_BIND="127.0.0.1:$port" \
  COPAL_AUTH_MODE=keys \
  COPAL_ADMIN_TOKEN="$CONSOLE_ADMIN_TOKEN" \
  COPAL_DB_URL="surrealkv://$work/db" \
  COPAL_BLOB_ROOT="$work/blobs" \
  COPAL_BLOB_ENCRYPTION_KEY="$(printf 'a%.0s' $(seq 1 64))" \
    "$server" > "$work/server.log" 2>&1 &
  started=$!

  for _ in $(seq 1 60); do
    if curl -fsS -m 2 "$CONSOLE_BASE/healthz" >/dev/null 2>&1; then break; fi
    if ! kill -0 "$started" 2>/dev/null; then
      echo "console: the server stopped before it listened" >&2
      tail -20 "$work/server.log" >&2
      exit 1
    fi
    sleep 1
  done

  # A tenant with a name, so the console has somewhere to point. The
  # pages render from the contract whether or not it holds rows.
  curl -fsS -X POST "$CONSOLE_BASE/v1/admin/tenants/${CONSOLE_TENANT:-demo}/keys" \
    -H "x-copal-admin-token: $CONSOLE_ADMIN_TOKEN" \
    -H 'content-type: application/json' \
    -d '{"name":"console-checks","scopes":["read","write","admin"]}' >/dev/null
fi

cd "$here"
node frame.mjs
node clipping.mjs
