#!/bin/sh
# One command, one table: stand the stack up, mint a credential, seed
# the source, run every client's scenario, and render the results.
#
#   ./conformance/run.sh
#
# The table lands in conformance/results/, named by the version under
# test. A prospect runs the same command on their hardware and gets
# the same table, which is what makes the compatibility claim
# self-verifiable rather than prose.
set -e
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1
TENANT=conformance
mkdir -p results
rm -f results/*.txt

# COPAL_HA=1 stands up TWO instances behind round-robin nginx and
# runs the same scenario through the proxy: the proof that leases,
# claims, and multipart survive instance hops.
if [ "${COPAL_HA:-0}" = "1" ]; then
    COMPOSE_FILES="-f docker-compose.yml -f ha-overlay.yml"
    TARGET=gateway
else
    COMPOSE_FILES="-f docker-compose.yml"
    TARGET=copal
fi

compose() { docker compose $COMPOSE_FILES "$@"; }

# A clean slate first. Reusing a running stack mixes states that
# must not mix: rebuilding the image recreates the Copal container
# with an empty blob root while the engine container keeps its
# in-memory metadata, which leaves records pointing at content the
# backend no longer holds. That reads exactly like a product defect
# and is not one.
echo "== clearing any previous stack"
compose down -v --remove-orphans > /dev/null 2>&1 || true

echo "== building and starting the stack"
compose build copal
if [ "${COPAL_HA:-0}" = "1" ]; then
    compose up -d --force-recreate surrealdb minio copal copal2 gateway
else
    compose up -d --force-recreate surrealdb minio copal
fi

echo "== waiting for copal"
# Round-robin hides a cold instance: the gateway answers while one
# backend still boots, and the next request 502s. Readiness is every
# instance ready, asked directly.
if [ "${COPAL_HA:-0}" = "1" ]; then
    WAIT_HOSTS="copal copal2"
else
    WAIT_HOSTS="copal"
fi
for host in $WAIT_HOSTS; do
    for i in $(seq 1 60); do
        if compose run --rm curl -c "curl -fsS http://$host:8080/readyz" > /dev/null 2>&1; then
            break
        fi
        sleep 2
    done
done

echo "== minting a credential"
CRED=$(compose run --rm curl -c \
    "curl -fsS -X POST http://$TARGET:8081/v1/admin/tenants/$TENANT/s3-credentials \
     -H 'x-copal-admin-token: conformance-admin'")
AK=$(printf '%s' "$CRED" | sed -n 's/.*"access_key_id":"\([^"]*\)".*/\1/p')
SK=$(printf '%s' "$CRED" | sed -n 's/.*"secret_access_key":"\([^"]*\)".*/\1/p')
[ -n "$AK" ] && [ -n "$SK" ] || { echo "credential mint failed: $CRED"; exit 1; }

echo "== seeding the source"
compose run --rm mc /runners/seed.sh

run_client() {
    echo "== running $1"
    compose run --rm \
        -e COPAL_ACCESS_KEY="$AK" -e COPAL_SECRET_KEY="$SK" -e COPAL_TENANT="$TENANT" \
        -e COPAL_S3_HOST="$TARGET" \
        "$1" "/runners/$1.sh" || true
}
run_client mc
run_client aws
run_client rclone
echo "== running mcp"
compose run --rm -T     -e COPAL_TENANT="$TENANT" -e COPAL_S3_HOST="$TARGET"     curl /runners/mcp.sh || true

# Client output alone cannot say whether a failure is the product or
# the client; the server's own view of the objects is the other half.
echo "== capturing server state"
compose run --rm curl -c     "curl -s 'http://copal:8080/v1/files?limit=100' -H 'x-copal-tenant: $TENANT'"     > results/server-state.json 2>/dev/null || true
# The blob side of the same question: a record naming a digest the
# backend does not hold is a different failure from a missing record.
compose exec -T copal sh -c 'find /data/blobs -type f'     > results/blob-inventory.txt 2>/dev/null || true
# The server's own account of the run: sweeps, refusals, and errors
# that no client can report.
compose logs copal > results/copal.log 2>&1 || true

echo "== rendering the table"
VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' ../crates/copal-server/Cargo.toml | head -1)
python3 render.py "${VERSION:-dev}" || python render.py "${VERSION:-dev}"

echo "== tearing down"
compose down -v

echo
cat "results/${VERSION:-dev}.md"
