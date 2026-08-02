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

# The build fetches private git dependencies through a BuildKit
# secret; locally the gh CLI supplies the token, CI supplies the org
# read PAT.
export ONEIRIQ_READ_PAT="${ONEIRIQ_READ_PAT:-$(gh auth token 2>/dev/null || true)}"

compose() { docker compose "$@"; }

echo "== building and starting the stack"
compose build copal
compose up -d surrealdb minio copal

echo "== waiting for copal"
for i in $(seq 1 60); do
    if compose run --rm curl -c "curl -fsS http://copal:8080/readyz" > /dev/null 2>&1; then
        break
    fi
    sleep 2
done

echo "== minting a credential"
CRED=$(compose run --rm curl -c \
    "curl -fsS -X POST http://copal:8081/v1/admin/tenants/$TENANT/s3-credentials \
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
        "$1" "/runners/$1.sh" || true
}
run_client mc
run_client aws
run_client rclone

# Client output alone cannot say whether a failure is the product or
# the client; the server's own view of the objects is the other half.
echo "== capturing server state"
compose run --rm curl -c     "curl -s 'http://copal:8080/v1/files?limit=100' -H 'x-copal-tenant: $TENANT'"     > results/server-state.json 2>/dev/null || true

echo "== rendering the table"
VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' ../crates/copal-server/Cargo.toml | head -1)
python3 render.py "${VERSION:-dev}" || python render.py "${VERSION:-dev}"

echo "== tearing down"
compose down -v

echo
cat "results/${VERSION:-dev}.md"
