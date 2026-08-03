#!/bin/sh
# One command, one envelope:
#
#   ./bench/run.sh
#
# Stands the conformance stack up twice, once with caller-bound engine
# sessions off and once with them on, takes the same measurements
# through the S3 gateway both times, and writes a table that names
# the machine it ran on. The point of the two passes is the column an
# operator actually wants: what the second enforcement layer costs.
#
# These are laptop-class numbers from a single-node stack. They say
# what the shape is and how the configurations compare; a tuned
# deployment on real hardware is a different measurement, and the
# table says so.
set -e
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1
TENANT=bench
COMPOSE="docker compose -f ../conformance/docker-compose.yml -f overlay.yml"
export ONEIRIQ_READ_PAT="${ONEIRIQ_READ_PAT:-$(gh auth token 2>/dev/null || true)}"

mkdir -p results
rm -f results/*.txt

measure() {
    sessions=$1
    echo "== pass: engine sessions $sessions"
    COPAL_ENGINE_SESSIONS="$sessions" $COMPOSE down -v --remove-orphans > /dev/null 2>&1 || true
    COPAL_ENGINE_SESSIONS="$sessions" $COMPOSE build copal > /dev/null 2>&1
    COPAL_ENGINE_SESSIONS="$sessions" $COMPOSE up -d --force-recreate surrealdb copal > /dev/null 2>&1

    for _ in $(seq 1 60); do
        if $COMPOSE run --rm curl -c "curl -fsS http://copal:8080/readyz" > /dev/null 2>&1; then
            break
        fi
        sleep 2
    done

    cred=$($COMPOSE run --rm curl -c \
        "curl -fsS -X POST http://copal:8081/v1/admin/tenants/$TENANT/s3-credentials \
         -H 'x-copal-admin-token: conformance-admin'")
    ak=$(printf '%s' "$cred" | sed -n 's/.*"access_key_id":"\([^"]*\)".*/\1/p')
    sk=$(printf '%s' "$cred" | sed -n 's/.*"secret_access_key":"\([^"]*\)".*/\1/p')
    [ -n "$ak" ] && [ -n "$sk" ] || { echo "credential mint failed: $cred"; exit 1; }

    $COMPOSE run --rm -T \
        -e COPAL_ACCESS_KEY="$ak" -e COPAL_SECRET_KEY="$sk" -e COPAL_TENANT="$TENANT" \
        mc /bench/bench.sh
    $COMPOSE run --rm -T \
        -e COPAL_TENANT="$TENANT" -e COPAL_API_HOST=copal \
        curl /bench/bench-api.sh
    mv results/bench.txt "results/sessions-$sessions.txt"
    COPAL_ENGINE_SESSIONS="$sessions" $COMPOSE down -v --remove-orphans > /dev/null 2>&1 || true
}

measure off
measure on

VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' ../crates/copal-server/Cargo.toml | head -1)
python3 render.py "${VERSION:-dev}" || python render.py "${VERSION:-dev}"
echo
cat "results/${VERSION:-dev}.md"
