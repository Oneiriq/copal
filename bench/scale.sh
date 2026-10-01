#!/bin/sh
# The scale bench:
#
#   ./bench/scale.sh
#
# Runs the ignored scale measurements one per process in release
# mode and collects their `name value` lines into results/scale.txt.
# One process per scenario is the point, not a convenience: the peak
# working set the scenarios report only ever ratchets upward, so a
# process that runs one scenario owns its peak, and two scenarios in
# one process would blame the first for the second's memory.
#
# The container bench (run.sh) measures the S3 gateway from outside
# with stock tooling; this one measures in-process, because peak
# memory and the embedded-vs-remote wire cost are not observable
# through a container boundary. The results feed the scale tables in
# docs/operations.md.
#
# The remote round-trip half needs a SurrealDB server. Unless
# COPAL_BENCH_DB_URL names one, the script runs a throwaway container
# at the version the conformance stack pins, in-memory, on a port of
# its own. A dedicated engine rather than whatever answers on 8000,
# because a developer's machine may have another project's engine
# there, and because the comparison is only honest when the remote
# engine matches the embedded one: same version, same memory backend,
# the wire as the only variable.
set -e
cd "$(dirname "$0")"
OUT=results/scale.txt

LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT

run() {
    package=$1
    name=$2
    echo "== $name" | tee -a "$OUT"
    # The log is captured whole and grepped afterwards, so a scenario
    # that fails stops the run with its output in view instead of
    # dying inside a pipeline where set -e reads the wrong status.
    if ! (cd .. && cargo test --release -p "$package" --test scale "$name" \
        -- --ignored --exact --nocapture) > "$LOG" 2>&1; then
        echo "FAILED: $name"
        tail -40 "$LOG"
        exit 1
    fi
    grep -E '^[a-z0-9_]+ [0-9]+$' "$LOG" | tee -a "$OUT"
}

mkdir -p results
{
    echo "# scale bench $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# host: $(hostname) ($(uname -sm))"
} > "$OUT"

# Compile once up front so the first scenario's timing does not
# include the build.
(cd .. && cargo test --release -p copal-server -p copal-store --test scale --no-run) > /dev/null 2>&1

for scenario in \
    scale_single_put_512mib \
    scale_single_put_1gib \
    scale_single_put_2gib \
    scale_single_put_1gib_plaintext \
    scale_multipart_512mib \
    scale_multipart_1gib \
    scale_multipart_2gib \
    scale_scan_pass_512mib \
    scale_scan_pass_1gib \
    scale_scan_pass_2gib
do
    run copal-server "$scenario"
done

run copal-store scale_vector_index_rebuild
run copal-store scale_round_trips_embedded

STARTED_ENGINE=0
if [ -z "$COPAL_BENCH_DB_URL" ]; then
    ENGINE_VERSION=$(sed -n 's/.*surrealdb\/surrealdb:\(v[0-9.]*\).*/\1/p' \
        ../conformance/docker-compose.yml | head -1)
    if docker run --rm -d --name copal-scale-engine -p 127.0.0.1:18000:8000 \
        "surrealdb/surrealdb:${ENGINE_VERSION:-v3.3.0}" \
        start --user root --pass root > /dev/null 2>&1; then
        STARTED_ENGINE=1
        COPAL_BENCH_DB_URL=ws://127.0.0.1:18000
        export COPAL_BENCH_DB_URL
        for _ in $(seq 1 30); do
            if curl -fsS http://127.0.0.1:18000/health > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
    fi
fi
if [ -n "$COPAL_BENCH_DB_URL" ]; then
    run copal-store scale_round_trips_remote
else
    echo "remote_round_trips skipped: docker unavailable and COPAL_BENCH_DB_URL unset" \
        | tee -a "$OUT"
fi
if [ "$STARTED_ENGINE" = "1" ]; then
    docker rm -f copal-scale-engine > /dev/null 2>&1 || true
fi

echo
cat "$OUT"
