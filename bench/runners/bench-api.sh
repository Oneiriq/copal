#!/bin/sh
# The retrieval-thesis measurements: search latency and an MCP tool
# call, timed through the API face after the S3 pass seeded content.
# curl's own timer does the measuring, because the image's busybox
# date has no millisecond format; three samples per metric like the
# S3 half.
OUT=/results/bench.txt
HOST="${COPAL_API_HOST:-copal}"
record() {
    echo "$1 $2" >> "$OUT"
}
timed_ms() {
    curl -o /dev/null -fsS -w '%{time_total}' "$@" 2> /dev/null \
        | awk '{ printf "%d", $1 * 1000 }'
}

# Extraction settles before timing starts, so the numbers measure
# retrieval rather than the pipeline's tail.
i=0
while [ $i -lt 30 ]; do
    hits=$(curl -fsS "http://$HOST:8080/v1/search?q=object" \
        -H "x-copal-tenant: $COPAL_TENANT" 2> /dev/null)
    case "$hits" in
        *'"file"'*) break ;;
    esac
    i=$((i + 1))
    sleep 1
done

pass=0
while [ $pass -lt 3 ]; do
    ms=$(timed_ms "http://$HOST:8080/v1/search?q=object&limit=20" \
        -H "x-copal-tenant: $COPAL_TENANT")
    record search_ms "${ms:-0}"

    ms=$(timed_ms -X POST "http://$HOST:8080/mcp" \
        -H "x-copal-tenant: $COPAL_TENANT" \
        -H "content-type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"files_list","arguments":{"limit":50}}}')
    record mcp_files_list_ms "${ms:-0}"

    pass=$((pass + 1))
done
echo "api bench done"
