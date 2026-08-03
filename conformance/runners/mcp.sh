#!/bin/sh
# The MCP scenario: the agent face proven with nothing but curl, the
# way the S3 face is proven with stock clients. The stack runs in
# trusted-header mode, so the tenant header is the identity.
OUT=/results/mcp.txt
: > "$OUT"
HOST="${COPAL_S3_HOST:-copal}"
check() {
    name=$1
    shift
    if "$@" > /tmp/check.log 2>&1; then
        echo "mcp $name PASS" >> "$OUT"
    else
        echo "mcp $name FAIL" >> "$OUT"
        head -5 /tmp/check.log | while read -r line; do
            echo "    $line" >> "$OUT"
        done
    fi
}
contains() {
    case "$1" in
        *"$2"*) return 0 ;;
        *) return 1 ;;
    esac
}
rpc() {
    curl -fsS -X POST "http://$HOST:8080/mcp" \
        -H "x-copal-tenant: $COPAL_TENANT" \
        -H "content-type: application/json" \
        -d "$1"
}

initialize_speaks_the_protocol() {
    answer=$(rpc '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}')
    echo "$answer"
    contains "$answer" "2025-06-18" && contains "$answer" "copal"
}
check initialize_speaks_the_protocol initialize_speaks_the_protocol

tools_list_serves_the_manifest() {
    answer=$(rpc '{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
    contains "$answer" "files_list" \
        && contains "$answer" "file_create" \
        && contains "$answer" "search"
}
check tools_list_serves_the_manifest tools_list_serves_the_manifest

files_list_sees_the_mirror() {
    answer=$(rpc '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"files_list","arguments":{"limit":100}}}')
    echo "$answer" | head -c 400
    contains "$answer" "acme-msa.txt"
}
check files_list_sees_the_mirror files_list_sees_the_mirror

# Extraction runs in the background after the mirror; the check
# retries until the passage ranks or the honest timeout fails it.
search_finds_extracted_text() {
    i=0
    while [ $i -lt 15 ]; do
        answer=$(rpc '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"search","arguments":{"q":"countersigned"}}}')
        if contains "$answer" "countersigned"; then
            return 0
        fi
        i=$((i + 1))
        sleep 1
    done
    echo "never ranked: $answer"
    return 1
}
check search_finds_extracted_text search_finds_extracted_text

cat "$OUT"
