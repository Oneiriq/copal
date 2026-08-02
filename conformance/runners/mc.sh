#!/bin/sh
# The mc scenario. Every check lands one PASS or FAIL line in the
# results file; the harness turns those into the table. The named
# checks include the defects the first live run found, so none of
# them can quietly return.
#
# The mc image carries no grep, sed, or awk, so matching uses shell
# case patterns. A check that shells out to a missing tool fails
# silently or, worse, passes: `! grep ...` succeeds when grep is
# absent, which reads as a green check that tested nothing.
OUT=/results/mc.txt
: > "$OUT"
check() {
    name=$1
    shift
    if "$@" > /tmp/check.log 2>&1; then
        echo "mc $name PASS" >> "$OUT"
    else
        echo "mc $name FAIL" >> "$OUT"
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

mc alias set minio http://minio:9000 minioadmin minioadmin > /dev/null
mc alias set copal "http://copal:9000" "$COPAL_ACCESS_KEY" "$COPAL_SECRET_KEY" > /dev/null

# minio-go asks GetBucketLocation with a trailing slash before doing
# anything; a stock listing proves both answers.
check bucket_reachable mc ls copal/"$COPAL_TENANT"

check mirror_full mc mirror minio/archive copal/"$COPAL_TENANT"

listing_complete() {
    src=$(mc ls -r minio/archive | wc -l)
    dst=$(mc ls -r copal/"$COPAL_TENANT" | wc -l)
    echo "source $src objects, target $dst"
    [ "$src" -eq "$dst" ]
}
check listing_complete listing_complete

second_pass_noop() {
    # --json because the human summary is a drawn table whose shape
    # is not a contract; the machine form states the transfer total.
    summary=$(mc mirror --json minio/archive copal/"$COPAL_TENANT" 2>&1 | tail -1)
    echo "second pass: $summary"
    contains "$summary" '"transferred":0'
}
check second_pass_noop second_pass_noop

diff_empty() {
    differences=$(mc diff minio/archive copal/"$COPAL_TENANT" 2>&1)
    echo "differences: ${differences:-none}"
    [ -z "$differences" ]
}
check diff_empty diff_empty

# mc refuses reads whose response lacks Last-Modified.
check stat_readable mc stat copal/"$COPAL_TENANT"/reports/telemetry-export.bin

multipart_roundtrip() {
    mc cp copal/"$COPAL_TENANT"/reports/telemetry-export.bin /tmp/back.bin > /dev/null
    got=$(sha256sum /tmp/back.bin | cut -d' ' -f1)
    want=$(cat /seed/checksum.txt)
    echo "returned $got, seeded $want"
    [ "$got" = "$want" ]
}
check multipart_roundtrip_digest multipart_roundtrip

delete_propagates() {
    mc rm copal/"$COPAL_TENANT"/contracts/2024/acme-msa.txt > /dev/null
    listing=$(mc ls copal/"$COPAL_TENANT"/contracts/2024/ 2>&1)
    echo "after delete: ${listing:-empty}"
    ! contains "$listing" acme-msa
}
check delete_propagates delete_propagates

# Restore for the next runner.
mc cp minio/archive/contracts/2024/acme-msa.txt \
    copal/"$COPAL_TENANT"/contracts/2024/acme-msa.txt > /dev/null 2>&1
cat "$OUT"
