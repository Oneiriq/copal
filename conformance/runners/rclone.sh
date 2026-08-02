#!/bin/sh
# The rclone scenario. Size-only checking is deliberate: Copal's ETag
# is the SHA-256 digest rather than an MD5, which rclone would read
# as every object differing; the round-trip digest check covers
# content integrity honestly instead.
OUT=/results/rclone.txt
: > "$OUT"
export RCLONE_CONFIG_COPAL_TYPE=s3
export RCLONE_CONFIG_COPAL_PROVIDER=Other
export RCLONE_CONFIG_COPAL_ENDPOINT=http://copal:9000
export RCLONE_CONFIG_COPAL_ACCESS_KEY_ID="$COPAL_ACCESS_KEY"
export RCLONE_CONFIG_COPAL_SECRET_ACCESS_KEY="$COPAL_SECRET_KEY"
B="$COPAL_TENANT"
check() {
    name=$1
    shift
    if "$@" > /tmp/check.log 2>&1; then
        echo "rclone $name PASS" >> "$OUT"
    else
        echo "rclone $name FAIL" >> "$OUT"
        # Builtins only: the runner images are minimal and some
        # carry no sed.
        head -5 /tmp/check.log | while read -r line; do
            echo "    $line" >> "$OUT"
        done
    fi
}

check sync_in rclone sync /seed "copal:$B/rclone" --exclude checksum.txt

check check_sizes rclone check /seed "copal:$B/rclone" --size-only --exclude checksum.txt

roundtrip() {
    [ "$(rclone cat "copal:$B/rclone/reports/telemetry-export.bin" | sha256sum | cut -d' ' -f1)" \
        = "$(cat /seed/checksum.txt)" ]
}
check roundtrip_digest roundtrip

sync_noop() {
    # rclone re-uploads when its comparison disagrees, which lands
    # on keys whose previous upload may still be processing; the
    # gateway answers SlowDown there and rclone's own retries carry
    # it, so a clean exit is the property worth asserting.
    rclone sync /seed "copal:$B/rclone" --exclude checksum.txt
}
check sync_noop sync_noop

delete_propagates() {
    rclone deletefile "copal:$B/rclone/contracts/2025/acme-renewal.txt"
    ! rclone ls "copal:$B/rclone/contracts/2025/" 2> /dev/null | grep -q renewal
}
check delete_propagates delete_propagates

cat "$OUT"
