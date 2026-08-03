#!/bin/sh
# The aws CLI scenario, against the same tenant bucket. The abort
# check is the named pin for ghost listings: an abandoned multipart
# upload must be invisible to listings, or sync tools refuse to
# resume over the phantom key.
OUT=/results/aws.txt
: > "$OUT"
EP="--endpoint-url http://copal:9000"
B="$COPAL_TENANT"
export AWS_ACCESS_KEY_ID="$COPAL_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$COPAL_SECRET_KEY"
check() {
    name=$1
    shift
    if "$@" > /tmp/check.log 2>&1; then
        echo "aws $name PASS" >> "$OUT"
    else
        echo "aws $name FAIL" >> "$OUT"
        # Builtins only: the runner images are minimal and some
        # carry no sed.
        head -5 /tmp/check.log | while read -r line; do
            echo "    $line" >> "$OUT"
        done
    fi
}

check sync_in aws $EP s3 sync /seed "s3://$B/awssync" --exclude "checksum.txt"

listing_complete() {
    src=$(find /seed -type f ! -name checksum.txt | wc -l)
    dst=$(aws $EP s3 ls "s3://$B/awssync/" --recursive | wc -l)
    [ "$src" -eq "$dst" ]
}
check listing_complete listing_complete

head_last_modified() {
    aws $EP s3api head-object --bucket "$B" \
        --key awssync/reports/telemetry-export.bin | grep -q LastModified
}
check head_last_modified head_last_modified

multipart_roundtrip() {
    aws $EP s3 cp "s3://$B/awssync/reports/telemetry-export.bin" /tmp/back.bin > /dev/null
    [ "$(sha256sum /tmp/back.bin | cut -d' ' -f1)" = "$(cat /seed/checksum.txt)" ]
}
check multipart_roundtrip_digest multipart_roundtrip

abort_leaves_no_ghost() {
    upload=$(aws $EP s3api create-multipart-upload --bucket "$B" \
        --key awssync/ghost.bin --query UploadId --output text) || return 1
    dd if=/dev/urandom of=/tmp/part.bin bs=1M count=5 2> /dev/null
    aws $EP s3api upload-part --bucket "$B" --key awssync/ghost.bin \
        --part-number 1 --upload-id "$upload" --body /tmp/part.bin > /dev/null || return 1
    aws $EP s3api abort-multipart-upload --bucket "$B" \
        --key awssync/ghost.bin --upload-id "$upload" || return 1
    ! aws $EP s3 ls "s3://$B/awssync/" --recursive | grep -q ghost.bin
}
check abort_leaves_no_ghost abort_leaves_no_ghost

sync_noop() {
    [ -z "$(aws $EP s3 sync /seed "s3://$B/awssync" --exclude "checksum.txt")" ]
}
check sync_noop sync_noop

# Conditional writes, the S3 2024+ semantics agents rely on for safe
# concurrent writes: create-only refuses over existing content, and
# replace-if-match refuses a stale belief.
conditional_create_refuses_existing() {
    echo conditional > /tmp/cond.txt
    aws $EP s3api put-object --bucket "$B" --key awssync/cond.txt         --body /tmp/cond.txt > /dev/null 2>&1 || return 1
    if aws $EP s3api put-object --bucket "$B" --key awssync/cond.txt         --body /tmp/cond.txt --if-none-match '*' > /tmp/cond-err.txt 2>&1; then
        echo "a second create-only PUT must refuse"
        return 1
    fi
    grep -q "PreconditionFailed" /tmp/cond-err.txt
}
check conditional_create_refuses_existing conditional_create_refuses_existing

conditional_replace_needs_the_current_etag() {
    etag=$(aws $EP s3api head-object --bucket "$B" --key awssync/cond.txt         --query ETag --output text | tr -d '"')
    echo replaced > /tmp/cond2.txt
    if aws $EP s3api put-object --bucket "$B" --key awssync/cond.txt         --body /tmp/cond2.txt --if-match "0000000000000000000000000000000000000000000000000000000000000000"         > /tmp/cond-err2.txt 2>&1; then
        echo "a stale If-Match must refuse"
        return 1
    fi
    grep -q "PreconditionFailed" /tmp/cond-err2.txt || return 1
    aws $EP s3api put-object --bucket "$B" --key awssync/cond.txt         --body /tmp/cond2.txt --if-match "$etag" > /dev/null 2>&1
}
check conditional_replace_needs_the_current_etag conditional_replace_needs_the_current_etag

delete_propagates() {
    aws $EP s3 rm "s3://$B/awssync/reports/q2-summary.md" > /dev/null
    ! aws $EP s3 ls "s3://$B/awssync/reports/" | grep -q q2-summary
}
check delete_propagates delete_propagates

cat "$OUT"
