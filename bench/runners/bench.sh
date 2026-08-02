#!/bin/sh
# The measurements, taken through the S3 gateway with stock tooling,
# because that is the path a migrating deployment actually uses.
#
# Every timing is taken three times and every sample is recorded, so
# the table can report the best beside the spread. A shared machine
# produces outliers upward, and the differences between the two
# configurations here are small enough that one sample cannot tell an
# outlier from an effect.
OUT=/results/bench.txt
: > "$OUT"
now_ms() {
    date +%s%3N
}
record() {
    echo "$1 $2" >> "$OUT"
}

mc alias set copal "http://copal:9000" "$COPAL_ACCESS_KEY" "$COPAL_SECRET_KEY" > /dev/null

dd if=/dev/urandom of=/tmp/large.bin bs=1M count=64 2> /dev/null
mkdir -p /tmp/small
i=0
while [ $i -lt 50 ]; do
    echo "small object $i, enough bytes to be a document rather than a marker" \
        > "/tmp/small/doc-$i.txt"
    i=$((i + 1))
done

pass=0
while [ $pass -lt 3 ]; do
    prefix="pass$pass"

    start=$(now_ms)
    mc cp /tmp/large.bin copal/"$COPAL_TENANT"/$prefix/large.bin > /dev/null 2>&1
    record ingest_64mib_ms $(( $(now_ms) - start ))

    # One client process for fifty objects: a loop of `mc cat` would
    # measure process launches at roughly 60ms each, which says more
    # about the container than about Copal.
    start=$(now_ms)
    mc cp --recursive /tmp/small copal/"$COPAL_TENANT"/$prefix/ > /dev/null 2>&1
    record ingest_50_small_ms $(( $(now_ms) - start ))

    start=$(now_ms)
    mc cp copal/"$COPAL_TENANT"/$prefix/large.bin /tmp/back-$pass.bin > /dev/null 2>&1
    record retrieve_64mib_ms $(( $(now_ms) - start ))

    start=$(now_ms)
    mc cp --recursive copal/"$COPAL_TENANT"/$prefix/small /tmp/fetched-$pass > /dev/null 2>&1
    record retrieve_50_small_ms $(( $(now_ms) - start ))

    start=$(now_ms)
    mc ls -r copal/"$COPAL_TENANT" > /dev/null 2>&1
    record listing_ms $(( $(now_ms) - start ))

    pass=$((pass + 1))
done

cat "$OUT"
