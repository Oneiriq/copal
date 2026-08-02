#!/bin/sh
# Seed the migration source: nested prefixes, small documents, and an
# 80 MiB binary that crosses every client's multipart threshold. The
# digest recorded here is what round trips must reproduce.
set -e
mc alias set minio http://minio:9000 minioadmin minioadmin > /dev/null

mkdir -p /seed/contracts/2024 /seed/contracts/2025 /seed/reports
echo "MSA countersigned 2024-03-11" > /seed/contracts/2024/acme-msa.txt
echo "Renewal executed 2025-01-20" > /seed/contracts/2025/acme-renewal.txt
echo "Q2 revenue narrative" > /seed/reports/q2-summary.md
dd if=/dev/urandom of=/seed/reports/telemetry-export.bin bs=1M count=80 2> /dev/null
sha256sum /seed/reports/telemetry-export.bin | cut -d' ' -f1 > /seed/checksum.txt

mc mb --ignore-existing minio/archive > /dev/null
mc cp --recursive /seed/contracts /seed/reports minio/archive/ > /dev/null
mc cp /seed/checksum.txt minio/archive/checksum.txt > /dev/null
echo "seeded: $(mc ls -r minio/archive | wc -l) objects, digest $(cat /seed/checksum.txt)"
