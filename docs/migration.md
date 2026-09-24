# Migrating from MinIO

MinIO's community edition stopped receiving development, security
patches, and binaries in 2026. Its usual replacements hand back a
bucket API and nothing else. Copal's S3 gateway accepts the same
tooling and adds what a plain object store lacks: every migrated file
gets dedupe, versioning, full-text search, an event stream, and a
typed API beside the bucket one, plus malware scanning and semantic
search when those services are configured.

The migration is one `mirror` run with stock tools. Nothing here is
Copal-specific tooling to install.

## 1. Prepare the target

Run Copal with the gateway enabled:

```
COPAL_BLOB_ENCRYPTION_KEY=<64 hex>   # gateway credentials are sealed under it
COPAL_S3_BIND=0.0.0.0:9000
COPAL_ADMIN_TOKEN=<operator token>   # the admin surface exists only with one
```

Mint a credential for the tenant that will own the content (on the
admin listener, if you set `COPAL_ADMIN_BIND`):

```
curl -X POST -H "x-copal-admin-token: $ADMIN" \
  http://copal:8080/v1/admin/tenants/acme/s3-credentials
```

The response carries `access_key_id` and `secret_access_key`, once.
The bucket is the tenant id: this credential reads and writes bucket
`acme` and nothing else.

## 2. Run the copy

With `mc`:

```
mc alias set old   http://minio:9000 <old key>   <old secret>
mc alias set copal http://copal:9000 <access key> <secret>
mc mirror old/data copal/acme
```

With rclone or the aws CLI, the same shape:

```
rclone sync old:data copal:acme
aws --endpoint-url http://copal:9000 s3 sync s3://data s3://acme
```

The tools use multipart for large objects, `CopyObject` for
server-side moves, and batch `DeleteObjects` when syncing removals;
the gateway serves all three. Path-style addressing is the gateway's
native form, which is what these tools emit when given a bare
endpoint URL.

## 3. What happens to the content

Every object lands through the same path as any other upload:

- Content is deduplicated by digest. Two identical objects store one
  blob, whatever their keys.
- Each file runs the upload pipeline: scanned when a scanner is
  configured, its text extracted, and its passages embedded when an
  embedding service is configured, so the corpus is searchable when
  the pipeline catches up with the mirror. Without a scanner, files
  serve as soon as their bytes land.
- Re-running the mirror is safe. Unchanged objects re-upload into the
  same digests; changed ones mint versions, and the previous content
  keeps serving until the new version is ready.

## 4. Verify

```
mc diff old/data copal/acme          # empty output means every object arrived
curl -H "x-copal-tenant: acme" "http://copal:8080/v1/search?q=<term>"
```

The second line searches the mirrored content. It uses header mode;
with `COPAL_AUTH_MODE=keys`, send `Authorization: Bearer <key>`
instead.

## The conformance table

The recorded run below became a harness: `conformance/run.sh` stands
the stack up, runs the same scenario through mc, the aws CLI, and
rclone with every check named, and renders a per-release results
table in `conformance/results/`. The claim is self-verifiable: run
the same command on your own hardware and compare tables.

## The recorded run

Every transcript below is from a live run: MinIO and SurrealDB in
containers, Copal on the host with `COPAL_S3_BIND` and an encryption
key, a credential minted on the admin surface, and MinIO's own `mc`
driving the migration. The source bucket held nested prefixes and an
80 MiB binary large enough to cross `mc`'s multipart threshold.

```
$ mc mirror minio/archive copal/legacy-corp
`minio/archive/checksum.txt` -> `copal/legacy-corp/checksum.txt`
`minio/archive/contracts/2024/acme-msa.txt` -> `copal/legacy-corp/contracts/2024/acme-msa.txt`
`minio/archive/reports/q2-summary.md` -> `copal/legacy-corp/reports/q2-summary.md`
`minio/archive/contracts/2025/acme-renewal.txt` -> `copal/legacy-corp/contracts/2025/acme-renewal.txt`
`minio/archive/reports/telemetry-export.bin` -> `copal/legacy-corp/reports/telemetry-export.bin`
Total: 80.00 MiB, Transferred: 80.00 MiB, Duration: 00m13s

$ mc ls -r copal/legacy-corp
[2026-08-01 21:32 UTC]    65B STANDARD checksum.txt
[2026-08-01 21:32 UTC]    29B STANDARD contracts/2024/acme-msa.txt
[2026-08-01 21:32 UTC]    28B STANDARD contracts/2025/acme-renewal.txt
[2026-08-01 21:32 UTC]    21B STANDARD reports/q2-summary.md
[2026-08-01 21:32 UTC]  80MiB STANDARD reports/telemetry-export.bin

$ mc mirror minio/archive copal/legacy-corp    # second run
Total: 0 B, Transferred: 0 B, Duration: 00m00s

$ mc diff minio/archive copal/legacy-corp
(no differences)

$ mc cp copal/legacy-corp/reports/telemetry-export.bin /tmp/back.bin
$ sha256sum /tmp/back.bin
899f019574284822b7015e1437f8ef8d9f4aaa3c9c256cee0e6efef394e7cecf
$ mc cat minio/archive/checksum.txt            # digest recorded at seed time
899f019574284822b7015e1437f8ef8d9f4aaa3c9c256cee0e6efef394e7cecf
```

The 80 MiB object crossed as five multipart parts under `mc`'s
streaming signatures and round-tripped byte-identical. The second
mirror moved nothing, and `mc diff` closes the loop from the source
side.

## Boundaries

- One credential reaches one tenant's bucket. Cross-tenant copies
  refuse.
- Bucket policies, object ACLs, and presigned MinIO URLs do not
  migrate; Copal's grants and edge tokens replace them, minted through
  the API after the content arrives.
- Object Lock and MinIO versioning history do not transfer; the
  mirror carries current objects, and versioning starts fresh on
  Copal's side.
