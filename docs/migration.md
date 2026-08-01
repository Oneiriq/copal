# Migrating from MinIO

MinIO's community edition stopped receiving development, security
patches, and binaries in 2026. Its usual replacements hand back a
bucket API and nothing else. Copal's S3 gateway accepts the same
tooling and adds what a plain object store cannot: every migrated file
gets scanning, dedupe, versioning, full-text and semantic search, an
event stream, and a typed API beside the bucket one.

The migration is one `mirror` run with stock tools. Nothing here is
Copal-specific tooling to install.

## 1. Prepare the target

Run Copal with the gateway enabled:

```
COPAL_BLOB_ENCRYPTION_KEY=<64 hex>   # gateway credentials are sealed under it
COPAL_S3_BIND=0.0.0.0:9000
```

Mint a credential for the tenant that will own the content:

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
- With a pipeline configured, each file is scanned, its text
  extracted, and its passages embedded, so the corpus is searchable
  when the mirror finishes. Without one, files serve immediately.
- Re-running the mirror is safe. Unchanged objects re-upload into the
  same digests; changed ones mint versions, and the previous content
  keeps serving until the new version is ready.

## 4. Verify

```
mc diff old/data copal/acme          # empty output = every object arrived
curl -H "x-copal-tenant: acme" "http://copal:8080/v1/search?q=<term>"
```

The second line is the point of moving: the mirrored bucket answers
questions now.

## Boundaries

- One credential reaches one tenant's bucket. Cross-tenant copies
  refuse.
- Bucket policies, object ACLs, and presigned MinIO URLs do not
  migrate; Copal's grants and edge tokens replace them, minted through
  the API after the content arrives.
- Object Lock and MinIO versioning history do not transfer; the
  mirror carries current objects, and versioning starts fresh on
  Copal's side.
