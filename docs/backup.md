# Backup and restore

A Copal deployment holds two stores: the metadata plane in SurrealDB
and content in one or more blob backends. Neither can be restored
usefully without the other, and the order matters. This page is the
procedure.

## What is where

**The metadata plane** holds every record: files, versions, blobs,
grants, keys, webhook endpoints, workflow journals, audit events. It
is the smaller store and the one that changes constantly.

**Blob backends** hold content, addressed by digest under
`objects/<aa>/<bb>/<digest>`. The path is derived from the content,
so an object is never rewritten in place: a write lands in
`staging/<ulid>` and is renamed onto its address. Content is
therefore append-only in practice, which is what makes the ordering
below safe.

Named residencies are separate backends (filesystem, S3-compatible,
Google Cloud Storage, or Azure Blob Storage) with their own roots and
possibly their own encryption keys. Each residency may also configure
storage tiers, which are further backends. A deployment has one blob
store per residency plus one per tier, and all of them are part of
the backup.

**Keys are not in either store.** `COPAL_BLOB_ENCRYPTION_KEY`,
per-residency keys, `COPAL_ENGINE_ACCESS_KEY`, and
`COPAL_ADMIN_TOKEN` live in the deployment's configuration, or the
master key in key custody. A backup of both stores without the keys
restores unreadable bytes: sealed objects open only under the key
that sealed them, and S3 gateway credentials, webhook secrets, and
edge keys are stored sealed. Back up the configuration with the
same discipline as the data, and separately.

## The ordering rule

**Back up blobs first, then metadata.** Metadata captured after
content can only reference objects the content backup already holds.
The reverse order can capture a record whose bytes were written
after the blob snapshot began, which restores as a file that exists
and cannot be read.

The same rule read from the other side: a blob with no record is
harmless. It is unreferenced content, and the sweep collects it a
grace period later. A record with no blob is a broken file. Order
the backup so the harmless failure is the only possible one.

## Taking a backup

1. **Blobs.** Copy each backend's root, tiers included. For a
   filesystem root, any file-level copy works (`rsync -a`, a
   filesystem snapshot, a volume snapshot). For a bucket, use the
   provider's replication or sync tooling to the backup location (for
   example `aws s3 sync`, `gcloud storage rsync`, or `azcopy sync`).
   No quiesce is needed: content is written to a staging name and
   renamed onto its address, so a partial write is never visible under
   its digest. Staging entries may be copied; they are swept on the
   staging TTL and are safe to exclude. If you use storage tiers, set
   `COPAL_TIER_ERASE_GRACE_SECS` to at least your backup cadence, so
   a tier move between the blob copy and the metadata export leaves
   the bytes findable in the backup.

2. **Metadata.** Export the database after the blob copy completes:

   ```
   surreal export --conn ws://127.0.0.1:8000 --user root --pass root \
       --ns copal --db copal copal-metadata.surql
   ```

   The export is a consistent read of the database at the time it
   runs. It does not stop writes, so a file uploaded during the
   export may or may not appear; either way it references content
   already backed up in step 1. On the embedded tier
   (`COPAL_DB_URL=surrealkv://...`) there is no server to export
   from; see the embedded tier section of
   [operations.md](operations.md).

3. **Configuration.** The encryption keys, the engine access key, the
   admin token, and the residency map. Store these separately from
   the data backup: an attacker holding both holds everything.

## Restoring

1. Stand up an empty SurrealDB and empty blob roots.
2. **Restore blobs first**, into the same residency and tier layout
   the deployment expects. Blob rows record which residency (and
   tier) holds each object, so `local` must land in the local root,
   each named residency in its own backend, and each tier in its own.
3. **Import the metadata**:

   ```
   surreal import --conn ws://127.0.0.1:8000 --user root --pass root \
       --ns copal --db copal copal-metadata.surql
   ```

4. Restore the configuration, then start Copal. Boot reconciles the
   schema against the code, so a backup taken under an older release
   receives every later definition on its first start under the newer
   one.

## What a restore gives you

**Content and records agree**, given the ordering above: every record
that survived references content the blob restore holds.

**Derived state rebuilds itself.** The cached tenant usage counter is
advisory and the sweep recomputes it. Rate windows are minute-scoped
and expire on their own. Neither needs care during restore.

**Upload claims may be stale.** A file left `uploading` when the
backup was taken restores that way; its lease expires and the sweep
reaps it back to `failed`, where a client can retry. This is the same
path a crashed process takes, so restore reuses recovery that already
exists.

**In-flight workflows resume.** Runs are journaled, so a restored
`running` run is claimed by a worker and replays from its journal
without repeating completed steps.

**Multipart sessions are lost in effect.** A session whose parts were
staged may reference staged objects the backup excluded; the session
expires on its TTL and the client re-uploads. S3 clients treat an
expired multipart upload as a normal failure and retry.

## Verifying a restore

A restore that has not been read from has not been verified. The
minimum drill:

1. `GET /readyz`, which proves Copal reached the database.
2. List files for a known tenant and confirm the count matches the
   source.
3. Download one file per residency, and one per tier, and compare its
   digest to the `digest` on its record. This is the check that proves
   the two stores agree, and it is the one worth automating.
4. If the deployment runs the S3 gateway, `mc diff` the source
   bucket against the restored one, which is the same check the
   conformance harness makes.

Practice the drill against a scratch deployment before you need it.

## What this procedure does not cover

Point-in-time recovery between backups. Copal keeps no write-ahead
archive of its own; restoring lands you at the last export. A
deployment needing tighter recovery points should take metadata
exports more often, since they are the small half, and rely on the
blob store's own versioning or replication for content.
