# Backup and restore

A Copal deployment holds two stores: the metadata plane in SurrealDB
and content in one or more blob backends. Neither can be restored
usefully without the other, and the order matters, so the procedure
is written down rather than left to inference.

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

Named residencies are separate backends with their own roots and
possibly their own encryption keys. A deployment with residencies
has one blob store per residency, and all of them are part of the
backup.

**Keys are not in either store.** `COPAL_BLOB_ENCRYPTION_KEY`,
per-residency keys, `COPAL_ENGINE_ACCESS_KEY`, and
`COPAL_ADMIN_TOKEN` live in the deployment's configuration. A backup
of both stores without the keys restores unreadable bytes: sealed
objects open only under the key that sealed them, and S3 gateway
credentials are stored sealed. Back up the configuration with the
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

1. **Blobs.** Copy each backend's root. For a filesystem root, any
   file-level copy works (`rsync -a`, a filesystem snapshot, a
   volume snapshot). For an S3-compatible residency, a bucket
   replication or `aws s3 sync` to the backup location. No quiesce is
   needed: content is written to a staging name and renamed onto its
   address, so a partial write is never visible under its digest.
   Staging entries may be copied; they are swept on the staging TTL
   and are safe to exclude.

2. **Metadata.** Export the database after the blob copy completes:

   ```
   surreal export --conn ws://127.0.0.1:8000 --user root --pass root \
       --ns copal --db copal copal-metadata.surql
   ```

   The export is a consistent read of the database at the time it
   runs. It does not stop writes, so a file uploaded during the
   export may or may not appear; either way it references content
   already backed up in step 1.

3. **Configuration.** The encryption keys, the engine access key, the
   admin token, and the residency map. Store these separately from
   the data backup: an attacker holding both holds everything.

## Restoring

1. Stand up an empty SurrealDB and empty blob roots.
2. **Restore blobs first**, into the same residency layout the
   deployment expects. Residency names are recorded on the version
   rows, so `local` must land in the local root and each named
   residency in its own.
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
3. Download one file per residency and compare its digest to the
   `digest` on its record. This is the check that proves the two
   stores agree, and it is the one worth automating.
4. If the deployment runs the S3 gateway, `mc diff` the source
   bucket against the restored one, which is the same check the
   conformance harness makes.

Practice the drill against a scratch deployment before needing it.
An untested backup is a claim rather than a procedure.

## What this procedure does not cover

Point-in-time recovery between backups. Copal keeps no write-ahead
archive of its own; restoring lands you at the last export. A
deployment needing tighter recovery points should take metadata
exports more often, since they are the small half, and rely on the
blob store's own versioning or replication for content.
