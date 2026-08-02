# Conformance

One command, one table:

```
./conformance/run.sh
```

The stack in `docker-compose.yml` builds Copal from this repository,
pins the engine to the version the code is developed against, seeds
MinIO as the migration source, and runs one scripted scenario per S3
client: MinIO's `mc`, the aws CLI, and rclone. Every check is named,
and the checks include the defects the first live migration run
surfaced, so none of them can quietly return:

- `bucket_reachable`: minio-go's trailing-slash bucket requests and
  `GetBucketLocation` answer.
- `stat_readable` / `head_last_modified`: served objects carry
  `Last-Modified`, which strict clients require before reading.
- `multipart_roundtrip_digest`: an 80 MiB object crosses as multipart
  under streaming signatures and returns byte-identical.
- `abort_leaves_no_ghost`: an abandoned multipart upload is invisible
  to listings, so sync tools resume instead of refusing over a
  phantom key.
- `second_pass_noop` / `sync_noop` / `diff_empty`: mirrors converge.

Results land in `results/<version>.md` and are committed per
release, which is what the migration guide links. The claim the
table makes is self-verifiable: run the same command on your own
hardware and compare.

The harness tests what it names, nothing more. Its credibility is
that the choices are visible, the suite is runnable by anyone, and
extending it is one script edit.
