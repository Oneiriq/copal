# Operations

## Configuration

Every value comes from the environment. Defaults target local development.

| Variable | Default | Meaning |
| --- | --- | --- |
| `COPAL_BIND` | `127.0.0.1:8080` | Tenant-facing listener. |
| `COPAL_ADMIN_BIND` | unset | Separate listener for the admin surface. When set, admin routes exist only there. |
| `COPAL_DB_URL` | `ws://127.0.0.1:8000` | SurrealDB endpoint. |
| `COPAL_DB_NS` / `COPAL_DB_NAME` | `copal` / `copal` | Namespace and database. |
| `COPAL_DB_USER` / `COPAL_DB_PASS` | `root` / `root` | Database credentials. Use a scoped user in deployments. |
| `COPAL_BLOB_ROOT` | `./data/blobs` | Filesystem blob store root. |
| `COPAL_MAX_UPLOAD_BYTES` | `1073741824` | Upload ceiling, enforced in-stream (413 past it). |
| `COPAL_UPLOAD_LEASE_SECS` | `900` | Upload claim lease. Expired claims are stealable and reaped. |
| `COPAL_AUTH_MODE` | `header` | `header` (development) or `keys`. The default flips to `keys` at 1.0. |
| `COPAL_ADMIN_TOKEN` | unset | Operator token. Unset disables the admin surface entirely. |
| `COPAL_BLOCKED_EXTENSIONS` | built-in list | Comma-separated denylist for the upload pipeline. |
| `COPAL_ENFORCE_TYPE_MATCH` | `false` | Quarantine declared-type lies instead of annotating them. |
| `COPAL_SWEEP_INTERVAL_SECS` | `60` | Maintenance cadence. |
| `COPAL_STAGING_TTL_SECS` | `86400` | Staging entries older than this are deleted. |
| `COPAL_GC_GRACE_SECS` | `86400` | A blob must stay unreferenced this long before its bytes go. |
| `COPAL_GC_BATCH` | `1000` | Blob rows per GC batch query. The pass loops batches until the population is covered. |
| `COPAL_SCAN_STALE_SECS` | `3600` | Files stuck in `scanning` with no live run are failed after this age. |
| `COPAL_REQUEST_TIMEOUT_SECS` | `30` | Deadline for ordinary requests (408 past it). |
| `COPAL_TRANSFER_TIMEOUT_SECS` | `3600` | Deadline for the byte routes; ends slow-drip connections. |
| `COPAL_CORS_ORIGINS` | unset | Comma-separated browser-origin allowlist. Unset attaches no CORS layer at all. |

## Key custody

Keys are minted, listed, and revoked through the admin surface, guarded by
`x-copal-admin-token` compared in constant time.

```
POST   /v1/admin/tenants/{tenant}/keys            body: { "name": "ci" }
GET    /v1/admin/tenants/{tenant}/keys
DELETE /v1/admin/tenants/{tenant}/keys/{key_id}
GET    /v1/admin/tenants/{tenant}/audit
```

The mint response carries the bearer token once. Store it; the server keeps
the hash. Key names are unique per tenant, so rotation reads as revoke old,
mint new under the next name. Listings never include hash material.

Custody and lifecycle actions land in the audit trail: `key.minted`,
`key.revoked`, `grant.issued`, `grant.revoked`, `file.removed`. Audit rows
are immutable inside the engine; an UPDATE or DELETE against one aborts in
SurrealDB itself.

## Maintenance sweeps

One interval loop runs five failure-isolated passes: expired upload claims to
`failed`, expired run claims back to `pending`, stale scans to `failed`,
staging deletion past TTL, and blob garbage collection (mark, grace, fresh
recount, collect).

Replicas elect a sweep leader per pass through a database lease. Losers skip
the pass. A crashed leader's lease expires and any replica takes over. Worker
loops need no election; run claims are already CAS-guarded.

GC deletes the database row first, then the object bytes, with a final row
existence check between the two: identical content re-registered inside that
window aborts the collection and the fresh bytes stay.

## Runbook: files stuck in scanning

`scanning` is transient. If files sit there:

1. `GET /v1/runs?status=failed` and look for `post_upload` runs naming the
   file. A terminal pipeline failure moves the file to `failed` on its own;
   a file still in `scanning` usually means the run is pending or running.
2. A failed run retries with `POST /v1/runs/{id}/retry` once the cause (for
   example, unreadable blob storage) is fixed. The file returns to
   `scanning`, a worker finishes the journaled steps, and the record
   finalizes.
3. Orphans with no run at all (a crash between upload completion and
   enqueue) are failed automatically by the stale-scan sweep. A failed file
   with a digest keeps serving its previous content and accepts a re-upload.

## Backup and restore

Back up the blob plane first, then the metadata plane. That order
makes every metadata reference in the backup point at bytes the
backup already holds; anything uploaded between the two passes is
simply absent from both. The reverse order can capture records whose
bytes are missing.

The GC grace period (`COPAL_GC_GRACE_SECS`, default one day) covers
the other direction: a blob unreferenced at backup time still has its
bytes for the full grace window, so a restore inside that window
never resurrects a record whose content is gone.

Restore in the same order: bytes into the blob root, then the
SurrealDB import, then start the server (schema application is
idempotent). Run one maintenance pass afterward; derived refcounts
recompute on their own.

## Health endpoints

`/healthz` is liveness: the process answers. `/readyz` is readiness:
one store round trip plus one blob-plane call must both succeed, so
orchestrators gate traffic on real dependencies. The container image
health check uses `/readyz`.

## Deployment security posture

Development defaults are open on purpose. An exposed deployment sets
`COPAL_AUTH_MODE=keys` with `COPAL_ADMIN_TOKEN`, sets `COPAL_ADMIN_BIND` so
custody lives off the tenant network, terminates TLS in front of the server,
scrubs grant URLs from proxy logs (tokens ride the path by design; TTLs bound
the exposure), runs the database as a scoped user, and rate-limits at the
proxy until built-in quotas land.

What the service enforces on its own: uniform 401s across the whole
credential path, timing included; server-owned scan verdicts; `nosniff` and
sanitized `Content-Disposition` with forced download for script-capable
types; `no-store` on private and grant bytes; access levels at the byte
boundary; the engine-immutable audit trail.
