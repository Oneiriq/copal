# API guide

Copal serves two faces from one contract: REST under `/v1` and GraphQL at
`/graphql`. Both dispatch into the same repositories and render rows through
the same wire mapper. The generated artifacts are the reference documents:

| Artifact | Path |
| --- | --- |
| OpenAPI 3.1 | `docs/openapi.json` |
| GraphQL SDL | `docs/schema.graphql` (also served at `GET /graphql`) |
| Rust client | `clients/client.rs` |
| TypeScript client | `clients/client.ts` |
| Python client | `clients/client.py` |
| Go client | `clients/client.go` |

This guide covers what the generated documents cannot: authentication, the
byte routes, error shapes, and the behaviors shared across faces.

## Authentication

Two modes, selected by `COPAL_AUTH_MODE`.

`keys` is the deployment mode. Requests carry `Authorization: Bearer
ck1.<key id>.<secret>`. The tenant comes out of the key row, so a caller
cannot name one. Every failure (absent, malformed, unknown, wrong secret,
revoked) returns the same 401 with the same timing.

`header` is the development mode and the default until 1.0. The
`x-copal-tenant` header is trusted as the tenant identity. The server logs a
warning at startup. See [operations.md](operations.md) for key custody.

Two routes skip tenant authentication by design: grant redemption (the token
is the authorization) and public-file content (anonymous by access level).

## Files

```
POST   /v1/files                    create a record (state: draft)
GET    /v1/files                    list (keyset pagination)
GET    /v1/files/{id}               fetch metadata
DELETE /v1/files/{id}               soft-delete
PUT    /v1/files/{id}/content       stream bytes in
GET    /v1/files/{id}/content       stream bytes out
GET    /v1/files/{id}/versions      version history (paginated)
GET    /v1/files/{id}/versions/{n}/content
POST   /v1/files/{id}/url           issue a signed URL
```

Create accepts `path`, `content_type`, `access`, `metadata`, and an optional
`idempotency_key`. A replayed key returns the original record with status 200
instead of 201. The `metadata.processing` namespace is server-owned; anything
a caller supplies there is stripped, because the pipeline writes its verdicts
under that key.

Upload is a single PUT of raw bytes. The server streams to staging while the
digest accumulates, then finalizes with a rename. The size ceiling
(`COPAL_MAX_UPLOAD_BYTES`) is enforced inside the stream and returns 413.
With a processing pipeline configured the record lands in `scanning` and a
run is enqueued; without one it lands in `ready`.

### Listing and cursors

List endpoints take `limit`, `cursor`, a filter, and `sort`. Filters and sorts
are contract-validated: every filterable column rides an index and every sort
is reachable through an index prefix. The response envelope is
`{ "items": [...], "next_cursor": "..." | null }`.

Cursors are opaque and embed their sort direction. Replaying a cursor under a
different sort returns 400, because the alternative is silently wrong pages.
A cursor minted on one face works on the other; both use the same codec.

### Access levels

The `access` field is enforced at the byte boundary.

| Level | Content behavior |
| --- | --- |
| `public` | Anonymous. Served with `Cache-Control: public, max-age=31536000, immutable`. |
| `private`, `tenant` | Owning tenant only. Served with `no-store`. The two levels coincide until principals within a tenant exist. |
| `grant` | Direct download refuses with 403 for everyone, owner included. Bytes flow through issued URLs only. |

Metadata, listing, and version routes require tenant authentication for every
level. Cross-tenant reads answer 404, so ids leak nothing.

### Serving behavior

Every byte route sends `ETag` (the content digest), `Accept-Ranges: bytes`,
`X-Content-Type-Options: nosniff`, and a sanitized `Content-Disposition`.
Script-capable types (HTML, SVG, XML) are forced to `attachment` so a hostile
upload cannot execute under the service origin.

`If-None-Match` answers 304 (weak comparison per RFC 9110). Single-range
`Range` requests answer 206 with `Content-Range`; unsatisfiable ranges answer
416; multi-range requests are served whole. `If-Range` with a stale validator
downgrades to the full object, so a resume across a re-upload cannot splice
two contents.

## Grants

`POST /v1/files/{id}/url` issues a capability for a servable file. The body
takes `ttl_secs` (default 900, capped at one year) and `max_uses`. The
response contains the bearer token exactly once; the store keeps its hash.

```
GET    /v1/grants/{token}     redeem: serve the bytes, no tenant header
DELETE /v1/grants/{id}        revoke, tenant-authenticated by grant id
```

Redemption refuses uniformly: malformed token, unknown grant, wrong secret,
revoked, expired, exhausted, and missing file all answer the same 404. A use
is consumed only when bytes actually serve. Refused redemptions and 304
revalidations leave the counter alone, and a revoked or expired grant stops
revalidating as well. Range requests consume per request, so media seeking
should use TTL-bounded grants without `max_uses`.

## Runs

```
POST   /v1/runs               start a workflow run
GET    /v1/runs               list (status filter, keyset pagination)
GET    /v1/runs/{id}          status plus the step journal
POST   /v1/runs/{id}/retry    retry a failed run
```

Start takes `workflow`, `input`, an optional subject `file`, an optional
`idempotency_key`, and `mode`. Async mode (the default) answers 202 with the
run id. Sync mode executes in-request and answers 200 with the output, or 202
with `status: "pending"` when a worker claimed the run first. A pending
answer means poll the run; it is distinct from a completed run whose output
is null.

Retry applies to failed runs only. The journal survives, completed steps
replay from their recorded output, and a failed subject file returns to
`scanning` first so the pipeline's finalize step applies. A run superseded by
a newer upload of different content refuses with 409. See
[processing.md](processing.md) for the underlying semantics.

## GraphQL

`POST /graphql` executes queries and mutations; `GET /graphql` serves the SDL.
The schema is built at startup from the contract, so it matches
`docs/schema.graphql` exactly.

Queries follow the contract vocabulary: `files(limit, cursor, state, sort)`,
`file(id)`, `runs(limit, cursor, status, sort)`, `run(id)`. Mutations map the
contract actions: `fileIssueUrl`, `fileRemove`, `runStart`, `runRetry`.

Depth is capped at 10 and complexity at 500, which rejects alias
amplification before any resolver runs. Auth failures surface as GraphQL
errors with `extensions.code`, keeping transport status 200.

## Errors

REST errors share one envelope:

```json
{ "error": { "kind": "bad_request", "message": "..." } }
```

GraphQL errors carry the same vocabulary in `extensions.code`. The kinds are
`bad_request`, `unauthorized`, `forbidden`, `not_found`, `conflict`,
`payload_too_large` (REST only), and `internal`. Internal failures log the
cause server-side and return no detail.

## Admin surface

Key custody and the audit trail live under `/v1/admin`, guarded by
`x-copal-admin-token` and excluded from the contract. With
`COPAL_ADMIN_BIND` set they exist only on that listener. See
[operations.md](operations.md).
