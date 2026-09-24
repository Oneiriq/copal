# Principals within tenants

Status: shipped. All five slices in the build order below are in the
code: the `principal` table and admin routes, scope intersection at
authentication, the `pr` claim and the ownership guard, per-principal
rate buckets, and `created_by` recording the handle. Two gaps remain,
both named where they apply: grant and tus uploads still record their
source labels instead of an actor, and the `private` and `tenant`
access levels are still enforced identically. The operator reference
is the principals section of [operations.md](operations.md).

This page explains the model and why it is shaped this way.

Without principals, the API key is the smallest identity Copal has: a
key belongs to a tenant, carries scopes, and every audit event names
the key that acted. That answers "which credential did this" but not
"who", which is the question a compliance reviewer asks and the one an
agent deployment asks about its own fleet.

## What a key alone gives

A key is an identity with scopes, an expiry, and a revocation. It
reaches the engine as a token claim (`id`, `tn`, `sc`, `adm`), so
engine `PERMISSIONS` filter by the tenant it belongs to and by the
scopes it holds. Field guards evaluate against a `Principal` whose
subject is the key id.

So the mechanism for identity-shaped policy exists end to end. A key
alone lacks a subject worth naming: every key is a peer of every other
key in its tenant, and two keys with the same scopes look the same to
every layer.

## What a principal adds

A **principal** is a named actor under a tenant: a person, a service,
or an agent. Keys become credentials that belong to a principal, which
makes the following expressible:

- **Audit that names the actor.** "ck1_abc removed the file" becomes
  "alice removed the file, using key ck1_abc". Rotating a key stops
  breaking the audit trail's continuity, because the actor outlives
  the credential.
- **Guards that distinguish people.** With keys alone a guard can ask
  "does the caller hold admin". With principals it can ask "did this
  principal create this row", which is the shape most per-field policy
  wants (`created_by` visible to its author and to admins).
- **Per-agent attribution and budget.** An agent fleet issues one
  principal per agent, so a runaway agent is identifiable, revocable,
  and metered on its own budget.
- **Delegation with a shorter leash.** A principal may hold fewer
  scopes than its tenant permits, and a key may hold fewer than its
  principal, so narrowing is always possible and widening never is.

## The model

One table, `principal`:

- `tenant_id`, the owning tenant.
- `handle`: stable, unique within the tenant (`alice`,
  `ingest-worker`, `agent-7`). What audit prints and what guards
  compare.
- `kind`: `human`, `service`, or `agent`. Carried for reporting; the
  enforcement layers do not branch on it, because a rule that treats
  agents differently from people is a policy decision a deployment
  should state explicitly.
- `scopes`: the ceiling for keys issued to this principal.
- `disabled_at`: disabling a principal refuses every one of its keys
  at once, which is the operation an incident needs.

`api_key` carries an optional `principal_id`. A key without one works
as a tenant-level credential, which made the change safe to adopt:
keys minted before principals existed behave exactly as they did.

## How it reaches the layers

The token claims carry `pr` (the principal handle) beside `id` (the
key). Everything downstream follows:

- **The application layer** puts the principal in `Principal.subject`
  in place of the key id, so guards compare actors. The key id stays
  available for audit's "using key" half.
- **The engine layer** has `$token.pr`, so a compiled `PERMISSIONS`
  clause can express ownership: `created_by = $token.pr` beside the
  tenancy rule. This lets the second enforcement layer say what the
  first says.
- **Scopes** resolve as the intersection of the key's and the
  principal's, computed at authentication. A widened key under a
  narrowed principal grants nothing extra. Minting refuses a key whose
  scopes exceed its principal's ceiling, so the narrowing shows up
  when it is applied.
- **Rate classes** key their buckets on the principal, so an agent's
  budget is its own. A key without a principal keeps a bucket of its
  own. Header mode has no key, so every header-mode caller, whatever
  tenant it names, shares one bucket per rate class.

## What the record says

A version's `created_by` holds the principal handle when the upload
came from a key with a principal, through the REST body path or an S3
credential. Grant and tus uploads still record their source labels,
and rows written before principals existed keep their old value. The
ownership guard treats any `created_by` that matches no handle as
nobody's: visible to admins, hidden from ordinary principals. Treating
unknown authorship as ownership would widen access on upgrade.

## The admin surface

Principals are managed on the admin listener, beside keys:

```
POST   /v1/admin/tenants/{tenant}/principals      { handle, kind, scopes }
GET    /v1/admin/tenants/{tenant}/principals
DELETE /v1/admin/tenants/{tenant}/principals/{handle}   (disables)
POST   /v1/admin/tenants/{tenant}/keys            { principal, name, scopes, ttl_secs }
```

Each writes an audit event (`principal.created`, `principal.disabled`,
`key.minted`) naming the acting admin, the principal, and the change,
because the trail of who was granted what is the reason this design
exists.

## Build order

1. The `principal` table, the `principal_id` column on `api_key`, and
   the admin surface. Nothing enforces differently yet. **Shipped.**
2. Authentication resolves the principal, intersects scopes, and
   fills `Principal.subject` with the handle. Audit gains the actor
   half. Guards keep working because `admin_only` reads scopes.
   **Shipped**: minting also answers to the ceiling, refusing scopes
   beyond the principal's rather than silently shrinking them, and
   disabling a principal refuses every one of its keys live.
3. The `pr` claim, and ownership expressible in compiled
   `PERMISSIONS`. The first ownership guard ships with it, which is
   `created_by` visible to its author. **Shipped**: guards see the
   row now, so the application layer projects per row (the author
   keeps the value, a stranger loses it, within one listing), the
   engine clause says the same thing (`$token.adm = true OR
   created_by = $token.pr`), and a partial viewer cannot filter by
   the column.
4. Rate buckets key on the principal. **Shipped**: keys under one
   principal share its budget, so an agent's spend is the agent's
   regardless of how many credentials it rotated through.
5. `created_by` writes the handle, with the unknown-authorship rule
   above. **Shipped**: the REST body path records the actor; grant
   and TUS uploads keep their source labels until those carriers
   learn principals, and every legacy value reads as nobody's. S3
   credentials learned principals afterward: uploads through SigV4
   attribute to the credential's actor.

Slices 1 and 2 are additive: a deployment with no principals behaves
exactly as it did before principals existed, so adopting them is
optional.
