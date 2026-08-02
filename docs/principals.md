# Principals within tenants

Design, ahead of code. The API key is the smallest identity Copal
has: a key belongs to a tenant, carries scopes, and every audit event
names the key that acted. That is enough to answer "which credential
did this" and not enough to answer "who", which is the question a
compliance reviewer asks and the one an agent deployment asks about
its own fleet.

## What the key model already gives

A key is an identity with scopes, an expiry, and a revocation. It
reaches the engine as a token claim (`id`, `tn`, `sc`, `adm`), so
engine `PERMISSIONS` already filter by the tenant it belongs to and
by the scopes it holds. Field guards evaluate against a `Principal`
whose subject is the key id.

So the mechanism for identity-shaped policy exists end to end. What
is missing is a subject worth naming: every key is a peer of every
other key in its tenant, and two keys with the same scopes are
indistinguishable to every layer.

## What a principal adds

A **principal** is a named actor under a tenant: a person, a service,
or an agent. Keys stop being identities and become credentials
*belonging to* a principal, which is what makes the following
expressible:

- **Audit that names the actor.** "ck1_abc removed the file" becomes
  "alice removed the file, using key ck1_abc". Rotating a key stops
  breaking the audit trail's continuity, because the actor outlives
  the credential.
- **Guards that distinguish people.** Today a guard can ask "does the
  caller hold admin". With principals it can ask "did this principal
  create this row", which is the shape most per-field policy actually
  wants (`created_by` visible to its author and to admins).
- **Per-agent attribution and budget.** An agent fleet issues one
  principal per agent, so a runaway agent is identifiable, revocable,
  and meterable on its own rather than sharing its tenant's ledger
  with every sibling.
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
  should state explicitly rather than inherit.
- `scopes`: the ceiling for keys issued to this principal.
- `disabled_at`: disabling a principal refuses every one of its keys
  at once, which is the operation an incident actually needs.

`api_key` gains `principal_id`. A key without one keeps working as a
tenant-level credential, which is what makes this migratable: today's
keys are principal-less and behave exactly as they do now.

## How it reaches the layers

The token claims gain `pr` (the principal handle) beside the existing
`id` (the key). Everything downstream follows:

- **The application layer** puts the principal in `Principal.subject`
  rather than the key id, so guards compare actors. The key id stays
  available for audit's "using key" half.
- **The engine layer** gains `$token.pr`, so a compiled `PERMISSIONS`
  clause can express ownership: `created_by = $token.pr` beside the
  tenancy rule. This is the point of the whole design, because it
  makes the second enforcement layer able to say what the first says.
- **Scopes** resolve as the intersection of the key's and the
  principal's, computed at authentication. A widened key under a
  narrowed principal grants nothing extra, and the narrowing is
  visible at the moment it is applied rather than at the moment it is
  needed.
- **Rate classes** key their buckets on the principal, so an agent's
  budget is its own. Keyless surfaces (header mode) keep the tenant
  bucket they use now.

## What changes in the record

`created_by` currently holds `"api"` or a key-derived string. It
becomes the principal handle, which is what makes the ownership guard
meaningful. Rows written before principals exist keep their current
value, so the guard must treat an unrecognized `created_by` as
"nobody in particular": visible to admins, hidden from ordinary
principals. Stated plainly because the alternative, treating unknown
authorship as ownership, would silently widen access on upgrade.

## The admin surface

Principals are managed on the admin listener, beside keys:

```
POST   /v1/admin/tenants/{tenant}/principals      { handle, kind, scopes }
GET    /v1/admin/tenants/{tenant}/principals
DELETE /v1/admin/tenants/{tenant}/principals/{handle}   (disables)
POST   /v1/admin/tenants/{tenant}/keys            { principal, name, scopes, ttl }
```

Every one writes an audit event naming the acting admin, the
principal, and the change, because the trail of who was granted what
is the reason this design exists.

## Build order

1. The `principal` table, the `principal_id` column on `api_key`, and
   the admin surface. Nothing enforces differently yet.
2. Authentication resolves the principal, intersects scopes, and
   fills `Principal.subject` with the handle. Audit gains the actor
   half. Guards keep working because `admin_only` reads scopes.
3. The `pr` claim, and ownership expressible in compiled
   `PERMISSIONS`. The first ownership guard ships with it, which is
   `created_by` visible to its author.
4. Rate buckets key on the principal.
5. `created_by` writes the handle, with the unknown-authorship rule
   above.

Slices 1 and 2 are additive: a deployment with no principals behaves
exactly as it does today, which is the property that lets this land
before anyone has to adopt it.
