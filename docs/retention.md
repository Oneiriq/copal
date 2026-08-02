# Retention, legal hold, and WORM

Design, ahead of code. The compliance tier enterprise buyers ask for
is three separate promises, and Copal already keeps parts of each by
accident of its architecture. What follows says which parts are
already true, what each promise adds, and where the additions
collide with mechanisms that exist.

## What is already true

**Versions are immutable.** A `file_version` row freezes at arming:
every scalar is `READONLY`, and an event throws on any update after
`armed = true`. History cannot be rewritten today, only added to.

**Deletion is a tombstone.** `DELETE` sets `deleted_at` and moves the
file to `deleted`, which releases the live-path slot and hides the
row from every read. Nothing erases bytes at that moment.

**Content outlives its links by a grace period.** The sweep marks a
blob whose derived link count is zero, and collects it only a full
`COPAL_GC_GRACE_SECS` (default 24h) after the first mark, aborting if
anything re-references it in between.

So Copal is already write-once for version content and
delete-is-hide for files. What compliance asks for is the ability to
make those properties *binding for a stated period*, and auditable.

## The three promises

**Retention** binds content for a period: until it expires, the
content cannot be erased, and after it expires, erasure becomes
possible (and, under a policy, automatic).

**Legal hold** suspends erasure indefinitely, outranking retention,
and is applied and released by an identified principal for a reason
that is recorded.

**WORM** is the strong form: for a stated period, even an
administrator cannot shorten the period or erase the content. The
difference between retention and WORM is not mechanism but authority:
whether the operator can undo their own policy.

## The design

### Where the clock lives

Retention attaches to the **version**. A file's identity is its
path, which callers reuse and rebind; a version is the frozen
artifact a regulator cares about. Two columns join `file_version`:

- `retain_until`: datetime, absent when no policy applies.
- `retention_mode`: `governance` or `compliance`, absent likewise.

`governance` means a principal holding a named scope may shorten or
clear it. `compliance` means nobody may, including the principal who
set it: the only path to erasure is waiting. That distinction is the
whole of WORM, and it is one column rather than a subsystem.

Legal hold is separate, because it is a different lifetime and a
different actor:

- `legal_hold`: bool on the version, default false.
- Holds are recorded as audit events carrying who, when, and the
  stated reason, so releasing one leaves a trail.

### What enforces it

One predicate, `erasable(version)`, is the whole enforcement surface:

```
erasable = !legal_hold
        && (retain_until is none || retain_until < now)
```

Three call sites consult it, and no others:

1. **The GC's collect step.** A blob is collected only when every
   version referencing it is erasable. The mark step stays as it is;
   the abort condition grows the predicate. Retention therefore rides
   the mechanism that already exists rather than introducing a second
   deletion path.
2. **Hard delete**, when it lands. Soft delete stays available under
   hold, because hiding a file is not erasing it; the tombstone
   already leaves content intact, and that is now load-bearing rather
   than incidental.
3. **Version pruning**, whenever a retention policy prunes old
   versions.

The engine layer mirrors this the way it mirrors tenancy: the
`PERMISSIONS` compiled from the contract can carry the predicate for
caller sessions, so a request-path bug that skips the check meets a
second refusal.

### Where policy comes from

Retention is set two ways, and the difference matters for audit:

- **Explicitly**, per version, by a principal holding `admin`.
- **By tenant policy**: a default `retain_for` duration and mode
  applied at version creation, so an operator states the rule once
  and every later upload inherits it. The applied value is stamped
  onto the version at creation and never recomputed, because a policy
  change must not shorten what already exists.

### The collisions, stated

**Grace period versus retention.** They measure different things: the
grace period protects against races (a blob unreferenced for a moment
between links), retention protects against policy. The grace period
stays as it is; retention gates the same collect step. A blob under
retention is simply never eligible, so it never enters the mark
window in the first place.

**Soft delete versus retention.** A held file can still be deleted in
the tombstone sense: its path frees, its rows hide, and its bytes
stay. This is the honest behavior, and it needs saying plainly in the
API documentation, because "deleted" meaning "hidden but retained" is
exactly the kind of thing a compliance reviewer must not discover by
surprise.

**Dedupe versus erasure.** Content is addressed by digest and shared
across tenants. If tenant A's version is under compliance retention
and tenant B deletes their copy, the blob stays, as it already would.
The consequence to state: erasure is per-reference, and bytes vanish
only when the last reference is both gone and erasable. A caller
asking "is this content gone" is asking a question about every
tenant, and the honest answer for a shared blob is scoped to their
own references.

**Quota versus retention.** Retained content occupies quota it cannot
release, so a tenant can reach their ceiling with nothing they are
allowed to delete. The usage accounting needs a retained-bytes figure
alongside the total, so an operator can see the difference between
"full" and "full of things I may not remove."

### What ships in what order

1. The two version columns, the `legal_hold` flag, and `erasable`
   consulted by the GC. This alone makes retention real, because the
   GC is the only thing that erases today. **Shipped**: the predicate
   lives in the reference recount, so a non-erasable version holds
   its blob alive through its file's tombstone and the blob never
   reaches the mark step; the freeze event names the frozen set so
   retention state moves on armed rows while link tampering still
   throws.
2. The admin surface: set and clear retention, apply and release
   holds, each writing an audit event with its reason. **Shipped**:
   the compliance authority line lives in the update's own WHERE
   clause, so two admins cannot race past it, and an expired
   compliance clock is an ordinary row again.
3. Tenant retention policy, stamped at version creation. **Shipped**,
   with version pruning riding it: `keep_last` removes erasable
   history beyond the depth, and holds and unexpired clocks survive
   any setting.
4. The engine-side predicate compiled into `PERMISSIONS`, so caller
   sessions meet it twice. **Shipped**: the delete clause on
   `file_version` refuses while anything binds the row.
5. Retained-bytes in usage accounting. **Shipped** on `/v1/usage` and
   the admin quota view.

Nothing here needs a new subsystem. Retention is a predicate, a pair
of columns, and the discipline to consult them in the three places
that erase.
