# Multi-region replication

Design, ahead of code. The roadmap states the gap in one line:
residencies place bytes; nothing copies them. A second region today
is a second deployment, with nothing flowing between the two. The
primitive that unblocks this arrived with surql 0.33: tables can
retain a mutation log (`CHANGEFEED <duration>` on the definition) and
a consumer can replay it resumably (`SHOW CHANGES FOR TABLE ... SINCE
<versionstamp>`, each entry a `ChangeSet` carrying the versionstamp
to resume from).

The topology this designs is **one writer, many readers**: a single
primary region takes every write, and replica regions apply the
primary's changes and serve reads. Multi-primary is a non-goal,
stated up front and argued at the end, because every correctness
property Copal has -- compare-and-swap everywhere, leases resolving
in the engine, one live path per tenant -- assumes one serialization
point, and multi-primary dissolves it.

Two planes replicate by two different means, deliberately:

- **Metadata** rides the changefeed. Rows are small, ordered, and
  carry their own identity.
- **Bytes do not.** Blob content does not pass through the database
  and must not: a 256 MiB object has no business in a mutation log.
  Bytes replicate at the object-store layer, keyed off the blob rows
  the changefeed already delivers. The seam is designed below.

## What carries a feed

The dividing line: a table carries a changefeed when a replica needs
its rows to serve reads. It does not when the rows are one process's
working state, ephemeral, or derivable.

| Fed | Why |
| --- | --- |
| `file`, `file_version`, `blob` | The records reads resolve through. |
| `file_text`, `text_chunk` | Search and text serve from these; replicas build their own BM25 and HNSW indexes from the applied rows. |
| `api_key`, `principal`, `edge_key`, `s3_credential` | Authentication must work in-region; sealed secrets replicate as the sealed values they are, under the key custody rule below. |
| `access_grant` | Replicated for visibility; redemption stays on the primary (it consumes a use, which is a write). |
| `tenant_storage`, `tenant_quota`, `tenant_retention` | Policy rows reads and refusals consult. |
| `webhook_endpoint` | Configuration, not deliveries. |
| `file_event`, `audit_event` | The event listing and the audit trail should answer in-region. |

| Not fed | Why |
| --- | --- |
| `workflow_run`, `workflow_step`, `service_lease` | Orchestration is the primary's working state; replicas execute nothing. |
| `rate_window` | Minute-scoped, expires on its own. |
| `tenant_usage` | An advisory cache the sweep recomputes; replicating a cache replicates its staleness. |
| `tus_upload`, `s3_multipart`, `s3_multipart_part` | Sessions reference staging bytes that exist only where the upload lands. |
| `webhook_delivery` | Delivery is primary-only work; replicating its bookkeeping invites a second dispatcher. |

**`INCLUDE ORIGINAL` is declined.** It adds the pre-mutation row to
every entry, roughly doubling the retained log for update-heavy
tables -- and `text_chunk` rows carry a thousand characters of body
plus an embedding vector each. The applier has no use for the
pre-image: it applies post-state (below), and a consumer that wants
deltas someday can opt one table in then. Paying double on every
table for a consumer that does not exist fails the same test OTel
spans failed.

Feeds cost the primary retained log space, so they are configured,
not constant: a deployment that never replicates pays nothing. The
retention duration is one knob (`COPAL_CHANGEFEED_RETENTION`), argued
below.

## The applier

A replica runs the same binary in a replica mode whose only writer is
the applier: a loop that, per fed table, reads `SHOW CHANGES SINCE
<last versionstamp> LIMIT <batch>`, applies the entries, and
checkpoints. Everything else about the mode is subtraction, listed
under serving semantics.

**Checkpoints live on the replica**, one row per fed table
(`replica_checkpoint`: table name, versionstamp, applied time,
unique on table). On the replica rather than the primary because a
checkpoint describes the replica's state, and storing it beside the
applied data means a replica restored from its own backup restores a
checkpoint consistent with what it restored. The primary holds no
per-replica bookkeeping at all, which is what keeps "add a replica"
from being a primary-side migration.

**Apply is idempotent by shape.** Changefeed entries carry the
post-mutation row: apply is an UPSERT of that content for
updates and creates, a DELETE for deletes. Applying the same entry
twice converges on the same row, so the loop can be at-least-once
with a clear conscience: apply the batch, then checkpoint, and a
crash between the two replays a suffix harmlessly. Resuming SINCE the
last versionstamp may re-deliver the entry at the boundary; same
answer. This is the same discipline the flow journal uses
(at-least-once execution, exactly-once effect), applied to rows
instead of steps.

**Ordering: strict per table, merged across tables.** One table's
feed replays in versionstamp order, and the applier preserves it.
Across tables the feeds are separate statements, but versionstamps
come from one engine clock, so the applier merges each round's
batches by versionstamp before applying. A primary transaction that
touched `file` and `file_version` lands in both feeds at the same
stamp and applies together. What the merge cannot promise is
atomicity at the batch edge: a round may end between the two halves
of such a pair, and for the gap of one round a `file.current_version`
may point at a version row not yet applied. The read paths already
treat every record link as nullable and every read as a moment in
time, so the honest framing is: a replica is always a consistent
prefix per table and a near-consistent join across tables, bounded by
one apply round. Serving semantics below say which faces tolerate
that; all of the ones a replica serves do.

**Schema changes: replicas are schema-followers of their own
binary.** The reconciler runs on the replica at boot exactly as it
does on the primary, bringing the local database to the local code's
definitions -- with two subtractions. Events are stripped: the
`file_event_outbox` event firing on an applied `file` update would
mint a second event row beside the one arriving through
`file_event`'s own feed, so on a replica the feed is the event
source and the trigger must not be. Changefeeds are also not defined
on the replica's own tables; chaining feeds is a topology nobody has
asked for. The applier ignores `define_table` entries in the feed
entirely: schema comes from the binary, not the wire. The operational
rule that falls out: **upgrade replicas first.** A newer replica
schema accepts older rows (reconciliation is additive, and defaults
fill absences); an older replica meeting newer columns on a
schemafull table would reject or drop them. One artifact is accepted
knowingly: columns with a `VALUE` clause recompute at apply, so a
replica's `updated_at` reads as apply time, not primary write time.
`live_marker` recomputes identically from identical inputs, so the
one load-bearing `VALUE` column is unaffected.

## Retention versus lag

The changefeed window is the maximum downtime a replica may survive
without a rebuild. The engine retains `CHANGEFEED <duration>` of
mutations; a replica whose checkpoint ages past that window has a gap
nothing can fill from the feed, and the dangerous part is that the
gap is silent -- `SHOW CHANGES` answers from what is retained and
does not announce what fell off. So the applier enforces the honesty
the engine does not: checkpoint age past half the window raises an
alarm (`/metrics` gauge, log line); past the full window the applier
refuses to continue and demands a resync, because applying past an
unprovable gap would build a replica that is silently missing rows
forever.

Default `COPAL_CHANGEFEED_RETENTION`: **3d**. It covers a weekend
outage of a replica with margin, and its cost is proportional to
write volume, not corpus size. Deployments with heavier churn or
laxer operations tune it; the applier's refusal rule makes the
consequence of undersizing visible instead of silent.

**Full resync is the backup procedure**, deliberately. Stand the
replica's object stores up first, then import a metadata export, then
start the applier -- the same bytes-first ordering
[backup.md](../backup.md) argues, for the same reason: a record
without bytes is a broken file, bytes without a record are harmless.
The applier then resumes `SINCE` a timestamp taken **before** the
export began (`ChangeSince::Timestamp`), and idempotent apply absorbs
the overlap between the export's contents and the feed's replay. No
new machinery: a resync is a restore drill against a live source, and
practicing one practices the other.

## Bytes

Blob rows ride the feed; blob bytes ride the object-store layer. The
residency vocabulary is what makes the seam small: a residency is
already a named backend, blob rows already record which residency
holds their bytes, and every read already resolves the backend from
the row. A replica region therefore configures **the same residency
names** bound to region-local backends -- `local` to its own root,
`eu` to its own `eu` bucket -- and per-residency encryption keys
travel under the same custody rule as every key: configuration, not
data, present in both regions or the bytes are unreadable there.

Two ways to move the bytes:

**A copy worker keyed off blob-row changes** (recommended). The
applier already sees every new `blob` row in the feed. Before a round
checkpoints, the copy worker fetches each new digest from the
primary's backend for that residency and writes it to the replica's,
staging-then-rename as every blob write is. Content addressing makes
the copy verifiable (the digest is the name) and idempotent (an
existing object is already the right bytes). Gating the checkpoint on
the copies preserves the ordering rule that backup.md states and
serving depends on: metadata never advances past the bytes it
references, so a replica-served record's bytes exist by construction.

**Object-store native replication** (S3 CRR, GCS dual-region, Azure
GRS) moves the bytes without Copal's involvement, and a deployment
already paying for it may prefer it. What it cannot promise is
ordering: nothing ties the store's replication lag to the feed's, so
an applied blob row may precede its bytes. A replica in this
configuration must treat a missing object as retryable lag -- 503
with retry semantics, or proxy the read to the primary -- rather
than as corruption. Both postures are workable; only the copy worker
keeps the bytes-before-metadata invariant, which is why it is the
recommendation and the native path is the documented alternative.

GC on a replica follows from read-only-by-construction: no sweeps
run, so nothing collects. Bytes leave a replica the same way they
arrive, by applied deletion -- the copy worker processes blob-row
deletes (the primary's GC, arriving through the feed) by deleting the
regional object after the row applies. Row before object, the same
order the primary's collector uses.

## Serving from a replica

A replica is read-only by construction, not by discipline: the
applier is the only writer, and the write-shaped faces refuse before
reaching a repository. Sweeps, the flow worker, the webhook
dispatcher, the embedding backfill: none run in replica mode. Each
would either fight the feed (a reaped lease un-reaped by the next
apply), duplicate the primary's work (a second webhook delivery), or
destroy data (a GC acting on locally derived counts).

What a replica serves, all read-only, all tolerant of bounded
staleness:

- **Content reads**: `GET .../content`, version content, renditions
  already derived. Digest-addressed bytes are immutable, so a stale
  read serves old-but-genuine content, never wrong content.
- **Listings, metadata gets, version histories.**
- **Search, facets, extracted text**: the applied chunks feed
  region-local BM25 and HNSW indexes; `PERMISSIONS` compile on the
  replica identically, so the engine's second layer holds there too.
- **The events listing** and subscriptions: LIVE SELECT fires on
  applied rows, so watchers see events at replication lag.
- **Edge token redemption**: `cg2` verification is HMAC against
  replicated sealed keys, no primary hop -- the stateless design
  finally cashes its cheque in the second region.

What always hits the primary: every write on every face (uploads,
deletes, renditions, runs, retention actions, admin), grant minting
and **grant redemption** (a redemption consumes a use inside a
guarded UPDATE; consuming it on a replica would fork the counter),
key minting, webhook registration, and the S3 write verbs. Replica
refusals name the primary so a client can follow.

The staleness contract, stated for callers: a replica serves a
consistent-per-table, lag-bounded view. A caller that writes and
immediately reads must read the primary; a revoked key or grant keeps
working on a replica for up to the replication lag (the same window a
`cg2` token already grants by design); a quarantine verdict reaches
replicas at the same lag, and until it does, a replica serves content
the primary just condemned. That last one is the sharpest edge of
serving stale reads and belongs in the operator documentation in
exactly these words.

## Failure modes

| Failure | Behavior |
| --- | --- |
| Primary down | Replicas keep serving reads at their last applied state; writes fail everywhere; lag grows until the primary returns, bounded by the retention window. |
| Replica down briefly | Resumes from its checkpoint; nothing else notices. |
| Replica down past retention | Applier detects checkpoint age, refuses, demands resync. Silent gaps are structurally impossible to apply past. |
| Applier crash mid-batch | At-least-once replay from checkpoint; UPSERT convergence makes the replay invisible. |
| Bytes lag metadata (native replication mode) | Missing object reads as retryable lag or proxies to the primary; never as absence. Copy-worker mode prevents this by gating. |
| Revocation inside the lag window | A replica honors it on apply; exposure is bounded by lag and equals the exposure `cg2` tokens already accept. |
| Quarantine inside the lag window | As above; named in operator docs. |
| Primary upgraded before replicas | Feed rows may carry columns the replica's schema lacks; the apply rejection alarms. Rule: replicas upgrade first. |
| Split brain (two primaries) | Cannot arise from this design: replicas hold no write path to fork. Promotion is a manual, single act. |
| Replica object store loses data | The digest check from the restore drill (fetch, hash, compare) detects it; re-copy from the primary heals it, content addressing making the repair exact. |

## What multi-primary would cost, and why it is deferred

Every CAS in the codebase -- upload claims, run claims, grant
consumption, lease expiry, the one-live-path index -- is correct
because one engine serializes it. Two writers mean either a consensus
layer under the database (someone else's product), or
conflict-resolution semantics for every guarded UPDATE (a rewrite of
the store's core discipline), or accepting lost updates (not this
service). Single-writer replication delivers the two things a second
region is actually asked for -- fast reads near the readers, and
survival of the primary region's loss with bounded data loss -- for
the cost of an applier and a copy worker. Write locality is the one
thing it does not deliver, and it is the honest price of keeping
every correctness property the service has.

Promotion of a replica to primary after a regional loss is a manual
runbook (verify lag, stop appliers, lift replica mode, repoint
writers), documented when serving ships. Automatic failover is
deferred with multi-primary: an automated promoter is a consensus
problem wearing a trench coat.

## What ships in what order

1. **Feed definitions and the observer applier.** `CHANGEFEED` on
   the fed tables behind configuration; a replica that applies and
   checkpoints but serves nothing; `/metrics` gauges for lag in
   seconds and versionstamps, rows applied, apply rejections. This
   stage proves the feed, the merge order, and the idempotence
   claims against a live workload while a bug costs a metrics graph,
   not a caller.
2. **The bytes plane.** The copy worker, checkpoint gating, and the
   digest verification drill (fetch one object per residency,
   compare to the row). The restore-as-resync path gets practiced
   here, because a resync that has never run is a claim.
3. **Serving.** Replica mode opens the read faces behind an explicit
   allowlist, refusals name the primary, and the staleness contract
   lands in operator documentation verbatim.
4. **The promotion runbook**, manual, tested against a scratch
   deployment the way backup.md demands restores be.

Each stage is independently reversible: turning off feeds returns the
primary to today, and a replica is disposable by construction until
stage three grants it traffic.

## Decisions

1. **Fed-table set.** As tabled above, or wider (deliveries,
   orchestration) for observability. *Recommendation: as tabled;
   every additional feed is retained log the primary pays for, and
   orchestration state is meaningless without the worker that owns
   it.*
2. **`INCLUDE ORIGINAL`.** *Recommendation: off everywhere; the
   applier applies post-state, and pre-images double the log for a
   consumer that does not exist.*
3. **Checkpoint placement.** Replica-side table versus primary-side
   registry. *Recommendation: replica-side; checkpoints restore with
   the data they describe, and the primary stays ignorant of its
   replicas.*
4. **Cross-table ordering.** Merge by versionstamp per round versus
   independent per-table streams. *Recommendation: merge; it buys
   near-transactional joins for one sort, and the residual edge is
   bounded by a round.*
5. **Schema authority on replicas.** Follower-of-own-binary with
   events stripped, versus applying `define_table` from the feed.
   *Recommendation: follower; schema-from-the-wire would let a
   primary reshape a replica it cannot see, and the upgrade rule
   (replicas first) is one line of runbook.*
6. **Retention default.** *Recommendation: 3d, alarm at half,
   refuse-and-resync past it.*
7. **Bytes seam.** Copy worker gating checkpoints versus
   object-store native replication. *Recommendation: the copy
   worker, for the bytes-before-metadata invariant; document the
   native path with its missing-object fallback for deployments
   already paying for it.*
8. **Grant redemption on replicas.** Primary-only versus
   replica-served with forked counters reconciled later.
   *Recommendation: primary-only; a use count is a CAS, and forking
   it is multi-primary through a side door.*
9. **Failover posture.** Manual promotion runbook versus automated
   promoter. *Recommendation: manual, shipped as stage four;
   automation is deferred with multi-primary.*
