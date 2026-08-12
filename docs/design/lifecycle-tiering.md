# Lifecycle and tiering

Design, ahead of code. The roadmap states the gap in one line:
objects live at one cost forever. A corpus ages the same way
everywhere -- most bytes are read in their first weeks and then
almost never -- while the bill treats every byte as if it were
uploaded this morning. A policy that moves cold bytes to cheaper
storage, and the recall path back, is the operating expense every
hosted service competes on, and the roadmap already named the
starting point: [retention](../retention.md) carries the policy
vocabulary this extends. This document designs the rest: what a tier
is and how it composes with residency, where the policy lives and
where access recency comes from, the mover and its crash windows,
what every face answers when bytes are cold, and what absent policy
costs (nothing, which is the point).

## What is already true

**A blob has exactly one location.** The blob row id carries the
residency (`blob:<digest>` for `local`, `blob:<residency>-<digest>`
otherwise), and the row's `store_key` and `storage_path` columns name
the backend and the object key.
`crates/copal-store/src/repo/blob.rs`'s `get_location` projects
exactly those two columns and answers one place or none; there is no
plural anywhere in the model. The REST byte routes do not even read
the row: the backend resolves from the residency parsed out of the
record's blob link, zero extra round trips. The S3 face reads
`get_location`. Either way, one digest in one residency means one
backend, forever.

**Backends are already a vocabulary.** A residency is a named
`BackendConfig` -- fs, S3-compatible, GCS, or Azure Blob -- behind
one OpenDAL port, with optional per-residency encryption keys.
Everything below reuses that shape rather than inventing a second
way to name storage.

**Version links are frozen.** `file_version` rows freeze at arming,
and the freeze event THROWs on any blob change of an armed row. This
is load-bearing below: any tiering design that changes what a
version's blob link points at is a design the engine refuses.

**The GC already practices mark, grace, erase.** Blobs collect on a
derived recount, a full grace period after first being marked, row
before object, with a final existence check. The sweeps elect a
leader through a lease; the reseal sweep moves sealed objects
between keys in background batches. The mover joins this family
rather than founding a new one.

**Retention policy is stated once, per tenant.** `tenant_retention`
holds a duration, a mode, and a depth; `tenant_storage` pins a
residency. Tiering policy is the third row in that drawer.

## What a tier is

**A tier is a second named backend inside a residency, plus the rule
for when bytes belong there.** One mechanism, two axes, and the axes
differ in mutability:

- **Residency** answers *where these bytes may live*: jurisdiction,
  key custody, dedupe scope. It is baked into the blob row id, and
  through the row id into every `file.blob` and `file_version.blob`
  link. Immutable per blob, by construction.
- **Tier** answers *what these bytes cost right now*: which of the
  residency's backends currently holds the object. Mutable, because
  changing it is the entire feature.

The tempting shape is tier-as-residency -- `eu-cold` beside `eu`,
reusing everything -- and it is the shape to refuse. Moving a blob
to a different residency means a different row id, and the row id is
in every link, and armed version links cannot be rewritten: the
freeze event throws, and it is right to. The alternatives are a
second row for the same content (dedupe forks, the recount counts a
copy as a reference) or an id that lies about where bytes are. There
is also a semantic mismatch: residency reassignment affects new
content only, by documented design, while tiering is precisely about
old content. So the tier is **one nullable column on the blob row**
-- the one row in the content path that is mutable -- where `NONE`
means the residency's primary backend, which is today's behavior.
The null is the migration, the same additive shape the chunk
`access` column and `principal_id` used: boot reconciles the column,
every existing row reads `NONE`, nothing backfills.

Configuration nests tiers inside the residency they belong to, in
the same JSON, with one addition -- a declared class:

```json
"eu": { "scheme": "s3", "bucket": "acme-eu", "...": "...",
  "tiers": { "cold": { "scheme": "s3", "bucket": "acme-eu-cold",
                       "class": "online" } } }
```

`class` is `online` (a GET answers: S3 Standard-IA, Glacier Instant
Retrieval, GCS Nearline/Coldline, Azure Cool) or `archive` (a GET
cannot answer until a restore: Glacier Flexible and Deep Archive,
Azure Archive). The class drives the recall path below. The local
root grows the same `tiers` block through a sibling knob; the
carriage is a detail the implementation settles.

**Encryption composes for free, and the composition is the argument
for one rule: a tier never carries its own key.** Keys are per
residency because a key boundary scopes dedupe exactly as a backend
boundary does; hot and cold copies of one digest are the same
object, so they seal under the same key, and the move ships the
CPE1 envelope opaque -- header, salt, frames, untouched. The cold
backend needs no key material at all. A per-tier key would turn
every move into a re-seal and every re-seal into a second copy of
the rotation machinery; declining it keeps the move a copy.

## The policy

One row per tenant, beside retention and storage, on the admin
surface:

```
PUT    /v1/admin/tenants/{tenant}/tiering
       body: { "tier": "cold", "after_seconds": 7776000,
               "basis": "accessed", "min_bytes": 131072 }
GET    /v1/admin/tenants/{tenant}/tiering
DELETE /v1/admin/tenants/{tenant}/tiering
```

- `tier` names a configured tier; naming an unknown one refuses at
  validation, so a policy can never point bytes at a backend that
  does not exist.
- `after_seconds` plus `basis`: `created` ages from the version's
  creation and needs no new tracking at all; `accessed` ages from
  the last byte read and needs the signal below.
- `min_bytes`: objects below this never move. S3 Standard-IA bills
  128 KiB per object minimum and every transition costs a request;
  small objects save nothing and can cost more cold than hot.

Per-file pins ride the admin surface too (`PUT
.../files/{id}/tier`, body `{"pin": "hot"}`), audited like every
operator action: the escape hatch for the file that is old, cold by
every measure, and needed in milliseconds anyway.

**Evaluated live, not stamped -- the deliberate opposite of
retention's rule, for the opposite fail direction.** Retention
stamps at version creation and never recomputes because its fail
direction is erasure: a policy change must not shorten what already
exists. Tiering's fail direction is latency, which is recoverable,
and a tiering policy is stated in order to reach the corpus it
describes -- stamping it at creation would exempt every byte already
stored, which is exactly the cold data the operator is paying for.
So the classifier evaluates the policy against the world as it
stands, every pass, and a policy change moves the existing corpus.

**Dedupe makes eligibility derived, not stored.** A blob is shared
across files and tenants, so "may this move" is a question about
every reference, answered the way the refcount already is: derived
over the inbound links at sweep time. A blob moves only when every
referencing tenant's policy marks it cold, no referencing file pins
it, and the recency signal agrees; the most demanding reference
wins. Tenant A's pin holds tenant B's copy hot, and the cost
accounting should say so rather than hide it -- the same honesty
retention.md demands of shared-blob erasure.

### Where access recency comes from

**What counts as access: byte reads.** Content and version content
GETs, ranges, S3 GETs, grant and edge-token redemptions, and a
rendition derive reading its source. Listings and metadata reads do
not count -- they are answered from the metadata plane without
touching bytes, and a nightly inventory script must not keep a
corpus hot.

**Where the signal lives: the blob row, not the file row.** The
file row is read on every listing, and worse, a file-row UPDATE
recomputes `updated_at` and fires the outbox event -- a
last-accessed column there turns every download into an event, a
webhook candidate, and a changed listing row. The blob row is the
unit that actually moves, is never listed, fires no events, and the
S3 serve path already reads it. One nullable `last_read` datetime
lands there.

**How often it writes: day-coarse, not per read.** The naive rule
(write on every read) is write amplification proportional to read
traffic, on the serving path, for precision nobody asked for --
policies are stated in months. Sampling (write one read in N) was
considered and declined: a rarely read blob is exactly the one whose
reads a sample misses, and those are the blobs the classifier is
deciding about. Coarse buckets instead: a read updates `last_read`
only when the stored value is older than one day, fire-and-forget
after the response, so the first read of a day costs one background
UPDATE and every later read that day costs nothing. A lost write
under-records at day granularity, which fails toward moving content
earlier -- latency, never loss. A blob with no `last_read` at all
(stored before the column existed, never read since) falls back to
creation age, which is the other honest basis, not to "unknown means
cold now".

## The mover

A sweep in the family: it rides the interval loop, respects the
sweep leader lease (losers skip the pass), and walks blob rows in
keyset batches exactly as the GC does. Its first shipped form is
**observe-only**: classify, count, and report -- `/metrics` gauges
for candidate blobs and bytes, and an admin listing of what would
move per tenant with projected savings -- while touching nothing.
That stage is not a formality; it is where the recall-rate
assumption in the cost model below gets measured against real
traffic before a byte moves.

The move itself, per blob, is four motions, and the windows between
them deserve the same analysis `completion.rs` gives its
transaction:

1. **Copy.** Stream the raw object -- envelope and all, opaque --
   from the hot backend to the cold backend's staging, then rename
   onto the same content address. Content addressing makes the copy
   idempotent: an object already present is already the right bytes.
2. **Verify.** Read the cold copy back through the ordinary open
   path (decrypt, stream, hash) and compare the digest to the row
   id. The digest is the name; nothing weaker is verification. This
   costs a full read of what was just written, and it is the price
   of never erasing a hot copy on the strength of an unchecked copy.
3. **Flip.** One guarded UPDATE: `SET tier = 'cold', demoted_at =
   time::now() WHERE tier IS NONE`. The CAS resolves races at the
   engine -- a rival mover matches nothing -- and from this
   statement on, every read resolves the cold backend.
4. **Erase hot, a grace period later.** Not in the same pass: later
   passes erase the hot copies of rows whose `demoted_at` has aged
   past the grace window. Mark, grace, erase -- the discipline the
   GC already trusts, applied to a copy instead of a corpse.

The crash windows:

| Crash after | State left behind | Recovery |
| --- | --- | --- |
| Copy, before verify | Unverified cold bytes; row says hot; reads serve hot | Next pass re-copies (byte-identical overwrite) and verifies; converges |
| Verify, before flip | Verified cold orphan; reads serve hot | Next pass finds the object present, re-verifies, flips |
| Flip, before erase | Both copies exist; reads serve cold | The erase is grace-delayed by design; a later pass performs it |
| Erase | The end state | Nothing to recover |

The invariant across every row: **the flip happens only after
verified bytes exist cold, and the erase only after the flip has
aged.** At no instant does the row name a tier whose object is
absent. This is [backup.md](../backup.md)'s bytes-before-metadata
rule applied to placement, and it is also what makes recall (the
same four motions, cold to hot, flipping `tier` back to `NONE`)
symmetric rather than a second mechanism.

The row model change, stated against the code: `get_location` today
answers `(store_key, storage_path)` -- a single location. It grows
`tier` into its projection, and byte serving resolves the backend
as residency-then-tier. That adds one projected point-read to the
REST byte path, which currently resolves from the link string alone;
a deployment with no tiers configured skips it entirely, so the
fail-safe covers performance too. Three columns join the blob row:
`tier`, `demoted_at`, `last_read`, all nullable, all `NONE` on every
existing row.

## The recall path

The class declared on the tier decides everything.

**Online tiers read through.** A GET against Standard-IA or Glacier
Instant Retrieval answers in ordinary latency at a retrieval fee; no
face changes at all, no recall machinery exists, and a deployment
whose cold tier is online gets the whole feature without a new
status code. Reads do not auto-promote: one monthly audit read
yanking a corpus hot would defeat the classifier and pay transition
churn plus minimum-duration penalties. Promotion is the symmetric
sweep (a blob whose reads make it ineligible for cold moves back on
a later pass) or the pin.

**Archive tiers cannot answer a GET, so recall is explicit and
asynchronous.** A recall is a journaled flow run -- the claims,
retries, and idempotence every worker already has -- that issues the
backend's restore (S3 `RestoreObject`, Azure rehydration), polls
until the object is readable, then runs the promote motions: copy,
verify, flip, grace-erase cold. What each face answers while bytes
are archive-cold:

| Face | Answer |
| --- | --- |
| REST bytes (`GET .../content`, version content, ranges) | 202 with a body naming the recall run and a `Retry-After`; the GET itself enqueues the recall, idempotently. Metadata routes are unaffected. |
| S3 face | `403 InvalidObjectState` on GET -- AWS's own vocabulary for exactly this, which existing S3 clients already speak. `RestoreObject` accepts and maps to the recall run; HEAD reports progress through `x-amz-restore`. |
| Signed URLs (grants, edge tokens) | Issuance succeeds -- it never touches bytes. Redemption answers the same 202. A TTL that expires mid-recall re-issues; the operator docs say so. |
| Renditions | An existing rendition serves normally: it is its own blob with its own temperature. An inline derive whose source is archive-cold answers 202 and recalls the source. |
| Search, facets, extracted text | Untouched entirely. Chunks and text live in the metadata plane, so an archive-cold file remains fully searchable and its excerpts keep serving; only following the hit to the bytes meets the 202. This is the design's best property and belongs in the tenant documentation in exactly these words. |

202 rather than 503, because the request started durable work: a run
exists, it is pollable at `/v1/runs/{id}`, and retrying the GET is
harmless. 503 says try later and means nothing started.

## Interactions

| Mechanism | Interaction |
| --- | --- |
| Retention and legal hold | Retention binds erasure, not placement. The mover erases only a copy whose replacement was digest-verified, so a held or compliance-locked version's blob may demote and recall freely: the content the predicate protects exists throughout, and the verification is what makes that claim checkable rather than asserted. `erasable` never consults `tier`. The one honest caveat: archive-class recall latency is an availability change, and a deployment whose holds imply prompt production pins held content hot. |
| GC | The recount is placement-blind -- references say nothing about where bytes are -- so mark and grace are unchanged. Collection erases the object from every tier the residency configures: delete is a no-op on absent paths, so the unconditional sweep across tiers is replay-safe and covers a grace-window double copy. Row before object, existence re-check, as today. |
| Backup | Bytes-before-metadata extends across tiers: every tier of every residency backs up before the metadata export. The demote grace should be at least the backup cadence, for the same reason the GC grace covers restores: a flip landing between a tier's backup and the export must leave the bytes findable in the hot backup. The restore drill grows one leg: download and digest-check one object per tier per residency. |
| Encryption | The envelope moves opaque; the cold backend holds ciphertext it cannot open and needs no key. Verification decrypts once through the same open path reads use, because the digest addresses plaintext. The reseal sweep walks tiers too: a residency's rotation re-seals hot and cold copies alike, counted by the same `copal_resealed_total`. |
| Multi-region | A tier flip is a blob-row update and rides the changefeed like any row. A replica configures its own tier backends under the same names, and its copy worker follows the flip: copy to its cold, grace-erase its hot. Replicas never classify -- no sweeps run there -- so the primary decides placement for the fleet, and a replica missing a tier its rows name refuses at boot the way an unknown residency already does. |
| Quotas and usage | Logical usage is placement-blind: a tenant's bytes count the same hot or cold, so nothing about refusal changes. The usage views grow a by-tier figure so the operator sees the savings beside the total. |
| Dedupe | One digest, one row, one placement per residency. A shared blob moves once for all its referents; the most demanding referent keeps it hot; a pin in one tenant holds another tenant's copy hot, stated in the accounting rather than hidden. |

## The cost sketch

S3 list prices, us-east-1, as of this writing, rounded -- the
arithmetic is the point, not the fourth decimal:

| Class | Storage /GB-mo | Retrieval /GB | Minimum duration | Access |
| --- | --- | --- | --- | --- |
| Standard | $0.023 | -- | -- | ms |
| Standard-IA | $0.0125 | $0.01 | 30 days | ms |
| Glacier Instant Retrieval | $0.004 | $0.03 | 90 days | ms |
| Deep Archive | $0.00099 | ~$0.0025 bulk | 180 days | hours |

Worked example: a 10 TB corpus, 80% cold at 90 days, 2% of the cold
set read back per month. All-Standard: $230/mo. Cold set on
Standard-IA: $46 + $100 + $1.60 retrieval = $148, 36% off. On
Glacier Instant Retrieval: $46 + $32 + $4.80 = $83, 64% off. On Deep
Archive: $46 + $7.92 + roughly $0.40 = $54, 76% off, with recalls in
hours. GCS (Nearline/Coldline/Archive) and Azure (Cool/Cold/Archive)
run the same shape at their own prices.

The traps the policy knobs exist for: minimum durations (demoting at
30 days and recalling at 35 pays the minimum anyway -- set
`after_seconds` comfortably past the class minimum), the 128 KiB
billing floor (`min_bytes`), and retrieval fees scaling with the
recall rate -- the 2% above is an assumption, and the observe-only
stage measures the real figure from `last_read` churn before a byte
moves.

**The alternative, stated honestly: bucket-native lifecycle.** S3
lifecycle rules, GCS lifecycle, and Azure management policies
transition objects in place with zero Copal involvement, and for
online classes every read keeps working today; S3 Intelligent-Tiering
goes further and places per object automatically for a monitoring
fee. A single-tenant, S3-only deployment content with those
economics can take that path now and skip the mover for online
classes entirely. What the native path cannot do: per-tenant policy
(the object layout is content-addressed and tenant-blind, so no
prefix rule can express a tenant), dedupe awareness (the bucket
cannot see references, so one tenant's cold decision moves another
tenant's shared bytes), pins, and any recall story -- an
archive-class GET simply fails, and Copal would surface it as a blob
error with no run to poll. The mover is the recommendation for the
same reason the multi-region doc recommends its copy worker over
store-native replication: only the path Copal drives preserves the
invariants Copal promises. The native path is documented, with its
blind spots, for deployments already paying for it.

## Non-goals

- **Automatic tier ladders.** The policy names one target tier.
  Nothing in the vocabulary precludes a second policy row and a
  third tier later; a built-in hot-cool-cold-archive cascade is
  complexity ahead of demand.
- **Partial-object tiering.** The unit is the blob. Splitting one
  object's ranges across tiers is document surgery on the byte
  plane, declined the way per-chunk authorization declined byte
  redaction.
- **Learned placement.** The policy is stated, not inferred.
  Intelligent-Tiering exists for deployments that want inference,
  and the native-path paragraph names it.
- **Cross-residency moves.** A tier never changes jurisdiction; the
  cold bucket an operator configures for `eu` is expected to be as
  `eu` as the hot one, and that covenant is the operator's, stated
  in the docs.
- **Tenant-facing tiering controls.** Temperature is operator
  economics. Tenants see the serving contract (fast, or 202 with a
  run), not placement; the pin is an admin action.
- **Tiering transient bytes.** Staging, tus sessions, and multipart
  parts never tier; they are short-lived by construction.

## The fail-safe table

| Absent or failing piece | Resulting behavior |
| --- | --- |
| No tiering policy | Nothing classifies, nothing moves. Today's behavior exactly, including the byte path's zero extra reads. |
| No tiers configured | Setting a policy that names one refuses at validation. Nothing moves. |
| `accessed` basis with no recorded reads | Creation age decides. Content may move earlier; latency, never loss. |
| Mover crash, any window | Re-copy and re-verify converge; the flip is the only commitment and it follows verification. |
| Cold backend unreachable before the flip | The copy fails, the blob stays hot, the pass logs and moves on. Reads unaffected. |
| Cold backend unreachable after the flip | Reads fail as retryable blob errors -- the honest answer, not silently masked by the grace-held hot copy, because a masked outage is an outage discovered later. |
| Recall's restore fails or stalls | The run retries on the flow engine's budget; the 202 keeps answering with the run's state. |
| Archive class configured before the recall stage ships | Refused at configuration: `class: "archive"` is invalid until the stage that serves it lands, so no deployment can strand bytes behind a GET nothing answers. |
| Existing deployments upgrading | Every blob row reads `tier: NONE`, the primary backend. The null is the migration; no backfill, no recompute. |

Every row degrades toward today's behavior or toward latency. None
degrades toward data loss, which is the property the mover's
copy-verify-flip-erase ordering exists to guarantee.

## What ships in what order

1. **Configuration and the observe-only classifier.** Tiers parse
   and validate, the policy surface lands, `last_read` starts
   accumulating, and the sweep classifies and reports: gauges for
   candidate blobs and bytes, an admin listing of what would move
   per tenant with projected savings, and the measured would-be
   recall rate. Proves the vocabulary, the recency signal, and the
   eligibility math while a bug costs a metrics graph, not a byte.
2. **The mover, online classes only.** Demote and the symmetric
   promote, grace-delayed erase, the GC, backup, and reseal legs,
   pins, and the restore drill's per-tier check. Archive classes
   still refuse at configuration.
3. **Recall.** The flow-run recall, the 202 answers, S3
   `InvalidObjectState` and `RestoreObject`, and archive classes
   unlock. The staleness-of-availability contract lands in operator
   and tenant documentation verbatim.

Each stage is independently reversible: delete the policy row and
the classifier idles; without the mover nothing is displaced; and
until recall ships, no configuration can create bytes that need it.

## Decisions

Each of these is open until you close it; a recommendation rides
each.

1. **The tier axis.** A mutable column within the residency, versus
   tier-as-residency reusing the existing vocabulary wholesale.
   *Recommendation: the column; frozen version links settle it --
   the engine THROWs on rewriting an armed row's blob link, and a
   design the engine refuses is refused.*
2. **Policy basis.** Creation age only (no tracking), access
   recency only, or both. *Recommendation: both under one `basis`
   field, defaulting to `accessed`; creation-only quietly demotes a
   file read every day.*
3. **The recency signal.** File row versus blob row; per-read
   versus sampled versus day-coarse writes. *Recommendation: blob
   row, day-coarse, fire-and-forget after the response; the file
   row turns reads into events, and sampling misses exactly the
   rarely read blobs the classifier is deciding about.*
4. **Evaluated live versus stamped at creation.** *Recommendation:
   live; retention stamps because its fail direction is erasure,
   tiering evaluates because its fail direction is latency and the
   existing corpus is the point.*
5. **The mover's home.** A sweep under the leader lease, versus a
   flow run per blob. *Recommendation: the sweep for policy moves
   (population-scale, nobody waiting), a flow run per recall
   (demand-driven, caller-visible, pollable).*
6. **Hot erase timing.** Immediately after the flip, versus
   grace-delayed. *Recommendation: grace-delayed, at least
   max(GC grace, backup cadence); it is the GC's own discipline and
   it is what keeps every restore able to find the bytes.*
7. **Cold read posture.** Read-through everywhere, 202 everywhere,
   or per-class. *Recommendation: per-class, declared on the tier;
   online tiers change no face, archive tiers answer 202 with a
   run on REST and `InvalidObjectState` plus `RestoreObject` on the
   S3 face, which is the dialect S3 clients already speak.*
8. **Auto-promotion on read.** *Recommendation: no; reads update
   recency and the symmetric sweep promotes when eligibility
   lapses, so one audit read cannot yank a corpus hot; the pin is
   the deliberate override.*
9. **Tiering under retention and holds.** Blocked, allowed, or
   allowed with a hold implying a hot pin. *Recommendation:
   allowed, digest-verified -- erasing a verified copy erases
   nothing the predicate protects; a hold does not imply a pin, and
   deployments needing prompt production pin explicitly.*
10. **Native lifecycle rules.** Recommend them, forbid them, or
    document them. *Recommendation: document as the alternative for
    S3-only online-class deployments, with the blind spots named
    (tenant-blind layout, dedupe, no recall); the mover is the
    recommendation because only the path Copal drives preserves the
    invariants Copal promises.*
