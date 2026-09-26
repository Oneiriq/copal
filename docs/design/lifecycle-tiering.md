# Lifecycle and tiering

Status: shipped, in the three stages this design declared:
configuration and the observe-only classifier, the mover (both
directions, with grace-delayed erase), and archive recall. The
operator reference is the storage tiers section of
[operations.md](../operations.md), and the tenant-facing behavior is
in the archive-cold section of [api.md](../api.md). Three pieces of
this design are not built:

- Archive-class tiers on Azure Blob Storage. Copal does not drive
  Azure rehydration, so an Azure archive tier refuses at boot. Azure
  deployments can tier to online classes.
- Projected savings in the tiering report. The report
  (`GET /v1/admin/tiering/report`) shows candidate and would-recall
  counts and bytes per tenant, without a cost figure.
- A by-tier figure in the usage views. Usage stays placement-blind.

The rest of this page is the design as written, lightly edited for
tense. It explains why the feature has the shape it has.

## The problem

Without tiering, objects live at one cost forever. A corpus ages the
same way everywhere (most bytes are read in their first weeks and then
almost never), while the bill treats every byte as if it were uploaded
this morning. A policy that moves cold bytes to cheaper storage, with
a recall path back, is the operating expense every hosted service
competes on. [Retention](../retention.md) already carried the policy
vocabulary this extends. This document designs the rest: what a tier
is and how it composes with residency, where the policy lives and
where access recency comes from, the mover and its crash windows, what
every face answers when bytes are cold, and what absent policy costs
(nothing).

## What was already true

**A blob has exactly one location.** The blob row id carries the
residency (`blob:<digest>` for `local`, `blob:<residency>-<digest>`
otherwise), and the row's `store_key` and `storage_path` columns name
the backend and the object key.
`crates/copal-store/src/repo/blob.rs`'s `get_location` projected
exactly those two columns and answered one place or none; nothing in
the model was plural. The REST byte routes did not even read the row:
the backend resolved from the residency parsed out of the record's
blob link, with zero extra round trips. The S3 face read
`get_location`. Either way, one digest in one residency meant one
backend.

**Backends are already a vocabulary.** A residency is a named
`BackendConfig` (fs, S3-compatible, GCS, or Azure Blob) behind one
OpenDAL port, with optional per-residency encryption keys. Everything
below reuses that shape instead of inventing a second way to name
storage.

**Version links are frozen.** `file_version` rows freeze at arming,
and the freeze event throws on any blob change of an armed row. This
matters below: any tiering design that changes what a version's blob
link points at is a design the engine refuses.

**The GC already practices mark, grace, erase.** Blobs collect on a
derived recount, a full grace period after first being marked, row
before object, with a final existence check. The sweeps elect a
leader through a lease; the re-seal sweep moves sealed objects
between keys in background batches. The mover joins this family.

**Retention policy is stated once, per tenant.** `tenant_retention`
holds a duration, a mode, and a depth; `tenant_storage` pins a
residency. Tiering policy is the third row of that kind.

## What a tier is

**A tier is a second named backend inside a residency, plus the rule
for when bytes belong there.** One mechanism, two axes, and the axes
differ in mutability:

- **Residency** answers where these bytes may live: jurisdiction,
  key custody, dedupe scope. It is baked into the blob row id, and
  through the row id into every `file.blob` and `file_version.blob`
  link. It is immutable per blob by construction.
- **Tier** answers what these bytes cost right now: which of the
  residency's backends currently holds the object. It is mutable,
  because changing it is the whole feature.

The tempting shape is tier-as-residency (`eu-cold` beside `eu`,
reusing everything), and it is the shape to refuse. Moving a blob to
a different residency means a different row id, the row id is in
every link, and armed version links cannot be rewritten: the freeze
event throws, and it is right to. The alternatives are a second row
for the same content (dedupe forks, and the recount counts a copy as a
reference) or an id that lies about where bytes are. There is also a
semantic mismatch: residency reassignment affects new content only,
by documented design, while tiering is about old content. So the tier
is one nullable column on the blob row, the one row in the content
path that is mutable, where `NONE` means the residency's primary
backend. The null is the migration, the same additive shape the chunk
`access` column and `principal_id` used: boot reconciles the column,
every existing row reads `NONE`, and nothing backfills.

Configuration nests tiers inside the residency they belong to, in the
same JSON, with one addition, a declared class:

```json
"eu": { "scheme": "s3", "bucket": "acme-eu", "...": "...",
  "tiers": { "cold": { "scheme": "s3", "bucket": "acme-eu-cold",
                       "class": "online" } } }
```

`class` is `online` (a GET answers: S3 Standard-IA, Glacier Instant
Retrieval, GCS Nearline or Coldline, Azure Cool) or `archive` (a GET
cannot answer until a restore: Glacier Flexible and Deep Archive,
Azure Archive). The class drives the recall path below. The local
root takes the same `tiers` block through a sibling variable,
`COPAL_LOCAL_TIERS`.

**Encryption composes with no extra work, which argues for one rule:
a tier never carries its own key.** Keys are per residency because a
key boundary scopes dedupe exactly as a backend boundary does. Hot and
cold copies of one digest are the same object, so they seal under the
same key, and the move ships the sealed envelope (header, salt,
frames) untouched. The cold backend needs no key material at all. A
per-tier key would turn every move into a re-seal and every re-seal
into a second copy of the rotation machinery; without one, the move
stays a copy.

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
  creation and needs no new tracking; `accessed` ages from the last
  byte read and needs the signal below.
- `min_bytes`: objects below this never move. S3 Standard-IA bills
  128 KiB per object minimum and every transition costs a request, so
  small objects save nothing and can cost more cold than hot.

Per-file pins ride the admin surface too (`PUT
.../files/{id}/tier`, body `{"pin": "hot"}`), audited like every
operator action. The pin is for the file that is old, cold by every
measure, and needed in milliseconds anyway.

**Evaluated live on every pass.** This is the opposite of retention's
rule, for the opposite fail direction. Retention stamps
at version creation and never recomputes because its fail direction
is erasure: a policy change must not shorten what already exists.
Tiering's fail direction is latency, which is recoverable, and a
tiering policy is stated in order to reach the corpus it describes.
Stamping it at creation would exempt every byte already stored, which
is the cold data the operator is paying for. So the classifier
evaluates the policy against the world as it stands, every pass, and a
policy change moves the existing corpus.

**Dedupe makes eligibility derived.** A blob is shared across files
and tenants, so "may this move" is a question about every reference,
answered the way the reference count is: derived over the inbound
links at sweep time. A blob moves only when every referencing
tenant's policy marks it cold, no referencing file pins it, and the
recency signal agrees; the most demanding reference wins. Tenant A's
pin holds tenant B's copy hot, and the report says so, the same way
retention.md states shared-blob erasure.

### Where access recency comes from

**What counts as access: byte reads.** Content and version content
GETs, ranges, S3 GETs, grant and edge-token redemptions, and a
rendition derive reading its source. Listings and metadata reads do
not count. They are answered from the metadata plane without touching
bytes, and a nightly inventory script must not keep a corpus hot.

**Where the signal lives: the blob row.** The file row is read on
every listing, and a file-row UPDATE recomputes `updated_at` and fires
the outbox event, so a last-accessed column there would turn every
download into an event, a webhook candidate, and a changed listing
row. The blob row is the unit that moves, is never listed, fires no
events, and the S3 serve path already reads it. One nullable
`last_read` datetime lives there.

**How often it writes: day-coarse.** Writing on every read is write
amplification proportional to read traffic, on the serving path, for
precision nobody needs: policies are stated in months. Sampling
(write one read in N) was considered and declined, because a rarely
read blob is the one whose reads a sample misses, and those are the
blobs the classifier is deciding about. A read updates `last_read`
only when the stored value is older than one day, fire-and-forget
after the response, so the first read of a day costs one background
UPDATE and every later read that day costs nothing. A lost write
under-records at day granularity, which fails toward moving content
earlier; that costs latency and loses nothing. A blob with no
`last_read` at all
(stored before the column existed, never read since) falls back to
creation age, which is the other defensible basis, and is never
treated as cold on the grounds of being unknown.

## The mover

A sweep in the family: it rides the interval loop, respects the sweep
leader lease (losers skip the pass), and walks blob rows in keyset
batches exactly as the GC does. Its first stage was observe-only:
classify, count, and report (`/metrics` gauges for candidate blobs and
bytes, and an admin listing of what would move per tenant) while
touching nothing. That stage is where the recall-rate assumption in
the cost model below gets measured against real traffic before a byte
moves.

The move itself, per blob, is four steps, and the windows between
them deserve the same analysis `completion.rs` gives its transaction:

1. **Copy.** Stream the raw object, envelope and all, as opaque bytes
   from the hot backend to the cold backend's staging, then rename
   onto the same content address. Content addressing makes the copy
   idempotent: an object already present is already the right bytes.
2. **Verify.** Read the cold copy back through the ordinary open path
   (decrypt, stream, hash) and compare the digest to the row id. The
   digest is the name, and nothing weaker counts as verification. This
   costs a full read of what was just written, and it is the price of
   never erasing a hot copy on the strength of an unchecked copy.
3. **Flip.** One guarded UPDATE: `SET tier = 'cold', demoted_at =
   time::now() WHERE tier IS NONE`. The compare-and-swap resolves
   races at the engine (a rival mover matches nothing), and from this
   statement on, every read resolves the cold backend.
4. **Erase hot, a grace period later.** Not in the same pass: later
   passes erase the hot copies of rows whose `demoted_at` has aged past
   the grace window. Mark, grace, erase: the discipline the GC already
   trusts, applied to a copy.

The crash windows:

| Crash after | State left behind | Recovery |
| --- | --- | --- |
| Copy, before verify | Unverified cold bytes; row says hot; reads serve hot | Next pass re-copies (byte-identical overwrite) and verifies; converges |
| Verify, before flip | Verified cold orphan; reads serve hot | Next pass finds the object present, re-verifies, flips |
| Flip, before erase | Both copies exist; reads serve cold | The erase is grace-delayed by design; a later pass performs it |
| Erase | The end state | Nothing to recover |

The invariant across every row: the flip happens only after verified
bytes exist cold, and the erase only after the flip has aged. At no
instant does the row name a tier whose object is absent. This is
[backup.md](../backup.md)'s bytes-before-metadata rule applied to
placement, and it is also what makes recall (the same four steps, cold
to hot, flipping `tier` back to `NONE`) symmetric instead of a second
mechanism.

The row model change, stated against the code: `get_location`
answered `(store_key, storage_path)`, a single location. It grew
`tier` into its projection, and byte serving resolves the backend as
residency, then tier. That adds one projected point read to the REST
byte path, which used to resolve from the link string alone; a
deployment with no tiers configured skips it entirely, so the
fail-safe covers performance too. Three columns joined the blob row:
`tier`, `demoted_at`, and `last_read`, all nullable and `NONE` on every
existing row.

## The recall path

The class declared on the tier decides everything.

**Online tiers read through.** A GET against Standard-IA or Glacier
Instant Retrieval answers in ordinary latency at a retrieval fee. No
face changes, no recall machinery runs, and a deployment whose cold
tier is online gets the whole feature without a new status code.
Reads do not auto-promote: one monthly audit read pulling a corpus hot
would defeat the classifier and pay transition churn plus
minimum-duration penalties. Promotion is the symmetric sweep (a blob
whose reads make it ineligible for cold moves back on a later pass)
or the pin.

**Archive tiers cannot answer a GET, so recall is explicit and
asynchronous.** A recall is a journaled flow run (with the claims,
retries, and idempotence every worker already has) that issues the
backend's restore (S3 `RestoreObject`), polls until the object is
readable, then runs the promote steps: copy, verify, flip, and
grace-delayed erase of the cold copy. What each face answers while
bytes are archive-cold:

| Face | Answer |
| --- | --- |
| REST bytes (`GET .../content`, version content, ranges) | 202 with a body naming the recall run and a `Retry-After`; the GET itself enqueues the recall, idempotently. Metadata routes are unaffected. |
| S3 face | `403 InvalidObjectState` on GET, AWS's own vocabulary for this case, which existing S3 clients already speak. `RestoreObject` accepts and maps to the recall run; HEAD reports progress through `x-amz-restore`. |
| Signed URLs (grants, edge tokens) | Issuance succeeds, since it never touches bytes. Redemption answers the same 202. A TTL that expires mid-recall re-issues; the operator docs say so. |
| Renditions | An existing rendition serves normally: it is its own blob with its own temperature. An inline derive whose source is archive-cold answers 202 and recalls the source. |
| Search, facets, extracted text | Untouched. Chunks and text live in the metadata plane, so an archive-cold file remains fully searchable and its excerpts keep serving; only following the hit to the bytes meets the 202. |

The status is 202 because the request started durable work: a run
exists, it is pollable at `/v1/runs/{id}`, and retrying the GET is
harmless. A 503 would say try later and mean nothing started.

## Interactions

| Mechanism | Interaction |
| --- | --- |
| Retention and legal hold | Retention binds erasure and says nothing about placement. The mover erases only a copy whose replacement was digest-verified, so a held or compliance-locked version's blob may demote and recall freely: the content the predicate protects exists throughout, and the verification is what makes that checkable. `erasable` never consults `tier`. One caveat: archive-class recall latency is an availability change, and a deployment whose holds imply prompt production pins held content hot. |
| GC | The recount is placement-blind, because references say nothing about where bytes are, so mark and grace are unchanged. Collection erases the object from every tier the residency configures: delete is a no-op on absent paths, so the unconditional sweep across tiers is replay-safe and covers a grace-window double copy. Row before object, with the existence re-check, as before. |
| Backup | Bytes-before-metadata extends across tiers: every tier of every residency backs up before the metadata export. The erase grace should be at least the backup cadence, for the same reason the GC grace covers restores: a flip landing between a tier's backup and the export must leave the bytes findable in the hot backup. The restore drill grows one step: download and digest-check one object per tier per residency. |
| Encryption | The envelope moves opaque; the cold backend holds ciphertext it cannot open and needs no key. Verification decrypts once through the same open path reads use, because the digest addresses plaintext. The re-seal sweep walks tiers too: a residency's rotation re-seals hot and cold copies alike, counted by the same `copal_resealed_total`. |
| Multi-region | Designed against the replication design, which is not implemented. A tier flip is a blob-row update and would ride the changefeed like any row. A replica would configure its own tier backends under the same names, and its copy worker would follow the flip. Replicas would never classify, since no sweeps run there, so the primary decides placement for the fleet. |
| Quotas and usage | Logical usage is placement-blind: a tenant's bytes count the same hot or cold, so nothing about refusal changes. The design proposed a by-tier figure in the usage views; it is not built. |
| Dedupe | One digest, one row, one placement per residency. A shared blob moves once for all its referents; the most demanding referent keeps it hot; a pin in one tenant holds another tenant's copy hot, and the report says so. |

## The cost sketch

S3 list prices, us-east-1, at the time of writing, rounded; the
arithmetic matters more than the fourth decimal:

| Class | Storage per GB-month | Retrieval per GB | Minimum duration | Access |
| --- | --- | --- | --- | --- |
| Standard | $0.023 | none | none | ms |
| Standard-IA | $0.0125 | $0.01 | 30 days | ms |
| Glacier Instant Retrieval | $0.004 | $0.03 | 90 days | ms |
| Deep Archive | $0.00099 | about $0.0025 bulk | 180 days | hours |

Worked example: a 10 TB corpus, 80% cold at 90 days, 2% of the cold
set read back per month. All-Standard: $230 a month. Cold set on
Standard-IA: $46 + $100 + $1.60 retrieval = $148, 36% off. On Glacier
Instant Retrieval: $46 + $32 + $4.80 = $83, 64% off. On Deep Archive:
$46 + $7.92 + about $0.40 = $54, 76% off, with recalls in hours. GCS
(Nearline, Coldline, Archive) and Azure (Cool, Cold, Archive) follow
the same shape at their own prices.

The traps the policy settings exist for: minimum durations (demoting
at 30 days and recalling at 35 pays the minimum anyway, so set
`after_seconds` comfortably past the class minimum), the 128 KiB
billing floor (`min_bytes`), and retrieval fees scaling with the
recall rate. The 2% above is an assumption, and the observe-only
report measures the real figure from `last_read` churn before a byte
moves.

**The alternative: bucket-native lifecycle.** S3 lifecycle rules, GCS
lifecycle, and Azure management policies transition objects in place
with no Copal involvement, and for online classes every read keeps
working. S3 Intelligent-Tiering goes further and places each object
automatically for a monitoring fee. A single-tenant, S3-only
deployment content with those economics can take that path and skip
the mover for online classes entirely. What the native path cannot
do: per-tenant policy (the object layout is content-addressed and
tenant-blind, so no prefix rule can express a tenant), dedupe
awareness (the bucket cannot see references, so one tenant's cold
decision moves another tenant's shared bytes), pins, and any recall
story (an archive-class GET fails, and Copal would surface it as a
blob error with no run to poll). The mover is the recommendation for
the same reason the multi-region design recommends its copy worker
over store-native replication: only the path Copal drives preserves
the invariants Copal promises. The native path is documented, with its
blind spots, for deployments already paying for it.

## Non-goals

- **Automatic tier ladders.** The policy names one target tier.
  Nothing in the vocabulary precludes a second policy row and a third
  tier later; a built-in hot-cool-cold-archive cascade is complexity
  ahead of demand.
- **Partial-object tiering.** The unit is the blob. Splitting one
  object's ranges across tiers is document surgery on the byte plane,
  declined the way per-chunk authorization declined byte redaction.
- **Learned placement.** The operator states the policy; Copal does
  not infer one. Intelligent-Tiering exists for deployments that want
  inference, and the native-path paragraph names it.
- **Cross-residency moves.** A tier never changes jurisdiction. The
  cold bucket an operator configures for `eu` is expected to be in the
  same jurisdiction as the hot one, and that promise is the
  operator's, stated in the docs.
- **Tenant-facing tiering controls.** Temperature is operator
  economics. Tenants see the serving contract (fast, or 202 with a
  run) and never see placement; the pin is an admin action.
- **Tiering transient bytes.** Staging, tus sessions, and multipart
  parts never tier; they are short-lived by construction.

## The fail-safe table

| Absent or failing piece | Resulting behavior |
| --- | --- |
| No tiering policy | Nothing classifies, nothing moves. The behavior before tiering, including the byte path's zero extra reads. |
| No tiers configured | Setting a policy that names one refuses at validation. Nothing moves. |
| `accessed` basis with no recorded reads | Creation age decides. Content may move earlier, which costs latency and loses nothing. |
| Mover crash, any window | Re-copy and re-verify converge; the flip is the only commitment, and it follows verification. |
| Cold backend unreachable before the flip | The copy fails, the blob stays hot, and the pass logs and moves on. Reads are unaffected. |
| Cold backend unreachable after the flip | Reads fail as retryable blob errors. The grace-held hot copy does not mask the outage, because a masked outage is an outage discovered later. |
| Recall's restore fails or stalls | The run retries within the flow engine's budget; the 202 keeps answering with the run's state. |
| Archive class on a backend whose restore Copal does not drive (Azure) | Refused at boot, so no deployment can strand bytes behind a GET nothing answers. |
| Existing deployments upgrading | Every blob row reads `tier: NONE`, the primary backend. The null is the migration: no backfill, no recompute. |

Every row degrades toward the behavior before tiering or toward
latency. None degrades toward data loss, which is the property the
copy, verify, flip, erase ordering exists to guarantee.

## Ship order

1. **Configuration and the observe-only classifier.** Tiers parse and
   validate, the policy surface lands, `last_read` starts
   accumulating, and the sweep classifies and reports: gauges for
   candidate blobs and bytes, an admin listing of what would move per
   tenant, and the measured would-be recall rate. Shipped, without the
   projected-savings figure the design also proposed.
2. **The mover, online classes only.** Demote and the symmetric
   promote, grace-delayed erase, the GC, backup, and re-seal legs,
   pins, and the restore drill's per-tier check. Archive classes
   still refused at configuration. Shipped.
3. **Recall.** The flow-run recall, the 202 answers, S3
   `InvalidObjectState` and `RestoreObject`, and archive classes
   unlocked for S3 (and for filesystem and GCS tiers, which need no
   restore call). The availability contract is in the operator and
   tenant documentation. Shipped; Azure archive remains refused.

Each stage was independently reversible: delete the policy row and the
classifier idles; without the mover nothing is displaced; and before
recall shipped, no configuration could create bytes that need it.

## Decisions

Each decision was closed with the choice shown, and the shipped code
follows it.

1. **The tier axis.** A mutable column within the residency, versus
   tier-as-residency reusing the existing vocabulary wholesale.
   Chosen: the column. Frozen version links settle it: the engine
   throws on rewriting an armed row's blob link, and a design the
   engine refuses is refused.
2. **Policy basis.** Creation age only (no tracking), access recency
   only, or both. Chosen: both under one `basis` field, defaulting to
   `accessed`; creation-only would demote a file read every day.
3. **The recency signal.** File row versus blob row; per-read versus
   sampled versus day-coarse writes. Chosen: blob row, day-coarse,
   fire-and-forget after the response. The file row turns reads into
   events, and sampling misses the rarely read blobs the classifier is
   deciding about.
4. **Evaluated live versus stamped at creation.** Chosen: live.
   Retention stamps because its fail direction is erasure; tiering
   evaluates live because its fail direction is latency and the
   existing corpus is the point.
5. **The mover's home.** A sweep under the leader lease, versus a flow
   run per blob. Chosen: the sweep for policy moves (population-scale,
   nobody waiting), and a flow run per recall (demand-driven,
   caller-visible, pollable).
6. **Hot erase timing.** Immediately after the flip, versus
   grace-delayed. Chosen: grace-delayed, at least the larger of the GC
   grace and the backup cadence. It is the GC's own discipline, and it
   keeps every restore able to find the bytes.
7. **Cold read posture.** Read-through everywhere, 202 everywhere, or
   per class. Chosen: per class, declared on the tier. Online tiers
   change no face; archive tiers answer 202 with a run on REST and
   `InvalidObjectState` plus `RestoreObject` on the S3 face, the
   dialect S3 clients already speak.
8. **Auto-promotion on read.** Chosen: no. Reads update recency, and
   the symmetric sweep promotes when eligibility lapses, so one audit
   read cannot pull a corpus hot; the pin is the explicit override.
9. **Tiering under retention and holds.** Blocked, allowed, or
   allowed with a hold implying a hot pin. Chosen: allowed,
   digest-verified. Erasing a verified copy erases nothing the
   predicate protects; a hold does not imply a pin, and deployments
   needing prompt production pin explicitly.
10. **Native lifecycle rules.** Recommend them, forbid them, or
    document them. Chosen: document them as the alternative for
    S3-only online-class deployments, with the blind spots named
    (tenant-blind layout, dedupe, no recall). The mover is the
    recommendation because only the path Copal drives preserves the
    invariants Copal promises.
