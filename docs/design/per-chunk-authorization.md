# Per-chunk authorization

Status: shipped. Markers ride the content PUT header
(`x-copal-markers`), the fetch body, and tus `Upload-Metadata`; they
persist on `file_version`, resolve at extraction, and set a level on
each `text_chunk`. Search, facets, the rerank window, and
`GET /v1/files/{id}/text` enforce them, and the engine carries the
`text_chunk` clause. Not built: the SDK upload helper described under
API surface (the generated clients have no byte upload methods), and
the items listed as non-goals. The tenant-facing reference is the
search section of [api.md](../api.md).

This page records the design. The load-bearing call came first:
sensitivity comes from per-upload markers, declared by the uploader,
who is the only party anywhere in the path that knows. A classifier
seam was rejected for a reason that shapes everything below. Every
seam in this service fails safe when it is absent: no extractor means
no text, no reranker means the fused order, no custody means the
server refuses to boot. A classifier deciding confidentiality would be
the first seam whose absence leaks, so the absence of markers must
mean exactly what the absence of everything else means: the behavior
before markers existed, nothing more exposed. This document covers the
rest: what a marker looks like on the wire, how it survives
extraction, where the level lands in the data model, where enforcement
lives, and what the API faces see.

## What was already true

Before markers, a chunk was withheld exactly when its file was. The
retrieval queries in `crates/copal-store/src/repo/text.rs` carried one
shared clause beside the tenant predicate (now `FILE_DISCLOSABLE`):
grant-access files answer no search, quarantined and deleted records
likewise, on both retrieval legs and in the facet counts. Enforcement
sits inside the query, so a withheld passage never becomes a hit that
has to be filtered later.

That machinery had the right shape at the wrong granularity, and both
halves of the fix already had names. The unit existed: `text_chunk`
rows are what retrieval returns, one per passage, linked to their
file. The vocabulary existed: `access INSIDE ['public', 'private',
'tenant', 'grant']` on the file row, the same four words
[principals.md](../principals.md) uses. What was missing was a level
on the chunk and a way for an upload to set it.

## The marker

### What a marker says

A marker names a span of the document and the access level of that
span, drawn from the file vocabulary. One rule governs its meaning:
**a marker narrows, never widens**. A chunk's effective level is at
least as restrictive as its file's, and a marker declaring a looser
level than the file's is a validation error before any byte moves.
The reason is the fail direction. Search is tenant-authenticated on
every face, so a `public` chunk inside a `private` file could not
reach anyone new today, and the moment an anonymous retrieval surface
exists, a widening marker becomes a leak vector that every marked
upload in history has already armed. Narrowing-only means historical
markers can only ever have withheld too much.

At the current enforcement granularity the operative restriction is
`grant`, because `public`, `private`, and `tenant` files all answer
the same tenant-scoped, read-scoped search. The full vocabulary is
accepted anyway. Principals exist but do not yet split `private` from
`tenant` on the read path; accepting the four words now means that
split, when it comes, reaches chunks with no wire change.

### Wire shapes considered

| Shape | Survives extraction | Faces it fits | Verdict |
| --- | --- | --- | --- |
| Char ranges over the content | Exactly for native text; not at all through an extractor | create/fetch/PUT/tus | Adopt, for native text |
| Delimiter sentinels in the content | Yes, by riding the text | Any, but mutates content | Reject |
| Per-part markers on multipart | N/A; parts are transport | S3 multipart, tus | Reject |
| Text anchors (quoted spans) | Yes, by construction | All | Adopt |

**Char ranges** are exact where the extracted text is the decoded
content, which is what `native` extraction means: for `text/*` and
JSON the stored body is the upload, so a character range in the
upload is a character range in the extraction. Through an extractor
the correspondence collapses. A PDF's bytes bear no offset
relationship to the text Apache Tika returns, and pretending
otherwise would mark spans nobody chose. Ranges are therefore valid
only for natively decoded content, and refused (400, at resolution
recorded as unresolvable, below) elsewhere.

**Delimiter sentinels** (the uploader embeds `<<copal:grant>> ...
<<end>>` in the document) survive extraction because they ride the
text, and are rejected anyway. The digest would cover them, downloads
would return them, and the stored content would stop being the
caller's content. Copal never rewrites bytes; a marker convention
that only works by polluting the artifact is a cost every reader of
the file pays forever.

**Per-part markers** are rejected because a part is a transport
artifact: at least 5 MiB on the S3 face, arbitrary on tus, invisible
on single PUT and fetch. No document's confidentiality boundary lands
on a part boundary except by accident.

**Text anchors** quote the document at itself: a span begins at the
first occurrence of `from` and ends at the next occurrence of
`until` (absent `until` means the document's end). Anchors survive
extraction by construction because they are resolved against the
extracted text, which is the text that will be chunked. The uploader
knows the words: they have the document. Ambiguity resolves in the
fail-safe direction: an anchor pair matching several times marks
every match.

The recommended wire shape is one input carrying both forms:

```json
"markers": [
  { "access": "grant", "range": { "start": 1200, "end": 3400 } },
  { "access": "grant", "from": "Pricing Schedule", "until": "Appendix B" }
]
```

### Where it rides

Markers describe content. A file's record outlives its content
across re-uploads, so a marker attached at create time
would describe bytes that had not arrived and would silently apply to
whatever future upload replaced them, which is wrong in both fail
directions at once. Markers therefore ride the calls that carry or
name content:

- `PUT /v1/files/{id}/content`: an `x-copal-markers` header carrying
  the JSON. The byte routes already carry their semantics in headers
  (conditionals, ranges), and this is one more.
- `POST /v1/files/fetch`: a `markers` field in the body, beside
  `content_type` and `access`.
- tus creation: a `markers` key in `Upload-Metadata`, base64 like its
  peers.

`POST /v1/files` does not take markers: there is no content there to
describe. A re-upload without markers is unmarked content, which is
the fail-safe.

Declared markers persist on `file_version`, which freezes at arming.
[Retention](../retention.md) put its clock on the version for the
same reason that applies here: the version is the frozen artifact,
and markers are facts about exactly that content. Persisting them is
what lets a re-extraction (an extractor upgrade, a pipeline retry)
re-resolve the same declaration against the same digest instead of
losing it, the way `put_embedding` already guards on digest so stale
vectors cannot attach to new text.

### Through extraction to chunk boundaries

Resolution happens in the pipeline's extract activity, at chunking
time, because that is the only moment the extracted text and the
markers are both in hand. Ranges resolve directly (native decodes
only); anchors resolve by search over the extracted text. Then the
chunker runs as it does today, and each chunk compares its span of
the text against the resolved marker spans:

**A chunk overlapping any marked span inherits the marker's level.**
The overlap windows make this coarse on purpose: consecutive chunks
share 150 characters, so a sensitive sentence's tail appears at the
head of the next chunk, and that next chunk inherits too. The fail
direction is the argument. Inheriting on any overlap over-withholds
at most one chunk's worth of neighboring text; any finer rule
(inherit only past a threshold, split chunks at marker edges) creates
cases where a fragment of a marked span sits in a chunk that did not
inherit, and a fragment of a confidential passage is a leak of the
confidential passage. Chunking serves retrieval; markers serve
confidentiality; where they collide, retrieval loses a passage and
confidentiality loses nothing.

**A marker that cannot be resolved restricts the whole file.** An
anchor whose text never occurs (extraction dropped a ligature, the
uploader typo'd), a range pointing past the extraction's truncation
ceiling, a range on extractor-produced text: each of these is a
declared intention that cannot be located. Marking nothing would turn
a resolution failure into disclosure of exactly the passage the
uploader tried to protect. So an unresolvable marker applies its
level to every chunk of the file, and the verdict lands under
`metadata.processing` where the uploader can see it and re-upload
with a corrected declaration. Mapping failure costs availability,
never confidentiality.

There is no unmarked window. The chunk's level lands in the same
`CREATE` payload as its body, so no crash between two writes can
leave a restricted passage readable; `put_chunks` already deletes
every old chunk before the new ones land, and that ordering keeps
serving this design.

## Data model

One nullable column on `text_chunk`:

- `access`: `NONE`, or one of the four levels. `NONE` means "the
  file's level", which is the behavior before markers existed.

The migration for existing chunks is the column's default. The boot
reconciler adds it, every existing row reads `NONE`, and `NONE`
defers to the file, so a deployment upgrades into exactly the
behavior it had. No backfill, nothing recomputed: the null is the
migration, the same additive shape principals used for
`principal_id` on `api_key`.

Two supporting pieces:

- `file_version.markers`: the declaration as data, readonly like
  every other version scalar, for re-resolution.
- `file_text.withheld`: the resolved spans (start, end, level) over
  the stored body, written in the same statement as the body, so the
  full-text read path can enforce without re-resolving anchors.

## Enforcement

Effective level of a chunk: its own `access` when set, else its
file's. Every enforcement point below speaks that one expression.

**The fused search.** The shared clause grows a chunk half
(`CHUNK_DISCLOSABLE`):

```
(access IS NONE OR access != 'grant')
```

beside the existing file clauses, in the same constant, so the
lexical leg, the semantic leg, and the facet query cannot drift apart;
they already share the string. A withheld chunk is never a candidate,
which settles the downstream surfaces with no extra work:

- **The rerank window** is built from returned hits, so the reranker
  never receives withheld text and cannot resurface it.
- **Facet counts** run the same WHERE, so a file whose only matching
  passages are withheld contributes to no bucket, and counts cannot
  reveal that a match exists. A file with some disclosable matching
  passages counts once, as today.
- **The semantic leg** filters as a residual over the KNN candidates,
  as the tenant clause already does; the over-fetch absorbs the loss
  and the trimmed limit means result counts reveal nothing.

**`GET /v1/files/{id}/text`.** The stored body contains the marked
spans, so the full-text read is the second door and must close with
the first. The response elides the withheld spans and carries
`"withheld": <n>` so a caller knows the text is partial; `chars`
counts the served text. Refusing the whole text instead would
recreate exactly the all-or-nothing behavior this design exists to
remove. Span positions are not disclosed: the length of a secret is
part of the secret.

**The engine's second layer.** `EnginePolicy.select_conjuncts`
already carries per-table clauses into compiled `PERMISSIONS`.
`text_chunk` gains one:

```
(access IS NONE OR access != 'grant') AND file.access != 'grant'
```

so a caller-bound engine session meets the refusal even when a
request-path bug drops the application clause, which is the entire
point of the second layer and the same treatment tenancy and
retention received. `file_text` gains the mirror clause over its
`withheld` spans' existence only if the engine face ever serves that
body raw; today no caller session reads it directly, and the
application path is the enforcement point.

**The byte boundary is untouched.** Downloads serve whole objects
under the file's level, as before. See non-goals.

## API surface

**Contract and differ.** `file_fetch` gains an optional `markers`
input. In Kayak's vocabulary that is `Change::Compatible` ("optional
input added"); only a required input would be `Breaking`, and nothing
here requires. The search query declaration does not change: no new
inputs, no new outputs; withheld passages never appear. The text
query's response gains `withheld`, an added field, likewise
compatible.

**Byte routes.** The `x-copal-markers` header on content PUT lives
where the byte routes live: outside the contract object, covered by
integration tests, like ranges and conditionals before it.

**SDKs.** The four generated clients pick up `file_fetch`'s optional
field on regeneration. The design also called for upload helpers with
a markers argument that sets the header. That part is not built: the
generated clients have no byte upload methods at all (see
[sdks.md](../sdks.md)). A caller that passes no markers behaves
identically, which is what let this ship without a major version.

**S3 gateway and tus.** tus carries markers in `Upload-Metadata` as
described. The S3 face carries none initially: S3's vocabulary has no
such concept, and inventing an `x-amz-meta-` convention is a decision
to make when someone migrating a bucket asks for it.

## Non-goals

- **Byte-level redaction.** A caller the file's level admits
  downloads every byte, marked spans included. Per-chunk
  authorization governs the retrieval surfaces: search, facets,
  excerpts, extracted text. Serving a partial PDF is a
  document-surgery problem this design does not touch. To keep a
  passage from a reader entirely, the file's own level must exclude
  that reader.
- **Widening.** No marker loosens; argued above.
- **A classifier seam.** Rejected before this design; restated here so
  this document cannot be read as reopening it.
- **Post-hoc marking.** Markers arrive with content. An "add markers
  later" endpoint is safe in the fail direction (it only narrows) and
  may earn its place later; it is out of scope here because its
  natural companion, post-hoc unmarking, is not safe, and shipping
  one without the other needs its own argument.
- **S3 marker carriage**, per above.

## The fail-safe table

| Absent or failing piece | Resulting behavior |
| --- | --- |
| No markers on an upload | Every chunk `NONE`: the file's level, exactly as before markers existed. |
| No extractor configured | No text, no chunks, nothing to mark or leak. |
| Anchor never matches | Whole file's chunks take the marker's level; verdict in `metadata.processing`. Over-withholds. |
| Range past the truncation ceiling, or on extracted (non-native) content | Same as an unmatched anchor. Over-withholds. |
| Re-upload without markers | New content is unmarked; file-level behavior. Markers die with their digest. |
| Marker widens, or names an unknown level | 400 before bytes move. |
| Embedding service absent | Lexical retrieval over the disclosable set. |
| Reranker absent or failing | Fused order over the disclosable set; the reranker never held withheld text to lose. |
| Engine access key unconfigured | The application-layer WHERE enforces alone, the same posture as every other guard. |
| Crash during chunk writes | Old chunks already deleted; new chunks land with their level in the same CREATE. No unmarked window. |

Every row degrades toward withholding or toward the unmarked
behavior. None degrades toward disclosure.

## Decisions

Each decision was closed with the choice shown, and the shipped code
follows it.

1. **Marker addressing.** Ranges only, anchors only, or both. Chosen:
   both under one input; ranges valid for native text, anchors
   everywhere, and unresolvable declarations restrict the whole file.
2. **Where markers ride.** Create-time, content-time, or a separate
   endpoint. Chosen: content-bearing calls only (PUT header, fetch
   body, tus metadata), persisted on `file_version`.
3. **Narrowing-only versus independent chunk levels.** Chosen:
   narrowing-only; widening is a non-goal until an anonymous
   retrieval surface exists to give it meaning.
4. **Chunk column shape.** Nullable `access` with `NONE` meaning
   inherit, versus a materialized effective level. Chosen: nullable
   inherit; the null is the migration, and a materialized level would
   have to chase every file-level change.
5. **Full-text reads under markers.** Elide spans, or refuse the
   document. Chosen: elide, with a `withheld` count and no span
   positions.
6. **The engine's second layer.** Add the `text_chunk` select
   conjunct now or defer. Chosen: now; it is one clause in a
   vocabulary (`EnginePolicy.select_conjuncts`) that already exists.
7. **Marker vocabulary.** Accept all four levels now, or `grant` only
   until principals split the read path. Chosen: all four; enforcement
   collapses to grant-versus-rest by itself, and the wire never has to
   change.
