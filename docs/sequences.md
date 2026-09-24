# Sequences

How the pieces behave over time. Every diagram here traces code that
runs; the file and function names are where to look next.

## Upload, end to end

Two calls from the client, and the record walks its state machine. The
claim lands before any byte, so a second uploader loses a
compare-and-swap instead of interleaving writes. The digest accumulates
while bytes stream, which is why the content address cannot be known
until the last byte.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant S as copal-server
    participant M as SurrealDB
    participant B as Blob store
    participant W as Flow worker

    C->>S: POST /v1/files {path}
    S->>M: create file row (draft)
    S-->>C: 201 {id, state: draft}

    C->>S: PUT /v1/files/{id}/content
    S->>M: claim_upload (CAS draft to uploading, lease)
    loop each chunk
        S->>S: hash plaintext, seal frame when a key is set
        S->>B: append to staging key
    end
    S->>B: land staging at objects/ab/cd/{digest}
    S->>M: record_sighting (dedupe refcount)
    S->>M: complete_upload (CAS uploading to scanning) + version row
    S->>M: enqueue post_upload run
    S-->>C: 200 {digest, size, state: scanning}

    W->>M: claim the run
    W->>W: sniff, policy, scan, extract, embed
    W->>M: finalize_upload (CAS scanning to ready)
    Note over M: the engine writes the file.ready outbox row<br/>in the same transaction as the transition
```

The record serves as soon as it has a digest, so a running scan never
blinks a previous version. Quarantine is the one verdict that blocks
the whole record.

## The post-upload pipeline

One journaled run, six activities, each one's output the next one's
input. Activities are idempotent by construction, so replay after a
crash is safe: sniffing and policy are pure over immutable content, and
the finalize compare-and-swap loses harmlessly on a repeat.

```mermaid
sequenceDiagram
    autonumber
    participant W as Flow worker
    participant J as Journal
    participant B as Blob store
    participant AV as clamd
    participant X as Extractor
    participant E as Embedder

    W->>J: claim run, read step cursor
    W->>B: read first bytes
    W->>W: sniff_type (declared vs actual)
    W->>W: extension_policy (denylist verdict)
    opt scanner configured
        W->>AV: stream content
        AV-->>W: clean or signature
    end
    W->>B: read content for extract_text
    alt text or JSON
        W->>W: decode natively
    else other formats, extractor configured
        W->>X: PUT /tika with the bytes
        X-->>W: text
    end
    W->>W: resolve markers, split into passages
    opt embedding service configured
        W->>E: embed_text for passages lacking vectors
        E-->>W: vectors
    end
    W->>J: finalize_upload (ready or quarantined)
    Note over W,J: every step records its output<br/>and a retry resumes at the cursor
```

## An agent through MCP

The tool manifest is generated from the contract, so an agent's tools
are the API's operations. The call path is the same dispatcher the
other contract faces use, which is why an agent cannot reach anything
a key could not.

```mermaid
sequenceDiagram
    autonumber
    participant A as Agent
    participant S as MCP endpoint
    participant D as Kayak dispatcher
    participant R as Resolver
    participant M as SurrealDB

    A->>S: initialize
    S-->>A: protocolVersion, serverInfo
    A->>S: tools/list
    S-->>A: 22 tools from generate_mcp_tools(contract)
    A->>S: tools/call {name: search, arguments: {q}}
    S->>S: authenticate, seed context (tenant, principal, scopes)
    S->>D: query("search", ctx, args)
    D->>D: scope check, argument validation, rate class
    D->>R: search resolver
    R->>M: lexical and vector retrieval over chunks
    M-->>R: passages
    R-->>D: answer
    D-->>S: JSON
    S-->>A: content[0].text
```

## A signed URL

Grants are stateful capabilities. The row holds a hash, so revocation
is one write and there is no signing key to leak. Redemption carries no
tenant identity, which is what makes the URL shareable.

```mermaid
sequenceDiagram
    autonumber
    participant C as Keyed client
    participant V as Anonymous viewer
    participant S as copal-server
    participant M as SurrealDB
    participant B as Blob store

    C->>S: POST /v1/files/{id}/url {ttl_secs, max_uses}
    S->>M: store grant row (hash, expiry, use cap)
    S-->>C: {url: /v1/grants/cg1...}
    Note over C,V: the URL travels however the caller likes

    V->>S: GET /v1/grants/cg1...
    S->>M: look up by id, verify hash, check expiry and uses
    S->>M: increment use count
    S->>B: open the content address
    B-->>S: bytes (decrypted on the way out)
    S-->>V: 200 with range and validator support
```

## The operator console

A browser has no admin header, so the console takes the admin token as
the Basic password. Pages and forms run the same dispatcher, so what
the console shows is what the API would answer.

```mermaid
sequenceDiagram
    autonumber
    participant O as Operator browser
    participant S as Console routes
    participant J as Kayak ConsoleRouter
    participant D as Kayak dispatcher
    participant M as SurrealDB

    O->>S: GET /admin/console
    S->>S: Basic auth against the admin token
    S->>M: tenant population, audit tail
    S-->>O: deployment home

    O->>S: GET /admin/console/t/acme/r/files
    S->>J: page("/r/files", query, operator context)
    J->>D: list("files", ctx, args)
    D->>M: through the resolver
    J-->>S: HTML with the declared columns and filters
    S-->>O: 200 text/html

    O->>S: POST .../r/files/{id}/a/issue_url (form)
    S->>J: submit(path, form pairs, ctx)
    J->>D: action("files", "issue_url", ctx, args)
    D-->>J: answer or refusal
    J-->>S: redirect target
    S-->>O: 303 back to the instance
```

## Master key rotation

Content addressing makes the sweep safe in place: the digest covers
plaintext, so a re-sealed object keeps its address and every reference
to it.

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant S as copal-server
    participant M as SurrealDB
    participant B as Blob store

    Op->>S: restart with KEY=new, KEY_PREVIOUS=old
    S->>M: re-seal sealed secrets (S3, webhook, edge)
    Note over S: boot pass, before traffic
    loop every five minutes until drained
        S->>B: list objects
        S->>B: probe first frame with the current key
        alt current key opens it
            S->>S: skip
        else only the retiring key opens it
            S->>B: decrypt, re-seal, land at the same address
            S->>S: copal_resealed_total += 1
        end
    end
    Op->>Op: watch the counter go quiet
    Op->>S: restart without KEY_PREVIOUS
```

Reads work throughout: every open probes the current key first and
falls back once, so a drained rotation costs nothing on the read path.

## Where a contract change goes

The gate is a test. A declaration edit that disagrees with the database
schema, or that would change a generated artifact without blessing it,
fails in CI with the offending name.

```mermaid
sequenceDiagram
    autonumber
    participant Dev as Contract change
    participant T as cargo test
    participant JV as Kayak validate
    participant JG as Kayak generate_all

    Dev->>T: run the contract test
    T->>JV: contract against the real surql-rs schema
    alt a filter or sort no index serves
        JV-->>T: violation naming the column
        T-->>Dev: red
    else clean
        JV-->>T: no violations
    end
    T->>JG: render every artifact
    JG-->>T: openapi.json, schema.graphql, mcp-tools.json, policy.json, four clients
    T->>T: compare against the checked-in copies
    alt bytes differ
        T-->>Dev: red, re-bless with COPAL_BLESS=1
    else identical
        T-->>Dev: green
    end
```
