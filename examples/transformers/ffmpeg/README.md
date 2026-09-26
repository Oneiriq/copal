# An ffmpeg transformer

Copal's built-in processing covers sniffing, scanning, text
extraction, embedding, and image renditions. Video and audio are
somebody else's expertise, so they belong behind the transform seam:
any HTTP service that takes bytes and answers with bytes becomes a
derivation step, and its output lands as a real file with a digest,
versions, retention, and every serving rule intact.

`transform.py` is that service in about a hundred lines. It carries
four recipes:

| Path | Produces | Parameters |
| --- | --- | --- |
| `/thumbnail` | One frame as JPEG | `at` (default `00:00:01`), `width` (default 640) |
| `/audio` | The audio track as mp3 | `bitrate` (default `96k`) |
| `/preview` | A short silent mp4 | `seconds` (default 5), `width` (default 480) |
| `/probe` | `ffprobe` output as JSON | none |

## Running it

The service needs `ffmpeg` and `ffprobe` on its path and nothing else.
It listens on `PORT`, which defaults to 9000. Copal's S3 gateway is
commonly bound to 9000 too (the migration guide and the conformance
stack both use it), so these examples run the transformer on 9100:

```sh
PORT=9100 TRANSFORM_SECRET=shared-with-copal python transform.py
```

Point Copal at it and restart:

```sh
COPAL_TRANSFORMERS='{
  "thumbnail": {"url":"http://127.0.0.1:9100/thumbnail","secret":"shared-with-copal","timeout_secs":120},
  "audio":     {"url":"http://127.0.0.1:9100/audio","secret":"shared-with-copal","timeout_secs":120},
  "probe":     {"url":"http://127.0.0.1:9100/probe","secret":"shared-with-copal"}
}'
```

Copal waits 60 seconds for a transformer unless `timeout_secs` says
otherwise (up to 600), and sends sources up to 64 MiB unless
`max_source_bytes` says otherwise. The service gives ffmpeg 120
seconds (`TRANSFORM_TIMEOUT_SECS`), so the long-running recipes above
set `timeout_secs` to match.

Then derive:

```sh
curl -X POST $COPAL_URL/v1/files/$ID/transform \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"transformer":"thumbnail","params":{"at":"00:00:02","width":320},"content_type":"image/jpeg"}'
```

The answer is 202 with the derived record and its run id. The
derivation appears under `GET /v1/files/{id}/renditions` beside image
renditions, because a transform is a derivative like any other.

## In Docker

```yaml
services:
  transformer:
    image: python:3.12-slim
    command: python /app/transform.py
    volumes: [./examples/transformers/ffmpeg:/app:ro]
    environment:
      PORT: "9100"
      TRANSFORM_SECRET: shared-with-copal
    # python:3.12-slim carries no ffmpeg; install it or start from an
    # image that has one.
```

Inside a compose network, point the transformer URLs at
`http://transformer:9100/...` instead of `127.0.0.1`.

An image with ffmpeg already in it (`linuxserver/ffmpeg`,
`jrottenberg/ffmpeg`) saves the install step, though most of those
set `ffmpeg` as the entrypoint and need it overridden.

## The wire contract, which is all a transformer must honor

Copal `POST`s the source bytes as the request body, with the
parameters JSON-encoded in a `params` query argument and these
headers: `content-type` (the source's declared type), `x-copal-tenant`,
`x-copal-source-file`, `x-copal-source-digest`, and
`x-copal-transform-secret` when the configuration carries a secret.

The answer decides what happens next:

- **200 with bytes**: the derivation lands and the derived file
  becomes ready.
- **Any 4xx**: the input is unusable. The derived record fails, the
  run completes, and the first 200 characters of the body are
  recorded as the reason. Asking ffmpeg to thumbnail a text file
  lands here, with ffmpeg's own words in the run output.
- **5xx, a timeout, or a refused connection**: infrastructure. The
  run retries on the flow engine's attempt budget.

`transform.py` maps ffmpeg's own failures to 422 for exactly this
reason: a file ffmpeg cannot open is not an outage.

## What happens to the output

Derived bytes arrived from another process, so they walk the same
pipeline an upload does: sniffed, checked against the extension
policy, scanned when a scanner is configured, and extracted. The
derived record passes through `scanning` on its way to `ready`.

Extraction is why that matters. The `/probe` recipe's JSON becomes
searchable text, so `GET /v1/search?q=h264` finds the videos whose
metadata says so. A transformer that produces a transcript or an OCR
pass makes that text findable the same way, with no second upload.

## Sizing

Large sources are written to a temporary file before ffmpeg sees them,
because ffmpeg seeks and a pipe cannot. Size the transformer's disk
for the largest source you expect. Two ceilings apply: Copal refuses
sources above the transformer's `max_source_bytes` (default 64 MiB)
before calling, and the service refuses sources above
`TRANSFORM_MAX_BYTES` (default 512 MiB) with a 413. Raise both to
match your largest source.
