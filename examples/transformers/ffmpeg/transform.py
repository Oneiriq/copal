#!/usr/bin/env python3
"""An external transformer that wraps ffmpeg.

Copal ships the source bytes in the request body and expects the
derived bytes back. Everything here is that contract and nothing
more: read the body, run ffmpeg over it, answer with what ffmpeg
produced. A 4xx tells Copal the input was unusable, so the derived
record fails with the reason instead of retrying forever; anything
else it treats as infrastructure and retries.

Run it beside Copal:

    python transform.py            # listens on 0.0.0.0:9000

and point Copal at it:

    COPAL_TRANSFORMERS='{"thumbnail":{"url":"http://127.0.0.1:9000/thumbnail"}}'
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import subprocess
import sys
import tempfile
import urllib.parse

SECRET = os.environ.get("TRANSFORM_SECRET")
TIMEOUT = int(os.environ.get("TRANSFORM_TIMEOUT_SECS", "120"))
MAX_BYTES = int(os.environ.get("TRANSFORM_MAX_BYTES", str(512 * 1024 * 1024)))


def thumbnail(source, params):
    """One frame, as a JPEG. `at` picks the timestamp."""
    at = str(params.get("at", "00:00:01"))
    width = int(params.get("width", 640))
    return [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", at, "-i", source,
        "-frames:v", "1",
        "-vf", f"scale={width}:-2",
        "-f", "image2", "-c:v", "mjpeg", "-",
    ]


def audio(source, params):
    """The audio track alone, as mp3, for transcription or preview."""
    bitrate = str(params.get("bitrate", "96k"))
    return [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", source,
        "-vn", "-b:a", bitrate,
        "-f", "mp3", "-",
    ]


def preview(source, params):
    """A short, small mp4: enough to see what the file is."""
    seconds = str(params.get("seconds", 5))
    width = int(params.get("width", 480))
    return [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", source, "-t", seconds,
        "-vf", f"scale={width}:-2",
        "-an", "-movflags", "frag_keyframe+empty_moov",
        "-f", "mp4", "-",
    ]


def probe(source, _params):
    """What ffprobe knows, as JSON. Useful as searchable metadata."""
    return [
        "ffprobe", "-hide_banner", "-loglevel", "error",
        "-show_format", "-show_streams",
        "-print_format", "json", source,
    ]


RECIPES = {
    "/thumbnail": thumbnail,
    "/audio": audio,
    "/preview": preview,
    "/probe": probe,
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("transformer: " + (fmt % args) + "\n")

    def refuse(self, status, message):
        body = message.encode()
        self.send_response(status)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        route = urllib.parse.urlparse(self.path)
        recipe = RECIPES.get(route.path)
        if recipe is None:
            return self.refuse(404, f"no recipe at {route.path}")

        if SECRET and self.headers.get("x-copal-transform-secret") != SECRET:
            return self.refuse(403, "shared secret does not match")

        length = int(self.headers.get("content-length") or 0)
        if length <= 0:
            return self.refuse(400, "no source bytes")
        if length > MAX_BYTES:
            return self.refuse(413, f"source exceeds {MAX_BYTES} bytes")

        params = {}
        raw = urllib.parse.parse_qs(route.query).get("params", ["null"])[0]
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                params = parsed
        except json.JSONDecodeError:
            return self.refuse(400, "params is not JSON")

        # ffmpeg seeks, so the source goes to a file rather than a pipe.
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            remaining = length
            while remaining > 0:
                block = self.rfile.read(min(1 << 20, remaining))
                if not block:
                    break
                handle.write(block)
                remaining -= len(block)
            source = handle.name

        try:
            command = recipe(source, params)
            done = subprocess.run(
                command, capture_output=True, timeout=TIMEOUT, check=False
            )
            if done.returncode != 0 or not done.stdout:
                detail = done.stderr.decode("utf-8", "replace")[:300].strip()
                # ffmpeg failing on the input is the input's problem,
                # which is a refusal rather than an outage.
                return self.refuse(422, detail or "ffmpeg produced nothing")
            self.send_response(200)
            self.send_header("content-type", "application/octet-stream")
            self.send_header("content-length", str(len(done.stdout)))
            self.end_headers()
            self.wfile.write(done.stdout)
        except subprocess.TimeoutExpired:
            self.refuse(504, f"ffmpeg exceeded {TIMEOUT}s")
        finally:
            os.unlink(source)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "9000"))
    print(f"transformer listening on 0.0.0.0:{port}", file=sys.stderr)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
