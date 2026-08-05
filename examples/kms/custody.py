#!/usr/bin/env python3
"""A key-custody service, in the shape Copal asks for.

Copal implements no key manager. It asks one for the blob master key
at boot, so the secret never sits in the environment and issuance,
revocation, and access logging stay where an operator already runs
them. The contract is one request:

    GET /keys/{key_id}
    Authorization: Bearer {token}
    -> 200 {"current": "<64 hex>", "previous": "<64 hex>" | null}

This reference reads keys from a directory, which is enough to run
and to test against. `adapt()` at the bottom is where a real
deployment reaches Vault, AWS KMS, or an HSM: everything above it is
the contract and stays the same.

    CUSTODY_DIR=./keys CUSTODY_TOKEN=shared python custody.py

A key file is named for its id and holds 64 hex characters. A
rotation adds `{key_id}.previous`, and removing that file is what
ends the rotation.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import pathlib
import secrets
import sys
import urllib.parse

DIRECTORY = pathlib.Path(os.environ.get("CUSTODY_DIR", "./keys"))
TOKEN = os.environ.get("CUSTODY_TOKEN")


def read_key(name):
    path = DIRECTORY / name
    if not path.exists():
        return None
    key = path.read_text(encoding="utf-8").strip()
    if len(key) != 64 or any(c not in "0123456789abcdefABCDEF" for c in key):
        raise ValueError(f"{path} does not hold 64 hex characters")
    return key


def adapt(key_id):
    """Where a real deployment reaches its key manager.

    Vault:      hvac.Client(...).secrets.kv.v2.read_secret_version(key_id)
    AWS KMS:    boto3.client("kms").decrypt(CiphertextBlob=wrapped)
    An HSM:     whatever the vendor's client offers.

    Answer with (current, previous). `previous` is the retiring key
    during a rotation and None otherwise.
    """
    current = read_key(key_id)
    if current is None:
        return None, None
    return current, read_key(f"{key_id}.previous")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # Custody access is worth logging: this is who asked for a key
        # and when.
        sys.stderr.write("custody: " + (fmt % args) + "\n")

    def answer(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        route = urllib.parse.urlparse(self.path).path
        if not route.startswith("/keys/"):
            return self.answer(404, {"error": "custody serves /keys/{key_id}"})

        if TOKEN:
            presented = (self.headers.get("authorization") or "").removeprefix("Bearer ")
            if not secrets.compare_digest(presented, TOKEN):
                return self.answer(403, {"error": "bearer token does not match"})

        key_id = urllib.parse.unquote(route[len("/keys/"):])
        if not key_id or "/" in key_id or ".." in key_id:
            return self.answer(400, {"error": "key id is a single plain name"})

        try:
            current, previous = adapt(key_id)
        except ValueError as bad:
            return self.answer(500, {"error": str(bad)})
        if current is None:
            return self.answer(404, {"error": f"no key named {key_id}"})
        self.answer(200, {"current": current, "previous": previous})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "9200"))
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    print(f"custody listening on 0.0.0.0:{port}, keys from {DIRECTORY}", file=sys.stderr)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
