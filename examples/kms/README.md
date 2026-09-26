# Key custody

The blob master key decides whether stored bytes can be read at all,
and by default it lives in the process environment, where it sits in
shell history, in a compose file, and in whatever inspects a running
container. A key guarding retention and WORM deserves the custody the
rest of a deployment's secrets already have.

Copal implements no key manager. It asks one for the key at boot and
holds it in memory for the process lifetime, which keeps the secret
out of the environment and leaves issuance, revocation, and access
logging where an operator already runs them.

## The contract

One request, so a short adapter in front of anything can serve it:

```
GET {COPAL_KMS_ADDR}/keys/{COPAL_KMS_KEY_ID}
Authorization: Bearer {COPAL_KMS_TOKEN}      (when configured)
```

The answer:

```
200 {"current": "<64 hex>", "previous": "<64 hex>" | null}
```

Both keys arrive in one answer because a rotation is a state. Reading
them separately can show a half that never existed. `previous` carries
the retiring key while the re-seal sweep drains, exactly as
`COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS` does.

## Running the reference

`custody.py` serves keys from a directory, which is enough to run and
to test against:

```sh
mkdir -p keys && head -c 32 /dev/urandom | xxd -p -c 32 > keys/blob
CUSTODY_DIR=./keys CUSTODY_TOKEN=shared python custody.py
```

Point Copal at it and leave `COPAL_BLOB_ENCRYPTION_KEY` unset:

```sh
COPAL_KMS_ADDR=http://127.0.0.1:9200
COPAL_KMS_KEY_ID=blob
COPAL_KMS_TOKEN=shared
```

The boot log says `master key taken from custody`, and the key is
nowhere in the environment.

## Reaching a real key manager

Everything above `adapt()` in `custody.py` is the contract and stays
as it is. `adapt()` is the twenty lines that change:

| Manager | What `adapt()` calls |
| --- | --- |
| Vault | `hvac.Client(...).secrets.kv.v2.read_secret_version(key_id)` |
| AWS KMS | `boto3.client("kms").decrypt(CiphertextBlob=wrapped)` against a stored wrapped key |
| An HSM | whatever the vendor's client offers |

Copal asks once per boot, so the call can be slow and the manager can
rate-limit it hard.

## What it refuses

A deployment configured for custody that cannot reach it **does not
start**, and does not fall back to the environment even when
`COPAL_BLOB_ENCRYPTION_KEY` is set. Coming up unable to open its own
content, or coming up on a key an operator thought they had retired,
are both worse than not coming up. An answer that is not 64 hex
characters is refused at the same point, where the cause is obvious,
rather than at the first read of a sealed object.

## Rotating

Write the new key, move the old one to `{key_id}.previous`, and
restart. Copal seals under the current key and falls back to the
retiring one, the sweep re-seals in place, and removing the
`.previous` file and restarting ends the rotation. The runbook in
[operations.md](../../docs/operations.md) is the same three motions;
custody just holds the keys instead of the environment.
