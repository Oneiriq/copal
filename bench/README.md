# Bench

One command, one envelope:

```
./bench/run.sh
```

A second command, one scale story:

```
./bench/scale.sh
```

The bench reuses the conformance stack, stands it up twice (engine
sessions off, then on), and takes the same measurements through the
S3 gateway both times. Measuring through the gateway is deliberate:
it is the path a migrating deployment uses, and it exercises
authorization, the session layer, the metadata plane, and the blob
store in one line of the table.

Results land in `results/<version>.md` and are committed per release.

## What the numbers are and are not

Timings are the best of three samples, with the spread printed
beside them. The two configurations differ by a few percent on a
single node, which is smaller than the spread this machine produces,
so the table says plainly where it cannot resolve a difference.
Publishing a mean without a spread would invite a conclusion the
measurement does not support.

These are single-node numbers with every container on one host: the
engine, Copal, and the client share CPU, so they describe the shape
of the system rather than a tuned deployment's ceiling. What they
are good for is comparison between configurations and detecting a
regression between releases, which is what an operator asking "what
does the second enforcement layer cost" actually wants to know.

The API face is measured beside the S3 face: search latency and an
MCP `tools/call`, timed by curl's own instrument because minimal
images carry no millisecond date.

## The scale bench

`scale.sh` answers a different question: not "what does a
configuration cost" but "what happens above 256 MiB". It runs the
ignored tests in `crates/copal-server/tests/scale.rs` and
`crates/copal-store/tests/scale.rs` one per process in release mode,
because the peak-working-set figure those tests report only ratchets
upward, and a process that runs one scenario is the only honest way
to attribute a peak to it. The measurements are in-process on
purpose: peak memory during a streamed upload and the
embedded-versus-remote wire cost are not observable through a
container boundary.

Scenarios: streamed single PUT with ranged and sequential reads at
512 MiB, 1 GiB, and 2 GiB (encryption on, one plaintext delta run);
multipart assembly at the aws CLI's 8 MiB part size at the same
sizes; the scan pass's buffered read at the same sizes; the F16
versus F32 vector index rebuild over a 100k-passage corpus; and the
repository round trips against mem:// and against a ws:// engine
(a throwaway container at the conformance-pinned version, in-memory,
so the wire is the only variable; `COPAL_BENCH_DB_URL` points the
run at an engine of your own instead). Raw lines land in
`results/scale.txt`;
the digested tables live in
[docs/operations.md](../docs/operations.md).

## Adding a measurement

`runners/bench.sh` records `name value` lines inside a three-pass
loop; `render.py` reduces the samples and derives rates from the best
timing so a rate and its timing always describe the same run. A new
measurement is a few lines in the runner and, if it needs a derived
rate, one entry in `DERIVED`.
