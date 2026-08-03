# Bench

One command, one envelope:

```
./bench/run.sh
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

## Adding a measurement

`runners/bench.sh` records `name value` lines inside a three-pass
loop; `render.py` reduces the samples and derives rates from the best
timing so a rate and its timing always describe the same run. A new
measurement is a few lines in the runner and, if it needs a derived
rate, one entry in `DERIVED`.
