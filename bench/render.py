"""Turn the two passes into the committed envelope table.

Each timing was sampled three times. The table reports the best
sample and the spread, because the differences between the two
configurations are small enough that a single sample cannot tell an
outlier from an effect, and a number published without its spread
invites a conclusion the measurement does not support.
"""

import pathlib
import platform
import sys

version = sys.argv[1] if len(sys.argv) > 1 else "dev"
results = pathlib.Path(__file__).parent / "results"

# Derived figures, computed from the best sample so a rate and its
# timing always describe the same run.
DERIVED = {
    "ingest_64mib_ms": ("ingest_throughput", 64 * 1000, "MiB/s"),
    "retrieve_64mib_ms": ("retrieve_throughput", 64 * 1000, "MiB/s"),
    "ingest_50_small_ms": ("ingest_small_objects", 50 * 1000, "objects/s"),
    "retrieve_50_small_ms": ("retrieve_small_objects", 50 * 1000, "objects/s"),
}
ORDER = []


def read(name):
    path = results / f"sessions-{name}.txt"
    samples = {}
    if not path.exists():
        return samples
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            metric, value = parts[0], int(parts[1])
            samples.setdefault(metric, []).append(value)
            if metric not in ORDER:
                ORDER.append(metric)
    return samples


off = read("off")
on = read("on")


def cell(samples, metric):
    values = samples.get(metric)
    if not values:
        return "-"
    best = min(values)
    spread = max(values) - best
    return f"{best} (+{spread})" if spread else str(best)


def derived_cell(samples, metric):
    values = samples.get(metric)
    if not values:
        return "-"
    _, numerator, _ = DERIVED[metric]
    return str(numerator // max(min(values), 1))


lines = [
    f"# Performance envelope: Copal {version}",
    "",
    "Produced by `bench/run.sh`, measured through the S3 gateway with",
    "stock tooling, because that is the path a migrating deployment",
    "uses. Two passes: caller-bound engine sessions off, then on, so",
    "the cost of the second enforcement layer is a column rather than",
    "a guess.",
    "",
    f"Machine: {platform.system()} {platform.machine()}, single-node stack,",
    "all containers on one host. Timings are the best of three samples",
    "with the spread beside them; where the spread is wider than the",
    "gap between the two columns, the honest reading is that this",
    "machine cannot resolve the difference.",
    "",
    "| measurement | sessions off | sessions on | unit |",
    "| --- | --- | --- | --- |",
]
for metric in ORDER:
    lines.append(f"| {metric} | {cell(off, metric)} | {cell(on, metric)} | ms |")
for metric in ORDER:
    if metric in DERIVED:
        name, _, unit = DERIVED[metric]
        lines.append(
            f"| {name} | {derived_cell(off, metric)} | {derived_cell(on, metric)} | {unit} |"
        )
lines.append("")

out = results / f"{version}.md"
out.write_text("\n".join(lines), encoding="utf-8")
print(f"wrote {out}")
