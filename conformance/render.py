"""Turn the runners' PASS/FAIL lines into the committed results table."""

import pathlib
import sys

version = sys.argv[1] if len(sys.argv) > 1 else "dev"
results = pathlib.Path(__file__).parent / "results"

clients = {}
for path in sorted(results.glob("*.txt")):
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2] in ("PASS", "FAIL"):
            client, name, outcome = parts
            clients.setdefault(client, []).append((name, outcome))

lines = [
    f"# Conformance results: Copal {version}",
    "",
    "Produced by `conformance/run.sh`: the stack in",
    "`docker-compose.yml`, one scenario per client, every check",
    "named. Run the same command to reproduce the table.",
    "",
]
for client, checks in clients.items():
    passed = sum(1 for _, outcome in checks if outcome == "PASS")
    lines.append(f"## {client}: {passed}/{len(checks)}")
    lines.append("")
    lines.append("| check | result |")
    lines.append("| --- | --- |")
    for name, outcome in checks:
        lines.append(f"| {name} | {outcome} |")
    lines.append("")

out = results / f"{version}.md"
out.write_text("\n".join(lines), encoding="utf-8")
print(f"wrote {out}")
