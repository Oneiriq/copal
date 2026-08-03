#!/usr/bin/env bash
# Assemble and validate the four SDK packages from the generated
# clients. Everything lands under sdks/dist (gitignored); the
# skeletons and the generated files stay untouched. This is the same
# entry CI runs, so a package that stops building fails a PR before
# it can fail a release.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
version="$(tr -d ' \r\n' < "$here/VERSION")"
dist="$here/dist"

echo "assembling SDKs at version $version"
rm -rf "$dist"
mkdir -p "$dist"

# Rust: skeleton + client as src/lib.rs, version stamped.
mkdir -p "$dist/rust/src"
sed "s/^version = \"0.0.0\"/version = \"$version\"/" "$here/rust/Cargo.toml" > "$dist/rust/Cargo.toml"
cp "$root/clients/client.rs" "$dist/rust/src/lib.rs"

# Python: skeleton + client as copal.py, version stamped.
mkdir -p "$dist/python"
sed "s/^version = \"0.0.0\"/version = \"$version\"/" "$here/python/pyproject.toml" > "$dist/python/pyproject.toml"
cp "$root/clients/client.py" "$dist/python/copal.py"

# TypeScript: skeleton + client as src/index.ts, version stamped.
mkdir -p "$dist/typescript/src"
sed "s/\"version\": \"0.0.0\"/\"version\": \"$version\"/" "$here/typescript/package.json" > "$dist/typescript/package.json"
cp "$here/typescript/tsconfig.json" "$dist/typescript/tsconfig.json"
cp "$root/clients/client.ts" "$dist/typescript/src/index.ts"

# Go: module + client; the version is a tag, nothing to stamp.
mkdir -p "$dist/go"
cp "$here/go/go.mod" "$dist/go/go.mod"
cp "$root/clients/client.go" "$dist/go/copal.go"

echo "validating rust"
(cd "$dist/rust" && cargo check --quiet)

echo "validating python"
PYTHONDONTWRITEBYTECODE=1 python -m py_compile "$dist/python/copal.py"
(cd "$dist/python" && python -c "import copal")

echo "validating typescript"
(cd "$dist/typescript" && npx -y -p typescript@5 tsc --noEmit)

echo "validating go"
(cd "$dist/go" && go build ./... && go vet ./...)

echo "all four SDKs assemble and validate at $version"
