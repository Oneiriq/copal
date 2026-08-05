#!/usr/bin/env bash
# Register another self-hosted runner on this host.
#
# A runner process takes one job at a time. CI has two independent
# jobs, so a single runner runs them back to back and a pull request
# waits for both; a second runner halves that, and nothing in the
# workflow has to change, because neither job declares `needs`.
#
# Run this ON the runner host (aur0), once per runner you want:
#
#     ./add-runner.sh 2
#     ./add-runner.sh 3
#
# It needs `gh` authenticated as someone who can administer the org's
# runners, or a registration token in RUNNER_TOKEN. Registration
# tokens expire an hour after they are minted.
set -euo pipefail

ORG="${RUNNER_ORG:-Oneiriq}"
LABELS="${RUNNER_LABELS:-aur0}"
VERSION="${RUNNER_VERSION:-2.330.0}"
INDEX="${1:?usage: add-runner.sh <index>, e.g. 2}"
NAME="${RUNNER_NAME:-aur0-oneiriq-$INDEX}"
ROOT="${RUNNER_ROOT:-$HOME/actions-runners}"
DIR="$ROOT/$NAME"

if [ -d "$DIR" ]; then
  echo "$DIR already exists; remove it first or pick another index" >&2
  exit 1
fi

token="${RUNNER_TOKEN:-}"
if [ -z "$token" ]; then
  echo "minting a registration token for $ORG"
  token=$(gh api -X POST "orgs/$ORG/actions/runners/registration-token" --jq .token)
fi

mkdir -p "$DIR"
cd "$DIR"

archive="actions-runner-linux-x64-$VERSION.tar.gz"
if [ ! -f "$ROOT/$archive" ]; then
  echo "downloading runner $VERSION"
  curl -fsSL -o "$ROOT/$archive" \
    "https://github.com/actions/runner/releases/download/v$VERSION/$archive"
fi
tar -xzf "$ROOT/$archive"

# --replace is deliberately absent: a name collision should fail here
# rather than quietly evict the runner that is already working.
./config.sh \
  --unattended \
  --url "https://github.com/$ORG" \
  --token "$token" \
  --name "$NAME" \
  --labels "$LABELS" \
  --work "_work"

# As a service, so a reboot does not silently halve CI capacity.
sudo ./svc.sh install
sudo ./svc.sh start

echo
echo "$NAME is registered and running."
echo "Confirm capacity with:"
echo "  gh api orgs/$ORG/actions/runners --jq '.total_count'"
echo
echo "To remove it later:"
echo "  cd $DIR && sudo ./svc.sh stop && sudo ./svc.sh uninstall"
echo "  ./config.sh remove --token \$(gh api -X POST orgs/$ORG/actions/runners/registration-token --jq .token)"
