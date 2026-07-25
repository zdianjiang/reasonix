#!/usr/bin/env bash
set -euo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$here/../.." && pwd)
name=reasonix-feishu-schedule-test

test -f "$here/.env" || { echo "missing $here/.env" >&2; exit 1; }
test -f "$here/reasonix.toml" || { echo "missing $here/reasonix.toml" >&2; exit 1; }
mkdir -p "$here/state"

docker build -f "$here/Dockerfile" -t "$name" "$root"
docker rm -f "$name" >/dev/null 2>&1 || true
exec docker run --rm --name "$name" \
  --env-file "$here/.env" \
  -e REASONIX_HOME=/state/reasonix \
  -e REASONIX_STATE_HOME=/state/reasonix \
  -v "$here/reasonix.toml:/config/reasonix.toml:ro" \
  -v "$here/state:/state" \
  -v "$root:/workspace:rw" \
  "$name" --config /config/reasonix.toml bot start --channels feishu --dir /workspace
