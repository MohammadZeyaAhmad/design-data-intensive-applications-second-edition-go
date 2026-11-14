#!/usr/bin/env bash
set -euo pipefail
replication_root="$(pwd)"
mkdir -p artifacts/replication
if test -n "${ATLAS_REPLICATION_PG_BIN:-}"; then
  exec artifacts/replication-binaries/replication-lab \
    --pg-bin "$ATLAS_REPLICATION_PG_BIN" \
    --pg-share "${ATLAS_REPLICATION_PG_SHARE:-}" \
    --atlas "$replication_root/artifacts/replication-binaries/atlas" \
    --workdir "${ATLAS_REPLICATION_WORKDIR:-/tmp}" \
    --output "$replication_root/artifacts/replication/native.json"
fi
# Only the disposable evidence directory is writable by the container user.
chmod 0777 artifacts/replication
docker compose -f deploy/compose/replication/compose.yaml -p ddia-go-replication run --rm --no-deps replication-lab
