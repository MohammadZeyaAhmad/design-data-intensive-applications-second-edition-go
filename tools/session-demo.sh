#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/session-consistency
if test -n "${ATLAS_SESSION_PG_BIN:-}"; then
  exec artifacts/session-binaries/session-lab --pg-bin "$ATLAS_SESSION_PG_BIN" --pg-share "${ATLAS_SESSION_PG_SHARE:-}" --atlas "$(pwd)/artifacts/session-binaries/atlas" --output "$(pwd)/artifacts/session-consistency/native.json"
fi
chmod 0777 artifacts/session-consistency
docker compose -f deploy/compose/session-consistency/compose.yaml -p ddia-go-session run --rm --no-deps session-consistency
