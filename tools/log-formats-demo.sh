#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/log-formats
if test -n "${ATLAS_LOG_PG_BIN:-}"; then
  exec artifacts/log-formats-binaries/log-formats-lab --pg-bin "$ATLAS_LOG_PG_BIN" --pg-share "${ATLAS_LOG_PG_SHARE:-}" --output "$(pwd)/artifacts/log-formats/native.json"
fi
chmod 0777 artifacts/log-formats
docker compose -f deploy/compose/log-formats/compose.yaml -p ddia-go-log-formats run --rm --no-deps log-formats
