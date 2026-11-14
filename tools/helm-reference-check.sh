#!/usr/bin/env bash
set -euo pipefail
helm_tools="${ATLAS_OPERATIONS_TOOLS:-${TMPDIR:-/tmp}/atlas-operations-tools}"
helm_bin="${ATLAS_HELM_BIN:-$helm_tools/helm}"
helm_digest=sha256:1111111111111111111111111111111111111111111111111111111111111111
"$helm_bin" lint deploy/helm/atlas --set "image.digest=$helm_digest"
"$helm_bin" template atlas deploy/helm/atlas --set "image.digest=$helm_digest" > /dev/null
for helm_case in 'credentials.worker=atlas-api-db' 'telemetry.durable=true'; do
  if "$helm_bin" template atlas deploy/helm/atlas --set "image.digest=$helm_digest" --set "$helm_case" >/dev/null 2>&1; then
    echo 'Helm accepted an unsafe credential or producer-gate configuration' >&2;exit 1
  fi
done
if "$helm_bin" template atlas deploy/helm/atlas >/dev/null 2>&1; then
  echo 'Helm accepted a mutable or missing image digest' >&2;exit 1
fi
