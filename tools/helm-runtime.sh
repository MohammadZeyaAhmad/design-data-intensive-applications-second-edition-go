#!/usr/bin/env bash
set -euo pipefail
helm_tools="${ATLAS_OPERATIONS_TOOLS:-${TMPDIR:-/tmp}/atlas-operations-tools}"
mkdir -p "$helm_tools"
helm_tools="$(cd "$helm_tools" && pwd)"
if ! test -x "$helm_tools/helm"; then
  helm_build="$(mktemp -d)"
  trap 'rm -rf "$helm_build"' EXIT
  cat > "$helm_build/go.mod" <<'MOD'
module atlas.tools/helm

go 1.27.0

require (
 helm.sh/helm/v3 v3.17.1
 github.com/klauspost/compress v1.18.7
)
MOD
  (cd "$helm_build" && GOWORK=off GOMODCACHE="${ATLAS_HELM_MODCACHE:-$helm_tools/modules}" go build -p=2 -mod=mod -o "$helm_tools/helm" helm.sh/helm/v3/cmd/helm)
fi
"$helm_tools/helm" version --short
