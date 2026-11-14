#!/usr/bin/env bash
set -euo pipefail
ops_tools="${ATLAS_OPERATIONS_TOOLS:-${TMPDIR:-/tmp}/atlas-operations-tools}"
ops_version=3.2.1
ops_sha=a622e3007c9109a7f470e1433cbd29bf392596715cf7eea8b81b37fa9d26b7be
test "$(uname -s)/$(uname -m)" = Linux/x86_64
mkdir -p "$ops_tools"
if ! test -x "$ops_tools/prometheus" || ! test -x "$ops_tools/promtool"; then
  curl -fsSL --max-time 120 "https://github.com/prometheus/prometheus/releases/download/v${ops_version}/prometheus-${ops_version}.linux-amd64.tar.gz" -o "$ops_tools/prometheus.tar.gz"
  echo "$ops_sha  $ops_tools/prometheus.tar.gz" | sha256sum --check --status
  tar -xzf "$ops_tools/prometheus.tar.gz" -C "$ops_tools" --strip-components=1 "prometheus-${ops_version}.linux-amd64/prometheus" "prometheus-${ops_version}.linux-amd64/promtool"
fi
"$ops_tools/promtool" --version
