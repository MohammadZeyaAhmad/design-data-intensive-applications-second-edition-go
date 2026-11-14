#!/usr/bin/env bash
set -euo pipefail
tf_tools="${ATLAS_OPERATIONS_TOOLS:-${TMPDIR:-/tmp}/atlas-operations-tools}"
mkdir -p "$tf_tools"
if ! test -x "$tf_tools/terraform"; then
  curl -fsSL --max-time 90 https://releases.hashicorp.com/terraform/1.11.0/terraform_1.11.0_linux_amd64.zip -o "$tf_tools/terraform.zip"
  echo "069e531fd4651b9b510adbd7e27dd648b88d66d5f369a2059aadbb4baaead1c1  $tf_tools/terraform.zip" | sha256sum --check --status
  unzip -oq "$tf_tools/terraform.zip" terraform -d "$tf_tools"
fi
"$tf_tools/terraform" version -json
