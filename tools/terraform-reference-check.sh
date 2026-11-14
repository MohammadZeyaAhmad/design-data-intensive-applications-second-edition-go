#!/usr/bin/env bash
set -euo pipefail
tf_tools="${ATLAS_OPERATIONS_TOOLS:-${TMPDIR:-/tmp}/atlas-operations-tools}"
tf_bin="$tf_tools/terraform"
tf_work="$(mktemp -d)"
trap 'rm -rf "$tf_work"' EXIT
cp deploy/terraform/aws-reference/*.tf deploy/terraform/aws-reference/.terraform.lock.hcl "$tf_work/"
# The checked-in zh values authenticate the downloaded archives. Init adds the
# derived h1 hash needed to verify the unpacked package, only in this fixture.
"$tf_bin" -chdir="$tf_work" init -backend=false -input=false
"$tf_bin" -chdir="$tf_work" validate
