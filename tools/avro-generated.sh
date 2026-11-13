#!/usr/bin/env bash
set -euo pipefail
avro_mode="${1:-check}"
case "$avro_mode" in generate|check) ;; *) echo 'usage: avro-generated.sh generate|check' >&2; exit 2;; esac
avro_generated=labs/ch05/avro/generated
mkdir -p "$avro_generated/schema"
for avro_version in v1 v2 v3; do
  mkdir -p "$avro_generated/$avro_version"
  (cd labs && go run ./cmd/avro-gen -schema "../contracts/avro/order/$avro_version.avsc" -package "avro$avro_version" -output "ch05/avro/generated/$avro_version/types.go")
  cp "contracts/avro/order/$avro_version.avsc" "$avro_generated/schema/$avro_version.avsc"
done
if test "$avro_mode" = check; then
  git diff --exit-code -- "$avro_generated"
  test -z "$(git ls-files --others --exclude-standard "$avro_generated")"
fi
