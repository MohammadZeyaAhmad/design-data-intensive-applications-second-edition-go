#!/usr/bin/env bash
set -euo pipefail
actors_contract_mode="${1:-check}"
case "$actors_contract_mode" in generate|check) ;; *) echo 'usage: actors-contracts.sh generate|check' >&2; exit 2;; esac
cp api/asyncapi/actors/asyncapi.json labs/ch05/actors/contracts/asyncapi.json
if test "$actors_contract_mode" = check; then
  git diff --exit-code -- labs/ch05/actors/contracts/asyncapi.json
  test -z "$(git ls-files --others --exclude-standard labs/ch05/actors/contracts/asyncapi.json)"
fi
