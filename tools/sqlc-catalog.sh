#!/usr/bin/env bash
set -euo pipefail
mode="${1:-check}"
case "$mode" in generate|check) ;; *) echo 'usage: sqlc-catalog.sh generate|check' >&2; exit 2;; esac
if command -v sqlc >/dev/null 2>&1 && test "$(sqlc version)" = 'v1.30.0'; then
  sqlc_command="$(command -v sqlc)"
else
  if test "$(uname -s)/$(uname -m)" != 'Linux/x86_64'; then
    echo 'Install sqlc v1.30.0 on PATH for this platform.' >&2; exit 1
  fi
  sqlc_workdir="$(mktemp -d)"
  trap 'rm -rf "$sqlc_workdir"' EXIT
  curl -fsSL --max-time 90 https://github.com/sqlc-dev/sqlc/releases/download/v1.30.0/sqlc_1.30.0_linux_amd64.tar.gz -o "$sqlc_workdir/sqlc.tar.gz"
  (cd "$sqlc_workdir" && echo '468aecee071bfe55e97fcbcac52ea0208eeca444f67736f3b8f0f3d6a106132e  sqlc.tar.gz' | sha256sum --check --status)
  tar -xzf "$sqlc_workdir/sqlc.tar.gz" -C "$sqlc_workdir" sqlc
  sqlc_command="$sqlc_workdir/sqlc"
fi
"$sqlc_command" generate -f sqlc.yaml
if test "$mode" = check; then
  git diff --exit-code -- internal/catalog/adapters/postgres/detailsqlc
fi
