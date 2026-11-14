#!/usr/bin/env bash
set -euo pipefail
release_output="${ATLAS_RELEASE_DIR:-artifacts/release}"
release_arch="${ATLAS_RELEASE_ARCHES:-amd64 arm64}"
release_revision="$(git rev-parse HEAD)"
release_clean="${ATLAS_RELEASE_REQUIRE_CLEAN:-false}"
go mod verify
for release_target in $release_arch; do
  case "$release_target" in amd64|arm64) ;; *) echo 'unsupported release architecture' >&2;exit 2;; esac
  mkdir -p "$release_output/linux-$release_target"
  GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH="$release_target" go build -trimpath -buildvcs=true -ldflags='-s -w' -o "$release_output/linux-$release_target/atlas" ./cmd/atlas
  go run ./cmd/releasecheck -binary "$release_output/linux-$release_target/atlas" -output "$release_output/linux-$release_target" -revision "$release_revision" -require-clean="$release_clean"
  bash tools/release-negative-check.sh "$release_output/linux-$release_target"
  tar -czf "$release_output/atlas-linux-$release_target.tar.gz" -C "$release_output/linux-$release_target" atlas sbom.cdx.json manifest.json SHA256SUMS
 done
