#!/usr/bin/env bash
set -euo pipefail
rdf_runtime=artifacts/rdf-runtime
mkdir -p "$rdf_runtime"
rdf_sha512=6892853e217afde119f9237ad424033c4d309ec7129bed344d0199ad6e233d9f166610e2040abd42b739ecbe120c4638f2b69ecf1576872669a3403c3391f691
if ! test -f "$rdf_runtime/fuseki-server.jar" || ! echo "$rdf_sha512  $rdf_runtime/fuseki-server.jar" | sha512sum --check --status; then
  rdf_download="$(mktemp "$rdf_runtime/.download-XXXXXX")"
  trap 'rm -f "$rdf_download"' EXIT
  curl -fsSL --max-time 90 https://repo.maven.apache.org/maven2/org/apache/jena/jena-fuseki-server/5.6.0/jena-fuseki-server-5.6.0.jar -o "$rdf_download"
  echo "$rdf_sha512  $rdf_download" | sha512sum --check --status
  mv "$rdf_download" "$rdf_runtime/fuseki-server.jar"
fi
cp deploy/compose/rdf/entrypoint.sh "$rdf_runtime/entrypoint.sh"
(cd labs && CGO_ENABLED=0 GOOS=linux go build -trimpath -o ../artifacts/rdf-runtime/health ./cmd/rdf-health)
