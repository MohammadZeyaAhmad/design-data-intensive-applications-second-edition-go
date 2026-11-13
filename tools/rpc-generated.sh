#!/usr/bin/env bash
set -euo pipefail
rpc_mode="${1:-check}"
case "$rpc_mode" in generate|check) ;; *) echo 'usage: rpc-generated.sh generate|check' >&2; exit 2;; esac
rpc_tools="$(pwd)/artifacts/encoding-toolchain"
mkdir -p "$rpc_tools"
test "$(uname -s)/$(uname -m)" = Linux/x86_64
rpc_sha=121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278
if ! test -f "$rpc_tools/protoc.zip" || ! echo "$rpc_sha  $rpc_tools/protoc.zip" | sha256sum --check --status; then
  curl -fsSL --max-time 90 https://github.com/protocolbuffers/protobuf/releases/download/v36.2/protoc-36.2-linux-x86_64.zip -o "$rpc_tools/protoc.zip"
fi
echo "$rpc_sha  $rpc_tools/protoc.zip" | sha256sum --check --status
unzip -oq "$rpc_tools/protoc.zip" -d "$rpc_tools/protoc"
test "$("$rpc_tools/protoc/bin/protoc" --version)" = 'libprotoc 36.2'
GOBIN="$rpc_tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
GOBIN="$rpc_tools/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
export PATH="$rpc_tools/bin:$PATH"
rpc_root=github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go
rpc_source=contracts/proto/inventory/v1/inventory.proto
"$rpc_tools/protoc/bin/protoc" -I contracts/proto --go_out=. --go_opt="module=$rpc_root" --go-grpc_out=. --go-grpc_opt="module=$rpc_root" "$rpc_source"
mkdir -p labs/ch05/rpc/wire
rpc_lab="$rpc_root/labs/ch05/rpc/wire"
"$rpc_tools/protoc/bin/protoc" -I contracts/proto --go_out=labs/ch05/rpc/wire --go_opt=paths=source_relative --go_opt="Minventory/v1/inventory.proto=$rpc_lab/inventory/v1" --go-grpc_out=labs/ch05/rpc/wire --go-grpc_opt=paths=source_relative --go-grpc_opt="Minventory/v1/inventory.proto=$rpc_lab/inventory/v1" "$rpc_source"
"$rpc_tools/protoc/bin/protoc" -I contracts/proto --descriptor_set_out=labs/ch05/rpc/wire/legacy.pb contracts/proto/inventory/legacy/inventory.proto
cp api/openapi/inventory/openapi.json labs/ch05/rpc/wire/openapi.json
gofmt -w api/grpc/inventory/v1 labs/ch05/rpc/wire
if test "$rpc_mode" = check; then
  git diff --exit-code -- api/grpc/inventory/v1 labs/ch05/rpc/wire
  test -z "$(git ls-files --others --exclude-standard api/grpc/inventory/v1 labs/ch05/rpc/wire)"
fi
