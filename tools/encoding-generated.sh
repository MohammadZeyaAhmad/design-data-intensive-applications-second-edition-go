#!/usr/bin/env bash
set -euo pipefail
encoding_mode="${1:-check}"
case "$encoding_mode" in generate|check) ;; *) echo 'usage: encoding-generated.sh generate|check' >&2; exit 2;; esac
encoding_tools="$(pwd)/artifacts/encoding-toolchain"
mkdir -p "$encoding_tools"
if test "$(uname -s)/$(uname -m)" != 'Linux/x86_64'; then
  echo 'The pinned portable generation toolchain currently supports Linux amd64.' >&2; exit 1
fi
encoding_protoc_sha=121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278
encoding_thrift_sha=e40529b79c558ee123aadcaa9a333b361d26b8b5cff038a61eb937845a63f6d3
if ! test -f "$encoding_tools/protoc.zip" || ! echo "$encoding_protoc_sha  $encoding_tools/protoc.zip" | sha256sum --check --status; then
  curl -fsSL --max-time 90 https://github.com/protocolbuffers/protobuf/releases/download/v36.2/protoc-36.2-linux-x86_64.zip -o "$encoding_tools/protoc.zip"
fi
echo "$encoding_protoc_sha  $encoding_tools/protoc.zip" | sha256sum --check --status
unzip -oq "$encoding_tools/protoc.zip" -d "$encoding_tools/protoc"
if ! test -f "$encoding_tools/thrift.deb" || ! echo "$encoding_thrift_sha  $encoding_tools/thrift.deb" | sha256sum --check --status; then
  curl -fsSL --max-time 90 https://deb.debian.org/debian/pool/main/t/thrift/thrift-compiler_0.24.0-3_amd64.deb -o "$encoding_tools/thrift.deb"
fi
echo "$encoding_thrift_sha  $encoding_tools/thrift.deb" | sha256sum --check --status
dpkg-deb -x "$encoding_tools/thrift.deb" "$encoding_tools/thrift"
test "$("$encoding_tools/protoc/bin/protoc" --version)" = 'libprotoc 36.2'
test "$("$encoding_tools/thrift/usr/bin/thrift" -version)" = 'Thrift version 0.24.0'
GOBIN="$encoding_tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
export PATH="$encoding_tools/bin:$PATH"
encoding_generated=labs/ch05/encoding/generated
mkdir -p "$encoding_generated/protov1" "$encoding_generated/protov2"
for encoding_version in v1 v2; do
  "$encoding_tools/protoc/bin/protoc" -I contracts/proto --go_out="$encoding_generated" --go_opt=module=github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated "contracts/proto/encoding/$encoding_version/snapshot.proto"
  "$encoding_tools/thrift/usr/bin/thrift" --gen go:skip_remote -out "$encoding_generated" "contracts/thrift/encoding/$encoding_version/snapshot.thrift"
done
"$encoding_tools/thrift/usr/bin/thrift" --gen go:skip_remote -out "$encoding_generated" contracts/thrift/encoding/wide/snapshot.thrift
mkdir -p "$encoding_generated/schema"
cp api/openapi/encoding/v1.schema.json "$encoding_generated/schema/v1.schema.json"
cp api/openapi/encoding/v2.schema.json "$encoding_generated/schema/v2.schema.json"
# Prove that the real compiler rejects reuse of a retired number and name.
cat > "$encoding_tools/reserved.proto" <<'PROTO'
syntax = "proto3";
message InvalidReuse {
  reserved 5;
  reserved "legacy_note";
  string legacy_note = 5;
}
PROTO
if "$encoding_tools/protoc/bin/protoc" -I "$encoding_tools" --descriptor_set_out="$encoding_tools/invalid.pb" "$encoding_tools/reserved.proto" > "$encoding_tools/reserved.log" 2>&1; then
  echo 'protoc accepted a reserved field number/name' >&2; exit 1
fi
grep -q 'reserved number 5' "$encoding_tools/reserved.log"
grep -Fq 'Field name "legacy_note" is reserved' "$encoding_tools/reserved.log"
gofmt -w "$encoding_generated"
if test "$encoding_mode" = check; then
  git diff --exit-code -- "$encoding_generated"
  test -z "$(git ls-files --others --exclude-standard "$encoding_generated")"
fi
