#!/usr/bin/env bash
set -euo pipefail
object_runtime="${ATLAS_OBJECT_RUNTIME:-$(pwd)/artifacts/object-storage-runtime}"
case "$object_runtime" in /*) ;; *) echo 'ATLAS_OBJECT_RUNTIME must be absolute' >&2; exit 2;; esac
object_lock="$(sha256sum tools/object-storage-runtime.lock | cut -d' ' -f1)"
if test -x "$object_runtime/bin/python" && test -f "$object_runtime/.atlas-object-runtime" && test "$(cat "$object_runtime/.atlas-object-runtime")" = "$object_lock"; then
  "$object_runtime/bin/python" -c 'import moto; from moto.server import ThreadedMotoServer; print("Pinned Moto",moto.__version__)'
  exit
fi
if test -d "$object_runtime" && test -n "$(ls -A "$object_runtime")" && ! test -f "$object_runtime/.atlas-object-runtime" && ! test -f "$object_runtime/.atlas-object-runtime.pending"; then
  echo 'Refusing to modify an unmarked existing runtime directory' >&2
  exit 2
fi
mkdir -p "$object_runtime"
printf '%s\n' "$object_lock" > "$object_runtime/.atlas-object-runtime.pending"
python3 -m venv "$object_runtime"
"$object_runtime/bin/python" -m pip install --require-hashes --no-cache-dir -r tools/object-storage-runtime.lock
"$object_runtime/bin/python" -c 'import moto; from moto.server import ThreadedMotoServer; print("Pinned Moto",moto.__version__)'
mv "$object_runtime/.atlas-object-runtime.pending" "$object_runtime/.atlas-object-runtime"
