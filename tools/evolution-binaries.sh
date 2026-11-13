#!/usr/bin/env bash
set -euo pipefail
evolution_root="$(pwd)"
evolution_legacy=393f703d7e097f96f09847b8196ead2c4429f260
evolution_bin="$evolution_root/artifacts/evolution-binaries"
if ! git cat-file -e "$evolution_legacy^{commit}" 2>/dev/null; then
  echo 'The rolling drill needs historical commit 393f703; fetch the full repository history first.' >&2
  exit 1
fi
mkdir -p "$evolution_bin"
evolution_source="$(mktemp -d)"
trap 'rm -rf "$evolution_source"' EXIT
git archive "$evolution_legacy" | tar -x -C "$evolution_source"
(cd "$evolution_source" && GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o "$evolution_bin/legacy-atlas" ./cmd/atlas)
GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o "$evolution_bin/current-atlas" ./cmd/atlas
GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o "$evolution_bin/schema" ./cmd/schema
python3 - <<'PY'
import hashlib,json,subprocess
from pathlib import Path
directory=Path('artifacts/evolution-binaries')
manifest={'legacy_revision':'393f703d7e097f96f09847b8196ead2c4429f260','current_revision':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'current_dirty':bool(subprocess.check_output(['git','status','--porcelain'])),'go_version':subprocess.check_output(['go','version']).decode().strip(),'binaries':{}}
for name in ['legacy-atlas','current-atlas','schema']:
 manifest['binaries'][name]=hashlib.sha256((directory/name).read_bytes()).hexdigest()
(directory/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
PY
