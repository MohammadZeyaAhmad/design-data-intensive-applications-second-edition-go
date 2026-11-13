#!/usr/bin/env bash
set -euo pipefail
actors_binary="$(pwd)/artifacts/actors-binaries"
mkdir -p "$actors_binary"
(cd labs && GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o "$actors_binary/actor-node" ./cmd/actor-node)
python3 - <<'PY'
import hashlib,json,subprocess
from pathlib import Path
d=Path('artifacts/actors-binaries')
r={'revision':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'dirty':bool(subprocess.check_output(['git','status','--porcelain'])),'go_version':subprocess.check_output(['go','version']).decode().strip(),'sha256':hashlib.sha256((d/'actor-node').read_bytes()).hexdigest()}
(d/'manifest.json').write_text(json.dumps(r,indent=2)+'\n')
PY
