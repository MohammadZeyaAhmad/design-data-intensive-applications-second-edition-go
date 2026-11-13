#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/rpc-binaries
GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o artifacts/rpc-binaries/atlas ./cmd/atlas
GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o artifacts/rpc-binaries/router ./cmd/router
python3 - <<'PY'
import hashlib,json,subprocess
from pathlib import Path
d=Path('artifacts/rpc-binaries')
r={'revision':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'dirty':bool(subprocess.check_output(['git','status','--porcelain'])),'go_version':subprocess.check_output(['go','version']).decode().strip(),'sha256':{}}
for name in ['atlas','router']: r['sha256'][name]=hashlib.sha256((d/name).read_bytes()).hexdigest()
(d/'manifest.json').write_text(json.dumps(r,indent=2)+'\n')
PY
