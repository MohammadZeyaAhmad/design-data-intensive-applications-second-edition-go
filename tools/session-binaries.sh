#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/session-binaries
chmod 0755 artifacts/session-binaries
GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o artifacts/session-binaries/atlas ./cmd/atlas
(cd labs && GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o ../artifacts/session-binaries/session-lab ./cmd/session-lab)
chmod 0755 artifacts/session-binaries/atlas artifacts/session-binaries/session-lab
python3 - <<'PY'
import hashlib,json,subprocess
from pathlib import Path
d=Path('artifacts/session-binaries')
r={'revision':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'dirty':bool(subprocess.check_output(['git','status','--porcelain'])),'go_version':subprocess.check_output(['go','version']).decode().strip(),'sha256':{n:hashlib.sha256((d/n).read_bytes()).hexdigest() for n in ['atlas','session-lab']}}
(d/'manifest.json').write_text(json.dumps(r,indent=2)+'\n')
PY
