#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/object-storage-binaries
(cd labs && GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o ../artifacts/object-storage-binaries/object-lab ./cmd/object-lab)
chmod 0755 artifacts/object-storage-binaries/object-lab
python3 - <<'PY'
import hashlib,json,subprocess
from pathlib import Path
d=Path('artifacts/object-storage-binaries')
r={'revision':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'dirty':bool(subprocess.check_output(['git','status','--porcelain'])),'go_version':subprocess.check_output(['go','version']).decode().strip(),'sha256':hashlib.sha256((d/'object-lab').read_bytes()).hexdigest()}
(d/'manifest.json').write_text(json.dumps(r,indent=2)+'\n')
PY
