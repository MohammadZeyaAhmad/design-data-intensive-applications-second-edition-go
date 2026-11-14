#!/usr/bin/env bash
set -euo pipefail
mkdir -p artifacts/replication-binaries
chmod 0755 artifacts/replication-binaries
GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o artifacts/replication-binaries/atlas ./cmd/atlas
(cd labs && GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o ../artifacts/replication-binaries/replication-lab ./cmd/replication-lab)
chmod 0755 artifacts/replication-binaries/atlas artifacts/replication-binaries/replication-lab
python3 - <<'PY'
import hashlib,json,subprocess
from pathlib import Path
d=Path('artifacts/replication-binaries')
r={'revision':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'dirty':bool(subprocess.check_output(['git','status','--porcelain'])),'go_version':subprocess.check_output(['go','version']).decode().strip(),'sha256':{}}
for name in ['atlas','replication-lab']: r['sha256'][name]=hashlib.sha256((d/name).read_bytes()).hexdigest()
(d/'manifest.json').write_text(json.dumps(r,indent=2)+'\n')
PY
chmod 0644 artifacts/replication-binaries/manifest.json
