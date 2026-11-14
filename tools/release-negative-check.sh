#!/usr/bin/env bash
set -euo pipefail
release_bundle="${1:?bundle directory required}"
release_test="$(mktemp -d)"
trap 'rm -rf "$release_test"' EXIT
python3 - "$release_bundle" "$release_test" <<'PY'
import hashlib,json,os,shutil,sys
from pathlib import Path
source,target=Path(sys.argv[1]).resolve(),Path(sys.argv[2])
os.symlink(source/'atlas',target/'atlas')
for name in ['sbom.cdx.json','manifest.json']:
 shutil.copyfile(source/name,target/name)
sbom=json.loads((target/'sbom.cdx.json').read_text())
assert sbom['components']
sbom['components'][0]['version']='v0.0.0-forged'
(target/'sbom.cdx.json').write_text(json.dumps(sbom))
manifest=json.loads((target/'manifest.json').read_text())
manifest['sbom_sha256']=hashlib.sha256((target/'sbom.cdx.json').read_bytes()).hexdigest()
(target/'manifest.json').write_text(json.dumps(manifest))
(target/'SHA256SUMS').write_text(''.join(hashlib.sha256((target/name).read_bytes()).hexdigest()+'  '+name+'\n' for name in ['atlas','manifest.json','sbom.cdx.json']))
PY
if go run ./cmd/releasecheck -verify -output "$release_test" > "$release_test/result.log" 2>&1; then
  echo 'A checksummed SBOM inconsistent with its binary was accepted' >&2;exit 1
fi
if ! python3 -c 'import pathlib,sys;sys.exit(0 if "SBOM does not describe the binary dependencies" in pathlib.Path(sys.argv[1]).read_text() else 1)' "$release_test/result.log"; then
  echo 'Negative release check did not reach the intended semantic rejection' >&2;exit 1
fi
