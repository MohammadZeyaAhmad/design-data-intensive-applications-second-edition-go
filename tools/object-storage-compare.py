"""Compare actual local and S3-emulator reports, without inferring cloud SLA."""
import json
from pathlib import Path

root = Path("artifacts/object-storage")
local = json.loads((root / "file.json").read_text())
emulator = json.loads((root / "emulator.json").read_text())
assert local["profile"] == "file" and emulator["profile"] == "emulator"
for section, fields in {
    "workload": ["final_generation", "final_rows", "final_state_sha256"],
    "native": ["final_generation", "final_rows", "final_state_sha256"],
}.items():
    for field in fields:
        assert local[section][field] == emulator[section][field], (section, field)
for report in [local, emulator]:
    assert report["contract"]["creator_wins"] == 1
    assert report["contract"]["replacement_wins"] == 1
    assert report["native"]["concurrent_writer_wins"] == 1
    assert report["native"]["concurrent_writer_conflicts"] == 1
    assert len(report["native"]["crashes"]) == 4
    assert report["workload"]["cache_cost"]["warm_gets"] == 1
assert emulator["owned_s3_prefix_cleanup_verified"]
print("Local and S3-emulator warehouse states agree through publication, compaction and process recovery")
