"""Read-only validation for Zhou Jiahao RYW runs produced by Zhao Yikun's fault lab."""
from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def validate(directory: Path):
    manifest = json.loads((directory / "manifest.json").read_text())
    assert manifest["experiment"] == "ryw"
    assert manifest["schema_version"] == 2
    assert manifest["fault_framework_author"] == "Zhao Yikun"

    for name, digest in manifest["artifact_sha256"].items():
        target = (directory / name).resolve()
        if not target.is_relative_to(directory.resolve()):
            raise ValueError("Artifact escapes run directory")
        assert hashlib.sha256(target.read_bytes()).hexdigest() == digest, name
    with tarfile.open(directory / "source.tar.gz") as archive:
        for name, digest in manifest["source_sha256"].items():
            extracted = archive.extractfile(name)
            assert extracted is not None, name
            assert hashlib.sha256(extracted.read()).hexdigest() == digest, name

    rows = [json.loads(line) for line in (directory / "operations.jsonl").read_text().splitlines()]
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["trial_id"]].append(row)
    results = [row for row in rows if row["record_type"] == "trial_result"]
    assert len(results) == manifest["completed_trials"]
    if manifest["status"] == "completed":
        assert len(results) == manifest["trials_per_config"] * len(manifest["configs"])
    configs = {config["config_id"]: config for config in manifest["configs"]}

    for result in results:
        assert result["recovery_completed"]
        convergence = json.loads((
            directory / "evidence" / result["trial_id"] / "convergence.json"
        ).read_text())
        nodes = convergence["nodes"]
        assert len(nodes) == 3 and convergence["stable_seconds"] >= 1
        assert all(node["document"] == nodes[0]["document"] for node in nodes)
        assert sorted(node["role"] for node in nodes) == ["PRIMARY", "SECONDARY", "SECONDARY"]

        operations = {
            row["operation"]: row
            for row in grouped[result["trial_id"]]
            if row["record_type"] == "operation"
        }
        write = operations.get("predecessor_write")
        read = operations.get("successor_read")
        assert write is not None
        config = configs[result["config_id"]]
        if write["status"] == "success":
            assert write["attempts"][-1]["wire_write_concern"]["w"] == config["write_concern"]
            if manifest["diagnostic_journal"]:
                assert write["attempts"][-1]["wire_write_concern"]["j"] is True
            assert read is not None
            assert datetime.fromisoformat(write["ended_at"].replace("Z", "+00:00")) <= datetime.fromisoformat(read["started_at"].replace("Z", "+00:00"))
        else:
            assert read is None
            assert result["classification"] == "inconclusive"
            assert result["is_violation"] is None

        for operation in [row for row in (write, read) if row is not None]:
            if config["causal_session"]:
                assert operation["session_id"] and operation["explicit_causal_session"]
            else:
                assert operation["session_id"] is None
        if read and config["causal_session"]:
            assert write["session_id"] == read["session_id"]
        if read and read["status"] == "success":
            expected_violation = read["returned_version"] is None or read["returned_version"] < 1
            assert result["is_violation"] is expected_violation
            assert result["classification"] == ("violation" if expected_violation else "no_violation")
        elif read:
            assert result["classification"] == "inconclusive"
            assert result["is_violation"] is None

    return {
        "experiment_id": manifest["experiment_id"],
        "status": manifest["status"],
        "trials_checked": len(results),
        "checksums": "pass",
        "source_snapshot": "pass",
        "write_then_read_order": "pass",
        "causal_sessions": "pass",
        "ryw_classification": "pass",
        "recovery": "pass",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(args.directory), indent=2))


if __name__ == "__main__":
    main()
