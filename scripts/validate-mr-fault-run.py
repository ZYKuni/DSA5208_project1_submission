#!/usr/bin/env python3
"""Validate one mr-fault-between-reads-v1 evidence directory."""
from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from experiments.run_mr_faults import classify


def load_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_source(directory, manifest):
    with tarfile.open(directory / "source.tar.gz", "r:gz") as archive:
        members = {member.name: member for member in archive.getmembers() if member.isfile()}
        for name, expected in manifest["source_sha256"].items():
            require(name in members, f"Source archive missing {name}")
            actual = hashlib.sha256(archive.extractfile(members[name]).read()).hexdigest()
            require(actual == expected, f"Source hash mismatch for {name}")


def validate(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    require(manifest["protocol"] == "mr-fault-between-reads-v1", "Wrong protocol")
    require(manifest["experiment"] == "mr", "Wrong experiment")
    require(manifest["scenario"] in ("secondary-stop", "primary-crash"), "Wrong scenario")
    require(manifest["status"] == "completed", "Run did not complete")
    validate_source(directory, manifest)

    rows = load_jsonl(directory / "operations.jsonl")
    operations = [row for row in rows if row["record_type"] == "operation"]
    trials = [row for row in rows if row["record_type"] == "trial_result"]
    expected_count = manifest["trials_per_config"] * len(manifest["configs"])
    require(len(trials) == expected_count == manifest["completed_trials"], "Trial count mismatch")
    events = load_jsonl(directory / "events.jsonl")

    for trial in trials:
        trial_id = trial["trial_id"]
        own = {row["operation"]: row for row in operations if row["trial_id"] == trial_id}
        for name in ("initialize", "independent_writer_update", "first_read", "second_read",
                     "primary_audit"):
            require(name in own, f"{trial_id}: missing {name}")
        require(own["initialize"]["status"] == "success", f"{trial_id}: setup failed")
        require(own["initialize"]["effective_options"]["write_concern"] == 3,
                f"{trial_id}: setup was not w=3")
        require(own["first_read"]["monotonic_ended_ns"] <= own["second_read"]["monotonic_started_ns"],
                f"{trial_id}: reads are not sequential")
        requests = [event for event in events if event.get("event") == "controller_request"
                    and event.get("trial_id") == trial_id and event.get("action") == "crash"]
        require(len(requests) == 1, f"{trial_id}: expected one crash request")
        crash = requests[0]
        require(own["first_read"]["monotonic_ended_ns"] <= crash["monotonic_ns"]
                <= own["second_read"]["monotonic_started_ns"],
                f"{trial_id}: fault is not between reads")
        if manifest["scenario"] == "secondary-stop":
            first_node = own["first_read"]["target_node"].split(":")[0]
            require(crash["node"] == first_node, f"{trial_id}: stopped the wrong Secondary")
            require(first_node != trial["primary_before"], f"{trial_id}: first read was not a Secondary")
        else:
            require(crash["node"] == trial["primary_before"], f"{trial_id}: wrong Primary target")
            require(trial["primary_after"] and trial["primary_after"] != trial["primary_before"],
                    f"{trial_id}: no new Primary was observed")
        if trial["causal_session"]:
            require(own["first_read"]["explicit_causal_session"], f"{trial_id}: no causal session")
            require(own["first_read"]["session_id"] == own["second_read"]["session_id"],
                    f"{trial_id}: C4 session changed between reads")
        expected = classify(own["first_read"], own["second_read"])
        require((trial["classification"], trial["is_violation"]) == expected[:2],
                f"{trial_id}: classification mismatch")
        require(trial["recovery_completed"], f"{trial_id}: recovery incomplete")

    for name, expected in manifest["artifact_sha256"].items():
        path = directory / name
        require(path.is_file(), f"Artifact missing: {name}")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == expected,
                f"Artifact hash mismatch: {name}")
    return {"status": "pass", "experiment_id": manifest["experiment_id"],
            "scenario": manifest["scenario"], "trials": len(trials),
            "evaluable": sum(row["is_violation"] is not None for row in trials),
            "violations": sum(row["is_violation"] is True for row in trials)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(args.directory), indent=2))


if __name__ == "__main__":
    main()
