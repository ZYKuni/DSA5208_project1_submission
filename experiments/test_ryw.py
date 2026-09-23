"""Zhou Jiahao's RYW normal-scenario pilot using result schema v1."""

import argparse
import json
import os
import platform
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pymongo
from jsonschema import Draft202012Validator, FormatChecker
from pymongo import MongoClient

# Support both `python -m experiments...` and historical direct scripts.
import sys
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.common import (
    NodeRecorder,
    config_snapshot,
    record_operation,
    utc_now,
)
from experiments.configs import get_config, session_scope


ROOT = Path(__file__).resolve().parents[1]


def run_trial(client, collection, recorder, config, environment,
              run_id, trial_id):
    document_id = f"ryw-{run_id}-{trial_id}"
    operations = []

    record = {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "trial_id": trial_id,
        "model": "RYW",
        "config": config_snapshot(config),
        "scenario": {"name": "normal", "fault": None},
        "status": "success",
        "violation": None,
        "classification_reason": "Trial started.",
        "started_at": utc_now(),
        "completed_at": None,
        "latency_ms": 0.0,
        "operations": operations,
        "model_data": {
            "write_version": 1,
            "read_version": None,
        },
        "environment": environment,
        "error": None,
        # No rollback investigation is performed in this experiment.
        "rollback_observed": None,
    }

    started = time.perf_counter()

    try:
        with session_scope(client, config) as session:
            record_operation(
                operations,
                recorder,
                stage="predecessor_write",
                operation="write",
                document_id=document_id,
                requested_version=1,
                client_seq=1,
                action=lambda: collection.insert_one(
                    {"_id": document_id, "version": 1},
                    session=session,
                ),
            )

            document = record_operation(
                operations,
                recorder,
                stage="successor_read",
                operation="read",
                document_id=document_id,
                action=lambda: collection.find_one(
                    {"_id": document_id},
                    session=session,
                    max_time_ms=5000,
                ),
            )

            version = None if document is None else document["version"]
            record["model_data"]["read_version"] = version
            record["violation"] = version is None or version < 1

            if version is None:
                reason = "Acknowledged insert followed by a missing document."
            elif version < 1:
                reason = "Read version is older than the acknowledged write."
            else:
                reason = "Read includes the acknowledged write version."

            record["classification_reason"] = reason

    except Exception:
        # Expected operation failures have already been recorded.
        # Errors outside an operation must not be silently hidden.
        if not operations or operations[-1]["status"] == "success":
            raise

        failed = operations[-1]
        record["status"] = failed["status"]
        record["violation"] = None
        record["error"] = failed["error"]
        record["classification_reason"] = (
            f"Not evaluable: {failed['stage']} ended with "
            f"{failed['status']}."
        )

    record["latency_ms"] = round(
        (time.perf_counter() - started) * 1000, 3
    )
    record["completed_at"] = utc_now()
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", choices=["C1", "C2", "C3", "C4"], required=True
    )
    parser.add_argument("--trials", type=int, default=3)
    args = parser.parse_args()

    if args.trials < 1:
        parser.error("--trials must be positive")

    commit = os.environ.get("GIT_COMMIT", "")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        parser.error("Pass the checked-out commit through GIT_COMMIT")

    schema = json.loads(
        (ROOT / "report/result-schema-v1.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(
        schema, format_checker=FormatChecker()
    )

    config = get_config(args.config)
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid4().hex[:8]
    )

    # Pilot only: source changes have not yet been committed.
    output_dir = ROOT / "results/pilot/ryw/normal/debug"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = (
        output_dir / f"ryw-{config.config_id}-normal-{run_id}.jsonl"
    )

    recorder = NodeRecorder()
    records = []

    with MongoClient(
        os.environ["MONGODB_URI"],
        serverSelectionTimeoutMS=5000,
        timeoutMS=10000,
        retryReads=False,
        retryWrites=False,
        event_listeners=[recorder],
    ) as client:
        client.admin.command("ping")

        environment = {
            "git_commit": commit,
            "python_version": platform.python_version(),
            "pymongo_version": pymongo.version,
            "mongodb_version": client.server_info()["version"],
        }

        collection = client["member_b_experiments"].get_collection(
            "ryw",
            **config.collection_options(),
        )

        with output_path.open("x", encoding="utf-8") as output:
            for trial_id in range(1, args.trials + 1):
                record = run_trial(
                    client, collection, recorder, config,
                    environment, run_id, trial_id,
                )

                # Reject a malformed record before writing it.
                validator.validate(record)

                output.write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )
                output.flush()
                records.append(record)

    valid = sum(r["status"] == "success" for r in records)
    violations = sum(r["violation"] is True for r in records)

    print(f"Config: {config.config_id}")
    print(f"Trials: {len(records)}")
    print(f"Evaluable trials: {valid}")
    print(f"Violations: {violations}")

    for status in ("timeout", "unavailable", "error", "invalid_trial"):
        count = sum(r["status"] == status for r in records)
        print(f"{status}: {count}")

    print(f"PASS: {len(records)} records passed schema validation")
    print(f"Pilot log: {output_path}")


if __name__ == "__main__":
    main()
