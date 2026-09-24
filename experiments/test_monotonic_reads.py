"""Zhou Jiahao's MR normal-scenario pilot using result schema v1."""

import argparse
import json
import os
import platform
import re
import time
from pathlib import Path

import pymongo
from jsonschema import Draft202012Validator, FormatChecker
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

# Support both `python -m experiments...` and historical direct scripts.
import sys
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.common import (
    NodeRecorder,
    config_snapshot,
    record_operation,
    schema_v1_run_id,
    utc_now,
)
from experiments.configs import get_config, session_scope


ROOT = Path(__file__).resolve().parents[1]


def run_trial(reader, baseline_collection, writer_collection,
              reader_collection, writer_recorder, reader_recorder,
              config, environment, run_id, trial_id):
    document_id = f"mr-{run_id}-{trial_id}"
    operations = []

    record = {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "trial_id": trial_id,
        "model": "MR",
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
            "first_read_version": None,
            "second_read_version": None,
        },
        "environment": environment,
        "error": None,
        "rollback_observed": None,
    }

    started = time.perf_counter()

    try:
        # Setup exception: w=3 establishes version 1 on all three nodes.
        # This is preparation, not the tested read configuration.
        record_operation(
            operations,
            writer_recorder,
            stage="setup_baseline_w3",
            operation="write",
            document_id=document_id,
            requested_version=1,
            client_seq=1,
            action=lambda: baseline_collection.insert_one(
                {"_id": document_id, "version": 1}
            ),
        )

        # Independent writer: no causal session is shared with the reader.
        def advance_version():
            result = writer_collection.update_one(
                {"_id": document_id},
                {"$set": {"version": 2}},
            )
            if result.matched_count != 1:
                raise RuntimeError("Baseline document was not found")
            return result

        record_operation(
            operations,
            writer_recorder,
            stage="independent_writer_update",
            operation="write",
            document_id=document_id,
            requested_version=2,
            client_seq=2,
            action=advance_version,
        )

        # The same reader session spans BOTH reads.
        with session_scope(reader, config) as session:
            for stage, field in (
                ("first_read", "first_read_version"),
                ("second_read", "second_read_version"),
            ):
                document = record_operation(
                    operations,
                    reader_recorder,
                    stage=stage,
                    operation="read",
                    document_id=document_id,
                    action=lambda: reader_collection.find_one(
                        {"_id": document_id},
                        session=session,
                        max_time_ms=5000,
                    ),
                )

                version = None if document is None else document["version"]
                record["model_data"][field] = version

                if stage == "first_read" and version is None:
                    record["status"] = "invalid_trial"
                    record["classification_reason"] = (
                        "First read found no baseline document; "
                        "no observed version exists for comparison."
                    )
                    break

        if record["status"] == "success":
            first = record["model_data"]["first_read_version"]
            second = record["model_data"]["second_read_version"]

            record["violation"] = second is None or second < first
            record["classification_reason"] = (
                "Previously observed document disappeared or version decreased."
                if record["violation"]
                else "Second read did not regress from the first read."
            )

    except Exception:
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
    parser.add_argument(
        "--run-id",
        help="Stable identifier used in the output filename; defaults to a timestamp and UUID",
    )
    args = parser.parse_args()

    if args.trials < 1:
        parser.error("--trials must be positive")

    commit = os.environ.get("GIT_COMMIT", "")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        parser.error("Pass the checked-out commit through GIT_COMMIT")

    schema = json.loads(
        (ROOT / "report/result-schema-v1.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(
        schema, format_checker=FormatChecker()
    )

    config = get_config(args.config)
    try:
        run_id = schema_v1_run_id(args.run_id, schema["properties"]["run_id"]["pattern"])
    except ValueError as error:
        parser.error(str(error))

    output_dir = ROOT / "results/pilot/mr/normal/debug"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = (
        output_dir / f"mr-{config.config_id}-normal-{run_id}.jsonl"
    )

    writer_recorder = NodeRecorder()
    reader_recorder = NodeRecorder()
    records = []

    options = {
        "serverSelectionTimeoutMS": 5000,
        "timeoutMS": 10000,
        "retryReads": False,
        "retryWrites": False,
    }

    with MongoClient(
        os.environ["MONGODB_URI"],
        event_listeners=[writer_recorder],
        **options,
    ) as writer, MongoClient(
        os.environ["MONGODB_URI"],
        event_listeners=[reader_recorder],
        **options,
    ) as reader:
        writer.admin.command("ping")
        reader.admin.command("ping")

        environment = {
            "git_commit": commit,
            "python_version": platform.python_version(),
            "pymongo_version": pymongo.version,
            "mongodb_version": writer.server_info()["version"],
        }

        writer_collection = writer["member_b_experiments"].get_collection(
            "mr",
            write_concern=config.write_concern,
        )
        baseline_collection = writer_collection.with_options(
            write_concern=WriteConcern(w=3, wtimeout=5000)
        )
        reader_collection = reader["member_b_experiments"].get_collection(
            "mr",
            **config.collection_options(),
        )

        with output_path.open("x", encoding="utf-8") as output:
            for trial_id in range(1, args.trials + 1):
                record = run_trial(
                    reader, baseline_collection, writer_collection,
                    reader_collection, writer_recorder, reader_recorder,
                    config, environment, run_id, trial_id,
                )
                validator.validate(record)
                output.write(json.dumps(record, ensure_ascii=False) + "\n")
                output.flush()
                records.append(record)

    print(f"Config: {config.config_id}")
    print(f"Trials: {len(records)}")
    print("Evaluable trials:",
          sum(r["status"] == "success" for r in records))
    print("Violations:",
          sum(r["violation"] is True for r in records))

    for status in ("timeout", "unavailable", "error", "invalid_trial"):
        print(f"{status}:",
              sum(r["status"] == status for r in records))

    print(f"PASS: {len(records)} records passed schema validation")
    print(f"Pilot log: {output_path}")


if __name__ == "__main__":
    main()
