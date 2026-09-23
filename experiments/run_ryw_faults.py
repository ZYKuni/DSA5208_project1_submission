"""Zhou Jiahao's RYW workload driven by Zhao Yikun's isolated fault controller."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
import tarfile
import time
from pathlib import Path

import pymongo
from pymongo import ReadPreference
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from analysis.analyze import export_csv, percentile
from experiments.common import (
    DEFAULT_URI, JsonlWriter, execute_operation, json_safe, make_client, utc_now,
)
from experiments.configs import get_config, session_scope
from experiments.fault_diagnostics import (
    Controller, ElectionObserver, Events, NODES, observer, wait_healthy,
)
from experiments.run_matrix import source_files

ROOT = Path(__file__).resolve().parents[1]
COLLECTION = "ryw_documents"


def save(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(value), indent=2) + "\n", encoding="utf-8")


def node_evidence(node, database, document_id):
    """Read the trial document and oplog directly from one exact node."""
    with observer(node, 5000) as direct:
        collection = direct[database][COLLECTION].with_options(
            read_preference=ReadPreference.SECONDARY_PREFERRED,
            read_concern=ReadConcern("local"),
        )
        oplog = direct.local["oplog.rs"].with_options(
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        namespace = f"{database}.{COLLECTION}"
        return json_safe({
            "node": node,
            "captured_at": utc_now(),
            "hello": direct.admin.command("hello"),
            "rbid": direct.admin.command("replSetGetRBID")["rbid"],
            "document": collection.find_one({"_id": document_id}),
            "oplog": list(oplog.find({
                "ns": namespace,
                "$or": [{"o._id": document_id}, {"o2._id": document_id}],
            }).sort("$natural", -1).limit(6)),
        })


def wait_convergence(database, document_id, expected_document, seconds=90):
    """Require the recovered three-node set to expose one identical document."""
    expected = json_safe(expected_document)
    deadline = time.monotonic() + seconds
    stable_since = None
    samples = []
    clients = {node: observer(node, 700) for node in NODES}
    try:
        while time.monotonic() < deadline:
            samples = []
            for node, direct in clients.items():
                try:
                    hello = direct.admin.command("hello")
                    role = ("PRIMARY" if hello.get("isWritablePrimary") else
                            "SECONDARY" if hello.get("secondary") else "TRANSITION")
                    document = direct[database][COLLECTION].with_options(
                        read_preference=ReadPreference.SECONDARY_PREFERRED,
                        read_concern=ReadConcern("local"),
                    ).find_one({"_id": document_id})
                    samples.append({
                        "node": node, "role": role, "document": json_safe(document)
                    })
                except PyMongoError as error:
                    samples.append({"node": node, "error": str(error)})
            roles = [sample.get("role") for sample in samples]
            converged = (
                len(samples) == 3
                and not any(sample.get("error") for sample in samples)
                and roles.count("PRIMARY") == 1
                and roles.count("SECONDARY") == 2
                and all(sample.get("document") == expected for sample in samples)
            )
            if converged:
                stable_since = stable_since or time.monotonic()
                if time.monotonic() - stable_since >= 1:
                    return {
                        "converged_at": utc_now(), "stable_seconds": 1, "nodes": samples
                    }
            else:
                stable_since = None
            time.sleep(0.1)
    finally:
        for direct in clients.values():
            direct.close()
    raise RuntimeError(f"RYW document did not converge: {json_safe(samples)}")


def classify(write, read):
    """Classify RYW only when an acknowledged predecessor write exists."""
    if not write or write["status"] != "success":
        return "inconclusive", None, "Predecessor write was not acknowledged; RYW is not evaluable."
    if not read:
        return "inconclusive", None, "Successor read did not run."
    if read["status"] != "success":
        return "inconclusive", None, (
            f"Successor read ended with {read['status']}; availability failure is not a consistency result."
        )
    version = read.get("returned_version")
    if version is None:
        return "violation", True, "Acknowledged write was followed by a successful missing-document read."
    if version < 1:
        return "violation", True, f"Acknowledged version 1 was followed by older version {version}."
    return "no_violation", False, f"Successor read returned version {version}, including the acknowledged write."


def run_trial(client, ops, events, controller, args, config, number, directory):
    trial_id = f"{args.experiment_id}-ryw-{config.config_id}-{args.scenario}-trial-{number:04d}"
    meta = {
        **config.to_log_record(),
        "schema_version": 2,
        "experiment_id": args.experiment_id,
        "trial_id": trial_id,
        "experiment": "ryw",
        "scenario": args.scenario,
        "client_id": "Zhou Jiahao-client",
        "document_id": trial_id,
        "retry_writes": args.retry_writes,
        "retry_reads": False,
        "timeout_ms": args.timeout_ms,
    }
    before = wait_healthy(client)
    old_primary = before["hello"]["primary"].split(":")[0]
    trial_dir = directory / "evidence" / trial_id
    save(trial_dir / "cluster-before.json", before)
    started_at = utc_now()
    events.emit("trial_start", trial_id=trial_id, primary=old_primary,
                term=before["status"]["term"])

    base = client[args.database][COLLECTION]
    setup = base.with_options(write_concern=WriteConcern(w=3, wtimeout=args.timeout_ms))
    initialize = execute_operation(
        client, ops, meta, "initialize",
        lambda: setup.insert_one({
            "_id": trial_id, "experiment_id": args.experiment_id,
            "version": 0, "value": "initial", "writer": "Zhou Jiahao-client",
        }),
        phase="setup", written_version=0,
        effective_options={"write_concern": 3},
    )

    write = read = audit = barrier = None
    fault = collected = after = None
    new_primary = None
    fault_error = None
    recovery_completed = False
    monitor = None
    before_write_evidence = node_evidence(old_primary, args.database, trial_id)
    save(trial_dir / "old-primary-before.json", before_write_evidence)

    options = config.collection_options()
    options["write_concern"] = WriteConcern(
        w=config.write_concern_w,
        wtimeout=args.timeout_ms,
        **({"j": True} if args.scenario == "replication-partition" else {}),
    )
    collection = base.with_options(**options)
    try:
        if initialize["status"] != "success":
            raise RuntimeError("Trial initialization failed")
        with session_scope(client, config) as session:
            if args.scenario == "replication-partition":
                monitor = ElectionObserver(old_primary, events, trial_id)
                monitor.start()
                fault = controller.request("partition", old_primary, trial_id=trial_id)

            write = execute_operation(
                client, ops, meta, "predecessor_write",
                lambda: collection.update_one(
                    {"_id": trial_id},
                    {"$set": {"version": 1, "value": "written"}},
                    session=session,
                ),
                session=session, written_version=1,
                effective_options={
                    "write_concern": config.write_concern_w,
                    "journal": True if args.scenario == "replication-partition" else None,
                    "read_preference": "primary (write command)",
                },
            )

            if write["status"] == "success":
                if write["target_node"] != old_primary + ":27017":
                    raise RuntimeError("Predecessor write did not execute on the selected fault target")
                after_write = node_evidence(old_primary, args.database, trial_id)
                save(trial_dir / "old-primary-after-write.json", after_write)
                if args.scenario == "primary-crash":
                    monitor = ElectionObserver(old_primary, events, trial_id)
                    monitor.start()
                    fault = controller.request("crash", old_primary, trial_id=trial_id)

                read = execute_operation(
                    client, ops, meta, "successor_read",
                    lambda: collection.find_one({"_id": trial_id}, session=session),
                    session=session, returned_document=True,
                    effective_options={
                        "read_concern": config.read_concern_level,
                        "read_preference": config.read_preference_name,
                    },
                )

        if monitor:
            new_primary = monitor.finish(wait_seconds=40)
        if not new_primary:
            raise RuntimeError("No new Primary observed within the diagnostic window")

        verifier = base.with_options(
            read_preference=ReadPreference.PRIMARY,
            read_concern=ReadConcern("local"),
        )
        audit = execute_operation(
            client, ops, meta, "primary_audit",
            lambda: verifier.find_one({"_id": trial_id}),
            phase="audit", returned_document=True,
            effective_options={"read_preference": "primary", "read_concern": "local"},
        )
        marker = client[args.database]["fault_audit_markers"].with_options(
            write_concern=WriteConcern(w="majority", wtimeout=args.timeout_ms)
        )
        barrier = execute_operation(
            client, ops, meta, "new_branch_barrier",
            lambda: marker.insert_one({"_id": trial_id, "experiment_id": args.experiment_id}),
            phase="audit", effective_options={"write_concern": "majority"},
        )
        if barrier["status"] != "success":
            raise RuntimeError("New branch majority barrier failed")
    except Exception as error:
        fault_error = f"{type(error).__name__}: {error}"
        events.emit("trial_error", trial_id=trial_id, error=fault_error)
    finally:
        if monitor:
            if new_primary is None:
                new_primary = monitor.finish()
            else:
                monitor.finish()
        try:
            controller.request("recover", old_primary, trial_id=trial_id)
            wait_healthy(client)
            expected = (audit.get("returned_document") if audit and audit["status"] == "success"
                        else base.with_options(
                            read_preference=ReadPreference.PRIMARY,
                            read_concern=ReadConcern("local"),
                        ).find_one({"_id": trial_id}))
            convergence = wait_convergence(args.database, trial_id, expected)
            save(trial_dir / "convergence.json", convergence)
            after = wait_healthy(client)
            save(trial_dir / "cluster-after.json", after)
            save(trial_dir / "old-primary-after-recovery.json",
                 node_evidence(old_primary, args.database, trial_id))
            collected = controller.request("collect", old_primary, trial_id=trial_id,
                                           since=started_at)
            recovery_completed = True
            events.emit("recovery_complete", trial_id=trial_id,
                        primary=after["hello"]["primary"])
        except Exception as recovery_error:
            recovery_message = f"{type(recovery_error).__name__}: {recovery_error}"
            fault_error = f"{fault_error}; recovery: {recovery_message}" if fault_error else recovery_message
            events.emit("recovery_error", trial_id=trial_id, error=recovery_message)

    classification, is_violation, reason = classify(write, read)
    if fault_error:
        classification, is_violation = "inconclusive", None
        reason = fault_error
    result = {
        **meta,
        "record_type": "trial_result",
        "classification": classification,
        "is_violation": is_violation,
        "candidate_violation": classification == "violation",
        "reason": reason,
        "started_at": started_at,
        "ended_at": utc_now(),
        "setup_status": initialize["status"],
        "write_status": write["status"] if write else "not_run",
        "read_status": read["status"] if read else "not_run",
        "write_version": 1 if write and write["status"] == "success" else None,
        "read_version": read.get("returned_version") if read else None,
        "write_target": write.get("target_node") if write else None,
        "read_target": read.get("target_node") if read else None,
        "write_latency_ms": write.get("latency_ms") if write else None,
        "read_latency_ms": read.get("latency_ms") if read else None,
        "read_attempt_count": read.get("attempt_count") if read else 0,
        "primary_before": old_primary,
        "primary_after": new_primary.get("node") if new_primary else None,
        "term_before": before["status"]["term"],
        "term_after": after["status"]["term"] if after else None,
        "fault_action": "crash" if args.scenario == "primary-crash" else "partition",
        "fault_confirmed_at": fault.get("ended_at") if fault else None,
        "election_observation": new_primary,
        "recovery_completed": recovery_completed,
        "fault_error": fault_error,
        "audit_version": audit.get("returned_version") if audit else None,
        "collection": COLLECTION,
        "controller_evidence": collected,
        "rollback": "not_assessed_for_ryw",
        "write_1_survived": None,
        "observation_status": "not_run",
        "observation_stale": None,
    }
    ops.write(result)
    return result


def summarize(directory, results):
    summary = []
    for config_id in dict.fromkeys(result["config_id"] for result in results):
        trials = [result for result in results if result["config_id"] == config_id]
        read_latencies = [
            result["read_latency_ms"] for result in trials
            if result["read_status"] == "success"
        ]
        summary.append({
            "config_id": config_id,
            "trials": len(trials),
            "evaluable": sum(result["is_violation"] is not None for result in trials),
            "violations": sum(result["is_violation"] is True for result in trials),
            "no_violation": sum(result["classification"] == "no_violation" for result in trials),
            "inconclusive": sum(result["classification"] == "inconclusive" for result in trials),
            "write_success": sum(result["write_status"] == "success" for result in trials),
            "write_timeout": sum(result["write_status"] == "timeout" for result in trials),
            "read_success": sum(result["read_status"] == "success" for result in trials),
            "read_timeout": sum(result["read_status"] == "timeout" for result in trials),
            "read_not_run": sum(result["read_status"] == "not_run" for result in trials),
            "recovery_completed": sum(result["recovery_completed"] for result in trials),
            "read_success_p50_ms": percentile(read_latencies, 0.5),
            "read_success_p95_ms": percentile(read_latencies, 0.95),
        })
    save(directory / "summary.json", summary)
    export_csv(directory / "summary.csv", summary)
    export_csv(directory / "trials.csv", results)
    rows = [json.loads(line) for line in (directory / "operations.jsonl").read_text().splitlines()]
    export_csv(directory / "operations.csv", [row for row in rows if row["record_type"] == "operation"])
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--scenario", choices=["primary-crash", "replication-partition"], required=True)
    parser.add_argument("--configs", nargs="+", required=True)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument("--retry-writes", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--database", default="dsa5208_fault_experiments")
    args = parser.parse_args()
    if args.scenario == "replication-partition" and args.retry_writes:
        parser.error("Use --no-retry-writes for partition diagnostic")
    if not os.environ.get("FAULT_CONTROL"):
        parser.error("Start using scripts/run-fault-experiment.py on the host")

    directory = ROOT / "results/raw" / args.experiment_id
    directory.mkdir(parents=True, exist_ok=False)
    events = Events(directory / "events.jsonl")
    controller = Controller(events)
    ops = JsonlWriter(directory / "operations.jsonl")
    results = []
    paths = source_files() + [ROOT / "docker/fault-compose.yml", ROOT / "config/init-replica-set.js"]
    paths = sorted(set(paths))
    source_hashes = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }
    with tarfile.open(directory / "source.tar.gz", "w:gz") as archive:
        for path in paths:
            archive.add(path, arcname=str(path.relative_to(ROOT)))
    manifest = {
        "schema_version": 2,
        "experiment_id": args.experiment_id,
        "scenario": args.scenario,
        "experiment": "ryw",
        "status": "running",
        "started_at": utc_now(),
        "configs": [get_config(config).to_log_record() for config in args.configs],
        "trials_per_config": args.trials,
        "timeout_ms": args.timeout_ms,
        "retry_writes": args.retry_writes,
        "retry_reads": False,
        "diagnostic_journal": args.scenario == "replication-partition",
        "git_base_commit": os.environ.get("SOURCE_BASE_COMMIT"),
        "git_worktree_status": os.environ.get("SOURCE_WORKTREE_STATUS"),
        "source_sha256": source_hashes,
        "python": platform.python_version(),
        "pymongo": pymongo.version,
        "lab_project": "dsa5208-mw-fault",
        "fault_framework_author": "Zhao Yikun",
        "workload_author": "Zhou Jiahao",
        "original_lab_modified": False,
        "setup_write_concern": 3,
        "recovery_gate": "direct-document-convergence-1s",
        "fault_mode": "SIGKILL with restart=no; selective replication-network disconnect",
        "argv": sys.argv,
    }
    save(directory / "manifest.json", manifest)
    code = 0
    try:
        with make_client(
            os.environ.get("MONGODB_URI", DEFAULT_URI),
            timeout_ms=args.timeout_ms,
            retry_writes=args.retry_writes,
        ) as client:
            manifest["cluster_before"] = wait_healthy(client)
            if client[args.database][COLLECTION].count_documents({"experiment_id": args.experiment_id}):
                raise RuntimeError("Experiment ID already exists in database")
            for config_id in args.configs:
                config = get_config(config_id)
                for number in range(1, args.trials + 1):
                    result = run_trial(client, ops, events, controller, args, config, number, directory)
                    results.append(result)
                    print(json.dumps({
                        "config": config_id,
                        "trial": number,
                        "classification": result["classification"],
                        "write_status": result["write_status"],
                        "read_status": result["read_status"],
                        "read_version": result["read_version"],
                        "read_target": result["read_target"],
                        "primary_before": result["primary_before"],
                        "primary_after": result["primary_after"],
                        "recovered": result["recovery_completed"],
                    }), flush=True)
                    if result["fault_error"]:
                        raise RuntimeError(result["fault_error"])
            manifest["cluster_after"] = wait_healthy(client)
            manifest["status"] = "completed"
    except BaseException as error:
        manifest["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
        manifest["error"] = f"{type(error).__name__}: {error}"
        code = 1
    finally:
        ops.close()
        events.close()
        manifest["ended_at"] = utc_now()
        manifest["completed_trials"] = len(results)
        summary = summarize(directory, results)
        manifest["artifact_sha256"] = {
            str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob("*")
            if path.is_file() and path.name != "manifest.json"
        }
        save(directory / "manifest.json", manifest)
    print(json.dumps({
        "experiment_id": args.experiment_id,
        "status": manifest["status"],
        "summary": summary,
        "error": manifest.get("error"),
    }, indent=2), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
