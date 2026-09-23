"""S2/S3: stop one/two Secondaries before sequential MW or RYW workloads."""
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
from pymongo import ReadPreference, ReturnDocument
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from experiments.common import DEFAULT_URI, JsonlWriter, execute_operation, json_safe, make_client, utc_now
from experiments.configs import get_config, session_scope
from experiments.fault_diagnostics import Controller, Events, NODES, observer, wait_healthy
from experiments.run_matrix import source_files
from experiments.run_ryw_faults import save, classify as classify_ryw, wait_convergence
from experiments.test_monotonic_writes import classify_trial, history_entry
from analysis.analyze import export_csv, percentile
ROOT = Path(__file__).resolve().parents[1]
# Shared document shape permits the existing exact-node recovery gate.
COLLECTION = "ryw_documents"


def select_targets(snapshot, scenario):
    members = snapshot['status']['members']
    primary = [m['name'].split(':')[0] for m in members if m['stateStr'] == 'PRIMARY']
    secondary = sorted(m['name'].split(':')[0] for m in members if m['stateStr'] == 'SECONDARY')
    if len(primary) != 1 or len(secondary) != 2 or not set(primary + secondary) == set(NODES):
        raise RuntimeError('Expected the isolated three-node set')
    return primary[0], secondary[:1] if scenario == 'secondary-stop' else secondary


def assess(workload, w1, w2, read, audit, error):
    if error:
        return 'invalid_trial', None, error
    if workload == 'ryw':
        return classify_ryw(w1, read)
    category, reason = classify_trial(w1, w2, audit)
    return category, False if category == 'no_violation' else None, reason


def run_trial(client, ops, events, controller, args, config, number, directory):
    trial_id = f'{args.experiment_id}-{args.workload}-{config.config_id}-trial-{number:04d}'
    meta = {**config.to_log_record(), 'schema_version': 2, 'experiment_id': args.experiment_id,
            'trial_id': trial_id, 'experiment': args.workload, 'scenario': args.scenario,
            'client_id': 'client-A', 'document_id': trial_id, 'retry_reads': False,
            'retry_writes': args.retry_writes, 'timeout_ms': args.timeout_ms}
    before = wait_healthy(client)
    primary, targets = select_targets(before, args.scenario)
    evidence = directory / 'evidence' / trial_id
    save(evidence / 'cluster-before.json', before)
    base = client[args.database][COLLECTION]
    initial = {'_id': trial_id, 'experiment_id': args.experiment_id, 'version': 0,
               'client_seq': 0, 'history': [], 'writer': 'client-A'}
    init = execute_operation(client, ops, meta, 'initialize', lambda: base.with_options(
        write_concern=WriteConcern(w=3, wtimeout=args.timeout_ms)).insert_one(initial),
        phase='setup', written_version=0)
    w1 = w2 = read = audit = None
    error = None
    recovered = False
    stopped = []
    replies = []
    started = utc_now()
    role = None
    try:
        if init['status'] != 'success':
            raise RuntimeError('Initialization not acknowledged by all three nodes')
        for target in targets:
            # Register before request: lost responses must still trigger recovery.
            stopped.append(target)
            replies.append(controller.request('crash', target, trial_id=trial_id))
        with observer(primary, 1000) as direct:
            deadline = time.monotonic() + 40
            while True:
                role = json_safe(direct.admin.command('hello'))
                if not args.scenario.endswith('settled') or not role.get('isWritablePrimary'):
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('Old Primary did not step down in settled S3 window')
                time.sleep(.2)
        save(evidence / 'fault-confirmation.json', {'responses': replies, 'role_before_workload': role,
                                                  'captured_at': utc_now()})
        options = config.collection_options()
        options['write_concern'] = WriteConcern(w=config.write_concern_w, wtimeout=args.timeout_ms)
        collection = base.with_options(**options)
        with session_scope(client, config) as session:
            for seq in range(1, 3 if args.workload == 'mw' else 2):
                name = f'write_{seq}'
                update = {'$set': {'version': seq, 'client_seq': seq, 'depends_on': seq-1},
                          '$push': {'history': history_entry(f'{trial_id}:{name}', seq)}}
                row = execute_operation(client, ops, meta, name,
                    lambda update=update: collection.find_one_and_update({'_id': trial_id}, update,
                        return_document=ReturnDocument.BEFORE, session=session),
                    written_version=seq, returned_document=True, session=session,
                    effective_options={'write_concern': config.write_concern_w, 'preimage': True})
                if seq == 1:
                    w1 = row
                else:
                    w2 = row
                if row['status'] != 'success' or row.get('returned_document') is None:
                    break
            last = w2 if args.workload == 'mw' else w1
            if last and last['status'] == 'success':
                read = execute_operation(client, ops, meta, 'successor_read',
                    lambda: collection.find_one({'_id': trial_id}, session=session),
                    returned_document=True, session=session,
                    effective_options={'read_concern': config.read_concern_level,
                                       'read_preference': config.read_preference_name})
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
    finally:
        recovery_errors = []
        for target in stopped:
            try:
                controller.request('recover', target, trial_id=trial_id)
            except Exception as exc:
                recovery_errors.append(str(exc))
        try:
            after = wait_healthy(client)
            audit = execute_operation(client, ops, meta, 'recovered_primary_audit',
                lambda: base.with_options(read_preference=ReadPreference.PRIMARY,
                    read_concern=ReadConcern('local')).find_one({'_id': trial_id}),
                phase='audit', returned_document=True)
            if audit['status'] != 'success':
                raise RuntimeError('Recovery audit unavailable')
            convergence = wait_convergence(args.database, trial_id, audit['returned_document'])
            save(evidence / 'convergence.json', convergence)
            save(evidence / 'cluster-after.json', after)
            if recovery_errors:
                raise RuntimeError('; '.join(recovery_errors))
            recovered = True
        except Exception as exc:
            error = f'{error or ""}; recovery: {exc}'
    category, violation, reason = assess(args.workload, w1, w2, read, audit, error)
    result = {**meta, 'record_type': 'trial_result', 'started_at': started, 'ended_at': utc_now(),
        'classification': category, 'is_violation': violation, 'reason': reason,
        'candidate_violation': category == 'candidate_violation', 'rollback': 'not_assessed',
        'fault_error': error, 'recovery_completed': recovered, 'stopped_nodes': targets,
        'fault_confirmed_at': replies[-1]['ended_at'] if replies else None,
        'primary_before': primary, 'primary_after': after['hello']['primary'] if recovered else None,
        'writable_before_workload': role.get('isWritablePrimary') if role else None,
        'write_status': w1['status'] if w1 else 'not_run',
        'write_2_status': w2['status'] if w2 else 'not_run',
        'read_status': read['status'] if read else 'not_run',
        'read_version': read.get('returned_version') if read else None,
        'read_target': read.get('target_node') if read else None,
        'audit_version': audit.get('returned_version') if audit else None}
    ops.write(result)
    return result


def summarize_secondary(directory, results):
    rows = [json.loads(line) for line in (directory / 'operations.jsonl').read_text().splitlines()]
    summary = []
    for cid in dict.fromkeys(r['config_id'] for r in results):
        trials = [r for r in results if r['config_id'] == cid]
        operations = [r for r in rows if r['record_type'] == 'operation' and r['phase'] == 'workload' and r['config_id'] == cid]
        latency = [r['latency_ms'] for r in operations if r['status'] == 'success']
        evaluable = sum(r['is_violation'] is not None for r in trials)
        violations = sum(r['is_violation'] is True for r in trials)
        summary.append({'config_id': cid, 'trials': len(trials), 'evaluable': evaluable,
            'violations': violations, 'violation_rate': violations/evaluable if evaluable else None,
            'write_success': sum(r['write_status'] == 'success' for r in trials),
            'write_timeout': sum(r['write_status'] == 'timeout' for r in trials),
            'recovered': sum(r['recovery_completed'] for r in trials),
            'successful_operation_p95_ms': percentile(latency, .95)})
    save(directory / 'summary.json', summary)
    export_csv(directory / 'summary.csv', summary)
    export_csv(directory / 'trials.csv', results)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--scenario", choices=["secondary-stop", "two-secondary-stop", "two-secondary-stop-settled"], required=True)
    parser.add_argument("--configs", nargs="+", required=True)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument("--retry-writes", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--database", default="dsa5208_fault_experiments")
    parser.add_argument("--workload", choices=["mw", "ryw"], required=True)
    parser.add_argument("--sample-stage", choices=["pilot", "formal"], default="pilot")
    args = parser.parse_args()
    if args.trials < 1 or args.timeout_ms < 1 or (args.sample_stage == "formal" and args.trials < 100):
        parser.error("Invalid trial count/timeout or formal stage below 100 trials/config")
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
        "experiment": args.workload,
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
        "workload_author": "Zhao Yikun",
        "protocol": "secondary-stop-v1",
        "sample_stage": args.sample_stage,
        "s3_phase": "settled" if args.scenario.endswith("settled") else "immediate",
        "original_lab_modified": False,
        "setup_write_concern": 3,
        "recovery_gate": "direct-document-convergence-1s",
        "fault_mode": "SIGKILL selected Secondaries BEFORE workload; restart=no",
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
        summary = summarize_secondary(directory, results)
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
