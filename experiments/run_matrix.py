"""Run member A's normal-operation MW matrix and preserve its evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import sys
import tarfile
import time
import uuid
from pathlib import Path

import pymongo
from pymongo import ReadPreference
from pymongo.read_concern import ReadConcern

from analysis.analyze import summarize
from experiments.common import (DEFAULT_URI, JsonlWriter, cluster_snapshot, json_safe,
                                make_client, require_healthy, utc_now)
from experiments.configs import get_config
from experiments.test_monotonic_writes import run_trial, history_entry

ROOT = Path(__file__).resolve().parents[1]


def source_files():
    paths = [ROOT / p for p in ['docker-compose.yml', 'requirements-experiments.txt',
                               'docker/runner.Dockerfile']]
    for folder, pattern in [('experiments', '*.py'), ('analysis', '*.py'),
                            ('scripts', '*.sh'), ('scripts', '*.py'), ('tests', '*.py')]:
        paths.extend((ROOT / folder).glob(pattern))
    return sorted(p for p in paths if p.is_file())


def capture_source(directory):
    paths = source_files()
    hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    with tarfile.open(directory / 'source.tar.gz', 'w:gz') as archive:
        for path in paths:
            archive.add(path, arcname=str(path.relative_to(ROOT)))
    return hashes


def inspect_nodes(snapshot, database, experiment_id, expected_ids, timeout_ms):
    """Post-run local reads on each exact node; never used as MW predecessors."""
    audits = []
    for member in snapshot['replica_set_config']['members']:
        node = member['host']
        with make_client(f'mongodb://{node}/', direct=True, retry_writes=False,
                         timeout_ms=timeout_ms) as client:
            deadline = time.monotonic() + timeout_ms / 1000
            collection = client[database]['mw_documents'].with_options(
                read_preference=ReadPreference.SECONDARY_PREFERRED, read_concern=ReadConcern('local'))
            while True:
                documents = list(collection.find({'experiment_id': experiment_id}).sort('_id', 1))
                actual_ids = {d['_id'] for d in documents}
                mismatches = [d['_id'] for d in documents if
                    d.get('version') != 2 or d.get('client_seq') != 2 or
                    d.get('history') != [history_entry(f"{d['_id']}:write_1", 1),
                                         history_entry(f"{d['_id']}:write_2", 2)]]
                ok = actual_ids == expected_ids and not mismatches
                if ok or time.monotonic() >= deadline:
                    break
                time.sleep(0.1)
            hello = client.admin.command('hello')
            audits.append({'node': node, 'captured_at': utc_now(),
                           'role': 'PRIMARY' if hello.get('isWritablePrimary') else 'SECONDARY',
                           'read_concern': 'local', 'direct_connection': True,
                           'count': len(documents), 'expected_count': len(expected_ids),
                           'converged': ok, 'mismatched_ids': mismatches,
                           'missing_ids': sorted(expected_ids - actual_ids),
                           'documents_sha256': hashlib.sha256(
                               json.dumps(json_safe(documents), sort_keys=True).encode()).hexdigest()})
    return audits


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment', choices=['mw'], default='mw')
    parser.add_argument('--scenario', choices=['normal'], default='normal')
    parser.add_argument('--configs', nargs='+', default=['C1', 'C2', 'C3', 'C4'])
    parser.add_argument('--trials', type=int, default=10, help='Trials per configuration')
    parser.add_argument('--experiment-id', default=None)
    parser.add_argument('--database', default='dsa5208_experiments')
    parser.add_argument('--output-root', type=Path, default=ROOT / 'results/raw')
    parser.add_argument('--timeout-ms', type=int, default=10000)
    parser.add_argument('--retry-writes', action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('--check-only', action='store_true')
    args = parser.parse_args()
    if args.trials < 1 or args.timeout_ms < 1:
        parser.error('trials and timeout-ms must be positive')
    args.experiment_id = args.experiment_id or (time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
                                               + '-mw-' + uuid.uuid4().hex[:10])
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,119}', args.experiment_id):
        parser.error('experiment-id must be 1-120 safe letters/digits/dots/underscores/hyphens')
    try:
        args.config_objects = [get_config(c) for c in args.configs]
    except ValueError as error:
        parser.error(str(error))
    if len({c.config_id for c in args.config_objects}) != len(args.config_objects):
        parser.error('Duplicate configuration IDs are not allowed')
    return args


def main():
    args = parse_args()
    uri = os.environ.get('MONGODB_URI', DEFAULT_URI)
    with make_client(uri, timeout_ms=args.timeout_ms, retry_writes=args.retry_writes) as client:
        snapshot = cluster_snapshot(client)
        require_healthy(snapshot)
        if args.check_only:
            print(json.dumps(snapshot, indent=2))
            return 0
        if client[args.database]['mw_documents'].count_documents({'experiment_id': args.experiment_id}):
            raise RuntimeError('experiment-id already exists in MongoDB; use a fresh ID')
        directory = args.output_root / args.experiment_id
        directory.mkdir(parents=True, exist_ok=False)
        manifest = {'schema_version': 1, 'experiment_id': args.experiment_id,
                    'experiment': 'mw', 'scenario': 'normal', 'status': 'running',
                    'started_at': utc_now(), 'trials_per_config': args.trials,
                    'configs': [c.to_log_record() for c in args.config_objects],
                    'database': args.database, 'retry_writes': args.retry_writes,
                    'retry_reads': False, 'timeout_ms': args.timeout_ms,
                    'execution_order': 'configuration blocks C1..C4, sequential trials',
                    'fault_injection': None, 'setup_write_concern': 3,
                    'python': platform.python_version(), 'pymongo': pymongo.version,
                    'platform': platform.platform(), 'argv': sys.argv,
                    'git_base_commit': os.environ.get('SOURCE_BASE_COMMIT', 'unavailable'),
                    'git_worktree_status': os.environ.get('SOURCE_WORKTREE_STATUS', 'unavailable'),
                    'cluster_before': snapshot, 'source_sha256': capture_source(directory)}
        manifest_path = directory / 'manifest.json'
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
        writer = JsonlWriter(directory / 'operations.jsonl')
        trials = []
        exit_code = 0
        try:
            for config in args.config_objects:
                for number in range(1, args.trials + 1):
                    trials.append(run_trial(client, writer, config, experiment_id=args.experiment_id,
                                            trial_number=number, database=args.database,
                                            timeout_ms=args.timeout_ms, retry_writes=args.retry_writes))
                print(f'{config.config_id}: {args.trials} trials completed', flush=True)
            manifest['cluster_after'] = cluster_snapshot(client)
            require_healthy(manifest['cluster_after'])
            audits = inspect_nodes(manifest['cluster_after'], args.database, args.experiment_id,
                                   {t['document_id'] for t in trials}, args.timeout_ms)
            (directory / 'node-audit.json').write_text(json.dumps(audits, indent=2) + '\n')
            all_ok = (all(t['classification'] == 'no_violation' for t in trials)
                      and all(a['converged'] for a in audits))
            manifest['status'] = 'completed' if all_ok else 'completed_with_findings'
            exit_code = 0 if all_ok else 2
        except BaseException as error:
            manifest['status'] = 'interrupted' if isinstance(error, KeyboardInterrupt) else 'failed'
            manifest['error'] = f'{type(error).__name__}: {error}'
            exit_code = 1
        finally:
            writer.close()
            manifest['ended_at'] = utc_now()
            manifest['completed_trials'] = len(trials)
            summary = summarize(directory / 'operations.jsonl', directory)
            manifest['artifact_sha256'] = {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(directory.iterdir()) if p.is_file() and p.name != 'manifest.json'}
            manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
        print(json.dumps({'experiment_id': args.experiment_id, 'status': manifest['status'],
                          'output': str(directory), 'summary': summary}, indent=2))
        return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
