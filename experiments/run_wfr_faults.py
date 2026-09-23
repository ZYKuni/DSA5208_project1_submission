"""WFR evidence runner dispatched by scripts/run-fault-experiment.py --workload wfr."""
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
from pathlib import Path

import pymongo
from pymongo import ReadPreference, ReturnDocument
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from analysis.analyze import percentile
from experiments.common import DEFAULT_URI, JsonlWriter, execute_operation, json_safe, make_client, utc_now
from experiments.configs import get_config, session_scope
from experiments.fault_diagnostics import Controller, ElectionObserver, Events, NODES, observer, wait_healthy
from experiments.run_matrix import source_files

ROOT = Path(__file__).resolve().parents[1]
COLLECTION = 'wfr_documents'
PROTOCOL = 'wfr-protocol-1'
PROTOCOL_V2 = 'wfr-committed-dependency-v2'


class InvalidTrial(RuntimeError):
    """A required experimental precondition was not established."""


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(value), indent=2) + '\n', encoding='utf-8')


def succeeded(row):
    return bool(row and row.get('status') == 'success' and not any(
        a.get('write_concern_error') or a.get('write_errors') for a in row.get('attempts', [])))


def availability(row):
    if not row:
        return 'not_run'
    if row['status'] == 'timeout':
        return 'timeout'
    if succeeded(row):
        return 'success'
    if row.get('error_type') in {'AutoReconnect', 'ConnectionFailure', 'NotPrimaryError',
                                'ServerSelectionTimeoutError', 'NetworkTimeout'} or row.get('error_code') in {
                                    6, 7, 89, 91, 189, 10107, 11600, 11602, 13435, 13436}:
        return 'unavailable'
    return 'error'


def classify(read, write, audit, *, invalid=None):
    """Never promote missing or failed evidence to a confirmed violation."""
    if invalid:
        return 'invalid_trial', None, invalid
    if not succeeded(read) or not succeeded(write):
        return 'inconclusive', None, 'Predecessor read or dependent write did not complete successfully.'
    document = read.get('returned_document')
    preimage = write.get('returned_document')
    if not isinstance(document, dict) or not isinstance(preimage, dict):
        return 'inconclusive', None, 'Missing read document or matched atomic write preimage.'
    version = document.get('x_version')
    history = preimage.get('x_history')
    if (type(version) is not int or not isinstance(history, list)
            or not isinstance(document.get('x_history'), list) or version not in document['x_history']):
        return 'inconclusive', None, 'Incomplete predecessor history evidence.'
    if not document.get('_id') or document['_id'] != preimage.get('_id'):
        return 'inconclusive', None, 'Read and preimage do not identify the same trial document.'
    expected_y = {'id': write['operation_id'], 'depends_on_version': version}
    if not succeeded(audit) or (audit.get('returned_document') or {}).get('y') != expected_y:
        return 'inconclusive', None, 'Dependent marker has not been corroborated by audit.'
    if audit['returned_document'].get('_id') != document['_id']:
        return 'inconclusive', None, 'Audit identifies a different trial document.'
    if not read.get('target_node') or not write.get('target_node'):
        return 'inconclusive', None, 'Missing actual command destinations.'
    if read.get('causal_session') or write.get('causal_session'):
        if (not read.get('explicit_causal_session') or not write.get('explicit_causal_session')
                or not read.get('session_id') or read['session_id'] != write.get('session_id')):
            return 'inconclusive', None, 'Causal read/write session continuity is not established.'
    if version not in history:
        return 'candidate_violation', None, 'Atomic write preimage lacks the successfully read history; review rollback.'
    return 'no_violation', False, 'Atomic write preimage includes the predecessor read history.'


def collection(client, database):
    return client[database][COLLECTION]


def node_evidence(node, database, trial_id):
    with observer(node, 2000) as direct:
        return json_safe({
            'node': node, 'hello': direct.admin.command('hello'), 'captured_at': utc_now(),
            'rbid': direct.admin.command('replSetGetRBID')['rbid'],
            'document': collection(direct, database).with_options(
                read_preference=ReadPreference.SECONDARY_PREFERRED,
                read_concern=ReadConcern('local')).find_one({'_id': trial_id}),
            'oplog': list(direct.local['oplog.rs'].with_options(
                read_preference=ReadPreference.SECONDARY_PREFERRED).find({
                    'ns': database + '.' + COLLECTION,
                    '$or': [{'o._id': trial_id}, {'o2._id': trial_id}]
                }).sort('$natural', -1).limit(10)),
        })


def wait_convergence(database, trial_id, expected=None, seconds=90):
    """Inspect each node directly; health alone does not prove convergence."""
    deadline, stable = time.monotonic() + seconds, None
    samples = []
    while time.monotonic() < deadline:
        samples = []
        for node in NODES:
            try:
                samples.append(node_evidence(node, database, trial_id))
            except pymongo.errors.PyMongoError as error:
                samples.append({'node': node, 'error': str(error)})
        docs = [s.get('document') for s in samples]
        hellos = [s.get('hello', {}) for s in samples]
        ok = (not any(s.get('error') for s in samples)
              and sum(bool(h.get('isWritablePrimary')) for h in hellos) == 1
              and sum(bool(h.get('secondary')) for h in hellos) == 2
              and all(d == docs[0] for d in docs)
              and (expected is None or docs[0] == expected))
        if ok:
            stable = stable if stable is not None else time.monotonic()
            if time.monotonic() - stable >= 1:
                return {'nodes': samples, 'stable_seconds': 1, 'captured_at': utc_now()}
        else:
            stable = None
        time.sleep(0.1)
    raise RuntimeError('WFR convergence deadline exceeded: ' + str(samples))


def run_trial(client, ops, events, controller, args, config, number, directory):
    trial_id = f'{args.experiment_id}-wfr-{config.config_id}-{args.scenario}-{number:04d}'
    partition = args.scenario == 'replication-partition'
    protocol = getattr(args, 'wfr_protocol', PROTOCOL)
    committed = protocol == PROTOCOL_V2
    legacy_partition = partition and not committed
    meta = {**config.to_log_record(), 'schema_version': 2, 'workload_schema': 'wfr-evidence-1',
            'protocol_version': protocol, 'record_type': 'operation', 'experiment': 'wfr',
            'experiment_id': args.experiment_id, 'trial_id': trial_id, 'scenario': args.scenario,
            'protocol_variant': PROTOCOL_V2 if committed else ('partition-primary-read' if partition else 'baseline'),
            'base_config': config.to_log_record(), 'retry_reads': False, 'retry_writes': False}
    started, clock = utc_now(), time.perf_counter()
    folder = directory / 'evidence' / trial_id
    old_primary = monitor = new_primary = None
    fault_attempted = False
    fault_node = None
    read = write = audit = None
    invalid = error = recovery_error = None
    recovered = False
    collected = None

    def operation(active, name, action, **kwargs):
        return execute_operation(active, ops, meta, name, action, **kwargs)

    try:
        before = wait_healthy(client)
        save(folder / 'cluster-before.json', before)
        old_primary = before['hello']['primary'].split(':')[0]
        initial = {'_id': trial_id, 'experiment_id': args.experiment_id,
                   'x_version': 0, 'x_history': [0], 'y': None}
        # P is independent of T, including its implicit session and cluster time.
        with make_client(args.uri, timeout_ms=args.timeout_ms, retry_writes=False) as producer:
            base = collection(producer, args.database)
            setup = operation(producer, 'initialize', lambda: base.with_options(
                write_concern=WriteConcern(w=3)).insert_one(initial), phase='setup',
                effective_options={'write_concern': 3})
            if not succeeded(setup):
                raise InvalidTrial('Common baseline write was not acknowledged.')
            save(folder / 'initial-convergence.json', wait_convergence(args.database, trial_id, initial))
            if legacy_partition:
                monitor = ElectionObserver(old_primary, events, trial_id)
                monitor.start()
                fault_attempted = True
                fault_node = old_primary
                events.emit('fault_confirmed', trial_id=trial_id,
                            response=controller.request('partition', old_primary, trial_id=trial_id))
            producer_uri = f'mongodb://observe-{old_primary}:27017/' if legacy_partition else args.uri
            with make_client(producer_uri, timeout_ms=args.timeout_ms, retry_writes=False,
                             direct=legacy_partition) as seed_client:
                seed_collection = collection(seed_client, args.database).with_options(
                    write_concern=WriteConcern(w=1, j=True) if legacy_partition else WriteConcern(w='majority'))
                seed = operation(seed_client, 'seed_x', lambda: seed_collection.find_one_and_update(
                    {'_id': trial_id}, {'$set': {'x_version': 1}, '$push': {'x_history': 1}},
                    return_document=ReturnDocument.AFTER), phase='setup', returned_document=True,
                    effective_options={'write_concern': 1 if legacy_partition else 'majority',
                                       'journal': True if legacy_partition else None, 'direct': legacy_partition})
            if not succeeded(seed) or not seed.get('returned_document'):
                raise InvalidTrial('External x=1 preparation failed.')
            if legacy_partition:
                samples = [node_evidence(n, args.database, trial_id) for n in NODES]
                save(folder / 'partition-preconditions.json', samples)
                if not all((s.get('document') or {}).get('x_version') ==
                           (1 if s['node'] == old_primary else 0) for s in samples):
                    raise InvalidTrial('Required isolated x=1 / majority-side x=0 was not established.')
            else:
                save(folder / 'seed-convergence.json', wait_convergence(
                    args.database, trial_id, seed['returned_document']))

        options = config.collection_options()
        if legacy_partition:
            options['read_preference'] = ReadPreference.PRIMARY
        workload = collection(client, args.database).with_options(**options)
        with session_scope(client, config) as session:
            read = operation(client, 'predecessor_read', lambda: workload.find_one(
                {'_id': trial_id}, session=session), session=session, returned_document=True,
                effective_options={'read_concern': config.read_concern_level,
                                   'read_preference': 'primary' if legacy_partition else config.read_preference_name})
            if succeeded(read):
                if (read.get('returned_document') or {}).get('x_version') != 1:
                    raise InvalidTrial('Predecessor read did not establish x=1.')
                if legacy_partition and read.get('target_node') != old_primary + ':27017':
                    raise InvalidTrial('Predecessor read did not execute on isolated old Primary.')
                events.emit('READ_COMPLETED', trial_id=trial_id, node=read['target_node'])
                if args.scenario == 'secondary-stop':
                    read_node = (read.get('target_node') or '').split(':')[0]
                    fault_node = read_node if read_node in NODES and read_node != old_primary else next(
                        n for n in NODES if n != old_primary)
                    evidence = node_evidence(fault_node, args.database, trial_id)
                    save(folder / 'secondary-before-stop.json', evidence)
                    if not evidence.get('hello', {}).get('secondary'):
                        raise InvalidTrial('S2 target is not a verified Secondary.')
                    fault_attempted = True
                    events.emit('fault_confirmed', trial_id=trial_id,
                                response=controller.request('crash', fault_node, trial_id=trial_id))
                if args.scenario == 'primary-crash' or (partition and committed):
                    monitor = ElectionObserver(old_primary, events, trial_id)
                    monitor.start()
                    fault_attempted = True
                    fault_node = old_primary
                    events.emit('fault_confirmed', trial_id=trial_id,
                                response=controller.request('partition' if partition else 'crash', old_primary,
                                                            trial_id=trial_id))
                if partition:
                    new_primary = monitor.finish(wait_seconds=40)
                    if not new_primary:
                        raise InvalidTrial('No new Primary before dependent write.')
                    evidence = node_evidence(new_primary['node'], args.database, trial_id)
                    save(folder / 'new-primary-before-write.json', evidence)
                    has_dependency = 1 in (evidence.get('document') or {}).get('x_history', [])
                    if legacy_partition and has_dependency:
                        raise InvalidTrial('New branch already includes predecessor history.')
                marker = {'id': trial_id + ':dependent_write', 'depends_on_version': 1}
                write = operation(client, 'dependent_write', lambda: workload.find_one_and_update(
                    {'_id': trial_id}, {'$set': {'y': marker}}, upsert=False,
                    return_document=ReturnDocument.BEFORE, session=session), session=session,
                    returned_document=True, effective_options={'write_concern': config.write_concern_w,
                                                               'return_document': 'before'})
                if partition and succeeded(write) and write.get('target_node') != new_primary['node'] + ':27017':
                    raise InvalidTrial('Dependent write did not target the observed new Primary.')
        if monitor and not new_primary:
            new_primary = monitor.finish(wait_seconds=40)
        # O uses a separate client: audit cannot advance T's causal clock.
        with make_client(args.uri, timeout_ms=args.timeout_ms, retry_writes=False) as verifier:
            audited = collection(verifier, args.database).with_options(
                read_preference=ReadPreference.PRIMARY, read_concern=ReadConcern('local'))
            audit = operation(verifier, 'primary_audit', lambda: audited.find_one({'_id': trial_id}),
                              phase='audit', returned_document=True)
            if fault_attempted:
                barrier = operation(verifier, 'new_branch_barrier', lambda: verifier[args.database][
                    'wfr_barriers'].with_options(write_concern=WriteConcern(w='majority')).insert_one(
                        {'_id': trial_id}), phase='audit')
                if not succeeded(barrier):
                    raise RuntimeError('New branch majority barrier failed.')
    except InvalidTrial as exc:
        invalid = str(exc)
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
    finally:
        # Recovery must still run if monitor cleanup itself fails.
        try:
            if monitor:
                new_primary = monitor.finish() or new_primary
        except Exception as exc:
            error = f'{error or ""}; monitor: {exc}'
        try:
            if fault_attempted:
                controller.request('recover', fault_node, trial_id=trial_id)
            if old_primary:
                save(folder / 'convergence.json', wait_convergence(args.database, trial_id))
                save(folder / 'cluster-after.json', wait_healthy(client))
                if fault_attempted:
                    collected = controller.request('collect', fault_node, trial_id=trial_id, since=started)
                recovered = True
        except Exception as exc:
            recovery_error = f'{type(exc).__name__}: {exc}'

    classification, violation, reason = classify(read, write, audit, invalid=invalid)
    if error:
        classification, violation, reason = 'inconclusive', None, error
    row = {**meta, 'record_type': 'trial_result', 'started_at': started, 'ended_at': utc_now(),
           'latency_ms': (time.perf_counter() - clock) * 1000,
           'classification': classification, 'is_violation': violation, 'reason': reason,
           'candidate_violation': classification == 'candidate_violation',
           'read_version': (read.get('returned_document') or {}).get('x_version') if read else None,
           'read_history': (read.get('returned_document') or {}).get('x_history') if read else None,
           'dependent_write_id': trial_id + ':dependent_write',
           'depends_on_version': 1 if write is not None else None,
           'dependent_write_preimage': write.get('returned_document') if write else None,
           'write_acknowledged': bool(succeeded(write) and write.get('returned_document')),
           'read_node': read.get('target_node') if read else None,
           'write_node': write.get('target_node') if write else None,
           'read_outcome': availability(read), 'write_outcome': availability(write),
           'read_latency_ms': read.get('latency_ms') if read else None,
           'write_latency_ms': write.get('latency_ms') if write else None,
           'primary_before': old_primary, 'fault_node': fault_node, 'election_observation': new_primary,
           'rollback': 'not_assessed', 'controller_evidence': collected,
           'recovery_completed': recovered, 'recovery_error': recovery_error, 'runner_error': error}
    ops.write(row)
    return row


def summarize(results):
    rows = []
    for config in dict.fromkeys(r['config_id'] for r in results):
        trials = [r for r in results if r['config_id'] == config]
        counts = {c: sum(t['classification'] == c for t in trials)
                  for c in ('no_violation', 'candidate_violation', 'inconclusive', 'invalid_trial')}
        evaluable = sum(t['is_violation'] is not None for t in trials)
        valid = len(trials) - counts['invalid_trial']
        completed = sum(t['read_outcome'] == 'success' and t['write_acknowledged']
                        for t in trials if t['classification'] != 'invalid_trial')
        rows.append({'config_id': config, 'trials': len(trials), **counts,
                     'evaluable': evaluable,
                     'confirmed': sum(t['is_violation'] is True for t in trials),
                     'violation_rate': sum(t['is_violation'] is True for t in trials) / evaluable
                     if evaluable else None,
                     'workload_completion_rate': completed / valid if valid else None,
                     'recovery_failures': sum(not t['recovery_completed'] for t in trials),
                     **{f'{stage}_{status}': sum(t[f'{stage}_outcome'] == status for t in trials)
                        for stage in ('read', 'write')
                        for status in ('success', 'timeout', 'unavailable', 'error', 'not_run')},
                     **{f'{stage}_p{p}_ms': percentile([t[f'{stage}_latency_ms'] for t in trials
                         if t[f'{stage}_outcome'] == 'success'], p / 100)
                        for stage in ('read', 'write') for p in (50, 95)}})
    return rows


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment-id', required=True)
    parser.add_argument('--scenario', choices=['normal', 'primary-crash', 'replication-partition', 'secondary-stop'], required=True)
    parser.add_argument('--wfr-protocol', choices=[PROTOCOL, PROTOCOL_V2], default=PROTOCOL)
    parser.add_argument('--sample-stage', choices=['pilot', 'formal'], default='pilot')
    parser.add_argument('--plan-id')
    parser.add_argument('--configs', nargs='+', choices=['C1', 'C2', 'C3', 'C4'], default=['C1', 'C4'])
    parser.add_argument('--trials', type=int, default=1)
    parser.add_argument('--timeout-ms', type=int, default=15000)
    parser.add_argument('--retry-writes', action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument('--database', default='dsa5208_wfr_experiments')
    args = parser.parse_args(argv)
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,90}', args.experiment_id):
        parser.error('Invalid experiment ID')
    if args.trials < 1 or args.timeout_ms < 1 or len(set(args.configs)) != len(args.configs):
        parser.error('Positive budgets and unique configurations required')
    if args.retry_writes:
        parser.error('Protocol 1 requires --no-retry-writes')
    if args.scenario == 'secondary-stop' and args.wfr_protocol != PROTOCOL_V2:
        parser.error('S2 requires the explicitly selected committed-dependency v2 protocol')
    if args.sample_stage == 'formal' and (args.trials < 100 or not args.plan_id or args.wfr_protocol != PROTOCOL_V2):
        parser.error('Formal WFR requires v2, plan-id and at least 100 fixed attempts/config')
    if args.scenario != 'normal' and not os.environ.get('FAULT_CONTROL'):
        parser.error('Fault scenarios require scripts/run-fault-experiment.py --workload wfr')
    args.uri = os.environ.get('MONGODB_URI', DEFAULT_URI)
    return args


def main(argv=None):
    args = parse_args(argv)
    directory = ROOT / 'results/raw' / args.experiment_id
    directory.mkdir(parents=True, exist_ok=False)
    results = []
    manifest = {'schema_version': 2, 'workload_schema': 'wfr-evidence-1',
                'protocol_version': args.wfr_protocol, 'experiment': 'wfr', 'experiment_id': args.experiment_id,
                'scenario': args.scenario, 'status': 'running', 'started_at': utc_now(),
                'configs': [get_config(c).to_log_record() for c in args.configs],
                'trials_per_config': args.trials, 'timeout_ms': args.timeout_ms,
                'retry_reads': False, 'retry_writes': False,
                'fault_framework_author': 'Zhao Yikun',
                'workload_author': 'Chen Zheke',
                'python': platform.python_version(), 'pymongo': pymongo.version,
                'git_base_commit': os.environ.get('SOURCE_BASE_COMMIT'),
                'git_worktree_status': os.environ.get('SOURCE_WORKTREE_STATUS'),
                'data_stage': args.sample_stage, 'sample_stage': args.sample_stage,
                'plan_id': args.plan_id, 'argv': sys.argv}
    code = 0
    ops = JsonlWriter(directory / 'operations.jsonl')
    events = Events(directory / 'events.jsonl')
    try:
        paths = sorted(set(source_files() + [ROOT / 'docker/fault-compose.yml',
                                            ROOT / 'config/init-replica-set.js']))
        manifest['source_sha256'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                     for p in paths}
        with tarfile.open(directory / 'source.tar.gz', 'w:gz') as archive:
            for path in paths:
                archive.add(path, arcname=str(path.relative_to(ROOT)))
        save(directory / 'manifest.json', manifest)
        controller = Controller(events) if args.scenario != 'normal' else None
        with make_client(args.uri, timeout_ms=args.timeout_ms, retry_writes=False) as inspector:
            snapshot = wait_healthy(inspector)
            save(directory / 'cluster-initial.json', snapshot)
            manifest['mongodb_version'] = snapshot.get('mongodb_version')
            if collection(inspector, args.database).find_one({'experiment_id': args.experiment_id}):
                raise RuntimeError('Experiment ID already exists in MongoDB; use a new ID.')
        # New T per trial avoids inheriting cluster time from a previous trial's audits.
        for cfg in args.configs:
            for number in range(1, args.trials + 1):
                with make_client(args.uri, timeout_ms=args.timeout_ms, retry_writes=False) as client:
                    result = run_trial(client, ops, events, controller, args, get_config(cfg), number, directory)
                results.append(result)
                if result['runner_error'] or not result['recovery_completed']:
                    raise RuntimeError(result['runner_error'] or result['recovery_error'])
        manifest['status'] = 'completed'
    except BaseException as error:
        manifest['status'] = 'interrupted' if isinstance(error, KeyboardInterrupt) else 'failed'
        manifest['error'] = f'{type(error).__name__}: {error}'
        code = 1
    finally:
        ops.close()
        events.close()
        save(directory / 'summary.json', summarize(results))
        manifest['completed_trials'] = len(results)
        manifest['ended_at'] = utc_now()
        manifest['artifact_sha256'] = {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in directory.rglob('*') if p.is_file() and p.name != 'manifest.json'}
        save(directory / 'manifest.json', manifest)
    print(json.dumps({'status': manifest['status'], 'directory': str(directory),
                      'error': manifest.get('error')}, indent=2))
    return code


if __name__ == '__main__':
    raise SystemExit(main())
