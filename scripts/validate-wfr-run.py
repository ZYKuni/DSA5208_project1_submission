"""Read-only WFR v2 evidence validation; no live database access or reclassification writes."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from experiments.run_wfr_faults import PROTOCOL_V2, classify, summarize, succeeded


def validate(directory):
    directory = Path(directory)
    def read(name):
        return json.loads((directory / name).read_text())
    def require(ok, message):
        if not ok:
            raise ValueError(f'{directory.name}: {message}')
    manifest = read('manifest.json')
    require(manifest['status'] == 'completed', 'batch incomplete')
    require(manifest['protocol_version'] == PROTOCOL_V2, 'use historical verifier for v1')
    require(manifest['data_stage'] == manifest['sample_stage'], 'inconsistent stage')
    if manifest['sample_stage'] == 'formal':
        require(manifest.get('plan_id') and manifest['trials_per_config'] >= 100, 'formal plan/budget')
        require(not manifest.get('git_worktree_status'), 'formal source checkout not clean')
    for name, digest in manifest['artifact_sha256'].items():
        target = (directory / name).resolve()
        require(target.is_relative_to(directory.resolve()), 'artifact path escapes run')
        require(hashlib.sha256(target.read_bytes()).hexdigest() == digest, f'hash {name}')
    with tarfile.open(directory / 'source.tar.gz') as archive:
        for name, digest in manifest['source_sha256'].items():
            require(hashlib.sha256(archive.extractfile(name).read()).hexdigest() == digest, f'source {name}')
    rows = [json.loads(line) for line in (directory / 'operations.jsonl').read_text().splitlines() if line.strip()]
    trials = [r for r in rows if r['record_type'] == 'trial_result']
    require(len(trials) == len({t['trial_id'] for t in trials}) == manifest['completed_trials'] ==
            len(manifest['configs']) * manifest['trials_per_config'], 'unique planned trial count')
    events = [json.loads(line) for line in (directory / 'events.jsonl').read_text().splitlines() if line.strip()]
    requests = {e['request_id']: e for e in events if e['event'] == 'controller_request'}
    receipts = {}
    for e in events:
        if e['event'] != 'controller_response' or not e.get('ok'):
            continue
        request = requests.get(e['request_id'], {})
        if request.get('action') in ('partition', 'crash'):
            require(e['action'] == request['action'] and e['node'] == request['node'], 'controller response mismatch')
            receipts.setdefault(request['trial_id'], []).append((request, e))
    for trial in trials:
        tid = trial['trial_id']
        require(trial['recovery_completed'] and not trial['runner_error'] and not trial['recovery_error'], f'{tid}: recovery')
        operations = [r for r in rows if r['record_type'] == 'operation' and r['trial_id'] == tid]
        ops = {r['operation']: r for r in operations}
        require(len(ops) == len(operations), f'{tid}: duplicate operation')
        predecessor, write, audit = (ops.get(n) for n in ('predecessor_read', 'dependent_write', 'primary_audit'))
        invalid = trial['reason'] if trial['classification'] == 'invalid_trial' else None
        expected = classify(predecessor, write, audit, invalid=invalid)
        require((trial['classification'], trial['is_violation']) == expected[:2], f'{tid}: classification')
        if invalid:
            require(not write and not receipts.get(tid), f'{tid}: invalid trial performed workload/fault')
        for op in operations:
            require(op['retry_count'] == 0, f'{tid}: retries')
            require(op['monotonic_started_ns'] <= op['monotonic_ended_ns'], f'{tid}: operation clock')
        if write:
            require(succeeded(predecessor) and predecessor['returned_document']['x_version'] == 1, f'{tid}: dependency')
            require(predecessor['monotonic_ended_ns'] <= write['monotonic_started_ns'], f'{tid}: R before W')
            if manifest['scenario'] != 'normal':
                require(len(receipts.get(tid, [])) == 1, f'{tid}: one matched fault receipt')
                request, receipt = receipts[tid][0]
                require(predecessor['monotonic_ended_ns'] <= request['monotonic_ns'] <=
                        receipt['monotonic_ns'] <= write['monotonic_started_ns'], f'{tid}: R fault W ordering')
                require(request['node'] == trial['fault_node'], f'{tid}: exact fault target')
                require(request['action'] == ('partition' if manifest['scenario'] == 'replication-partition' else 'crash'),
                        f'{tid}: fault action')
            if trial['config_id'] == 'C4':
                require(predecessor['explicit_causal_session'] and write['explicit_causal_session'] and
                        predecessor['session_id'] == write['session_id'], f'{tid}: session continuity')
        convergence = read(f'evidence/{tid}/convergence.json')
        nodes = convergence['nodes']
        require(len(nodes) == 3 and convergence['stable_seconds'] >= 1, f'{tid}: stable three nodes')
        require(sum(bool(n['hello'].get('isWritablePrimary')) for n in nodes) == 1 and
                sum(bool(n['hello'].get('secondary')) for n in nodes) == 2, f'{tid}: roles')
        require(all(n['document'] == nodes[0]['document'] for n in nodes), f'{tid}: convergence')
    require(read('summary.json') == summarize(trials), 'summary differs from recomputed terminals')
    host_path = directory / 'host-recovery.json'
    if not host_path.exists():
        host_path = directory.parents[1] / 'fault-control' / directory.name / 'host-recovery.json'
    require(set(json.loads(host_path.read_text())['nodes']) == {'mongo1', 'mongo2', 'mongo3'}, 'host cleanup')
    return {'experiment_id': manifest['experiment_id'], 'status': 'pass', 'trials': len(trials),
            'stage': manifest['sample_stage'], 'protocol': PROTOCOL_V2}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    print(json.dumps(validate(parser.parse_args().directory), indent=2))
