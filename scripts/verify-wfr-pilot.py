"""Read-only verification of recorded WFR pilot artifacts, not a live DB test."""
import argparse
import hashlib
import json
from pathlib import Path


def verify(directory):
    def read(name):
        return json.loads((directory / name).read_text())

    checks = 0

    def require(condition, label):
        nonlocal checks
        if not condition:
            raise ValueError(f'{directory.name}: {label}')
        checks += 1

    manifest = read('manifest.json')
    require(manifest['status'] == 'completed', 'run completed')
    for name, digest in manifest['artifact_sha256'].items():
        require(hashlib.sha256((directory / name).read_bytes()).hexdigest() == digest,
                f'artifact hash: {name}')
    rows = [json.loads(line) for line in (directory / 'operations.jsonl').read_text().splitlines()]
    trials = [r for r in rows if r['record_type'] == 'trial_result']
    require(len(trials) == manifest['completed_trials'] ==
            len(manifest['configs']) * manifest['trials_per_config'], 'planned trial count')
    for trial in trials:
        tid = trial['trial_id']
        require(trial['recovery_completed'] and not trial['runner_error'] and not trial['recovery_error'],
                f'{tid}: recovery and runner status')
        recovery = read(f'evidence/{tid}/convergence.json')
        nodes = recovery['nodes']
        require(len(nodes) == 3 and recovery['stable_seconds'] >= 1, f'{tid}: three stable nodes')
        require(sum(bool(n['hello'].get('isWritablePrimary')) for n in nodes) == 1 and
                sum(bool(n['hello'].get('secondary')) for n in nodes) == 2, f'{tid}: roles')
        require(all(n['document'] == nodes[0]['document'] for n in nodes), f'{tid}: documents converge')
        ops = {r['operation']: r for r in rows if r['record_type'] == 'operation' and r['trial_id'] == tid}
        predecessor = ops['predecessor_read']
        for op in ops.values():
            require(op['retry_count'] == 0, f'{tid}: no retry for {op["operation"]}')
        require(len(predecessor['attempts']) == 1, f'{tid}: read wire evidence')
        read_wire = predecessor['attempts'][0]
        require(read_wire['wire_read_concern']['level'] == trial['read_concern'], f'{tid}: read concern')
        write = ops.get('dependent_write')
        if trial['classification'] == 'invalid_trial':
            require(trial['is_violation'] is None and trial['read_version'] != 1 and write is None,
                    f'{tid}: unmet predecessor excluded')
            continue
        require(write is not None and write['status'] == 'success', f'{tid}: write succeeded')
        require(predecessor['ended_at'] <= write['started_at'], f'{tid}: read before write')
        require(len(write['attempts']) == 1, f'{tid}: write wire evidence')
        write_wire = write['attempts'][0]
        require(write_wire['wire_write_concern']['w'] == trial['write_concern'], f'{tid}: write concern')
        if trial['config_id'] == 'C4':
            require(predecessor['explicit_causal_session'] and write['explicit_causal_session'] and
                    predecessor['session_id'] == write['session_id'] ==
                    read_wire['wire_session_id'] == write_wire['wire_session_id'], f'{tid}: causal session')
        history = write['returned_document']['x_history']
        require(ops['primary_audit']['returned_document']['y'] == {
            'id': write['operation_id'], 'depends_on_version': trial['read_version']}, f'{tid}: audit marker')
        expected = 'no_violation' if trial['read_version'] in history else 'candidate_violation'
        require(trial['classification'] == expected, f'{tid}: preimage classification')
        require(trial['is_violation'] is (False if expected == 'no_violation' else None),
                f'{tid}: candidate not confirmed')
    host = directory.parents[1] / 'fault-control' / directory.name / 'host-recovery.json'
    require(host.is_file(), 'host recovery record')
    require(set(json.loads(host.read_text())['nodes']) == {'mongo1', 'mongo2', 'mongo3'}, 'host recovery nodes')
    return {'experiment_id': directory.name, 'checks_passed': checks, 'trials': len(trials),
            'classifications': [t['classification'] for t in trials]}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+', type=Path)
    print(json.dumps([verify(path) for path in parser.parse_args().directories], indent=2))
