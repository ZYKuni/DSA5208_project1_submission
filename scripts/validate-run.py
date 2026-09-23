"""Validate run integrity, sequence evidence, configuration and causal sessions."""
import argparse
import hashlib
import json
import tarfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def validate(directory, expect_clean=False):
    manifest = json.loads((directory / 'manifest.json').read_text())
    for name, digest in manifest['artifact_sha256'].items():
        assert Path(name).name == name, 'Unsafe artifact name'
        assert hashlib.sha256((directory / name).read_bytes()).hexdigest() == digest, name
    with tarfile.open(directory / 'source.tar.gz') as archive:
        for name, digest in manifest['source_sha256'].items():
            assert hashlib.sha256(archive.extractfile(name).read()).hexdigest() == digest, name
    records = [json.loads(s) for s in (directory / 'operations.jsonl').read_text().splitlines()]
    groups = defaultdict(list)
    for row in records:
        groups[row['trial_id']].append(row)
    if expect_clean:
        assert manifest['status'] == 'completed'
        assert len(groups) == manifest['trials_per_config'] * len(manifest['configs'])
        assert all(n['converged'] for n in json.loads((directory / 'node-audit.json').read_text()))
    required = {'experiment_id', 'trial_id', 'config_id', 'scenario', 'client_id',
                'session_id', 'operation', 'target_node', 'written_version', 'returned_version',
                'started_at', 'ended_at', 'latency_ms', 'status', 'is_violation',
                'error', 'primary_at_start', 'attempts', 'operation_id'}
    config_map = {c['config_id']: c for c in manifest['configs']}
    targets = defaultdict(set)
    for trial_id, rows in groups.items():
        operations = [r for r in rows if r['record_type'] == 'operation']
        outcomes = [r for r in rows if r['record_type'] == 'trial_result']
        if expect_clean:
            assert len(outcomes) == 1 and outcomes[0]['classification'] == 'no_violation'
            assert [o['operation'] for o in operations] == [
                'initialize', 'write_1', 'write_2', 'configured_read', 'primary_audit']
        for op in operations:
            assert required <= op.keys(), (trial_id, 'missing required fields')
            assert op['latency_ms'] >= 0
            if expect_clean:
                assert op['status'] == 'success'
                assert op['attempt_count'] >= 1 and op['target_node']
                assert all(a['status'] == 'success' and not a.get('write_concern_error')
                           and not a.get('write_errors') for a in op['attempts'])
            if op['target_node']:
                targets[op['operation']].add(op['target_node'])
        if not expect_clean:
            continue
        init, w1, w2, read, audit = operations
        assert datetime.fromisoformat(w1['ended_at'].replace("Z", "+00:00")) <= datetime.fromisoformat(w2['started_at'].replace("Z", "+00:00"))
        assert w1['returned_version'] == 0 and w2['returned_version'] == 1
        assert [h['client_seq'] for h in audit['returned_document']['history']] == [1, 2]
        config = config_map[w1['config_id']]
        for w in (w1, w2):
            assert w['attempts'][-1]['command_name'] == 'findAndModify'
            assert w['attempts'][-1]['wire_write_concern']['w'] == config['write_concern']
        assert read['attempts'][-1]['wire_read_concern']['level'] == config['read_concern']
        if config['causal_session']:
            assert w1['session_id'] and w1['session_id'] == w2['session_id'] == read['session_id']
            for op in (w1, w2, read):
                assert op['explicit_causal_session']
                assert op['attempts'][-1]['wire_session_id'] == op['session_id']
            assert 'afterClusterTime' in read['attempts'][-1]['wire_read_concern']
        else:
            assert all(op['session_id'] is None and not op['explicit_causal_session']
                       for op in (w1, w2, read))
        assert init['attempts'][-1]['wire_write_concern']['w'] == 3
    return {'experiment_id': manifest['experiment_id'], 'verified_trials': len(groups),
            'verified_records': len(records), 'artifact_checksums': 'pass',
            'source_checksums': 'pass', 'targets': {k: sorted(v) for k, v in targets.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--expect-clean', action='store_true')
    args = parser.parse_args()
    print(json.dumps(validate(args.directory, args.expect_clean), indent=2))


if __name__ == '__main__':
    main()
