"""Read-only validation of fault evidence; --archive explicitly copies the run."""
import argparse
import hashlib
import json
import shutil
import tarfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def validate(directory):
    manifest=json.loads((directory/'manifest.json').read_text())
    for name,digest in manifest['artifact_sha256'].items():
        target=(directory/name).resolve()
        if not target.is_relative_to(directory.resolve()):
            raise ValueError('Artifact escapes run directory')
        assert hashlib.sha256(target.read_bytes()).hexdigest()==digest,name
    with tarfile.open(directory/'source.tar.gz') as archive:
        for name,digest in manifest['source_sha256'].items():
            assert hashlib.sha256(archive.extractfile(name).read()).hexdigest()==digest,name
    rows=[json.loads(line) for line in (directory/'operations.jsonl').read_text().splitlines()]
    grouped=defaultdict(list)
    for row in rows:
        grouped[row['trial_id']].append(row)
    results=[r for r in rows if r['record_type']=='trial_result']
    assert len(results)==manifest['completed_trials']
    if manifest['status']=='completed':
        assert len(results)==manifest['trials_per_config']*len(manifest['configs'])
    configs={c['config_id']:c for c in manifest['configs']}
    for result in results:
        assert result['recovery_completed']
        if manifest.get('recovery_gate') == 'direct-document-convergence-1s':
            convergence=json.loads((directory/'evidence'/result['trial_id']/'convergence.json').read_text())
            nodes=convergence['nodes']
            assert len(nodes)==3 and convergence['stable_seconds']>=1
            assert all(n['document']==nodes[0]['document'] for n in nodes)
            assert sorted(n['role'] for n in nodes)==['PRIMARY','SECONDARY','SECONDARY']
        assert result['primary_after']!=result['primary_before'] and result['primary_after']
        assert result['term_after']>result['term_before']
        operations={r['operation']:r for r in grouped[result['trial_id']] if r['record_type']=='operation'}
        w1=operations.get('write_1')
        w2=operations.get('write_2')
        for write in [w for w in (w1,w2) if w]:
            cfg=configs[result['config_id']]
            if write['status']=='success':
                assert write['attempts'][-1]['wire_write_concern']['w']==cfg['write_concern']
                assert write['returned_document'] is not None
                if manifest['diagnostic_journal']:
                    assert write['attempts'][-1]['wire_write_concern']['j'] is True
            if cfg['causal_session']:
                assert write['session_id'] and write['explicit_causal_session']
            else:
                assert write['session_id'] is None
        if w1 and w2:
            assert w1['status']=='success'
            assert datetime.fromisoformat(w1['ended_at'].replace("Z", "+00:00"))<=datetime.fromisoformat(w2['started_at'].replace("Z", "+00:00"))
            if configs[result['config_id']]['causal_session']:
                assert w1['session_id']==w2['session_id']
        if result['rollback']=='confirmed':
            evidence=json.loads((directory/'evidence'/result['trial_id']/'rollback-evidence.json').read_text())
            assert evidence['rollback_completed_log']
            assert any(d['contains_w1'] for d in evidence['matching_rollback_documents'])
            assert result['classification']=='rollback_observed'
            assert result['is_violation'] is None
        if result['expected_unacknowledged_w1']:
            assert w1['status']!='success' and w2 is None
            assert result['write_1_survived'] is None
    return {'experiment_id':manifest['experiment_id'],'status':manifest['status'],
            'trials_checked':len(results),'checksums':'pass','source_snapshot':'pass',
            'sequential_writes_and_sessions':'pass','rollback_classification':'pass'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--archive',action='store_true')
    args=parser.parse_args()
    result=validate(args.directory)
    if args.archive:
        destination=ROOT/'results/archive'/result['experiment_id']
        shutil.copytree(args.directory,destination)
        result['archive']=str(destination)
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    main()
