"""Validate S2/S3 evidence; optionally archive immutable runs under results/archive."""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import tarfile


def check(condition, message):
    if not condition:
        raise ValueError(message)


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def verify_stop_order(events, trial, work):
    # Never compare the host wall clock with the runner VM clock.
    requests = [(i,e) for i,e in enumerate(events) if e.get('event')=='controller_request'
                and e.get('trial_id')==trial['trial_id'] and e.get('action')=='crash']
    check({e['node'] for _,e in requests}==set(trial['stopped_nodes']), 'Stop requests missing')
    check(len(requests)==len(trial['stopped_nodes']), 'Duplicate stop requests')
    receipts=[]
    for index,request in requests:
        matched=[(i,e) for i,e in enumerate(events) if e.get('event')=='controller_response'
                 and e.get('request_id')==request['request_id']]
        check(len(matched)==1, 'Stop response missing or duplicated')
        response_index,response=matched[0]
        check(response_index>index and response.get('ok') and response.get('action')=='crash'
              and response.get('node')==request['node'] and response['state']['Running'] is False,
              'Stop handshake not confirmed')
        receipts.append(response)
    check(bool(work), 'No workload operations')
    if all('monotonic_ns' in r for r in receipts) and all('monotonic_started_ns' in r for r in work):
        check(min(r['monotonic_started_ns'] for r in work)>=max(r['monotonic_ns'] for r in receipts),
              'Workload preceded runner receipt (monotonic)')
        return 'runner-monotonic'
    # Historical UTC fields below both originate in the runner process.
    check(min(timestamp(r['started_at']) for r in work)>=max(timestamp(r['time']) for r in receipts),
          'Workload preceded runner receipt (legacy runner UTC)')
    return 'legacy-runner-UTC'


def validate(directory):
    directory = Path(directory)
    manifest = json.loads((directory / 'manifest.json').read_text())
    check(manifest['protocol'] == 'secondary-stop-v1', 'Unexpected protocol')
    check(manifest['status'] == 'completed', 'Incomplete run')
    for name, digest in manifest['artifact_sha256'].items():
        path = (directory / name).resolve()
        check(path.is_relative_to(directory.resolve()), 'Unsafe artifact path')
        check(hashlib.sha256(path.read_bytes()).hexdigest() == digest, 'Artifact mismatch: '+name)
    with tarfile.open(directory / 'source.tar.gz') as archive:
        for name, digest in manifest['source_sha256'].items():
            member = archive.extractfile(name)
            check(member is not None and hashlib.sha256(member.read()).hexdigest() == digest,
                  'Source mismatch: '+name)
    rows = [json.loads(line) for line in (directory / 'operations.jsonl').read_text().splitlines()]
    events = [json.loads(line) for line in (directory / 'events.jsonl').read_text().splitlines()]
    order_methods = set()
    trials = [row for row in rows if row['record_type'] == 'trial_result']
    check(len({t['trial_id'] for t in trials}) == len(trials) == manifest['completed_trials'], 'Duplicate/count mismatch')
    counts = Counter(t['config_id'] for t in trials)
    check(dict(counts) == {c['config_id']: manifest['trials_per_config'] for c in manifest['configs']}, 'Cell counts mismatch')
    for trial in trials:
        tid = trial['trial_id']
        check(trial['recovery_completed'] and not trial['fault_error'], tid+': recovery failed')
        evidence = directory / 'evidence' / tid
        fault = json.loads((evidence / 'fault-confirmation.json').read_text())
        targets = trial['stopped_nodes']
        check(len(targets) == (1 if manifest['scenario']=='secondary-stop' else 2), 'Wrong fault size')
        check(trial['primary_before'] not in targets, 'Primary targeted')
        check({r['node'] for r in fault['responses']} == set(targets), 'Missing stop response')
        check(all(r['ok'] and r['state']['Running'] is False for r in fault['responses']), 'Stop not confirmed')
        if manifest['scenario'].endswith('settled'):
            check(fault['role_before_workload']['isWritablePrimary'] is False, 'Settled condition missing')
        convergence = json.loads((evidence / 'convergence.json').read_text())
        nodes = convergence['nodes']
        check(convergence['stable_seconds'] >= 1 and len(nodes)==3, 'Recovery gate missing')
        check(sorted(n['role'] for n in nodes)==['PRIMARY','SECONDARY','SECONDARY'], 'Roles not healthy')
        check(all(n['document']==nodes[0]['document'] for n in nodes), 'Documents not converged')
        operations = [r for r in rows if r['trial_id']==tid and r['record_type']=='operation']
        work = [r for r in operations if r['phase']=='workload']
        by_name = {r['operation']: r for r in operations}
        check(by_name['initialize']['status']=='success', 'Setup failed')
        order_methods.add(verify_stop_order(events, trial, work))
        for previous, following in zip(work, work[1:]):
            if 'monotonic_ended_ns' in previous and 'monotonic_started_ns' in following:
                check(previous['monotonic_ended_ns'] <= following['monotonic_started_ns'], 'Concurrent requests')
            else:
                check(timestamp(previous['ended_at']) <= timestamp(following['started_at']), 'Concurrent requests')
        for op in work:
            check(op['explicit_causal_session']==trial['causal_session'], 'Wrong explicit session')
            if trial['causal_session']:
                check(op['session_id']==work[0]['session_id'] and bool(op['session_id']), 'Causal session changed')
            for attempt in op['attempts']:
                if trial['causal_session']:
                    check(attempt['wire_session_id']==op['session_id'], 'Wire causal session mismatch')
                if op['operation']=='successor_read':
                    concern=attempt['wire_read_concern'] or {}
                    check(concern.get('level')==trial['read_concern'], 'Wrong read concern')
                    if trial['causal_session']:
                        check('afterClusterTime' in concern, 'Causal read dependency missing')
            if not trial['retry_writes']:
                check(op['retry_count']==0, 'Unexpected retry')
            if op['operation'].startswith('write_'):
                for attempt in op['attempts']:
                    check(attempt['wire_write_concern']['w']==trial['write_concern'], 'Wrong write concern')
        w1=by_name['write_1']; w2=by_name.get('write_2'); read=by_name.get('successor_read')
        if w1['status']!='success' or (manifest['experiment']=='mw' and (not w2 or w2['status']!='success')):
            check(trial['classification']=='inconclusive' and trial['is_violation'] is None, 'Failed write treated as consistency result')
        elif manifest['experiment']=='ryw':
            check(read is not None, 'RYW successor missing')
            if read['status']=='success':
                expected = read['returned_version'] is None or read['returned_version']<1
                check(trial['is_violation'] is expected, 'Incorrect RYW verdict')
            else:
                check(trial['is_violation'] is None, 'Unavailable read treated as verdict')
        elif trial['classification']=='no_violation':
            check(w1['returned_document']['history']==[] and w1['returned_document']['version']==0, 'MW initial state')
            pre=w2['returned_document']; final=by_name['recovered_primary_audit']['returned_document']
            check(pre['version']==1 and [h['operation_id'] for h in pre['history']]==[w1['operation_id']], 'MW predecessor missing')
            check(final['version']==2 and [h['operation_id'] for h in final['history']]==[w1['operation_id'],w2['operation_id']], 'MW final order')
    return {'experiment_id':manifest['experiment_id'], 'trials_checked':len(trials),
            'artifacts_checked':len(manifest['artifact_sha256']), 'status':'pass',
            'stop_order_evidence': sorted(order_methods)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--archive', action='store_true')
    args=parser.parse_args()
    result=validate(args.directory)
    if args.archive:
        root=Path(__file__).resolve().parents[1]
        destination=root/'results/archive'/result['experiment_id']
        shutil.copytree(args.directory,destination)
        control=root/'results/fault-control'/result['experiment_id']
        if control.exists():
            shutil.copytree(control,destination/'host-control')
        result['archive']=str(destination)
    print(json.dumps(result,indent=2))

if __name__=='__main__':
    main()
