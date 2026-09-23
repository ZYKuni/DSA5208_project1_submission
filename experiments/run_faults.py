"""Member A: isolated Primary crash and replication-partition diagnostics."""
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

from analysis.analyze import export_csv, percentile
from experiments.common import DEFAULT_URI, JsonlWriter, make_client, execute_operation, utc_now, json_safe
from experiments.configs import get_config, session_scope
from experiments.run_matrix import source_files
from experiments.test_monotonic_writes import history_entry
from experiments.fault_diagnostics import (Events, Controller, ElectionObserver, wait_healthy,
    evidence_on_node, assess_fault, read_rollback_evidence, wait_document_convergence)

ROOT = Path(__file__).resolve().parents[1]


def save(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(obj), indent=2) + '\n')


def trial(client, ops, events, controller, args, config, number, directory):
    trial_id=f'{args.experiment_id}-mw-{config.config_id}-{args.scenario}-trial-{number:04d}'
    meta={**config.to_log_record(), 'schema_version':2,'experiment_id':args.experiment_id,
          'trial_id':trial_id,'experiment':'mw','scenario':args.scenario,'client_id':'client-A',
          'document_id':trial_id,'retry_writes':args.retry_writes,'retry_reads':False,
          'timeout_ms':args.timeout_ms,'diagnostic_journal':args.scenario=='replication-partition'}
    before=wait_healthy(client)
    old=before['hello']['primary'].split(':')[0]
    trial_directory=directory/'evidence'/trial_id
    save(trial_directory/'cluster-before.json',before)
    started_at=utc_now()
    events.emit('trial_start',trial_id=trial_id,primary=old,term=before['status']['term'])
    base=client[args.database]['mw_documents']
    setup=base.with_options(write_concern=WriteConcern(w=3,wtimeout=args.timeout_ms))
    init=execute_operation(client,ops,meta,'initialize',lambda:setup.insert_one({
        '_id':trial_id,'experiment_id':args.experiment_id,'version':0,'client_seq':0,
        'value':'initial','writer':'client-A','depends_on':None,'history':[]}),
        phase='setup',written_version=0,effective_options={'write_concern':3})
    kwargs={'w':config.write_concern_w,'wtimeout':args.timeout_ms}
    if args.scenario=='replication-partition':
        kwargs['j']=True
    collection=base.with_options(**{**config.collection_options(),'write_concern':WriteConcern(**kwargs)})
    w1=w2=audit=before_crash=after_recovery=collected=None
    rollback_evidence={}
    fault=None
    monitor=None
    new_primary=None
    recovered=False
    after=None
    fault_error=None
    before_evidence=evidence_on_node(old,args.database,trial_id)
    save(trial_directory/'old-primary-before.json',before_evidence)
    try:
        if init['status']!='success':
            raise RuntimeError('Trial initialization failed')
        with session_scope(client,config) as session:
            if args.scenario=='replication-partition':
                monitor=ElectionObserver(old,events,trial_id)
                monitor.start()
                fault=controller.request('partition',old,trial_id=trial_id)
                # W1 is issued immediately; observation queries occur AFTER its acknowledgement.
            for seq in (1,2):
                name=f'write_{seq}'
                update={'$set':{'version':seq,'client_seq':seq,'depends_on':seq-1,'value':f'value-{seq}'},
                        '$push':{'history':history_entry(f'{trial_id}:{name}',seq)}}
                row=execute_operation(client,ops,meta,name,
                    lambda update=update:collection.find_one_and_update({'_id':trial_id},update,
                        upsert=False,return_document=ReturnDocument.BEFORE,session=session),
                    session=session,written_version=seq,returned_document=True,
                    effective_options={'write_concern':config.write_concern_w,'journal':kwargs.get('j'),
                                       'read_preference':'primary (write command)','preimage':True})
                if seq==1:
                    w1=row
                else:
                    w2=row
                if row['status']!='success' or row.get('returned_document') is None:
                    break
                if seq==1:
                    if row['target_node']!=old+':27017':
                        raise RuntimeError('W1 executed on a different Primary; fault window not established')
                    before_crash=evidence_on_node(old,args.database,trial_id)
                    save(trial_directory/'old-primary-after-w1.json',before_crash)
                    if monitor is None:
                        monitor=ElectionObserver(old,events,trial_id)
                        monitor.start()
                    crash=controller.request('crash',old,trial_id=trial_id)
                    if fault is None:
                        fault=crash
                    # No wait-for-primary gate: immediately issue W2 while election is in progress.
        if monitor:
            new_primary=monitor.finish(wait_seconds=40)
        if not new_primary:
            raise RuntimeError('No new Primary observed within the diagnostic window')
        verifier=base.with_options(read_preference=ReadPreference.PRIMARY,read_concern=ReadConcern('local'))
        audit=execute_operation(client,ops,meta,'primary_audit',lambda:verifier.find_one({'_id':trial_id}),
            phase='audit',returned_document=True,
            effective_options={'read_preference':'primary','read_concern':'local'})
        # Establish a majority-committed new branch BEFORE allowing old Primary to rejoin.
        marker=client[args.database]['fault_audit_markers'].with_options(write_concern=WriteConcern(w='majority',wtimeout=args.timeout_ms))
        barrier=execute_operation(client,ops,meta,'new_branch_barrier',
            lambda:marker.insert_one({'_id':trial_id,'experiment_id':args.experiment_id}),
            phase='audit',effective_options={'write_concern':'majority'})
        if barrier['status']!='success':
            raise RuntimeError('New branch majority barrier failed')
    except Exception as error:
        fault_error=f'{type(error).__name__}: {error}'
        events.emit('trial_error',trial_id=trial_id,error=fault_error)
    finally:
        if monitor:
            if new_primary is None:
                new_primary=monitor.finish()
            else:
                monitor.finish()
        controller.request('recover',old,trial_id=trial_id)
        after=wait_healthy(client)
        expected=(audit.get('returned_document') if audit and audit['status']=='success' else
                  base.with_options(read_preference=ReadPreference.PRIMARY,
                                    read_concern=ReadConcern('local')).find_one({'_id':trial_id}))
        convergence=wait_document_convergence(client,args.database,trial_id,expected)
        save(trial_directory/'convergence.json',convergence)
        after=wait_healthy(client)
        after_recovery=evidence_on_node(old,args.database,trial_id)
        save(trial_directory/'old-primary-after-recovery.json',after_recovery)
        save(trial_directory/'cluster-after.json',after)
        collected=controller.request('collect',old,trial_id=trial_id,since=started_at)
        rollback_evidence=read_rollback_evidence(ROOT,collected,trial_id,f'{trial_id}:write_1')
        save(trial_directory/'rollback-evidence.json',rollback_evidence)
        recovered=True
        events.emit('recovery_complete',trial_id=trial_id,primary=after['hello']['primary'])
    assessment=assess_fault(w1,w2,audit,before_crash,after_recovery,rollback_evidence)
    if fault_error and assessment['classification']=='no_violation':
        assessment.update(classification='inconclusive',is_violation=None,reason=fault_error)
    result={**meta,**assessment,'record_type':'trial_result','ended_at':utc_now(),
        'primary_before':old,'primary_after':new_primary['node'] if new_primary else None,
        'term_before':before['status']['term'],'term_after':after['status']['term'],
        'fault_confirmed_at':fault.get('ended_at') if fault else None,
        'election_observation':new_primary, 'recovery_completed':recovered,'fault_error':fault_error,
        'setup_status':init['status'],'write_1_status':w1['status'] if w1 else 'not_run',
        'write_2_status':w2['status'] if w2 else 'not_run',
        'write_2_attempt_count':w2['attempt_count'] if w2 else 0,
        'write_2_retry_count':w2['retry_count'] if w2 else 0,
        'write_2_latency_ms':w2['latency_ms'] if w2 else None,
        'expected_unacknowledged_w1':bool(args.scenario=='replication-partition' and
            config.write_concern_w=='majority' and w1 and w1['status']!='success'),
        'rbid_before':before_evidence['rbid'],'rbid_after':after_recovery['rbid'],
        'audit_status':audit['status'] if audit else 'not_run',
        'observation_status':'not_run','observation_stale':None}
    ops.write(result)
    return result


def summarize(directory, results):
    summary=[]
    for cfg in dict.fromkeys(t['config_id'] for t in results):
        ts=[t for t in results if t['config_id']==cfg]
        latencies=[t['write_2_latency_ms'] for t in ts if t['write_2_status']=='success']
        summary.append({'config_id':cfg,'trials':len(ts),
            'primary_changed':sum(t['primary_after'] is not None and t['primary_after']!=t['primary_before'] for t in ts),
            'w1_acknowledged':sum(t['write_1_status']=='success' for t in ts),
            'w2_success':sum(t['write_2_status']=='success' for t in ts),
            'w2_failure':sum(t['write_2_status']=='failure' for t in ts),
            'w2_timeout':sum(t['write_2_status']=='timeout' for t in ts),
            'w2_not_run':sum(t['write_2_status']=='not_run' for t in ts),
            'w2_retried_trials':sum(t['write_2_retry_count']>0 for t in ts),
            'w2_wire_retries':sum(t['write_2_retry_count'] for t in ts),
            'w1_survived':sum(t['write_1_survived'] is True for t in ts),
            'rollback_confirmed':sum(t['rollback']=='confirmed' for t in ts),
            'no_violation':sum(t['classification']=='no_violation' for t in ts),
            'candidate_violation':sum(t['candidate_violation'] for t in ts),
            'inconclusive':sum(t['classification']=='inconclusive' for t in ts),
            'expected_unacknowledged_w1':sum(t['expected_unacknowledged_w1'] for t in ts),
            'recovery_completed':sum(t['recovery_completed'] for t in ts),
            'w2_success_p50_ms':percentile(latencies,.5),'w2_success_p95_ms':percentile(latencies,.95)})
    save(directory/'summary.json',summary)
    export_csv(directory/'summary.csv',summary)
    export_csv(directory/'trials.csv',results)
    operations=[json.loads(l) for l in (directory/'operations.jsonl').read_text().splitlines()]
    export_csv(directory/'operations.csv',[r for r in operations if r['record_type']=='operation'])
    return summary


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment-id',required=True)
    parser.add_argument('--scenario',choices=['primary-crash','replication-partition'],required=True)
    parser.add_argument('--configs',nargs='+',required=True)
    parser.add_argument('--trials',type=int,default=5)
    parser.add_argument('--timeout-ms',type=int,default=30000)
    parser.add_argument('--retry-writes',action=argparse.BooleanOptionalAction,default=True)
    parser.add_argument('--database',default='dsa5208_fault_experiments')
    args=parser.parse_args()
    if args.scenario=='replication-partition' and args.retry_writes:
        parser.error('Use --no-retry-writes for partition diagnostic')
    if not os.environ.get('FAULT_CONTROL'):
        parser.error('Start using scripts/run-fault-experiment.py on the host')
    directory=ROOT/'results/raw'/args.experiment_id
    directory.mkdir(parents=True,exist_ok=False)
    events=Events(directory/'events.jsonl')
    controller=Controller(events)
    ops=JsonlWriter(directory/'operations.jsonl')
    results=[]
    paths=source_files()+[ROOT/'docker/fault-compose.yml',ROOT/'config/init-replica-set.js']
    source_hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    with tarfile.open(directory/'source.tar.gz','w:gz') as archive:
        for path in paths:
            archive.add(path,arcname=str(path.relative_to(ROOT)))
    manifest={'schema_version':2,'experiment_id':args.experiment_id,'scenario':args.scenario,
        'experiment':'mw','status':'running','started_at':utc_now(),
        'configs':[get_config(c).to_log_record() for c in args.configs],
        'trials_per_config':args.trials,'timeout_ms':args.timeout_ms,
        'retry_writes':args.retry_writes,'retry_reads':False,
        'diagnostic_journal':args.scenario=='replication-partition',
        'git_base_commit':os.environ.get('SOURCE_BASE_COMMIT'),
        'git_worktree_status':os.environ.get('SOURCE_WORKTREE_STATUS'),
        'source_sha256':source_hashes,'python':platform.python_version(),'pymongo':pymongo.version,
        'lab_project':'dsa5208-mw-fault','original_lab_modified':False,
        'recovery_gate':'direct-document-convergence-1s',
        'fault_mode':'SIGKILL with restart=no; selective replication-network disconnect in partition scenario',
        'argv':sys.argv}
    save(directory/'manifest.json',manifest)
    code=0
    try:
        with make_client(os.environ.get('MONGODB_URI',DEFAULT_URI),timeout_ms=args.timeout_ms,
                         retry_writes=args.retry_writes) as client:
            manifest['cluster_before']=wait_healthy(client)
            if client[args.database]['mw_documents'].count_documents({'experiment_id':args.experiment_id}):
                raise RuntimeError('Experiment ID already exists in database')
            for cfg in args.configs:
                for number in range(1,args.trials+1):
                    result=trial(client,ops,events,controller,args,get_config(cfg),number,directory)
                    results.append(result)
                    print(json.dumps({'config':cfg,'trial':number,'classification':result['classification'],
                        'primary_before':result['primary_before'],'primary_after':result['primary_after'],
                        'w2_status':result['write_2_status'],'w2_retries':result['write_2_retry_count'],
                        'rollback':result['rollback'],'recovered':result['recovery_completed']},ensure_ascii=False),flush=True)
                    if result['fault_error']:
                        raise RuntimeError(result['fault_error'])
            manifest['cluster_after']=wait_healthy(client)
            manifest['status']='completed'
    except BaseException as error:
        manifest['status']='interrupted' if isinstance(error,KeyboardInterrupt) else 'failed'
        manifest['error']=f'{type(error).__name__}: {error}'
        code=1
    finally:
        ops.close()
        events.close()
        manifest['ended_at']=utc_now()
        manifest['completed_trials']=len(results)
        summary=summarize(directory,results)
        manifest['artifact_sha256']={str(p.relative_to(directory)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in directory.rglob('*') if p.is_file() and p.name!='manifest.json'}
        save(directory/'manifest.json',manifest)
    print(json.dumps({'experiment_id':args.experiment_id,'status':manifest['status'],
                      'summary':summary,'error':manifest.get('error')},indent=2),flush=True)
    return code


if __name__=='__main__':
    raise SystemExit(main())
