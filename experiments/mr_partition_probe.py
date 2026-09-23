"""One diagnostic trial. Host controller owns partition and recovery.
Not schema-v1: the effective read preference is tagged secondary.
"""
import json
import os
import sys
import time
from contextlib import ExitStack
import platform
import pymongo
from pymongo import MongoClient
from pymongo.read_preferences import Secondary
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
# Support both `python -m experiments...` and historical direct scripts.
import sys
from pathlib import Path
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.common import NodeRecorder, record_operation, classify_error, utc_now


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


def classify_pair(first, second):
    if first != 2:
        return 'invalid_trial', None
    return 'success', second is None or second < first


def main():
    config, document_id = sys.argv[1:3]
    strong = config == 'C4'
    resume = '--resume-after-heal' in sys.argv[3:]
    if resume and not strong:
        raise SystemExit('--resume-after-heal requires C4')
    ops = []
    evidence = {'kind': 'diagnostic_result', 'base_config': config,
                'status': 'error', 'violation': None, 'operations': ops,
                'document_id': document_id,
                'effective_settings': {'read_concern': 'majority' if strong else 'local',
                    'write_concern': 'majority' if strong else 1,
                    'read_preference': 'secondary',
                    'tag_sequence': [{'target':'mongo2'}, {'target':'mongo3'}],
                    'causal_session': strong, 'retry_reads': False, 'retry_writes': False,
                    'baseline_write_concern': 3},
                'started_at': utc_now(), 'read_versions': [],
                'diagnostic_format': 'zhou-jiahao-mr-partition-1',
                'scenario': 'network_partition', 'resume_after_heal': resume}
    options = dict(timeoutMS=7000, serverSelectionTimeoutMS=4000,
                   retryReads=False, retryWrites=False)
    try:
        with ExitStack() as stack:
            wr = NodeRecorder(); rr = NodeRecorder(); observer = NodeRecorder()
            writer = stack.enter_context(MongoClient(os.environ['MONGODB_URI'], event_listeners=[wr], **options))
            reader = stack.enter_context(MongoClient(os.environ['MONGODB_URI'], event_listeners=[rr], **options))
            direct = {n: stack.enter_context(MongoClient(f'mongodb://{n}:27017/?directConnection=true',event_listeners=[observer], **options)) for n in ('mongo2','mongo3')}
            if writer.admin.command('hello').get('primary') != 'mongo1:27017':
                raise RuntimeError('Precondition: mongo1 must be primary')
            for n,c in direct.items():
                if not c.admin.command('hello').get('secondary'):
                    raise RuntimeError(f'Precondition: {n} must be secondary')
            evidence['environment'] = dict(python_version=platform.python_version(),pymongo_version=pymongo.version,mongodb_version=writer.server_info()['version'])
            coll=writer.zhou_jiahao_mr_partition.probe
            record_operation(ops,wr,stage='baseline_w3',operation='write',document_id=document_id,requested_version=1,
                action=lambda: coll.with_options(write_concern=WriteConcern(w=3,wtimeout=5000)).insert_one({'_id':document_id,'version':1}))
            def observe(n,stage):
                return record_operation(ops,observer,stage=stage,operation='observe',document_id=document_id,
                    action=lambda: direct[n].zhou_jiahao_mr_partition.probe.with_options(read_preference=Secondary(),read_concern=ReadConcern('local')).find_one({'_id':document_id},max_time_ms=3000))
            for n in direct:
                d=observe(n,'baseline_'+n)
                if d is None or d['version']!=1: raise RuntimeError('Baseline not visible')
            emit({'kind':'ready_for_partition'})
            if input().strip() != 'partitioned': raise RuntimeError('Missing controller acknowledgement')
            def update():
                r=coll.with_options(write_concern=WriteConcern(w='majority' if strong else 1,wtimeout=5000)).update_one({'_id':document_id},{'$set':{'version':2}})
                if r.matched_count!=1: raise RuntimeError('Missing baseline')
                return r
            record_operation(ops,wr,stage='update_v2',operation='write',document_id=document_id,requested_version=2,action=update)
            deadline=time.monotonic()+5
            while True:
                d=observe('mongo2','check_newer')
                if d and d['version']==2: break
                if time.monotonic()>deadline: raise RuntimeError('Version 2 not visible on mongo2')
                time.sleep(.05)
            d=observe('mongo3','check_lagging')
            if not d or d['version']!=1:
                evidence['status']='invalid_trial'
                evidence['reason']='Version gap was not established'
            else:
                evidence['verified_gap']={'mongo2':2,'mongo3':1}
                session=stack.enter_context(reader.start_session(causal_consistency=strong))
                evidence['session_id']=str(session.session_id['id'])
                for n,stage in [('mongo2','first_read'),('mongo3','second_read')]:
                    c=reader.zhou_jiahao_mr_partition.probe.with_options(read_preference=Secondary(tag_sets=[{'target':n}]),read_concern=ReadConcern('majority' if strong else 'local'))
                    try:
                        d=record_operation(ops,rr,stage=stage,operation='read',document_id=document_id,
                            action=lambda: c.find_one({'_id':document_id},session=session,max_time_ms=3000))
                    except Exception as exc:
                        if not (resume and stage == 'second_read' and classify_error(exc) == 'timeout'):
                            raise
                        # Keep the original timeout classification and the session alive.
                        evidence['status']='timeout'
                        evidence['error']={'type':type(exc).__name__,'message':str(exc)}
                        follow={'status':'error','version':None,'node':None,
                                'session_id':str(session.session_id['id'])}
                        evidence['post_heal_read']=follow
                        emit({'kind':'ready_for_recovery','session_id':evidence['session_id'],
                              'original_status':'timeout','read_versions':evidence['read_versions']})
                        if input().strip() != 'healed':
                            raise RuntimeError('Missing recovery acknowledgement')
                        try:
                            d=record_operation(ops,rr,stage='post_heal_read',operation='read',document_id=document_id,
                                action=lambda: c.find_one({'_id':document_id},session=session,max_time_ms=3000))
                            follow['version']=None if d is None else d['version']
                            follow['node']=ops[-1]['node']
                            follow['same_session']=str(session.session_id['id']) == evidence['session_id']
                            follow['status']='success' if follow['same_session'] and follow['node']=='mongo3:27017' and follow['version']==2 else 'invalid_trial'
                        except Exception as recovery_exc:
                            follow['status']=classify_error(recovery_exc)
                            follow['error']={'type':type(recovery_exc).__name__,'message':str(recovery_exc)}
                            follow['node']=ops[-1]['node']
                        break
                    v=None if d is None else d['version']; evidence['read_versions'].append(v)
                    if ops[-1]['node']!=n+':27017' or (stage=='first_read' and v!=2):
                        evidence['status']='invalid_trial'; evidence['reason']='Target or first-read precondition failed';break
                else:
                    evidence['status'],evidence['violation']=classify_pair(*evidence['read_versions'])
    except Exception as exc:
        evidence['status']=classify_error(exc)
        evidence['violation']=None
        evidence['error']={'type':type(exc).__name__,'message':str(exc)}
    finally:
        evidence['completed_at']=utc_now();emit(evidence)


if __name__=='__main__': main()
