"""Host-side single-trial controller for the dedicated MR partition project."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import selectors
import subprocess
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
COMPOSE=['docker','compose','-f',str(ROOT/'docker-compose.mr.yml')]
NETWORK='dsa5208-mr_replication'
CONTAINER='dsa5208-mr-mongo3-1'

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z')
def run(args,**kw):
    return subprocess.run(args,cwd=ROOT,text=True,capture_output=True,check=True,timeout=45,**kw)
def networks():
    return json.loads(run(['docker','inspect',CONTAINER]).stdout)[0]['NetworkSettings']['Networks']

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--config',choices=['C1','C4'],required=True);ap.add_argument('--resume-after-heal',action='store_true',help='C4: retry a read in the original session after healing');a=ap.parse_args()
    if a.resume_after_heal and a.config != 'C4':
        ap.error('--resume-after-heal requires --config C4')
    nets=networks()
    if NETWORK not in nets or 'dsa5208-mr_client3' not in nets:
        raise SystemExit('Preflight failed: both replication and client3 must be connected.')
    rid=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    out=ROOT/'results/pilot/mr/network_partition/debug'/f'mr-{a.config}-{rid}'
    out.mkdir(parents=True,exist_ok=False)
    manifest={'diagnostic_format':'dsa5208-mr-partition-1','formal':False,'run_id':rid,'base_config':a.config,
      'started_at':now(),'git_commit':run(['git','rev-parse','HEAD']).stdout.strip(),
      'git_status':run(['git','status','--porcelain']).stdout,'events':[], 'recovery_verified':False,
      'resume_after_heal':a.resume_after_heal,'source_sha256':{}}
    for rel in ['docker-compose.mr.yml','experiments/mr_partition_probe.py','experiments/common.py','scripts/run_mr_partition.py']:
        data=(ROOT/rel).read_bytes();manifest['source_sha256'][rel]=hashlib.sha256(data).hexdigest()
        dest=out/'source'/rel;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
    proc=None;partition_attempted=False
    try:
        with (out/'stderr.txt').open('w') as err, (out/'events.jsonl').open('w') as events:
            proc=subprocess.Popen(COMPOSE+['exec','-T','client','/opt/venv/bin/python','-u','experiments/mr_partition_probe.py',a.config,rid]+(['--resume-after-heal'] if a.resume_after_heal else []),cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err)
            sel=selectors.DefaultSelector();sel.register(proc.stdout,selectors.EVENT_READ)
            deadline=time.monotonic()+80;buffer=b'';finished=False
            try:
                while not finished:
                    if time.monotonic()>deadline: raise TimeoutError('Diagnostic exceeded 80 seconds')
                    if not sel.select(timeout=1): continue
                    chunk=os.read(proc.stdout.fileno(),65536)
                    if not chunk: raise RuntimeError('Probe exited without result; inspect stderr.txt')
                    buffer+=chunk
                    while b'\n' in buffer:
                        line,buffer=buffer.split(b'\n',1)
                        e=json.loads(line);events.write(json.dumps(e,ensure_ascii=False)+'\n');events.flush()
                        if e['kind']=='ready_for_partition':
                            partition_attempted=True
                            run(['docker','network','disconnect',NETWORK,CONTAINER])
                            if NETWORK in networks(): raise RuntimeError('Disconnect verification failed')
                            manifest['events'].append({'action':'partition','completed_at':now()})
                            print('Partition active; running tagged reads...',flush=True)
                            proc.stdin.write(b'partitioned\n');proc.stdin.flush()
                        elif e['kind']=='ready_for_recovery':
                            if not a.resume_after_heal or not partition_attempted:
                                raise RuntimeError('Unexpected recovery request')
                            if NETWORK not in networks():
                                run(['docker','network','connect','--alias','mongo3',NETWORK,CONTAINER])
                            if NETWORK not in networks():
                                raise RuntimeError('Reconnect verification failed')
                            manifest['events'].append({'action':'heal_for_same_session_read','completed_at':now(),
                                                       'session_id':e['session_id']})
                            print('Network restored; reading again in the original session...',flush=True)
                            proc.stdin.write(b'healed\n');proc.stdin.flush()
                        elif e['kind']=='diagnostic_result':
                            (out/'result.json').write_text(json.dumps(e,ensure_ascii=False,indent=2)+'\n')
                            print('Result:',e['status'],'versions:',e['read_versions'],'violation:',e['violation'],flush=True)
                            if 'post_heal_read' in e:
                                print('Post-heal read:',json.dumps(e['post_heal_read'],ensure_ascii=False),flush=True)
                            finished=True
            finally: sel.close()
    except BaseException as exc:
        manifest['controller_error']={'type':type(exc).__name__,'message':str(exc)}
        raise
    finally:
        try:
            if partition_attempted and NETWORK not in networks():
                run(['docker','network','connect','--alias','mongo3',NETWORK,CONTAINER])
            manifest['events'].append({'action':'network_restored','completed_at':now()})
            if proc is not None:
                try: proc.wait(timeout=10)
                except subprocess.TimeoutExpired: proc.terminate();proc.wait(timeout=5)
            # Verify original trial document converged, not merely a new health marker.
            check="""import sys,time,json
from pymongo import MongoClient
from pymongo.read_preferences import SecondaryPreferred
rid=sys.argv[1]; expected=json.loads(sys.argv[2]); clients=[MongoClient('mongodb://'+n+':27017/?directConnection=true',timeoutMS=3000) for n in ('mongo1','mongo2','mongo3')]
end=time.monotonic()+25
while True:
 h=[c.admin.command('hello') for c in clients]
 d=[c.dsa5208_mr_partition.probe.with_options(read_preference=SecondaryPreferred()).find_one({'_id':rid}) for c in clients]
 versions=[None if x is None else x['version'] for x in d]
 if sum(bool(x.get('isWritablePrimary')) for x in h)==1 and sum(bool(x.get('secondary')) for x in h)==2 and len(set(versions))==1 and (expected is None or versions[0]==expected):
  print(json.dumps({'versions':versions,'roles_ok':True}));break
 if time.monotonic()>end: raise RuntimeError('Recovery convergence timed out')
 time.sleep(.3)
"""
            expected=None
            if (out/'result.json').exists():
                result=json.loads((out/'result.json').read_text())
                writes=[o['version'] for o in result['operations'] if o['operation']=='write' and o['status']=='success']
                expected=max(writes) if writes else None
            r=run(COMPOSE+['exec','-T','client','/opt/venv/bin/python','-c',check,rid,json.dumps(expected)])
            manifest['recovery']=json.loads(r.stdout);manifest['recovery_verified']=True
            print('Recovery verified:',r.stdout.strip())
        except Exception as exc:
            manifest['recovery_error']=str(exc)
            print('RECOVERY FAILED: stop further trials. Inspect manifest.json.')
        finally:
            manifest['completed_at']=now();(out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
            print('Evidence:',out)
        if not manifest['recovery_verified']: raise SystemExit(2)

if __name__=='__main__': main()
