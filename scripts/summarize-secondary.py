"""Export per-run Secondary fault statistics without mixing protocols or stages."""
import argparse,csv,json,math
from collections import Counter
from pathlib import Path


def percentile(values,q):
    if not values:return None
    ordered=sorted(values)
    return ordered[max(0,math.ceil(q*len(ordered))-1)]


def summarize(directory):
    manifest=json.loads((directory/'manifest.json').read_text())
    if manifest.get('protocol')!='secondary-stop-v1' or manifest.get('status')!='completed':return []
    rows=[json.loads(line) for line in (directory/'operations.jsonl').read_text().splitlines()]
    result=[]
    for config in manifest['configs']:
        cid=config['config_id']
        trials=[r for r in rows if r['record_type']=='trial_result' and r['config_id']==cid]
        ops=[r for r in rows if r['record_type']=='operation' and r['config_id']==cid and r['phase']=='workload']
        classifications=Counter(t['classification'] for t in trials)
        evaluable=sum(t['is_violation'] is not None for t in trials)
        base={'run':directory.name,'workload':manifest['experiment'],'scenario':manifest['scenario'],
              'stage':manifest['sample_stage'],'config':cid,'trials':len(trials),
              'evaluable':evaluable,'confirmed_violations':sum(t['is_violation'] is True for t in trials),
              'violation_rate':sum(t['is_violation'] is True for t in trials)/evaluable if evaluable else None,
              'inconclusive':classifications['inconclusive'],'invalid':classifications['invalid_trial'],
              'candidate':classifications['candidate_violation'],
              'recovered':sum(t['recovery_completed'] for t in trials),
              'primary_writable_at_start':sum(t['writable_before_workload'] is True for t in trials),
              'timeout_ms':manifest['timeout_ms'],'retry_writes':manifest['retry_writes'],
              'source_sha256':manifest['source_sha256']['experiments/run_secondary_faults.py']}
        # Do not pool read/write latency or omit failed attempts from availability.
        for kind,names in [('w1',{'write_1'}),('w2',{'write_2'}),('read',{'successor_read'})]:
            selected=[op for op in ops if op['operation'] in names]
            statuses=Counter(op['status'] for op in selected)
            base[kind+'_attempted']=len(selected)
            base[kind+'_skipped']=len(trials)-len(selected) if kind!='w2' or manifest['experiment']=='mw' else None
            for status in ['success','failure','timeout']:base[kind+'_'+status]=statuses[status]
            base[kind+'_availability']=statuses['success']/len(selected) if selected else None
            for status in ['success','failure','timeout']:
                values=[op['latency_ms'] for op in selected if op['status']==status]
                for label,q in [('p50',.5),('p95',.95)]:base[f'{kind}_{status}_{label}_ms']=percentile(values,q)
        result.append(base)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories',nargs='+',type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    rows=[r for directory in args.directories for r in summarize(directory)]
    if not rows:raise SystemExit('No completed Secondary runs')
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'secondary-statistics.json').write_text(json.dumps(rows,indent=2)+'\n')
    with (args.output/'secondary-statistics.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    print(json.dumps({'runs':len(args.directories),'cells':len(rows),'trials':sum(r['trials'] for r in rows)}))

if __name__=='__main__':main()
