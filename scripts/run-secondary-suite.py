"""Run a serial Secondary fault matrix; formal runs require validated pilot evidence."""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('secondary_validator',ROOT/'scripts/validate-secondary-run.py')
VALIDATOR=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)
SCENARIOS={'s2':'secondary-stop','s3':'two-secondary-stop','s3-settled':'two-secondary-stop-settled'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prefix',required=True,help='New output prefix, e.g. a-repro-0920')
    parser.add_argument('--stage',choices=['pilot','formal'],required=True)
    parser.add_argument('--workloads',nargs='+',choices=['mw','ryw'],default=['mw','ryw'])
    parser.add_argument('--scenarios',nargs='+',choices=list(SCENARIOS),default=list(SCENARIOS))
    parser.add_argument('--pilot-prefix',help='Prefix of validated pilot runs for formal stage')
    args=parser.parse_args()
    if not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,40}',args.prefix):parser.error('Invalid prefix')
    if args.stage=='formal' and not args.pilot_prefix:parser.error('Formal requires --pilot-prefix')
    plans=[]
    for tag in args.scenarios:
        for workload in args.workloads:
            run=f'{args.prefix}-{tag}-{workload}-{args.stage}'
            output=ROOT/'results/raw'/run
            if output.exists():parser.error(f'Output already exists: {run}')
            if args.stage=='formal':
                pilot=ROOT/'results/raw'/f'{args.pilot_prefix}-{tag}-{workload}-pilot'
                VALIDATOR.validate(pilot)
                manifest=json.loads((pilot/'manifest.json').read_text())
                if not (manifest['sample_stage']=='pilot' and manifest['trials_per_config']>=20
                        and manifest['timeout_ms']==5000 and manifest['retry_writes'] is False
                        and manifest['experiment']==workload and manifest['scenario']==SCENARIOS[tag]
                        and {c['config_id'] for c in manifest['configs']}=={'C1','C4'}):
                    parser.error(f'Pilot does not match fixed protocol: {pilot}')
            plans.append((run,tag,workload))
    for run,tag,workload in plans:
        cmd=[sys.executable,str(ROOT/'scripts/run-fault-experiment.py'),'--experiment-id',run,
             '--scenario',SCENARIOS[tag],'--workload',workload,'--configs','C1','C4',
             '--trials','100' if args.stage=='formal' else '20','--sample-stage',args.stage,
             '--timeout-ms','5000','--no-retry-writes']
        subprocess.run(cmd,cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'scripts/validate-secondary-run.py'),
                        str(ROOT/'results/raw'/run),'--archive'],cwd=ROOT,check=True)

if __name__=='__main__':main()
