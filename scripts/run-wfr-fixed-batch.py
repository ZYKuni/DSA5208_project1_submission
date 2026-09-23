"""Run the frozen 0922 WFR queue serially; stop on any runner/validation failure."""
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
PLAN = 'wfr-v2-fixed-0922'


def main():
    output = ROOT/'results/batches'/PLAN
    output.mkdir(parents=True, exist_ok=False)
    state = {'plan_id': PLAN, 'started_at': datetime.now(timezone.utc).isoformat(),
             'status': 'running', 'completed': [], 'current': None}
    def save():
        target = output/'status.json'
        temporary = output/'status.tmp'
        temporary.write_text(json.dumps(state, indent=2)+'\n')
        temporary.replace(target)
    save()
    try:
        for stage, count in [('pilot', 20), ('formal', 100)]:
            for scenario in ['secondary-stop', 'primary-crash', 'replication-partition']:
                if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
                    raise RuntimeError('Source checkout is not clean; queue stopped')
                run_id = f'c-wfr-v2-{scenario}-{stage}-0922'
                state['current'] = run_id
                save()
                command = [sys.executable, 'scripts/run-fault-experiment.py', '--workload', 'wfr',
                           '--experiment-id', run_id, '--scenario', scenario, '--configs', 'C1', 'C4',
                           '--trials', str(count), '--sample-stage', stage, '--wfr-protocol',
                           'wfr-committed-dependency-v2', '--plan-id', PLAN, '--no-retry-writes']
                print(f'Starting {run_id}: {count} fixed attempts/config', flush=True)
                with (output/f'{run_id}.log').open('x') as log:
                    subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
                validation = subprocess.check_output([sys.executable, 'scripts/validate-wfr-run.py',
                                                     f'results/raw/{run_id}'], cwd=ROOT, text=True)
                verdict = json.loads(validation)
                (output/f'{run_id}.validation.json').write_text(validation)
                state['completed'].append(verdict)
                save()
                print(validation, flush=True)
        state['status'] = 'completed'
        state['current'] = None
    except BaseException as error:
        state['status'] = 'stopped'
        state['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        state['updated_at'] = datetime.now(timezone.utc).isoformat()
        save()


if __name__ == '__main__':
    main()
