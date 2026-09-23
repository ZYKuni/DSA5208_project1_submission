"""Host-only controller for the isolated fault lab; no Docker socket in runner."""
from __future__ import annotations
import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = 'dsa5208-mw-fault'
NETWORK = PROJECT + '_replication'
COMPOSE = ['docker', 'compose', '-p', PROJECT, '-f', str(ROOT / 'docker/fault-compose.yml')]
NODES = ('mongo1', 'mongo2', 'mongo3')
WORKLOAD_MODULES = {'mw': 'experiments.run_faults', 'ryw': 'experiments.run_ryw_faults',
                    'mr': 'experiments.run_mr_faults', 'wfr': 'experiments.run_wfr_faults'}


def now():
    return datetime.now(timezone.utc).isoformat()


def command(args, *, check=True, timeout=120):
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=check)


def container(node):
    if node not in NODES:
        raise ValueError('Invalid fault target')
    cid = command(COMPOSE + ['ps', '-a', '-q', node]).stdout.strip()
    if not cid:
        raise RuntimeError('Fault-lab container not found')
    info = json.loads(command(['docker', 'inspect', cid]).stdout)[0]
    if info['Config']['Labels'].get('com.docker.compose.project') != PROJECT:
        raise RuntimeError('Refusing to control a different project')
    return cid, info


def reconnect(node):
    cid, info = container(node)
    if NETWORK not in info['NetworkSettings']['Networks']:
        command(['docker', 'network', 'connect', '--alias', node, NETWORK, cid])
    command(COMPOSE + ['start', node])


def recover_all():
    errors = []
    for node in NODES:
        try:
            reconnect(node)
        except Exception as error:
            errors.append(f'{node}: {error}')
    if errors:
        raise RuntimeError('; '.join(errors))


def handle(request, experiment_id):
    action, node = request['action'], request['node']
    cid, info = container(node)
    result = {'action': action, 'node': node, 'started_at': now()}
    if action == 'crash':
        command(['docker', 'kill', '--signal', 'SIGKILL', cid])
        state = json.loads(command(['docker', 'inspect', cid]).stdout)[0]['State']
        if state['Running']:
            raise RuntimeError('Fault target unexpectedly restarted')
        result['state'] = state
    elif action == 'partition':
        if NETWORK not in info['NetworkSettings']['Networks']:
            raise RuntimeError('Target is already partitioned')
        command(['docker', 'network', 'disconnect', NETWORK, cid])
        result['remaining_networks'] = list(json.loads(
            command(['docker', 'inspect', cid]).stdout)[0]['NetworkSettings']['Networks'])
    elif action == 'recover':
        reconnect(node)
    elif action == 'collect':
        trial = request['trial_id']
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', trial):
            raise ValueError('Invalid evidence directory')
        directory = ROOT / 'results/raw' / experiment_id / 'evidence' / trial
        directory.mkdir(parents=True, exist_ok=True)
        log = command(['docker', 'logs', '--since', request['since'], '--timestamps', cid], check=False)
        (directory / 'old-primary.log').write_text(log.stdout + log.stderr)
        listing = command(['docker', 'exec', cid, 'find', '/data/db/rollback', '-type', 'f',
                           '-name', '*.bson'], check=False)
        copied = []
        for source in listing.stdout.splitlines():
            if not source.startswith('/data/db/rollback/') or '..' in Path(source).parts:
                raise ValueError('Invalid rollback path')
            filename = '__'.join(Path(source).parts[4:])
            command(['docker', 'cp', cid + ':' + source, str(directory / filename)])
            copied.append(str((directory / filename).relative_to(ROOT)))
        result.update(log_path=str((directory / 'old-primary.log').relative_to(ROOT)),
                      rollback_files=copied)
    else:
        raise ValueError('Unsupported controller action')
    result.update(ended_at=now(), ok=True)
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment-id', required=True)
    parser.add_argument('--workload', choices=list(WORKLOAD_MODULES), default='mw',
                        help='Client-centric consistency workload to run')
    parser.add_argument('--scenario', choices=['primary-crash', 'replication-partition', 'normal', 'secondary-stop', 'two-secondary-stop', 'two-secondary-stop-settled'],
                        default='primary-crash', help='normal is supported only for WFR')
    parser.add_argument('--configs', nargs='+', choices=['C1','C2','C3','C4'], default=['C1','C4'])
    parser.add_argument('--trials', type=int, default=5)
    parser.add_argument('--sample-stage', choices=['pilot', 'formal'], default='pilot',
                        help='Secondary-stop runs only; formal requires at least 100 trials/config')
    parser.add_argument('--wfr-protocol', choices=['wfr-protocol-1', 'wfr-committed-dependency-v2'], default='wfr-protocol-1')
    parser.add_argument('--plan-id')
    parser.add_argument('--timeout-ms', type=int, default=None,
                        help='Default: WFR 15000 ms; MW/RYW 30000 ms')
    parser.add_argument('--retry-writes', action=argparse.BooleanOptionalAction, default=None,
                        help='Default: disabled for WFR (required), enabled for MW/RYW')
    args = parser.parse_args(argv)
    if args.timeout_ms is None:
        args.timeout_ms = 15000 if args.workload == 'wfr' else 30000
    if args.retry_writes is None:
        args.retry_writes = args.workload != 'wfr'
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,90}', args.experiment_id):
        parser.error('Invalid experiment ID')
    if args.trials < 1 or args.timeout_ms < 1 or len(set(args.configs)) != len(args.configs):
        parser.error('Invalid trials, timeout, or duplicate configurations')
    if args.sample_stage == 'formal' and args.trials < 100:
        parser.error('Formal stage requires at least 100 trials/config')
    if args.workload == 'wfr' and args.retry_writes:
        parser.error('WFR protocol 1 requires --no-retry-writes')
    if args.workload == 'wfr' and args.scenario in ('two-secondary-stop', 'two-secondary-stop-settled'):
        parser.error('WFR supports only one Secondary stop')
    if args.workload == 'wfr' and args.scenario == 'secondary-stop' and args.wfr_protocol != 'wfr-committed-dependency-v2':
        parser.error('WFR S2 requires committed-dependency v2')
    if args.workload == 'wfr' and args.sample_stage == 'formal' and (not args.plan_id or args.wfr_protocol != 'wfr-committed-dependency-v2'):
        parser.error('Formal WFR requires v2 and plan-id')
    if 'secondary-stop' in args.scenario and args.workload not in ('mw', 'ryw', 'mr', 'wfr'):
        parser.error('Secondary-stop protocol currently supports MW, RYW and MR')
    if args.workload == 'mr' and args.scenario not in ('secondary-stop', 'primary-crash'):
        parser.error('MR fault protocol supports secondary-stop and primary-crash')
    if args.scenario == 'normal' and args.workload != 'wfr':
        parser.error('This fault entry point supports normal only for WFR')
    if args.scenario == 'replication-partition' and args.retry_writes:
        parser.error('Partition diagnostic requires --no-retry-writes to avoid W1 retrying on a new primary')
    if (ROOT / 'results/raw' / args.experiment_id).exists():
        parser.error('Experiment output already exists')
    return args


def runner_command(args, runner_name):
    """Pure command construction: test dispatch without touching Docker."""
    secondary = 'secondary-stop' in args.scenario and args.workload in ('mw', 'ryw')
    module = 'experiments.run_secondary_faults' if secondary else WORKLOAD_MODULES[args.workload]
    return COMPOSE + ['run','--rm','--no-deps','--name',runner_name,
        '-e','SOURCE_BASE_COMMIT','-e','SOURCE_WORKTREE_STATUS','-e','FAULT_CONTROL',
        'runner','python','-m',module,
        *(['--workload', args.workload, '--sample-stage', args.sample_stage] if secondary else
          ['--sample-stage', args.sample_stage] if args.workload in ('mr', 'wfr') else []),
        *(['--wfr-protocol', args.wfr_protocol] + (['--plan-id', args.plan_id] if args.plan_id else [])
          if args.workload == 'wfr' else []),
        '--experiment-id',args.experiment_id,'--scenario',args.scenario,
        '--trials',str(args.trials),'--timeout-ms',str(args.timeout_ms),
        '--retry-writes' if args.retry_writes else '--no-retry-writes', '--configs',*args.configs]


def main(argv=None):
    args = parse_args(argv)
    base = ROOT / 'results/fault-control'
    base.mkdir(parents=True, exist_ok=True)
    with (base / 'lab.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        control = base / args.experiment_id
        control.mkdir(exist_ok=False)
        runner_name = PROJECT + '-run-' + args.experiment_id
        process = None
        try:
            print('Starting isolated fault lab (original baseline lab is unchanged)', flush=True)
            command(COMPOSE + ['up', '-d', '--wait', 'mongo1','mongo2','mongo3'])
            command(COMPOSE + ['exec','-T','mongo1','mongosh','--quiet','--file','/opt/init.js'])
            env = os.environ.copy()
            env['SOURCE_BASE_COMMIT'] = command(['git','-C',str(ROOT),'rev-parse','HEAD'], check=False).stdout.strip() or 'unavailable-source-package'
            env['SOURCE_WORKTREE_STATUS'] = command(['git','-C',str(ROOT),'status','--porcelain'], check=False).stdout
            if env['SOURCE_BASE_COMMIT'] == 'unavailable-source-package':
                env['SOURCE_WORKTREE_STATUS'] = 'No .git metadata; exact source_sha256 and source.tar.gz retained'
            env['FAULT_CONTROL'] = '/workspace/results/fault-control/' + args.experiment_id
            cmd = runner_command(args, runner_name)
            process = subprocess.Popen(cmd, env=env)
            seen = set()
            while process.poll() is None:
                for path in sorted(control.glob('*.request.json')):
                    if path.name in seen:
                        continue
                    request = json.loads(path.read_text())
                    try:
                        reply = handle(request, args.experiment_id)
                    except Exception as error:
                        reply = {'ok':False,'error':f'{type(error).__name__}: {error}', 'ended_at':now()}
                    destination = path.with_name(path.name.replace('.request.json','.response.json'))
                    temp = destination.with_suffix('.tmp')
                    temp.write_text(json.dumps(reply))
                    temp.replace(destination)
                    seen.add(path.name)
                time.sleep(0.05)
            return process.returncode
        finally:
            if process is not None and process.poll() is None:
                command(['docker','stop','--time','5',runner_name], check=False)
                process.wait(timeout=30)
            recover_all()
            (control / 'host-recovery.json').write_text(json.dumps({'recovered_at':now(),'nodes':NODES}))
            print('Host cleanup: all isolated-lab nodes started and reconnected', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
