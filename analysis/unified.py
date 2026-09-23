"""Read-only adapters for archived evidence. No experiment/runtime imports."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import fnmatch
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORMAL_A = {'20260908-mw-normal-001', '20260908-mw-crash-001', '20260908-mw-partition-001'}
WORKLOAD = {
    'MW': {'write_1': 'write', 'write_2': 'write'},
    'WFR': {'predecessor_read': 'read', 'dependent_write': 'write'},
    'RYW': {'predecessor_write': 'write', 'successor_read': 'read'},
    'MR': {'first_read': 'read', 'second_read': 'read'},
}
UNAVAILABLE = {'ConnectionFailure', 'AutoReconnect', 'NotPrimaryError', 'ServerSelectionTimeoutError',
               'NetworkTimeout'}


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def percentile(values, q=.95):
    values = sorted(v for v in values if type(v) in (int, float) and math.isfinite(v) and v >= 0)
    if not values:
        return None
    x = (len(values) - 1) * q
    a, b = math.floor(x), math.ceil(x)
    return values[a] + (values[b] - values[a]) * (x - a)


def outcome(op):
    status = op.get('status')
    if status in {'timeout', 'unavailable', 'not_run'}:
        return status
    error = op.get('error') or {}
    error_type = op.get('error_type') or (error.get('type') if isinstance(error, dict) else None)
    if status != 'success':
        return 'unavailable' if error_type in UNAVAILABLE else 'failure'
    if any(a.get('write_concern_error') or a.get('write_errors') for a in op.get('attempts', [])):
        return 'failure'
    return 'success'


def classification(row):
    """Respect source adjudication; never promote candidate or rollback to violation."""
    value = row.get('is_violation', row.get('violation'))
    label = row.get('classification', row.get('status'))
    if label in {'candidate_violation', 'invalid_trial', 'inconclusive', 'rollback_observed', 'incomplete'}:
        return label
    if 'record_type' not in row and row.get('status') != 'success':
        return 'invalid_trial' if label == 'invalid_trial' else 'inconclusive'
    if value is True:
        return 'confirmed_violation'
    if value is False:
        return 'no_violation'
    return 'inconclusive'


def operation(op, kind):
    return {'kind': kind, 'status': outcome(op), 'latency_ms': op.get('latency_ms'),
            'started_at': op.get('started_at'), 'ended_at': op.get('ended_at', op.get('completed_at')),
            'source_name': op.get('operation'), 'stage': op.get('stage'),
            'monotonic_started_ns': op.get('monotonic_started_ns'),
            'monotonic_ended_ns': op.get('monotonic_ended_ns'),
            'node': op.get('target_node', op.get('node'))}


def adapt(rows, manifest, fmt):
    trials = []
    if fmt == 'operation-log':
        ops = defaultdict(list)
        terminals = {}
        metadata = {}
        for row in rows:
            tid = str(row['trial_id'])
            metadata[tid] = row
            if row['record_type'] == 'operation':
                ops[tid].append(row)
            elif row['record_type'] == 'trial_result':
                if tid in terminals:
                    raise ValueError(f'duplicate terminal trial {tid}')
                terminals[tid] = row
        for tid, meta in metadata.items():
            row = terminals.get(tid, meta)
            model = row['experiment'].upper()
            secondary = manifest.get('protocol') == 'secondary-stop-v1'
            mr_fault = manifest.get('protocol') == 'mr-fault-between-reads-v1'
            mapping = WORKLOAD[model]
            if secondary:
                mapping = {'write_1':'write', 'successor_read':'read'}
                if model == 'MW':
                    mapping = {**mapping, 'write_2':'write'}
            work = [o for o in ops[tid] if o.get('phase') == 'workload']
            unknown = {o['operation'] for o in work} - mapping.keys()
            if unknown:
                raise ValueError(f'Unmapped workload operations: {unknown}')
            selected = [operation(o, mapping[o['operation']]) for o in work if o.get('status') != 'not_run']
            status_fields = ({'write_1':'write_status', 'write_2':'write_2_status', 'successor_read':'read_status'}
                             if secondary else {'write_1':'write_1_status','write_2':'write_2_status',
                             'predecessor_write':'write_status','successor_read':'read_status',
                             'predecessor_read':'read_outcome','dependent_write':'write_outcome',
                             'first_read':'first_read_status','second_read':'second_read_status'})
            recorded_names = {o['source_name'] for o in selected}
            skipped = sum(name not in recorded_names and row.get(status_fields.get(name)) == 'not_run'
                          for name in mapping)
            trials.append({'id': tid, 'model': model, 'config': row['config_id'],
                'scenario': row['scenario'], 'classification': classification(row) if tid in terminals else 'incomplete',
                'recovery': row.get('recovery_completed'), 'operations': selected,
                'expected_operations': len(mapping), 'known_skipped': skipped,
                'source_classification': row.get('classification'),
                'source_violation': row.get('is_violation'), 'source_candidate': row.get('candidate_violation'),
                'rollback': row.get('rollback'),
                'variant': ('secondary-stop-v1:'+manifest.get('s3_phase','unspecified')) if secondary else
                           manifest['protocol'] if mr_fault else
                           row.get('protocol_variant', 'journal-partition' if row.get('diagnostic_journal') else 'baseline'),
                'settings': {k: row.get(k) for k in ('read_concern','write_concern','read_preference',
                                                   'causal_session','retry_writes','retry_reads','timeout_ms')},
                'fault_at': None, 'host_fault_at_unaligned': row.get('fault_confirmed_at'),
                'fault_monotonic_ns': None,
                'election_at': (row.get('election_observation') or {}).get('observed_at')})
    else:
        for row in rows:
            model = 'MR' if fmt == 'mr-diagnostic' else row['model'].upper()
            config = row.get('config', {})
            config_id = config.get('id') if isinstance(config, dict) else config
            selected = [operation(o, WORKLOAD[model][o['stage']]) for o in row.get('operations', [])
                        if o.get('stage') in WORKLOAD[model]]
            scenario = row.get('scenario', 'normal')
            if isinstance(scenario, dict):
                scenario = scenario['name']
            trials.append({'id': str(row.get('trial_id', row.get('document_id'))), 'model': model,
                'config': config_id or row.get('config_id', row.get('base_config')),
                'scenario': scenario.replace('network_partition', 'replication-partition'),
                'classification': classification(row), 'operations': selected,
                'expected_operations': None if fmt == 'legacy' else 2,
                'recovery': manifest.get('recovery_verified') if fmt == 'mr-diagnostic' else None,
                'source_classification': row.get('status'), 'source_violation': row.get('violation'),
                'source_candidate': None, 'rollback': row.get('rollback_observed'),
                'variant': ('tagged-secondary-resume' if row.get('resume_after_heal') else 'tagged-secondary')
                if fmt == 'mr-diagnostic' else 'baseline',
                'settings': row.get('effective_settings', config) if fmt != 'legacy' else {
                    k: row.get(k) for k in ('read_concern','write_concern','read_preference','causal_session')},
                'fault_at': None, 'fault_monotonic_ns': None,
                'host_fault_at_unaligned': next((e.get('completed_at') for e in manifest.get('events', [])
                                  if e.get('action') == 'partition'), None), 'election_at': None})
    return trials


def discover(root):
    paths = set()
    for folder in ('results/raw', 'results/archive'):
        paths.update((root / folder).glob('*/operations.jsonl'))
    paths.update((root / 'results/legacy').rglob('*.jsonl'))
    paths.update((root / 'results/pilot').rglob('*.jsonl'))
    paths.update((root / 'results/pilot').rglob('result.json'))
    return sorted(p for p in paths if 'source' not in p.parts and p.name != 'events.jsonl')


def stage_for(path, manifest):
    if 'legacy' in path.parts:
        return 'legacy'
    if path.parent.name in FORMAL_A:
        return 'formal'  # Explicitly identified in docs/Zhao Yikun methodology/results.
    if manifest.get('data_stage'):
        return manifest['data_stage']
    if manifest.get('sample_stage'):
        return manifest['sample_stage']
    if 'pilot' in str(path) or 'pilot' in path.parent.name or 'Zhou Jiahao/runs' in str(path):
        return 'pilot'
    return 'unreviewed'


def load_sources(root, select_sources=()):
    catalog, normalized, seen = [], [], {}
    matched_patterns = set()
    for path in discover(root):
        relative = str(path.relative_to(root))
        manifest_path = path.parent / 'manifest.json'
        manifest = read_json(manifest_path) if manifest_path.exists() else {}
        rows = ([read_json(path)] if path.suffix == '.json' else
                [json.loads(line) for line in path.read_text().splitlines() if line.strip()])
        if not rows:
            raise ValueError(f'Empty source: {relative}')
        first = rows[0]
        if first.get('record_type') in {'operation', 'trial_result'}:
            fmt = 'operation-log'
        elif first.get('diagnostic_format') == 'dsa5208-mr-partition-1':
            fmt = 'mr-diagnostic'
        elif first.get('schema_version') == '1.0.0':
            fmt = 'schema-v1'
        elif 'model' in first and 'violation' in first and 'operations' not in first:
            fmt = 'legacy'
        else:
            raise ValueError(f'Unsupported source: {relative}')
        digest = sha(path)
        stage = stage_for(path, manifest)
        # Repeat-series selection is pinned to the documented 09:27–09:28 MR batch.
        repeat = fmt == 'mr-diagnostic' and ('20260910T0927' in relative or '20260910T0928' in relative)
        secondary_cohort = manifest.get('protocol') == 'secondary-stop-v1' and path.parent.name.startswith('a-s')
        explicit_patterns = [pattern for pattern in select_sources
                             if fnmatch.fnmatchcase(relative, pattern)]
        matched_patterns.update(explicit_patterns)
        selected = ((stage == 'formal' or secondary_cohort or 'baseline' in path.parts or 'handoff' in path.parts
                     or path.parent.name.startswith('c-wfr-') or 'results/archive/' in relative or repeat)
                    or bool(explicit_patterns)) and manifest.get('status', 'completed') == 'completed'
        entry = {'source': relative, 'sha256': digest, 'format': fmt, 'stage': stage,
                 'selected': bool(selected), 'duplicate_of': seen.get(digest),
                 'selection_reason': ('explicit-reproduction-source' if explicit_patterns else
                                      'documented-cohort' if selected else 'not-selected'),
                 'manifest_status': manifest.get('status'),
                 'planned_trials': (manifest.get('trials_per_config',0)*len(manifest.get('configs',[]))) or None,
                 'supporting_sha256': {str(manifest_path.relative_to(root)):sha(manifest_path)} if manifest_path.exists() else {},
                 'manifest': str(manifest_path.relative_to(root)) if manifest_path.exists() else None}
        catalog.append(entry)
        if digest in seen:
            entry['selected'] = False
            continue
        seen[digest] = relative
        trials = adapt(rows, manifest, fmt)
        entry['trial_count'] = len(trials)
        event_path = path.parent/'events.jsonl'
        receipts = defaultdict(list)
        if fmt == 'operation-log' and event_path.exists():
            entry['supporting_sha256'][str(event_path.relative_to(root))] = sha(event_path)
            requests = {}
            for line in event_path.read_text().splitlines():
                e = json.loads(line)
                if e.get('event') == 'controller_request':
                    requests[e['request_id']] = e
                elif e.get('event') == 'controller_response' and e.get('ok') is True:
                    request = requests.get(e.get('request_id'), {})
                    if request.get('action') in {'crash','partition'} and e.get('action') == request.get('action') and e.get('node') == request.get('node'):
                        receipts[str(request['trial_id'])].append(e)
        for trial in trials:
            if receipts[trial['id']]:
                receipt = receipts[trial['id']][-1]
                trial.update(fault_at=receipt['time'], fault_monotonic_ns=receipt.get('monotonic_ns'))
            trial.update(source=relative, format=fmt, stage=stage, selected=entry['selected'])
            trial['batch'] = ('mr-repeat5' if repeat else
                              str(path.parent.relative_to(root)) if fmt in {'legacy','schema-v1'} else path.parent.name)
            normalized.append(trial)
    unmatched = set(select_sources) - matched_patterns
    if unmatched:
        raise ValueError('Selection pattern matched no evidence: ' + ', '.join(sorted(unmatched)))
    return catalog, normalized


def summarize(trials):
    groups = defaultdict(list)
    for t in trials:
        # Settings separate timeout/retry and routing conditions; source batches remain distinct.
        key = tuple(t[k] for k in ('model','config','scenario','stage','format','variant','batch','selected'))
        key += (json.dumps(t['settings'], sort_keys=True),)
        groups[key].append(t)
    summaries = []
    for index, (key, ts) in enumerate(sorted(groups.items()), 1):
        c = Counter(t['classification'] for t in ts)
        ops = [o for t in ts for o in t['operations']]
        statuses = Counter(o['status'] for o in ops)
        expected = [t['expected_operations'] for t in ts]
        evaluable = c['confirmed_violation'] + c['no_violation']
        summary = dict(zip(('model','config','scenario','stage','format','variant','batch','selected'), key[:8]))
        summary.update(group_id=f'G{index:03d}', settings=json.loads(key[8]),
            sources=sorted({t['source'] for t in ts}), trials=len(ts), classifications=dict(c),
            evaluable=evaluable, confirmed_violations=c['confirmed_violation'],
            violation_rate=c['confirmed_violation'] / evaluable if evaluable else None,
            operation_attempts=len(ops), operation_statuses=dict(statuses),
            operation_success_rate=statuses['success'] / len(ops) if ops else None,
            operation_slots=sum(expected) if all(n is not None for n in expected) else None,
            unrecorded_slots=sum(expected)-len(ops) if all(n is not None for n in expected) else None,
            known_skipped=sum(t.get('known_skipped',0) for t in ts),
            recovery_success=sum(t['recovery'] is True for t in ts),
            recovery_failure=sum(t['recovery'] is False for t in ts),
            recovery_unknown=sum(t['recovery'] is None for t in ts),
            rollback_confirmed=sum(t['rollback'] == 'confirmed' for t in ts))
        denom = summary['recovery_success'] + summary['recovery_failure']
        slots = summary['operation_slots']
        summary['planned_operation_success_rate'] = statuses['success']/slots if slots else None
        summary['missing_operation_evidence'] = (summary['unrecorded_slots']-summary['known_skipped']) if slots is not None else None
        summary['recovery_success_rate'] = summary['recovery_success']/denom if denom else None
        summary['stage_metrics'] = {}
        for stage in sorted({o['stage'] or o['source_name'] for o in ops}):
            stage_ops = [o for o in ops if (o['stage'] or o['source_name']) == stage]
            summary['stage_metrics'][stage] = {
                'statuses': dict(Counter(o['status'] for o in stage_ops)),
                'success_p95_ms': percentile([o['latency_ms'] for o in stage_ops if o['status'] == 'success'])}
        for kind in ('read','write'):
            values = [o['latency_ms'] for o in ops if o['kind'] == kind and o['status'] == 'success']
            summary[f'{kind}_p95_ms'] = percentile(values)
            summary[f'{kind}_latency_samples'] = sum(type(v) in (int,float) and math.isfinite(v) and v >= 0 for v in values)
        summaries.append(summary)
    return summaries


def write_index(summaries, directory):
    """Human-readable selected-cohort table with explicit denominators and missingness."""
    def rate(value):
        return 'N/A' if value is None else f'{value:.1%}'
    lines = ['# Unified evidence summary', '',
             'Selected cohorts only; selection does not promote pilot/legacy data to formal evidence.',
             'Source-adjudicated violation counts are not new independent adjudications. See the package README for denominator rules.', '',
             'Planned slots refer to recorded/started trials, not unstarted trials. Skip is explicit not_run; missing is unexplained absent evidence.', '',
             '| Group | Model/config | Scenario / stage / variant | Trials | Violations / evaluable | Rate | Candidate / invalid / inconclusive / rollback / incomplete | Attempted operation success | Planned slot success | Skip / missing | Timeout / unavailable / failure | Recovery success / failure / unknown |',
             '| --- | --- | --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- |']
    for g in summaries:
        if not g['selected']:
            continue
        c, s = g['classifications'], g['operation_statuses']
        counts = ' / '.join(str(c.get(k,0)) for k in
                            ('candidate_violation','invalid_trial','inconclusive','rollback_observed','incomplete'))
        lines.append(f"| {g['group_id']} | {g['model']} {g['config']} | {g['scenario']} / {g['stage']} / {g['variant']} | "
                     f"{g['trials']} | {g['confirmed_violations']} / {g['evaluable']} | {rate(g['violation_rate'])} | {counts} | "
                     f"{rate(g['operation_success_rate'])} ({s.get('success',0)}/{g['operation_attempts']}) | "
                     f"{rate(g['planned_operation_success_rate'])} | {g['known_skipped']} / {g['missing_operation_evidence']} | "
                     f"{s.get('timeout',0)} / {s.get('unavailable',0)} / {s.get('failure',0)} | "
                     f"{g['recovery_success']} / {g['recovery_failure']} / {g['recovery_unknown']} |")
    lines += ['', '## Provenance', '', 'Each group below maps to unmodified source files and settings in summary.json.', '']
    for g in summaries:
        if g['selected']:
            lines.append(f"- {g['group_id']}: " + ', '.join(f'`{p}`' for p in g['sources']))
    with (directory / 'README.md').open('x', encoding='utf-8') as file:
        file.write('\n'.join(lines)+'\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, default=ROOT / 'results/summary/unified-0914')
    parser.add_argument('--select-source', action='append', default=[], metavar='GLOB',
                        help='Explicitly include matching repository-relative evidence in this analysis; repeatable')
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error('Output already exists; use a new output directory to retain previous analysis.')
    catalog, trials = load_sources(args.root, args.select_source)
    summaries = summarize(trials)
    # Verify no evidence changed while it was being read.
    assert all(sha(args.root / e['source']) == e['sha256'] for e in catalog)
    assert all(sha(args.root / p)==digest for e in catalog for p,digest in e['supporting_sha256'].items())
    args.output.mkdir(parents=True)
    for name, value in [('catalog',catalog), ('trials',trials), ('summary',summaries)]:
        (args.output / f'{name}.json').write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False)+'\n')
    write_index(summaries, args.output)
    print(json.dumps({'sources':len(catalog), 'trials':len(trials), 'groups':len(summaries),
                      'selected_groups':sum(g['selected'] for g in summaries), 'output':str(args.output)}))


if __name__ == '__main__':
    main()
