"""Rebuild auditable MW summaries from raw JSONL; no pandas dependency."""
import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    return round(values[low] + (values[high] - values[low]) * (position - low), 6)


def export_csv(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list))
                             else v for k, v in row.items()})


def summarize(raw_path, output_dir=None):
    output_dir = Path(output_dir or raw_path.parent)
    output_dir.mkdir(parents=True, exist_ok=True)
    records = [json.loads(line) for line in raw_path.read_text().splitlines() if line.strip()]
    trials = [r for r in records if r['record_type'] == 'trial_result']
    operations = [r for r in records if r['record_type'] == 'operation']
    assessments = {t['trial_id']: t for t in trials}
    for row in operations:
        trial = assessments.get(row['trial_id'], {})
        row['trial_classification'] = trial.get('classification', 'incomplete')
        row['trial_is_violation'] = trial.get('is_violation')
        row['trial_reason'] = trial.get('reason', 'No terminal trial assessment')
    summary = []
    for config_id in sorted({r['config_id'] for r in records}):
        ts = [t for t in trials if t['config_id'] == config_id]
        all_ops = [o for o in operations if o['config_id'] == config_id]
        writes = [o for o in all_ops if o['phase'] == 'workload']
        successful = [o['latency_ms'] for o in writes if o['status'] == 'success']
        classes = Counter(t['classification'] for t in ts)
        status = Counter(o['status'] for o in writes)
        row = {'config_id': config_id, 'trials': len(ts),
               'incomplete_trials': len({o['trial_id'] for o in all_ops} - set(assessments)),
               'no_violation': classes['no_violation'],
               'candidate_violation': classes['candidate_violation'],
               'inconclusive': classes['inconclusive'], 'invalid_trial': classes['invalid_trial'],
               'confirmed_violation': sum(t['is_violation'] is True for t in ts),
               'rollback_not_assessed': sum(t['rollback'] == 'not_assessed' for t in ts),
               'workload_writes': len(writes), 'write_success': status['success'],
               'write_failure': status['failure'], 'write_timeout': status['timeout'],
               'wire_retries': sum(o['retry_count'] for o in writes),
               'observation_stale': sum(t['observation_stale'] is True for t in ts),
               'all_operation_failures': sum(o['status'] == 'failure' for o in all_ops),
               'all_operation_timeouts': sum(o['status'] == 'timeout' for o in all_ops),
               'write_latency_p50_ms': percentile(successful, 0.50),
               'write_latency_p95_ms': percentile(successful, 0.95)}
        summary.append(row)
    export_csv(output_dir / 'summary.csv', summary)
    export_csv(output_dir / 'operations.csv', operations)
    export_csv(output_dir / 'trials.csv', trials)
    (output_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw_jsonl', type=Path)
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.raw_jsonl, args.output_dir), indent=2))


if __name__ == '__main__':
    main()
