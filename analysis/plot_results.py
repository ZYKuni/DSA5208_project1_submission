"""Offline figures from unified JSON; never reads or modifies MongoDB."""
import argparse
from datetime import datetime
from functools import lru_cache
import json
import os
from pathlib import Path
import tempfile


@lru_cache(maxsize=1)
def plotting_modules():
    """Load optional rendering dependencies only when figures are requested."""
    os.environ.setdefault('MPLCONFIGDIR', str(Path(tempfile.gettempdir()) / 'dsa5208-matplotlib'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    plt.rcParams.update({'font.size': 9, 'axes.spines.top': False, 'axes.spines.right': False,
                         'svg.fonttype': 'none', 'savefig.facecolor': 'white'})
    return plt, np


def label(g):
    scenario = {'replication-partition':'partition', 'primary-crash':'crash'}.get(g['scenario'], g['scenario'])
    variant = '' if g['variant'] == 'baseline' else '\n' + g['variant']
    return f"{g['group_id']}  {g['config']} · {scenario} · {g['stage']}{variant}"


def save(fig, output, name):
    plt, _ = plotting_modules()
    for suffix in ('png', 'svg'):
        fig.savefig(output / f'{name}.{suffix}', dpi=160, bbox_inches='tight')
        if suffix == 'svg':
            path = output / f'{name}.{suffix}'
            path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines())+'\n')
    plt.close(fig)


def charts(groups, output):
    plt, np = plotting_modules()
    names = []
    pages = []
    for model in sorted({g['model'] for g in groups}):
        rows = [g for g in groups if g['model'] == model]
        for start in range(0,len(rows),12):
            pages.append((model if len(rows)<=12 else f'{model}-p{start//12+1}',rows[start:start+12]))
    for model, rows in pages:
        y = np.arange(len(rows))
        for metric in ('violation', 'success', 'latency'):
            fig, ax = plt.subplots(figsize=(12, max(4, len(rows)*.65+1.8)))
            ax.set_yticks(y, [label(g) for g in rows])
            ax.invert_yaxis()
            ax.set_axisbelow(True)
            ax.grid(axis='x', alpha=.18)
            if metric == 'latency':
                for kind, offset, color in [('read',-.16,'#2878a8'),('write',.16,'#db8135')]:
                    values = [g[f'{kind}_p95_ms'] for g in rows]
                    ax.barh(y+offset, [v or 0 for v in values], height=.28, color=color, label=kind)
                    for i, v in enumerate(values):
                        ax.text((v or 0)+.015, i+offset,
                                'N/A' if v is None else f'{v:.2f} ms (n={rows[i][kind+"_latency_samples"]})',
                                va='center', fontsize=8)
                ax.set_xscale('symlog', linthresh=1)
                ax.set_xlim(0, max([1]+[g[k] or 0 for g in rows for k in ('read_p95_ms','write_p95_ms')])*8)
                ax.set_xlabel('Successful workload operations only; p95 ms (symlog scale, linear below 1 ms)')
                ax.legend(loc='lower right')
            else:
                field = 'violation_rate' if metric == 'violation' else 'operation_success_rate'
                values = [g[field] for g in rows]
                ax.barh(y-.13 if metric == 'success' else y, [(v or 0)*100 for v in values],
                        height=.24 if metric == 'success' else .5, color='#277c76', label='attempted denominator')
                if metric == 'success':
                    ax.barh(y+.13, [(g.get('planned_operation_success_rate') or 0)*100 for g in rows],
                            height=.24,color='#91aca8',label='planned slots (started trials)')
                ax.set_xlim(0, 160)
                ax.set_xticks([0,25,50,75,100], ['0%','25%','50%','75%','100%'])
                for i, g in enumerate(rows):
                    v = g[field]
                    if metric == 'violation':
                        c = g['classifications']
                        extra = f"{g['confirmed_violations']}/{g['evaluable']}; cand={c.get('candidate_violation',0)}, invalid={c.get('invalid_trial',0)}, inc={c.get('inconclusive',0)}"
                    else:
                        s = g['operation_statuses']
                        planned = g.get('planned_operation_success_rate')
                        planned_label = 'N/A' if planned is None else f'{planned:.1%}'
                        extra = f"{s.get('success',0)}/{g['operation_attempts']}; planned={planned_label}\nTO={s.get('timeout',0)}, unavail={s.get('unavailable',0)}, fail={s.get('failure',0)}, skip={g.get('known_skipped',0)}"
                    ax.text(102, i, ('N/A' if v is None else f'{v:.1%}')+'  '+extra, va='center', fontsize=8)
                ax.set_xlabel('Source-adjudicated violations / evaluable trials' if metric == 'violation'
                              else 'Dark: successful / attempted; light: successful / planned slots in recorded trials')
            ax.set_title(f'{model} | {metric.upper()} | Archived evidence, separate cohorts', loc='left', pad=16)
            fig.text(.01, .005, 'Pilot, legacy and formal rows are NOT pooled. N/A means no denominator/data, not zero.\n'
                     'p95 pools workload stages of the same kind; stage-specific metrics are in summary.json. Small samples are descriptive only.', fontsize=8)
            fig.tight_layout(rect=(0,.05,1,1))
            name = f'{model.lower()}-{metric}'
            save(fig, output, name)
            names.append(name)
    return names


def timeline_coordinates(t):
    """Never align host timestamps to runner timestamps. Durations use recorded latency."""
    timed = [o for o in t['operations'] if o.get('started_at') and o.get('latency_ms') is not None]
    if not timed:
        return None
    mono = all(o.get('monotonic_started_ns') is not None for o in timed)
    parse = lambda s: datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
    start = lambda o: o['monotonic_started_ns']/1e9 if mono else parse(o['started_at'])
    origin = min(start(o) for o in timed)
    spans = [(start(o)-origin, o['latency_ms']/1000) for o in timed]
    marks = {}
    if mono and t.get('fault_monotonic_ns') is not None:
        marks['fault receipt (runner)'] = t['fault_monotonic_ns']/1e9-origin
    elif not mono and t.get('fault_at'):
        marks['fault receipt (runner)'] = parse(t['fault_at'])-origin
    # Election observer timestamps are runner-local; omit when the axis is monotonic.
    if not mono and t.get('election_at'):
        marks['election observed (runner)'] = parse(t['election_at'])-origin
    return timed, spans, marks, 'runner monotonic' if mono else 'runner UTC (legacy; gaps may reflect clock adjustments)'


def timelines(trials, output):
    plt, _ = plotting_modules()
    # Exact documented examples: no post-hoc search for a favourable trial.
    examples = [t for t in trials if
                (t['model']=='WFR' and t['scenario']=='primary-crash' and t['config']=='C4'
                 and t['variant']=='baseline') or
                t['id']=='c-wfr-v2-replication-partition-smoke-0921-wfr-C4-replication-partition-0001' or
                (t['model']=='MR' and t['id']=='20260910T093656Z-de865e66') or
                t['id']=='a-clock-evidence-smoke-0915-mw-C1-trial-0001']
    names = []
    for t in examples:
        coordinates = timeline_coordinates(t)
        if coordinates is None:
            continue
        timed, spans, marks, basis = coordinates
        fig, ax = plt.subplots(figsize=(11,4))
        left = min([s for s,d in spans] + list(marks.values()))
        right = max([s+d for s,d in spans] + list(marks.values()))
        width = max(right-left, .001)
        for i, op in enumerate(timed):
            start, duration = spans[i]
            end = start+duration
            ax.barh(i, duration, left=start, height=.4,
                    color='#bb493a' if op['status'] != 'success' else '#2878a8')
            ax.text(end+width*.03, i, f"{op['status']} · {duration*1000:.3f}ms", va='center')
        for field, value in marks.items():
            ax.axvline(value, color='#db8135', linestyle='--', label=field)
        if marks:
            ax.legend()
        ax.set_yticks(range(len(timed)), [o['stage'] or o['source_name'] for o in timed])
        ax.invert_yaxis()
        ax.set_xlim(left=left-width*.03, right=right+width*.65)
        ax.set_xlabel(f'Seconds, {basis}; duration = recorded operation latency; host events NOT aligned')
        ax.set_title(f"{t['model']} {t['config']} {t['scenario']} | {t['classification']}", loc='left')
        fig.tight_layout()
        name = f"timeline-{t['model'].lower()}-{t['config'].lower()}"
        if t['variant'] == 'wfr-committed-dependency-v2':
            name += '-v2-partition'
        save(fig, output, name)
        names.append(name)
    return names


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=False)
    groups = json.loads((args.input/'summary.json').read_text())
    trials = json.loads((args.input/'trials.json').read_text())
    names = charts([g for g in groups if g['selected']], args.output)
    names += timelines(trials, args.output)
    (args.output/'README.md').write_text('# Archived evidence figures\n\n'
        'Generated from the selected cohorts in unified summary.json; see the package README for denominators.\n\n'+
        '\n'.join(f'- [{name}]({name}.png) ([SVG]({name}.svg))' for name in names)+'\n')
    print(f'Generated {len(names)} figures, PNG + SVG, at {args.output}')


if __name__ == '__main__':
    main()
