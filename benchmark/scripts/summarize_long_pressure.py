#!/usr/bin/env python3
"""Summarize validated Long pressure runs; exclude profiling/checkpoint runs."""
import argparse
import json
from pathlib import Path
import statistics


def summarize(root):
    groups = {}
    runs = []
    for file in sorted(root.glob('briskstate_*/measurement.json')):
        m = json.loads(file.read_text())
        directory = file.parent
        if (m['profiled'] or m['keys'] != 32000000 or m['records'] != 128000000
                or m['state_ops_per_record'] != 16 or m['rate'] != 0 or m['skew'] != 0):
            continue
        command = json.loads((directory / 'command.json').read_text())
        if int(command[command.index('--checkpointInterval') + 1]) != 0:
            continue
        metrics = json.loads((directory / 'metrics.json').read_text())
        job = json.loads((directory / 'job.json').read_text())
        assert job['state'] == 'FINISHED' and metrics['sink_records'] == 128000000
        name = m['variant'].removesuffix('-return')
        row = dict(m, backend=metrics['backend'], job_id=job['jid'], directory=str(directory))
        runs.append(row)
        groups.setdefault((name, metrics['backend']), []).append(row)
    summary = []
    for (variant, backend), rows in sorted(groups.items()):
        times = [x['job_duration_seconds'] for x in rows]
        median = statistics.median(times)
        summary.append(dict(variant=variant, backend=backend, count=len(rows), times_seconds=times,
                            median_seconds=median, min_seconds=min(times), max_seconds=max(times),
                            median_updates_per_second=128000000 * 16 / median,
                            median_tm_cpu_seconds=statistics.median(sum(x['cpu_seconds'].values()) for x in rows)))
    return dict(workload=dict(keys=32000000, records=128000000, state_updates=2048000000,
                              parallelism=8, rate_limit=0, skew=0, checkpoint_interval_ms=0),
                groups=summary, runs=runs,
                notes=['Profiled jobs are excluded from timing.',
                       'All reported runs are retained; no best-run selection.',
                       'Each group includes cold/warm JVM variation; sample count is small.',
                       'ForL0 uses fused increments; heap uses ValueState read/update for the same logical increments.',
                       'This is a repeated-key-per-record fast-path workload, not a general workload ranking.',
                       'TM RSS includes JVM memory retained from previous jobs, not just live state size.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1] / 'results/runs')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    data = summarize(args.root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2) + '\n')
    for row in data['groups']:
        print(row)

if __name__ == '__main__':
    main()
