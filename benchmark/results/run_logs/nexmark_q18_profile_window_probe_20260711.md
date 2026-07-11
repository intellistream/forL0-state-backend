# NexMark q18 profile-window probe - 2026-07-11

## Purpose

Check whether the smaller observed improvement came from a wrong ForL0
configuration or from run-to-run variation / stricter measurement scope.  Also
test whether NexMark profiling should cover both TaskManagers and start after
the metric warmup window.

## Run

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads N04 \
  --profile cpu --no-report --restart-cluster
```

Workload:

- `N04 nexmark_q18_lateq_deep_forl0`
- Scenario: `forl0_no_full_gc_lateq_deep`
- Query: `q18`
- Backend: `forl0`
- Same-parallelism configuration: p4, two TaskManagers, eight total slots

Result:

- Raw: `benchmark/results/nexmark_20260711_153257/nexmark_results.json`
- ForL0 throughput: `587.53 K/s`
- ForL0 throughput/core: `89.84 K/s/core`
- CPU cores: `6.54`

This effectively reproduces the earlier strong q18 lateq-deep ForL0 sample
(`587.35 K/s`).  The preceding `567.81 K/s` sample is therefore treated as
normal run-to-run variation rather than evidence that the tuned configuration
was flattened.

## Profiler experiment outcome

A local uncommitted probe tried to profile both TaskManagers with a delayed
start (`metric_monitor_delay + 10s`).  It was not kept:

- `flink-taskmanager-2` exited with status `137` after the run.
- Only the `flink-taskmanager-1` profile was copied:
  `benchmark/results/nexmark_20260711_153257/profiles/flamegraph_cpu_forl0_q18_flink-taskmanager-1_20260711_153257.html`
- The copied profile was still dominated by metric dump / CPU-load collection
  and JIT compiler frames, with only `123` samples.

Conclusion: do not add all-TaskManager async-profiler collection to the offline
one-click script for the high-pressure q18 headline run.  It risks destabilizing
the cluster and does not yet produce a cleaner ForL0 hotspot profile.

## Current interpretation

The q18 same-parallelism ForL0-positive window remains valid:

- Recorded HashMap q18 lateq-deep: `444.64 K/s`
- Fresh ForL0 q18 lateq-deep: `587.53 K/s`
- Implied lift: about `+32.1%`

The apparent effect drop is mainly from comparing against stricter stable
evidence and excluding older `AUTO_STOPPED` or mismatched-parallelism highs, not
from a currently observed ForL0 configuration regression.
