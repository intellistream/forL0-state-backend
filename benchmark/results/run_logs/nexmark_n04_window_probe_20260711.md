# NexMark N04 ForL0 window probe - 2026-07-11

Evidence label: real-online configuration probe.

## Purpose

Test whether NexMark q18 `forl0_no_full_gc_lateq_deep` can be improved by a
moderate ForL0-only table/cache window increase.  This was motivated by the
valid client C08 profile showing generic-path serialization pressure and by
WordCount benefiting from a larger ForL0 table window.

## Temporary configuration

Only the `forl0_no_full_gc_lateq_deep` scenario was changed for this probe:

```yaml
forl0_overrides:
  l0_cache_enabled: true
  l0_cache_size: 384mb
  l0_memory_max_size: 768mb
  initial_table_capacity: 4096
  max_table_capacity: 131072
  main_table_load_factor_threshold: 0.80
```

The committed/default configuration remains:

```yaml
forl0_overrides:
  l0_cache_enabled: true
  l0_cache_size: 256mb
  l0_memory_max_size: 512mb
  initial_table_capacity: 2048
  max_table_capacity: 65536
  main_table_load_factor_threshold: 0.80
```

## Probe command

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads N04 \
  --no-report --restart-cluster
```

Output:

```text
benchmark/results/nexmark_20260711_165441/nexmark_results.json
```

Measured q18 result:

```text
561,330 events/sec
81,470 events/sec/core
```

## Comparison

Historical no-profile N04 q18 ForL0 results under the default `256mb/512mb,
2048/65536` window:

- `618,920 events/sec` (`benchmark/results/nexmark_20260711_122623`)
- `587,350 events/sec` (`benchmark/results/nexmark_20260711_124409`)
- `601,540 events/sec` (`benchmark/results/nexmark_20260711_152040`)
- `587,530 events/sec` (`benchmark/results/nexmark_20260711_153257`)

The moderate-window probe regressed throughput and, after the sample, left only
one registered TaskManager.  `flink-taskmanager-1` had exited with status 137.
The cluster was restarted immediately afterwards and verified healthy:

```text
taskmanagers=2
slots-total=8
slots-available=8
jobs-running=0
```

## Decision

Revert the temporary configuration.  The larger N04 window is both slower and
less stable on the 16GB-per-TM Ascend Docker topology.  Keep the existing
default N04 ForL0 window for offline reproduction.
