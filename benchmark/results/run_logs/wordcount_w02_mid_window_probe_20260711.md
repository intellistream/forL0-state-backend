# WordCount W02 mid-window probe - 2026-07-11

Evidence label: real-online configuration probe.

## Purpose

After the default W02 large ForL0 window reproduced the conservative
`~566 K records/s/core` stable line, test whether a middle-sized table/cache
window can reduce variance while preserving or improving throughput.

## Baseline rerun

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --no-report --restart-cluster
```

Default W02 ForL0 window:

```yaml
l0_cache_size: 512mb
l0_memory_max_size: 1024mb
initial_table_capacity: 8192
max_table_capacity: 262144
```

Output:

```text
benchmark/results/raw/wordcount_forl0_20260711_170217.json
```

Result:

```text
566,912 records/s/core
```

This matches the known conservative stable line for the large-window W02
configuration.

## Temporary mid-window probe

Only W02 `wordcount_scenarios.stateful_counter_fastpath.forl0_overrides` was
changed:

```yaml
l0_cache_size: 384mb
l0_memory_max_size: 768mb
initial_table_capacity: 4096
max_table_capacity: 131072
```

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --no-report --restart-cluster
```

Output:

```text
benchmark/results/raw/wordcount_forl0_20260711_170549.json
```

Result:

```text
519,614 records/s/core
```

## Decision

Revert the mid-window probe.  It regresses W02 to approximately the HashMap
baseline range and is worse than both the committed large-window stable line
and the previously observed large-window peak.  Keep the committed default W02
ForL0 window:

```yaml
l0_cache_size: 512mb
l0_memory_max_size: 1024mb
initial_table_capacity: 8192
max_table_capacity: 262144
```

The cluster remained healthy after the probe:

```text
taskmanagers=2
slots-total=8
slots-available=8
jobs-running=0
```
