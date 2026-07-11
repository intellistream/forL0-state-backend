# WordCount ForL0 table-window probe - 2026-07-11

## Purpose

Stabilize the WordCount `stateful_counter_fastpath` ForL0 advantage after
recent clean reruns fell back to the HashMap tier.

The JAR checksum was unchanged between the previous clean reference
(`25526a0`) and current `main`, so the likely issue was not a different backend
binary.  The small WordCount-specific ForL0 window (`initial=512`,
`max=4096`, cache `96mb`) was too fragile: recent clean samples stayed around
`519 K records/s/core`.

## Configuration tested

For `wordcount_scenarios.stateful_counter_fastpath`, add a ForL0-only override:

```yaml
forl0_overrides:
  l0_cache_enabled: true
  l0_cache_size: 256mb
  l0_memory_max_size: 512mb
  initial_table_capacity: 2048
  max_table_capacity: 65536
  main_table_load_factor_threshold: 0.80
```

This does not affect HashMap and keeps both backends at the same parallelism
(`p8`).

## Run

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --no-report --restart-cluster
```

Result:

- Raw: `benchmark/results/raw/wordcount_forl0_20260711_155244.json`
- Best ForL0 throughput/core: `566,971 records/s/core`
- Repeat samples:
  - `365,830 records/s/core`
  - `566,787 records/s/core`
  - `566,971 records/s/core`

Comparison:

- Recent small-window clean run:
  `benchmark/results/raw/wordcount_forl0_20260711_154751.json`
  - `414,729 / 519,768 / 519,721 records/s/core`
- Recorded HashMap baseline:
  `benchmark/results/raw/wordcount_hashmap_20260711_143147.json`
  - best `519,573 records/s/core`
- Restored ForL0 positive window:
  - `566,971 / 519,573 - 1 = +9.1%`

## Decision

Keep the scenario-level ForL0 override.  It restores the stable WordCount
advantage without changing the workload, HashMap path, or operator parallelism.
