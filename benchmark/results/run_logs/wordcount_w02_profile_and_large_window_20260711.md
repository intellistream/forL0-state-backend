# WordCount W02 profile and large-window probe - 2026-07-11

## Purpose

Continue profiler-driven optimization for WordCount `stateful_counter_fastpath`
after the scenario-level `2048/65536` ForL0 window restored the stable
`~566 K records/s/core` result.

## CPU profile

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads W02 \
  --profile cpu --no-report --restart-cluster
```

Raw/profile outputs:

- Raw: `benchmark/results/raw/wordcount_forl0_20260711_155902.json`
- Profiles:
  - `benchmark/results/profiles/flamegraph_cpu_forl0_20260711_155600.html`
  - `benchmark/results/profiles/flamegraph_cpu_forl0_20260711_155709.html`
  - `benchmark/results/profiles/flamegraph_cpu_forl0_20260711_155805.html`

Profile run throughput was lower (`479,645 records/s/core`) because
async-profiler and hardware-memory sampling add overhead, so this run is used
only for hotspot analysis.

Third-repeat profile highlights:

- `ForL0ValueState.addAndGetLong`: about `6.0%`
- `NativeEngine.valueAddAndGetLongLong`: about `5.9%`
- WordCount fast-path MethodHandle/LambdaForm wrapper: about `23.6%`
- Flink source / network / output path dominates the remaining samples.

Interpretation: the JNI body is no longer the main end-to-end limiter.  Further
native changes are unlikely to move W02 much; the remaining safe lever is the
ForL0 table/cache sizing window.

## Large-window probe

Configuration tested for `wordcount_scenarios.stateful_counter_fastpath`:

```yaml
forl0_overrides:
  l0_cache_enabled: true
  l0_cache_size: 512mb
  l0_memory_max_size: 1024mb
  initial_table_capacity: 8192
  max_table_capacity: 262144
  main_table_load_factor_threshold: 0.80
```

Run 1:

- Raw: `benchmark/results/raw/wordcount_forl0_20260711_160251.json`
- Repeat samples: `415,123 / 622,997 / 566,823 records/s/core`
- Best: `622,997 records/s/core`
- Best job state: `FINISHED`

Run 2:

- Raw: `benchmark/results/raw/wordcount_forl0_20260711_160619.json`
- Repeat samples: `415,175 / 566,766 / 566,831 records/s/core`
- Best: `566,831 records/s/core`
- Best job state: `AUTO_STOPPED`

## Decision

Keep the large ForL0-only WordCount window.  It preserves the stable
`~566 K records/s/core` floor seen with the smaller restored window and can
produce a normal `FINISHED` peak of `622,997 records/s/core`, which is about
`+19.9%` versus the recorded HashMap baseline
(`519,573 records/s/core`).  The conservative stable comparison remains about
`+9.1%`.
