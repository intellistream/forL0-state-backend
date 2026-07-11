# NexMark N04 q18 ForL0 memory-window probe, 2026-07-11

Evidence label: real-online probes on the Ascend host.

## Goal

The previous monitor-window probes showed that N04 q18 reaches much higher
late-window throughput, but long delayed windows can leave the cluster with one
TaskManager after the run. This follow-up tested whether reducing ForL0's
cache/memory window stabilizes the delayed window.

## Probe A: 30s/90s monitor, smaller ForL0 cache/memory

Temporary changes limited to `forl0_no_full_gc_lateq_deep`:

```yaml
query_overrides:
  q18:
    metric_monitor_delay: 30s
    metric_monitor_duration: 90s
forl0_overrides:
  l0_cache_size: 128mb
  l0_memory_max_size: 256mb
```

Result:

```text
benchmark/results/nexmark_20260711_175627/nexmark_results.json
ForL0 q18: 693,880 events/s, 104,816/core
```

The benchmark command returned success, but post-run cluster inspection showed:

```text
flink-taskmanager-2 Exited (137)
```

Decision: unstable, do not keep.

## Probe B: 30s/60s monitor, smaller ForL0 cache/memory

Temporary changes limited to `forl0_no_full_gc_lateq_deep`:

```yaml
query_overrides:
  q18:
    metric_monitor_delay: 30s
    metric_monitor_duration: 60s
forl0_overrides:
  l0_cache_size: 128mb
  l0_memory_max_size: 256mb
```

Result:

```text
benchmark/results/nexmark_20260711_180014/nexmark_results.json
ForL0 q18: 618,700 events/s, 92,068/core
```

The benchmark command returned success, but post-run checks were inconsistent:
the REST overview still had a q18 job stuck in `CANCELLING` while Docker showed
`flink-taskmanager-2 Exited (137)`. The cluster required a restart.

Decision: unstable, do not keep.

## Conclusion

Reducing the ForL0 cache/memory window does not solve the N04 q18 delayed-window
stability problem. It still exposes the same late-window high-throughput shape,
but the offline one-click default must remain on the committed `15s/90s`,
`256mb/512mb` configuration until the TaskManager 137 failure is addressed by a
real runtime or memory-pressure fix.

All temporary YAML changes were reverted after the probes.
