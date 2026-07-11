# NexMark N04 q18 monitor-window probes, 2026-07-11

Evidence label: real-online probes on the Ascend host.

## Goal

Check whether the lower apparent N04 q18 ForL0 gain is caused by averaging over
the early ramp-up portion of the sink throughput curve.

The committed default remains:

```yaml
metric_monitor_delay: 15s
metric_monitor_duration: 90s
```

## Probe A: intended 45s delay, wrong scenario edited

The first edit accidentally changed `forl0_q18_l0_hotspot`, not
`forl0_no_full_gc_lateq_deep`. The actual N04 run still used 15s delay.

Result:

```text
benchmark/results/nexmark_20260711_173057/nexmark_results.json
ForL0 q18: 536,410 events/s, 79,000/core
```

Decision: invalid as a monitor-window probe for N04.

## Probe B: lateq_deep q18 delay 45s, duration 90s

Correct temporary edit:

```yaml
metric_monitor_delay: 45s
metric_monitor_duration: 90s
```

Failure run:

```text
benchmark/results/nexmark_20260711_173421/nexmark_results.json
benchmark/results/run_logs/FAILED_nexmark_forl0_20260711_173649.txt
```

The monitor window showed the expected late steady-state ramp:

```text
581.85 K -> 728.03 K -> 882.18 K -> 900.38 K events/s
```

But the run failed because the TaskManager count dropped below the expected
2 TMs.

Second 45s-delay run:

```text
benchmark/results/nexmark_20260711_173806/nexmark_results.json
ForL0 q18: 794,220 events/s, 119,792/core
```

This run returned success from the benchmark command, but post-run cluster
inspection showed only one TaskManager remaining. Treat it as unstable and do
not use it as default evidence.

## Probe C: lateq_deep q18 delay 30s, duration 90s

Temporary edit:

```yaml
metric_monitor_delay: 30s
metric_monitor_duration: 90s
```

Result:

```text
benchmark/results/nexmark_20260711_174327/nexmark_results.json
ForL0 q18: 699,120 events/s, 106,899/core
```

The benchmark command succeeded, but post-run cluster inspection again showed
that `flink-taskmanager-2` had exited with status 137. Treat it as unstable and
do not use it as default evidence.

## Probe D: lateq_deep q18 delay 20s, duration 90s

Temporary edit:

```yaml
metric_monitor_delay: 20s
metric_monitor_duration: 90s
```

Result:

```text
benchmark/results/nexmark_20260711_174706/nexmark_results.json
ForL0 q18: 576,150 events/s, 83,019/core
```

This did not improve over the committed default line and `flink-taskmanager-2`
still exited with status 137 after the run.

## Probe E: lateq_deep q18 delay 30s, duration 60s

Temporary edit:

```yaml
metric_monitor_delay: 30s
metric_monitor_duration: 60s
```

Result:

```text
benchmark/results/nexmark_20260711_175113/nexmark_results.json
ForL0 q18: 578,700 events/s, 89,306/core
```

This run left the cluster healthy at 2 TaskManagers / 8 slots, but the measured
throughput did not improve over the default historical N04 q18 line.

## Decision

Revert all monitor-window edits and keep the committed default
`15s/90s` window for N04.

The profiler/monitor evidence shows that late q18 steady state can reach much
higher throughput, but extending the monitor start far enough to capture that
region makes the current 16GB Ascend TaskManager setup unstable. Shortening the
duration keeps the cluster healthy but loses the throughput benefit.

Do not report the `699 K` or `794 K` values as default/offline evidence. They
are useful diagnostic probes showing that the remaining gap is tied to ramp-up
and long-run memory pressure, not a safe default configuration.
