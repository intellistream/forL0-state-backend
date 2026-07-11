# NexMark N04 profiler target probe - 2026-07-11

Evidence label: real-online profiler probe.

## Purpose

After fixing the generic `run_all_apps.sh` profiler container override, verify
that the NexMark-specific container profiler also honors
`FLINK_TASKMANAGER_CONTAINER`.

## Change

`benchmark/scripts/run_nexmark.py` now tries the explicit
`FLINK_TASKMANAGER_CONTAINER` first when selecting the container-side
async-profiler target, then falls back to the previous running-TaskManager
order.

## Probe

Command:

```bash
FLINK_TASKMANAGER_CONTAINER=flink-taskmanager-2 \
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads N04 \
  --profile cpu --no-report --restart-cluster
```

The runner correctly reported:

```text
Profile TM:  flink-taskmanager-2
Started cpu profiling in container (flink-taskmanager-2, event=cpu)
```

Result file:

```text
benchmark/results/nexmark_20260711_164917/nexmark_results.json
```

Measured profiler-run throughput:

```text
q18: 553,100 events/sec, 82,799/core
```

## Outcome

The profiler target override works, but N04/q18 lateq-deep profiling on the
active TaskManager is not stable enough for the offline one-shot path.  The run
finished the benchmark sample, but async-profiler failed to stop/copy the file
and `flink-taskmanager-2` exited with status 137 after the run.

The cluster was restarted immediately afterwards and verified healthy:

```text
taskmanagers=2
slots-total=8
slots-available=8
jobs-running=0
```

## Interpretation

Do not use N04 container CPU profiling as default evidence for the report.  It
is useful as a targeted debug probe only.  The workload logs still show why this
scenario is sensitive to ForL0 configuration:

```text
[ForL0] RowData has variable-length field (VARCHAR), using generic path.
```

So the remaining safe path for the offline report is to keep the measured
throughput comparison, not a profiler-derived native/JNI optimization claim.
