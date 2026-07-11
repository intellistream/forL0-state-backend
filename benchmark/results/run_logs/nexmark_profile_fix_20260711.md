# NexMark profiler fix and effect check - 2026-07-11

## Question

The current headline improvement looked smaller than earlier records.  I checked
whether the latest script/configuration had drifted or whether the difference was
mostly a measurement-window issue.

## Findings

- The strongest earlier WordCount point (`623,592 records/s/core`) came from an
  `AUTO_STOPPED` run.  The clean later A/B baseline is:
  - HashMap: `519,573 records/s/core`
  - ForL0: `566,784 records/s/core`
  - Stable lift: `+9.1%`
- The same-parallelism NexMark q18 lateq-deep headline pair from the final
  30%-run log was:
  - HashMap: `444.64 K/s`
  - ForL0: `587.35 K/s`
  - Lift: `+32.1%`
- A fresh N04 profiler run after the script fix produced:
  - ForL0: `567.81 K/s`, `84.12 K/s/core`
  - Profile: `benchmark/results/nexmark_20260711_152559/profiles/flamegraph_cpu_forl0_q18_20260711_152559.html`
  - Profile quality: `OK`, idle `15.1%`

The fresh ForL0 sample is about `3.3%` below the earlier q18 lateq-deep ForL0
sample, but it is still in the same positive window.  Compared with the recorded
HashMap q18 lateq-deep sample (`444.64 K/s`), this run would still be about
`+27.7%`.

## Script fix

`benchmark/scripts/run_nexmark.py` previously assumed the profiled container was
always `flink-taskmanager-1` and the Java process was always PID `1`.  That is
fragile after cluster restarts or container-number drift.  The fix:

- selects a currently running TaskManager container,
- detects the actual TaskManager Java PID with `jps`, `pgrep`, or PID-1 fallback,
- records the selected PID in the profiler session,
- stops the same PID and still attempts to copy an already-written profile if
  `asprof stop` reports a non-fatal error.

This does not change ForL0 implementation code or non-ForL0 workload behavior.

## Interpretation

The lower overall lift is mainly from using stricter, fairer samples:

- same operator/global parallelism is now enforced for comparisons,
- unstable `AUTO_STOPPED` highs are not treated as default evidence,
- the default Ascend list favors stable same-parallelism cases rather than the
  older per-core-only or mismatched-parallelism probes.

There was also a real reproducibility bug in the profiler path, now fixed, but
the N04 ForL0 throughput itself did not collapse after the fix.
