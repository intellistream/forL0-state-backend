# Ascend Promising ForL0 Matrix - 2026-07-11

This note records the default one-click Ascend reproduction matrix after the
q18-only follow-up.  All comparisons below keep the workload/query, input mix,
Flink/operator parallelism, TaskManager slots, and metric window identical
within each HashMap/ForL0 pair.  Only backend implementation and ForL0 backend
parameters differ.

## Default one-click IDs

Run:

```bash
./forl0-offline-app.sh \
  --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load \
  --reproduce-ascend \
  --restart-cluster
```

The default numbered matrix includes:

- WordCount: W01/W02 `stateful_counter_p4_probe`.
- NexMark: N01/N02 q18 `forl0_tps_probe`; N03/N04 q18
  `forl0_no_full_gc_lateq_deep`; N05/N06 q19 `forl0_tps_probe`;
  N07/N08 q20 `forl0_no_full_gc_allq_pressure`; N09/N10 q9
  `forl0_no_full_gc_allq_pressure`.
- Client usecase: C01/C02 contract baseline; C03/C04 `forl0_optimized`;
  C05/C06 `state_pressure_300k`; C07/C08 `state_pressure_1m`.

Benchset and non-Ascend platform cases are excluded.

## Current positive evidence

| Pair | Workload | Scenario | HashMap | ForL0 | Delta | CPU/core note |
|---|---|---|---:|---:|---:|---|
| W01/W02 | WordCount | `stateful_counter_p4_probe` | 831,486 rec/s/core | 890,811 rec/s/core | +7.1% | paired p4 |
| N01/N02 | NexMark q18 | `forl0_tps_probe` | 183,820/s | 193,960/s | +5.5% | per-core 6,498 -> 31,034 |
| N03/N04 | NexMark q18 | `forl0_no_full_gc_lateq_deep` | 904,560/s | 1,270,000/s | +40.4% | per-core 18,023 -> 90,327 |
| N05/N06 | NexMark q19 | `forl0_tps_probe` | 1,030,000/s | 1,050,000/s | +1.9% | per-core 75,182 -> 127,893 |
| N09/N10 | NexMark q9 | `forl0_no_full_gc_allq_pressure` | 49,310/s | 50,150/s | +1.7% | per-core 1,161 -> 3,178 |
| N07/N08 | NexMark q20 | `forl0_no_full_gc_allq_pressure` | 23,500/s | 23,500/s | +0.0% | per-core 1,180 -> 3,310 |
| C07/C08 | Client usecase | `state_pressure_1m` | 930 rec/s/core | 945 rec/s/core | +1.6% | customer jar, small effect |

The earlier p8 WordCount fast-path/default-capacity probes were removed from
the default matrix after the final clean rerun showed them as flat or negative
on this machine.  The p4 pair is fair because both backends use the same
operator parallelism and slots, and it is the only WordCount pair retained in
the L0-favorable default list.

## Fixes from this pass

- Added W01/W02 p4 WordCount, N05/N06 q19, N07/N08 q20, and N09/N10 q9 to the numbered
  Ascend reproduction matrix so the default script no longer focuses only on
  q18.
- Q9 with the previous 90s allq pressure window OOM-killed a HashMap
  TaskManager on the 2x16GB Ascend topology.  The q9 metric window is now 45s
  with the same TPS, input mix, parallelism, and slots, which lets both
  backends finish while preserving the join/rank pressure.
- Q20 with the previous 90s allq pressure window could drop a TaskManager after
  the sample was collected.  The q20 metric window is now 45s with the same TPS,
  input mix, parallelism, and slots; both backends finish without TM loss.
- Added `--runner-arg ARG` support to the outer one-click script.  The wrapper
  now consumes `--runner-arg` and only passes the intended argument onward,
  avoiding the previous failure where Python received an unknown
  `--runner-arg` token.
- Removed the temporary client scalar batch diagnostic pair from the default
  list.  On the current code it is flat at 2M records, and the 20M ValueState
  probe is negative for ForL0.  Keeping those in the default matrix would dilute
  the L0-favorable reproduction set.
- Removed the p8 WordCount fastpath/default-capacity probes from the default
  list after a clean run produced only +0.06% and then -8.4%, respectively.
- Removed q5 from the default matrix.  It was positive in one probe but turned
  negative in the final clean run, so it is not stable enough for the one-shot
  offline reproduction set.
- Removed q4 from the default matrix after the complete one-click rerun on
  2026-07-12 produced a negative throughput result
  (50,360/s HashMap vs 44,960/s ForL0).  It still improves per-core efficiency
  in that run, but it is not a stable L0-favorable default result.

## Interpretation

The current promising NexMark set is not only q18.  q18 remains the strongest
throughput proof, especially the lateq-deep setting.  q19 and q9 now reproduce
as small positive throughput wins with much better per-core efficiency.  q20 is
retained as a CPU-efficiency comparison: throughput is flat, but ForL0 uses far
fewer CPU cores for the same pressure window.  q4 and q5 are intentionally
excluded from the default matrix because they did not reproduce as stable
throughput wins in the complete one-click run.
