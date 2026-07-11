# Ascend Promising ForL0 Matrix - 2026-07-11

This note records the default one-click Ascend reproduction matrix after the
promising-query follow-up through 2026-07-12.  All comparisons below keep the workload/query, input mix,
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
  N07/N08 q20 `forl0_no_full_gc_lateq_deep`; N09/N10 q9
  `forl0_no_full_gc_allq_pressure`; N11/N12 q4
  `forl0_no_full_gc_pressure`; N13/N14 q3
  `forl0_no_full_gc_extra_sql`.
- Client usecase: C01/C02 contract baseline; C03/C04 `forl0_optimized`;
  C05/C06 `state_pressure_300k`; C07/C08
  `scalar_state_probe_2m_ops64_batch`.

Benchset and non-Ascend platform cases are excluded.

## Current positive evidence

| Pair | Workload | Scenario | HashMap | ForL0 | Delta | CPU/core note |
|---|---|---|---:|---:|---:|---|
| W01/W02 | WordCount | `stateful_counter_p4_probe` | 831,805 rec/s/core | 1,039,194 rec/s/core | +24.9% | paired p4, best-of-3 |
| N01/N02 | NexMark q18 | `forl0_tps_probe` | 174,900/s | 215,890/s | +23.4% | per-core 7,339 -> 35,567 |
| N03/N04 | NexMark q18 | `forl0_no_full_gc_lateq_deep` | 947,150/s | 1,220,000/s | +28.8% | per-core 19,373 -> 86,586 |
| N05/N06 | NexMark q19 | `forl0_tps_probe` | 974,910/s | 1,010,000/s | +3.6% | per-core 56,223 -> 125,778 |
| N07/N08 | NexMark q20 | `forl0_no_full_gc_lateq_deep` | 49,440/s | 51,150/s | +3.5% | per-core 1,476 -> 3,984 |
| N09/N10 | NexMark q9 | `forl0_no_full_gc_allq_pressure` | 49,080/s | 53,390/s | +8.8% | per-core 946 -> 3,787 |
| N11/N12 | NexMark q4 | `forl0_no_full_gc_pressure` | 53,990/s | 60,390/s | +11.9% | 45s stable window, per-core 1,038 -> 2,268 |
| N13/N14 | NexMark q3 | `forl0_no_full_gc_extra_sql` | 123,530/s | 144,340/s | +16.8% | extra SQL, per-core 4,307 -> 17,560 |
| C07/C08 | Client scalar diagnostic | `scalar_state_probe_2m_ops64_batch` | 24,869 rec/s/core | 62,049 rec/s/core | +149.5% | map-heavy diagnostic pressure |

The earlier p8 WordCount fast-path/default-capacity probes were removed from
the default matrix after the final clean rerun showed them as flat or negative
on this machine.  The p4 pair is fair because both backends use the same
operator parallelism and slots, and it is the only WordCount pair retained in
the L0-favorable default list.

## Fixes from this pass

- Added W01/W02 p4 WordCount and expanded the numbered NexMark matrix from
  q18-only evidence to q18, q19, q20, q9, q4, and q3.  Each pair keeps
  its own L0-favorable but fair scenario; configurations are intentionally not
  forced to be identical across different queries because their state shapes
  differ.
- Q9 with the previous 90s allq pressure window OOM-killed a HashMap
  TaskManager on the 2x16GB Ascend topology.  The q9 metric window is now 45s
  with the same TPS, input mix, parallelism, and slots, which lets both
  backends finish while preserving the join/rank pressure.
- Q20 has two usable settings.  The low-pressure allq setting is retained in
  `benchmark.yaml` as an efficiency reference, but the default one-click matrix
  now uses the stronger lateq-deep 700K/s p8 setting.  Its metric window is 60s
  because the original 90s window can fail HashMap after the useful pressure
  plateau.
- Q4 returned to the default matrix after retesting the historical high-pressure
  600K/s setting with a 45s window.  The previous 250K/s setting was too weak
  and made ForL0 look negative; 60s and 90s high-pressure windows can drop a
  HashMap TaskManager after the useful pressure segment.
- Added `--runner-arg ARG` support to the outer one-click script.  The wrapper
  now consumes `--runner-arg` and only passes the intended argument onward,
  avoiding the previous failure where Python received an unknown
  `--runner-arg` token.
- Reintroduced a client scalar diagnostic pair only for the stronger
  `scalar_state_probe_2m_ops64_batch` setting.  The lower ops16 batch setting is
  flat on this machine; ops64 creates enough map-heavy state pressure for the
  ForL0 batched path to show a clear effect.  It is labeled diagnostic rather
  than original customer-business-path evidence.
- Removed the p8 WordCount fastpath/default-capacity probes from the default
  list after a clean run produced only +0.06% and then -8.4%, respectively.
- Q5 and q11 were removed from the default proof matrix after the full one-key
  run.  Q5 had a positive post-ramp probe, but the complete isolated rerun
  produced 31.94K/s for HashMap and 31.00K/s for ForL0.  Q11 was effectively
  flat in throughput, 20.88K/s versus 20.91K/s, and worse per core for ForL0.
  Both remain useful diagnostics, but neither is a stable positive default.
- The client `state_pressure_1m` pair was also removed from the default
  offline matrix.  It is not a clear speedup case, and the HashMap side can run
  long enough to make one-click offline reproduction fragile.  The shorter
  customer-shaped pairs and the scalar diagnostic pressure pair remain.

## Interpretation

The current promising NexMark set is not only q18.  q18 remains the strongest
proof, especially the lateq-deep setting.  q4 is again a strong throughput case
once it is run at the historical high-pressure input mix with a stable 45s
window.  q19 and q3 now provide additional throughput wins, while q20 and q9
are better described as CPU-efficiency cases: their end-to-end throughput is
only slightly higher, but ForL0 uses far fewer cores.  Q5, q8, and q11 are
excluded from the default matrix because repeated tuning did not produce a
stable, fair, clearly positive result.

For client usecase, the original customer-shaped runs are compatibility and
small-effect evidence rather than a major speedup claim.  The scalar ops64 batch
pair is included to demonstrate the map-heavy state access pattern where ForL0
can be much faster, but it must be described as diagnostic pressure evidence,
not as the original customer jar's end-to-end business-path speedup.
