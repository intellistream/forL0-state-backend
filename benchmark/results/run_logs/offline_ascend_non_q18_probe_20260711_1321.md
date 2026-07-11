# Ascend non-q18 NexMark probe, 2026-07-11 13:21

Evidence label: real-online.

Goal: continue beyond q18 and identify same-parallelism non-q18 NexMark
queries that can be kept in the one-click Ascend reproduction list.

All probes used the current main commit:

```text
6dff53d9344278ac753469e5ae75d4504b263567
```

Each valid pair used the same query, input proportions, operator parallelism,
TaskManager count, slot count, and monitor window for HashMap and ForL0.

## Completed pairs

| Query | Scenario | HashMap | ForL0 | Delta | Decision |
| --- | --- | ---: | ---: | ---: | --- |
| q20 | `forl0_no_full_gc_allq_pressure` | 29.09 K/s | 31.77 K/s | +9.2% | Add as non-q18 positive/boundary default pair |
| q4 | `forl0_no_full_gc_promising` | 48.49 K/s | 48.58 K/s | +0.2% | Do not add; stable but effectively tied |
| q19 | `forl0_tps_probe` | 978.82 K/s | 1.00 M/s | +2.2% | Add as non-q18 positive/boundary default pair |

Result directories:

```text
benchmark/results/nexmark_20260711_132613  # q20 hashmap
benchmark/results/nexmark_20260711_132845  # q20 forl0
benchmark/results/nexmark_20260711_133341  # q4 hashmap, low-pressure
benchmark/results/nexmark_20260711_133539  # q4 forl0, low-pressure
benchmark/results/nexmark_20260711_133753  # q19 hashmap
benchmark/results/nexmark_20260711_134023  # q19 forl0
```

## Failed high-pressure probes

These probes are not one-click-safe on the current Ascend environment because
the HashMap side lost a TaskManager and the fail-fast guard stopped the run.

| Query | Scenario | Failure marker |
| --- | --- | --- |
| q4 | `forl0_no_full_gc_pressure`, 600 K/s | `benchmark/results/run_logs/FAILED_nexmark_hashmap_20260711_132319.txt` |
| q20 | `forl0_no_full_gc_lateq_deep`, 700 K/s | `benchmark/results/run_logs/FAILED_nexmark_hashmap_20260711_132549.txt` |
| q4 | `forl0_q4_no_full_gc_auction_heavy`, 500 K/s | `benchmark/results/run_logs/FAILED_nexmark_hashmap_20260711_133318.txt` |

## Interpretation

The current non-q18 results do not replace q18 as the headline: q18 remains the
only Ascend default query with a verified 30%+ same-parallelism total-throughput
gain in the latest one-click run. However, q19 and q20 are stable non-q18
positive samples and should be present in the default reproduction list so the
suite is not q18-only. q4 is useful as a boundary case: pressure settings that
previously looked promising now destabilize the HashMap baseline, while the
safe lower-pressure setting is essentially tied.
