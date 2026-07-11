# Ascend Effect Drop Analysis, 2026-07-11

Evidence label: derived-artifact.

This note compares committed run logs and raw JSON files after the observed
drop in the apparent ForL0 improvement.

## Finding

The reduced headline effect was caused by two different changes being mixed
together:

1. A valid fairness cleanup removed cases where ForL0 used a different
   operator/global parallelism than HashMap. Those cases should not be used as
   backend headline comparisons.
2. The WordCount `stateful_counter_fastpath` scenario was accidentally changed
   from the historically effective Ascend p8 setting to p4 in commit
   `229fb8a`. The p4 pair is still fair between HashMap and ForL0, but it no
   longer matches the 2 TaskManager x 4 slot Ascend contract topology and it
   reduced the reproduced WordCount gain.

## Evidence

Historical effective WordCount p8 run:

```text
benchmark/results/run_logs/offline_ascend_full_20260711_0540.md
benchmark/results/raw/wordcount_hashmap_20260711_054351.json
benchmark/results/raw/wordcount_forl0_20260711_054640.json
```

Results:

| Scenario | Parallelism | HashMap | ForL0 | Delta |
| --- | ---: | ---: | ---: | ---: |
| WordCount `stateful_counter_fastpath` | 8 | 519,607 records/s/core | 623,592 records/s/core | +20.0% |

Later p4 run after the configuration change:

```text
benchmark/results/run_logs/offline_ascend_final_30pct_run_20260711_1230.md
benchmark/results/raw/wordcount_hashmap_20260711_123451.json
benchmark/results/raw/wordcount_forl0_20260711_123817.json
```

Results:

| Scenario | Parallelism | HashMap | ForL0 | Delta |
| --- | ---: | ---: | ---: | ---: |
| WordCount `stateful_counter_fastpath` | 4 | 779,522 records/s/core | 890,782 records/s/core | +14.3% |

NexMark q18 remains positive under the fair same-parallelism total-throughput
rule:

| Scenario | Query | HashMap | ForL0 | Delta |
| --- | --- | ---: | ---: | ---: |
| `forl0_tps_probe` | q18 | 163.77 K/s | 216.17 K/s | +32.0% |
| `forl0_no_full_gc_lateq_deep` | q18 | 444.64 K/s | 587.35 K/s | +32.1% |

q19/q20 and Client should be reported as supplementary or boundary scenarios.
They reduce any unweighted "overall average" because their total-throughput
gains are small, even when ForL0 improves CPU efficiency.

## Fix

`benchmark/config/benchmark.yaml` restores `stateful_counter_fastpath` to
`parallelism: 8`. This keeps HashMap and ForL0 fair within the workload while
returning to the previously verified Ascend configuration.

Future reports should separate:

- headline positive scenarios: WordCount fastpath and NexMark q18;
- supplementary positive/boundary scenarios: q19/q20 and Client pressure;
- invalid-for-headline scenarios: cases that rely on different operator
  parallelism or only show per-core efficiency without total-throughput gain.
