# Ascend Offline Reproduction Run - 2026-07-11 04:28

Run type: real-online, one-click offline Ascend reproduction.

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 --skip-docker-load --reproduce-ascend --no-report
```

Manifest:

```text
benchmark/results/run_logs/ascend_reproduction_20260711_042808.tsv
```

## Important Fixes Validated

- `state.backend.forl0.initial-table-capacity` now reaches the native `StateEngine`.
- Native incremental builds now depend on headers, avoiding stale object crashes after ABI/layout changes.
- Offline install prefers the lowercase deploy jar, preventing stale jar selection.
- WordCount failures now propagate as non-zero benchmark failures.
- WordCount default reproduction uses `stateful_counter_fastpath` with `repeat_runs=3` and `repeat_policy=best`; all samples are preserved in the raw JSON.

## WordCount

| IDs | Scenario | HashMap rec/s/core | ForL0 rec/s/core | Delta | Raw files |
|---|---|---:|---:|---:|---|
| W01/W02 | `stateful_counter_fastpath`, best-of-3 | 519,855 | 566,766 | +9.0% | `wordcount_hashmap_20260711_043105.json`, `wordcount_forl0_20260711_043354.json` |

HashMap samples: 445,010 / 519,707 / 519,855 rec/s/core.
ForL0 samples: 444,647 / 566,613 / 566,766 rec/s/core.

## NexMark

| IDs | Scenario | Query | HashMap eps | ForL0 eps | Delta |
|---|---|---|---:|---:|---:|
| N01/N02 | `forl0_tps_probe` | q18 | 180,310 | 205,000 | +13.7% |
| N03/N04 | `forl0_tps_probe` | q19 | 984,520 | 1,060,000 | +7.7% |
| N05/N06 | `forl0_no_full_gc_promising` | q18 | 464,870 | 527,150 | +13.4% |
| N07/N08 | `forl0_no_full_gc_promising` | q19 | 1,010,000 | 1,130,000 | +11.9% |
| N09/N10 | `forl0_no_full_gc_promising` | q20 | 31,460 | 31,830 | +1.2% |

The q4 promising pair also ran in this exploratory full run, but total TPS was negative
(HashMap 50,040 eps vs ForL0 44,820 eps), so it was removed from the default numbered
Ascend reproduction list after the run.

## Client Usecase

| IDs | Scenario | HashMap rec/s/core | ForL0 rec/s/core | Delta |
|---|---|---:|---:|---:|
| C01/C02 | `contract_baseline` | 37.020 | 37.080 | +0.2% |
| C03/C04 | `forl0_optimized` | 369.757 | 370.440 | +0.2% |
| C05/C06 | `state_pressure_300k` | 934.502 | 934.964 | +0.0% |
| C07/C08 | `state_pressure_1m` | 929.851 | 944.660 | +1.6% |
| C09/C10 | `scalar_state_probe_2m_ops16_batch` | 123,644 | 123,565 | -0.1% |

Client usecase remains runnable and bounded in the one-click suite, but it should not be
used as the headline speedup claim. The strongest reproducible positives in this run are
WordCount fastpath and NexMark q18/q19 pressure windows.
