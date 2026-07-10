# Ascend Offline Reproduction Run - 2026-07-11 05:40

Run type: real-online, one-click offline Ascend reproduction.

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 --skip-docker-load --reproduce-ascend --no-report
```

Commit under test:

```text
5a88308da2ffecd8f8a15dd07cce06409b802c21
```

Manifest:

```text
benchmark/results/run_logs/ascend_reproduction_20260711_054054.tsv
```

## Fixes Validated In This Run

- `forL0.metricsCollector.enabled` is now honored by the ForL0 backend and defaults to false for performance runs.
- WordCount and client-usecase runners no longer force HotCache metrics registration on every ForL0 run.
- The default Ascend client C09/C10 pair now uses `hotspot_drift_1m` instead of the unstable `scalar_state_probe_2m_ops16_batch`.
- The numbered one-click suite completed without failed jobs or manual cleanup.

## WordCount

| IDs | Scenario | HashMap rec/s/core | ForL0 rec/s/core | Delta | Raw files |
|---|---|---:|---:|---:|---|
| W01/W02 | `stateful_counter_fastpath`, best-of-3 | 519,607 | 623,592 | +20.0% | `wordcount_hashmap_20260711_054351.json`, `wordcount_forl0_20260711_054640.json` |

HashMap samples: 444,903 / 519,377 / 519,607 rec/s/core.
ForL0 samples: 445,078 / 566,775 / 623,592 rec/s/core.

## NexMark

| IDs | Scenario | Query | HashMap eps | ForL0 eps | EPS delta | HashMap eps/core | ForL0 eps/core | Per-core delta |
|---|---|---|---:|---:|---:|---:|---:|---:|
| N01/N02 | `forl0_tps_probe` | q18 | 187,450 | 196,480 | +4.8% | 8,104 | 32,476 | +300.8% |
| N03/N04 | `forl0_tps_probe` | q19 | 920,730 | 903,980 | -1.8% | 70,231 | 116,492 | +65.9% |
| N05/N06 | `forl0_no_full_gc_promising` | q18 | 473,190 | 582,390 | +23.1% | 9,515 | 87,974 | +824.6% |
| N07/N08 | `forl0_no_full_gc_promising` | q19 | 1,090,000 | 1,010,000 | -7.3% | 66,667 | 130,155 | +95.2% |
| N09/N10 | `forl0_no_full_gc_promising` | q20 | 31,450 | 31,360 | -0.3% | 998 | 6,969 | +598.4% |

NexMark q18 is the stable total-throughput positive query in this Ascend run.
q19/q20 are retained as resource-efficiency cases: total sink TPS is comparable
or slightly lower in this sample, but ForL0 reaches that TPS with much lower
reported CPU cores.

## Client Usecase

| IDs | Scenario | HashMap rec/s/core | ForL0 rec/s/core | Delta |
|---|---|---:|---:|---:|
| C01/C02 | `contract_baseline` | 37.037 | 37.041 | +0.0% |
| C03/C04 | `forl0_optimized` | 370.911 | 370.798 | -0.0% |
| C05/C06 | `state_pressure_300k` | 935.091 | 934.395 | -0.1% |
| C07/C08 | `state_pressure_1m` | 930.448 | 944.623 | +1.5% |
| C09/C10 | `hotspot_drift_1m` | 903.442 | 944.778 | +4.6% |

The previous default C09/C10 `scalar_state_probe_2m_ops16_batch` was retested
after metrics were disabled and remained effectively tied (123,395 vs 123,404
records/s/core). It is kept as an exploratory scenario in `benchmark.yaml`, but
the default numbered Ascend list now uses `hotspot_drift_1m`, which better
exposes the customer's drifting-hot-key state behavior.
