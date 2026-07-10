# Offline Ascend Full Reproduction Run - 2026-07-11 01:54

## Run command

```bash
./forl0-offline-app.sh --reproduce-ascend --no-report --skip-docker-load --flink-home /home/shuhao/flink-1.20.3
./forl0-offline-app.sh --report-only --skip-docker-load --flink-home /home/shuhao/flink-1.20.3
```

## Runtime

- Platform: Ascend offline server
- Git commit: `216e6943b9ca2dec8b455a9c595da434d46e874b`
- Flink: `/home/shuhao/flink-1.20.3`
- TaskManagers: 2
- Slots: 8
- Default workload list: W01-W02, N01-N12, C01-C10
- Excluded from default list: benchset, high-risk client ops64/ops128 probes
- Report: `benchmark/results/reports/benchmark_report.html`

## Result summary

The one-key Ascend reproduction completed successfully and report-only generation completed successfully.
No benchmark runner failure was observed in this run.

ForL0 shows clear positive results on multiple NexMark scenarios and on client state-pressure/scalar-batch scenarios.
WordCount high-cardinality remains unstable and was negative in this full run, despite an earlier paired fixed-JAR probe being slightly positive.

## WordCount

| ID | Scenario | HashMap rec/s/core | ForL0 rec/s/core | ForL0 delta | Raw files |
| --- | --- | ---: | ---: | ---: | --- |
| W01/W02 | `wordcount_high_cardinality` | 566,594 | 402,276 | -29.0% | `wordcount_hashmap_20260711_010028.json`, `wordcount_forl0_20260711_011108.json` |

## NexMark

| ID pair | Query/config | HashMap eps | ForL0 eps | eps delta | HashMap eps/core | ForL0 eps/core | eps/core delta | Result dirs |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| N01/N02 | q18 `forl0_tps_probe` | 190,090 | 193,840 | +2.0% | 8,103 | 33,192 | +309.6% | `nexmark_20260711_011124`, `nexmark_20260711_011306` |
| N03/N04 | q19 `forl0_tps_probe` | 1,030,000 | 1,110,000 | +7.8% | 69,314 | 126,424 | +82.4% | `nexmark_20260711_011446`, `nexmark_20260711_011707` |
| N05/N06 | q4 `forl0_promising` | 48,500 | 50,310 | +3.7% | 2,315 | 3,018 | +30.4% | `nexmark_20260711_011928`, `nexmark_20260711_012120` |
| N07/N08 | q18 `forl0_promising` | 480,280 | 589,770 | +22.8% | 9,556 | 86,098 | +801.0% | `nexmark_20260711_012317`, `nexmark_20260711_012533` |
| N09/N10 | q19 `forl0_promising` | 1,040,000 | 1,100,000 | +5.8% | 69,987 | 133,333 | +90.5% | `nexmark_20260711_012749`, `nexmark_20260711_013042` |
| N11/N12 | q20 `forl0_promising` | 31,840 | 32,440 | +1.9% | 1,097 | 7,083 | +545.6% | `nexmark_20260711_013334`, `nexmark_20260711_013559` |

## Client use case

| ID pair | Scenario | HashMap rec/s/core | ForL0 rec/s/core | ForL0 delta | Raw files |
| --- | --- | ---: | ---: | ---: | --- |
| C01/C02 | `contract_baseline` | 37.055 | 36.981 | -0.2% | `client_usecase_hashmap_20260711_013837.json`, `client_usecase_forl0_20260711_013903.json` |
| C03/C04 | `forl0_optimized` | 370.637 | 369.403 | -0.3% | `client_usecase_hashmap_20260711_013930.json`, `client_usecase_forl0_20260711_013956.json` |
| C05/C06 | `state_pressure_300k` | 889.169 | 934.509 | +5.1% | `client_usecase_hashmap_20260711_014142.json`, `client_usecase_forl0_20260711_014324.json` |
| C07/C08 | `state_pressure_1m` | 903.975 | 944.650 | +4.5% | `client_usecase_hashmap_20260711_014823.json`, `client_usecase_forl0_20260711_015313.json` |
| C09/C10 | `scalar_state_probe_2m_ops16_batch` | 122,638 | 123,636 | +0.8% | `client_usecase_hashmap_20260711_015335.json`, `client_usecase_forl0_20260711_015357.json` |

## Follow-up

- Keep the current NexMark and client state-pressure/scalar-batch entries as the stable positive Ascend default set.
- Treat WordCount high-cardinality as unresolved: the full one-key run was negative, so it should not be used as a headline positive result without additional tuning or repeated-run stability evidence.
- The latest script fixes successfully prevented silent NexMark failures and stale backend JAR installation in this full run.
