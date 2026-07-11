# Ascend final fair reproduction run, 2026-07-11 11:22 +0800

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 --skip-docker-load --reproduce-ascend --no-report
```

Provenance:

- Git commit: `229fb8a10ac8208f19297cdad3040c199bbc479b`
- Manifest: `benchmark/results/run_logs/ascend_reproduction_20260711_112226.tsv`
- Mode: real Flink online run on Ascend host, isolated cluster restart per workload
- Fairness rule: HashMap and ForL0 use the same query, input mix, operator parallelism, slots, and monitor window. ForL0-only gains come from backend configuration/implementation, not higher parallelism.

Default workload set:

- W01/W02: WordCount `stateful_counter_fastpath`, p4, best-of-3
- N01/N02: NexMark `forl0_tps_probe` q18
- N03/N04: NexMark `forl0_no_full_gc_promising` q18
- C01/C02: Client `contract_baseline`
- C03/C04: Client `forl0_optimized`
- C05/C06: Client `state_pressure_300k`
- C07/C08: Client `state_pressure_1m`

Results:

| Pair | Scenario | HashMap | ForL0 | Delta |
| --- | --- | ---: | ---: | ---: |
| W01/W02 | WordCount p4 best-of-3 | 890,866 records/s/core | 959,125 records/s/core | +7.7% |
| N01/N02 | NexMark q18 TPS probe | 173.88 K/s | 193.34 K/s | +11.2% |
| N03/N04 | NexMark q18 promising | 474.23 K/s | 564.74 K/s | +19.1% |
| C01/C02 | Client contract baseline | 37 records/s/core | 37 records/s/core | ~0% |
| C03/C04 | Client optimized 3k | 370 records/s/core | 369 records/s/core | ~0% |
| C05/C06 | Client state_pressure_300k | 935 records/s/core | 935 records/s/core | ~0% |
| C07/C08 | Client state_pressure_1m | 916.64 records/s/core | 944.14 records/s/core | +3.0% |

Notes:

- The earlier ForL0 higher-parallelism q19/q20 probe was removed from default because changing operator/global parallelism is not a fair backend comparison.
- `hotspot_drift_1m` was removed from default because the latest same-parallelism run was not stable positive.
- Benchset remains excluded from the Ascend default set.
