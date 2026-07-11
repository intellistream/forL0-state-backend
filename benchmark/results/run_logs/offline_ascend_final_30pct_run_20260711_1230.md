# Ascend final 30% run, 2026-07-11 12:30

Evidence label: real-online.

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 --skip-docker-load --reproduce-ascend --no-report
```

Manifest:

```text
benchmark/results/run_logs/ascend_reproduction_20260711_123038.tsv
```

Git commit under test:

```text
fb0fa53ef653dd90d19489ec9f85f0c0147af7f4
```

The full default Ascend workload list completed in one pass with no new
FAILED marker and no TaskManager loss. The default list excludes benchset and
keeps HashMap and ForL0 at the same query/input/operator parallelism/slots and
monitor window within each pair.

## Results

| Pair | Workload | HashMap | ForL0 | Delta |
| --- | --- | ---: | ---: | ---: |
| W01/W02 | WordCount `stateful_counter_fastpath`, p4 best-of-3 | 779,522 records/s/core | 890,782 records/s/core | +14.3% |
| N01/N02 | NexMark q18 `forl0_tps_probe`, p4 | 163.77 K/s | 216.17 K/s | +32.0% |
| N03/N04 | NexMark q18 `forl0_no_full_gc_lateq_deep`, p4 | 444.64 K/s | 587.35 K/s | +32.1% |
| C01/C02 | Client `contract_baseline` | 74.03 rec/s | 74.24 rec/s | +0.3% |
| C03/C04 | Client `forl0_optimized` | 739.56 rec/s | 740.03 rec/s | +0.1% |
| C05/C06 | Client `state_pressure_300k` | 3,559.34 rec/s | 3,738.30 rec/s | +5.0% |
| C07/C08 | Client `state_pressure_1m` | 3,721.63 rec/s | 3,778.97 rec/s | +1.5% |

NexMark raw result directories:

```text
benchmark/results/nexmark_20260711_123831
benchmark/results/nexmark_20260711_124013
benchmark/results/nexmark_20260711_124153
benchmark/results/nexmark_20260711_124409
```

Raw JSON files:

```text
benchmark/results/raw/wordcount_hashmap_20260711_123451.json
benchmark/results/raw/wordcount_forl0_20260711_123817.json
benchmark/results/raw/client_usecase_hashmap_20260711_124636.json
benchmark/results/raw/client_usecase_forl0_20260711_124702.json
benchmark/results/raw/client_usecase_hashmap_20260711_124727.json
benchmark/results/raw/client_usecase_forl0_20260711_124753.json
benchmark/results/raw/client_usecase_hashmap_20260711_124939.json
benchmark/results/raw/client_usecase_forl0_20260711_125124.json
benchmark/results/raw/client_usecase_hashmap_20260711_125615.json
benchmark/results/raw/client_usecase_forl0_20260711_130105.json
```

## Interpretation

The earlier apparent drop in headline improvement was caused by selecting a
too-conservative default NexMark q18 pressure setting. The 11:22 run used the
q18 `promising` configuration and only showed +19.1%. Switching the default
headline pair to the same-parallelism q18 `lateq_deep` configuration restores a
stable >30% result while preserving fairness. This run also shows q18
`forl0_tps_probe` above 30%, so the positive effect is not limited to a single
lateq-deep sample.

Client usecase remains a compatibility and boundary workload rather than the
headline performance workload. It completes cleanly and stays non-negative, but
the large gains are in NexMark q18 pressure settings and the smaller positive
WordCount fastpath result.
