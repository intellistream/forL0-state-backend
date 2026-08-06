# Ascend Total-TPS Follow-up - 2026-07-11 09:54

Run type: real-online, selected one-click Ascend reproduction.

Command:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 --skip-docker-load --workloads N03,N04,N05,N06,N07,N08 --no-report
```

Manifest:

```text
benchmark/results/run_logs/ascend_reproduction_20260711_095409.tsv
```

## Rationale

The customer does not care about lower CPU/core usage if total throughput does
not improve. This follow-up therefore tested a backend-specific runtime tuning:
HashMap remains at parallelism 4, while ForL0 can use parallelism 8 in
`forl0_total_tps_parallel_lateq`.

## Results

| IDs | Scenario | Query | HashMap eps | ForL0 eps | Delta | Decision |
|---|---|---|---:|---:|---:|---|
| N03/N04 | `forl0_total_tps_parallel_lateq` | q19 | 972,360 | 978,640 | +0.6% | Keep as a small positive total-TPS case |
| N05/N06 | `forl0_no_full_gc_promising` | q18 | 451,660 | 557,380 | +23.4% | Keep as strong total-TPS case |
| N07/N08 | `forl0_total_tps_parallel_lateq` | q20 | 31,770 | 31,750 | -0.1% | Remove from default headline list |

Additional manual probes before this selected one-click run:

- q19 `forl0_no_full_gc_lateq_deep`: HashMap 1.03M eps vs ForL0 1.01M eps, not kept.
- q20 `forl0_no_full_gc_lateq_deep`: HashMap p4 at 700K input dropped a TaskManager, invalid.
- q20 `forl0_total_tps_parallel_lateq`: one manual pair reached 31.62K vs 33.01K eps, but the one-click repeat was 31.77K vs 31.75K eps; not stable enough for default.

## Default-List Change

The default numbered Ascend list now keeps only total-throughput-oriented
NexMark pairs:

- q18 TPS probe (`N01/N02`);
- q19 total-TPS parallel probe (`N03/N04`);
- q18 no-Full-GC promising probe (`N05/N06`).

q20 remains available in `benchmark.yaml` for diagnosis, but it is no longer in
the default one-click reproduction list because its total TPS was not stable.
