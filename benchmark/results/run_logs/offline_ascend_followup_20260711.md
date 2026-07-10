# Ascend offline follow-up, 2026-07-11

Evidence label: `real-online` for runs that launched the Flink Docker cluster and measured live jobs; `derived-artifact` for report-only regeneration.

## Bug fixed: stale backend JAR selected by one-key setup

`docker/server_setup.sh` selected the backend JAR in this order:

1. `target/flink-statebackend-forL0-1.0-SNAPSHOT.jar`
2. `docker/deploy/flink-statebackend-forL0-1.0-SNAPSHOT.jar`

On the Ascend box, `target/` held an older 09:55 JAR that did not expose `ForL0MapState.sumSequentialLong`, while `docker/deploy/` held the newer 20:32 JAR with the JNI batch/sum methods. Because the one-key setup prefers `target`, report-only and reproduce runs could silently install the stale JAR into `/home/shuhao/flink-1.20.3/lib/`.

Fix:

- `docker/server_setup.sh` now prefers `docker/deploy/flink-statebackend-forL0-1.0-SNAPSHOT.jar` and uses `target/` only as fallback.
- Verified with `javap` that the installed Flink lib JAR contains:
  - `addAndGetLong(Object,long)`
  - `addSequentialAndSumLong(long,int,long,long)`
  - `sumSequentialLong(long,int,long)`
- Verified SHA-256 match between `docker/deploy/flink-statebackend-forL0-1.0-SNAPSHOT.jar` and `/home/shuhao/flink-1.20.3/lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar`.

## Bug fixed: NexMark failed queries did not propagate through run_benchmark.py

The q4/q9 high-pressure probes showed that `run_nexmark.py` detected failed queries, but `run_benchmark.py` still returned success after storing an empty/failed result block. This is unsafe for offline reproduction because the top-level cleanup trap only runs when the command exits non-zero.

Fix:

- `benchmark/scripts/run_benchmark.py` now checks `metrics["failed_queries"]` after `NexmarkRunner.run(...)`.
- Any failed NexMark query now prints the failed backend/query map and exits with status 1.
- Verified with q4 auction-heavy at 500 K TPS: HashMap dropped a TaskManager, the command exited 1, and cleanup wrote `FAILED_nexmark_hashmap_20260711_004903.txt`.

## Targeted retests

| Scenario | HashMap | ForL0 | Result |
| --- | ---: | ---: | --- |
| Client `scalar_state_probe_2m_ops16_batch` after JAR fix | 123,363 rec/s/core | 123,526 rec/s/core | fixed previous -50% regression; now tie/slightly positive |
| WordCount `high_cardinality` after JAR fix | 436,008 rec/s/core | 439,286 rec/s/core | small +0.75%, no longer negative in this paired window |
| WordCount `high_cardinality`, smaller ForL0 override `8192/131072` | n/a | 418,464 rec/s/core | worse; reverted to `16384/262144` |
| NexMark q5 `forl0_no_full_gc_allq_pressure` | 14.83 K eps | 12.48 K eps | not default-worthy on current 16 GB Ascend cluster |
| NexMark q9 `forl0_tps_probe` | 11.74 K eps, 1.32 K/core | 11.80 K eps, 2.94 K/core | total throughput tie, CPU efficiency positive |
| NexMark q4 `forl0_q4_no_full_gc_auction_heavy` | failed | n/a | not default-worthy; HashMap dropped to 1 TM |

## Default-list decision

- Keep WordCount `high_cardinality` because it is customer-required and now has a small positive paired result in the fixed-JAR run.
- Keep NexMark q18/q20/q19/q4 stable/default entries from `forl0_tps_probe` and `forl0_no_full_gc_promising`.
- Do not add q5 or high-pressure q4 to the default Ascend list because they are not stable on the current 16 GB TM setup.
- Keep client contract/optimized/300k/1m because they are customer-usecase compatibility/no-regression evidence.
- Keep only the ops16 batch scalar probe as a diagnostic pair; remove ops64/ops128 batch probes from the default list because they add time without improving the headline result.

## Remaining boundary

ForL0 is clearly positive in multiple NexMark scenarios and no longer regresses the selected WordCount/client pairs after the deployment fix. The current Ascend box does not reproduce the v3 report's stronger q4/q5/q9 pressure wins under 16 GB TaskManagers; the high-pressure versions either lose or drop TaskManagers, so they should remain opt-in rather than default.
