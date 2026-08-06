# Client Usecase ForL0 effect analysis - 2026-07-09

## Context

The current customer example still does not show a meaningful ForL0 advantage. I rechecked the report, raw results, scenario definitions, and the customer jar bytecode. The conclusion is that this workload is a real-business compatibility/no-regression case, not a strong ForL0 performance case.

## Observed results

Raw results under `benchmark/results/raw/client_usecase_*.json` group as follows:

| Scenario shape | HashMap rec/s/core | ForL0 rec/s/core | Delta |
|---|---:|---:|---:|
| `csv_replay`, 300 records, parallelism 2 | 36.9 median | 37.3 median | +1.2% |
| 300k state pressure / csv-like path, parallelism 4 | 890.0 | 935.3 | +5.1% |
| 1M state pressure / csv-like path, parallelism 4 | 944.5 | 944.9 | +0.0% |
| `hotspot_drift_300k`, right delay 2 ms | 889.1 | 934.9 | +5.2% |
| `hotspot_drift_1m`, right delay 2 ms | 930.0 | 944.7 | +1.6% |
| `hotspot_state_left_2m`, lightweight payload | 41,303.4 | 41,542.6 | +0.6% |
| `hotspot_state_join_2m`, lightweight payload | 8,884.9 | 8,902.2 | +0.2% |
| `hotspot_drift_300k`, no right delay | 9,265.6 | 6,217.2 | -32.9% |

The latest HTML report path focuses on the latest `contract_baseline` run, which is only 300 records and completes in about 4 seconds. That is a smoke/contract path, not a performance window.

## Bytecode findings

`HuaweiTestFunction` extends `MvIncrementProcessFunction`. The parent function declares:

- `MapState<Long, List<PVMVLogType>> leftCache`
- `MapState<Long, PVMVLogType> rightCache`
- `MapState<String, Long> leftDuplicateRcd`
- `MapState<String, Long> rightDuplicateRcd`
- several `ValueState<Long>` and `ValueState<PVMVLogType>`

The stream is keyed by `PVMVLogType::joinKey`, and `joinKey()` builds a `String` from `partitionLogChannel`, `partitionApp`, `sessionKey`, and `requestKey`. Therefore the keyed state backend sees a string/generic outer key, not a compact long key.

For the main left-side path, `processElement1` does:

- duplicate check via `MapState<String, Long>.contains/put`
- `leftCache.get(eventTimeStamp)`
- mutate a Java `ArrayList<PVMVLogType>`
- `leftCache.put(eventTimeStamp, list)`
- timer registration through `ValueState<Long>`

For the right-side path, `processElement2` does:

- duplicate check via `MapState<String, Long>.contains/put`
- full `leftCache.iterator()` scan
- nested `List<PVMVLogType>` iteration
- `iterator.remove()`
- `rightCache.put(eventTimeStamp, PVMVLogType)`

Cleanup timers also scan `MapState.iterator()` and remove entries.

## Why ForL0 does not pop here

1. The job does not hit the fast strategy.
   `ForL0MapState` only chooses the strongest paths when the outer keyed-state key is `TYPE_INT64` and, ideally, map keys/values are primitive long. This customer job is keyed by a generated string join key, so `ForL0MapState` falls into `GENERIC`.

2. The dominant value is not scalar.
   The most important state value is `List<PVMVLogType>`, where each update serializes/deserializes a Java list of POJO objects. ForL0 can store the bytes off-heap, but it cannot remove the Java object/list/serializer cost.

3. The right path is iterator/drain heavy.
   Right-stream processing scans `leftCache.iterator()`, iterates lists, calls customer match logic, and removes entries. This dilutes the per-entry native hash table benefit.

4. The original source is expensive.
   The realistic source path calls `Gen7KBData.genDataSimple()` for each event. This object-construction and POJO payload cost can dominate the short and medium runs.

5. The reported baseline is a short bounded job.
   The 300-record contract baseline finishes in about 4 seconds, so startup, source completion detection, auto-cancel, and Flink scheduling overhead are a large fraction of wall time.

## Comparison with NexMark/WordCount positive windows

NexMark positive settings work because HashMap enters heap/GC pressure while ForL0 moves state storage off heap. WordCount positive settings isolate a compact high-frequency `ValueState<Long>` loop. The customer usecase does neither: it uses string keys, POJO/list values, iterator cleanup, and business join logic.

## Recommended framing

Use Client Usecase as:

- real customer job compatibility evidence
- no-regression evidence
- small positive result under 300k pressure, about +5%

Do not use it as the headline ForL0 speedup claim. The headline should remain NexMark heap/GC pressure windows and WordCount high-cardinality scalar-state windows.

## If we still want to force a customer-like win

The least artificial next probe would be a new non-contract driver/operator variant that keeps the customer's two-stream shape but changes the hot state to a compact scalar path:

- key by a numeric long instead of string join key
- replace `MapState<Long, List<PVMVLogType>>` with either `MapState<Long, Long>` counters or `ValueState<Long>` accumulators
- keep a bounded high-rate source and report steady-state TPS/backpressure, not a 4-second bounded completion time

That would demonstrate the mechanism, but it would no longer be the original customer operator semantics.

## Follow-up probes on 2026-07-09

I added a `scalar_state` mode to `benchmark/client-drift`, plus scenario entries under `client_usecase_scenarios`. The mode keeps the dual-stream source shape but uses numeric keyed state and synthetic scalar state paths.

Implementation notes:

- `scalar_state_probe_1m`: numeric key, `ValueState<Long>` + `MapState<Long,Long>`, 1M records, 8 scalar map operations per record.
- `scalar_state_probe_2m_ops16`: same shape, 2M records, 16 map operations per record.
- `scalar_state_probe_2m_ops64`: same shape, 2M records, 64 map operations per record.
- `scalar_value_probe_5m_ops16`: numeric key, `ValueState<Long>` only, 5M records.
- `scalar_value_probe_20m_ops16`: larger `ValueState<Long>` only, 20M records.

Observed results:

| Probe | Cluster | HashMap rec/s/core | ForL0 rec/s/core | Delta | Notes |
|---|---|---:|---:|---:|---|
| `scalar_state_probe_1m` | default 16G TM | 62,309 | 62,331 | +0.0% | 4s bounded run; no separation |
| `scalar_state_probe_2m_ops16` | default 16G TM | 122,879 | 60,284 | -50.9% | ForL0 auto-stopped after sources finished; downstream slower |
| `scalar_state_probe_2m_ops64` | default 16G TM | 31,164 | failed | n/a | ForL0 TM became unreachable after long Full GC / heartbeat loss |
| `scalar_value_probe_5m_ops16` | default 16G TM | 311,406 | 155,738 | -50.0% | ValueState-only still dominated by repeated JNI/native calls |
| `scalar_value_probe_5m_ops16` | tight 8G TM | 154,845 | 155,731 | +0.6% | Tight heap makes the two sides roughly tie |
| `scalar_value_probe_20m_ops16` | tight 8G TM | 415,339 | 207,698 | -50.0% | Larger run again favors HashMap |

Interpretation:

These probes did not find a customer-like positive window. The synthetic numeric-key/scalar-state variants are useful because they isolate possible explanations:

- The original customer example is weak because it uses string keys, POJO/list values, iterator drains, and source/object-construction overhead.
- Replacing that with `MapState<Long,Long>` does not help here; repeated ForL0 MapState JNI calls become the bottleneck before HashMap hits meaningful GC pressure.
- Replacing MapState with `ValueState<Long>` also does not produce a stable win in this Docker/Kunpeng setting. Under tight heap, a 5M run reaches parity, but a 20M run falls back to a large ForL0 deficit.
- The report's strong positive windows still appear workload-specific: NexMark under heap/Full-GC pressure and WordCount/Intel high-cardinality scalar-state scans. This customer-like synthetic family is not currently a strong ForL0 demonstration.

## JNI fusion probe on 2026-07-09

I added two non-contract ForL0-only MapState fast paths:

- `ForL0MapState.addAndGetLong(UK, long)` -> native `mapAddAndGetLongLong(...)`
- `ForL0MapState.addSequentialAndSumLong(long startUserKey, int count, long modulo, long delta)` -> native `mapAddSequentialAndSumLongLong(...)`

The first path fuses a single `get + put` update. The second path is intentionally benchmark-specific: it batches all per-record sequential bucket increments into one JNI call and returns the checksum contribution.

Important implementation finding:

- `RuntimeContext.getMapState()` returns `org.apache.flink.runtime.state.UserFacingMapState`, not the raw `ForL0MapState`.
- A direct `instanceof ForL0MapState` does not hit the fast path.
- The benchmark has to unwrap the `originalState` field once in `open()` before calling ForL0-specific fast paths.

Observed results after deployment:

| Probe | Backend | Wall time | rec/s/core | Fast path | Result file |
|---|---|---:|---:|---|---|
| `scalar_state_probe_2m_ops16_fused` | ForL0 | 8.033s | 62,245 | single fused add, direct call | `client_usecase_scalar_state_probe_2m_ops16_fused_forl0_20260709_120756.json` |
| `scalar_state_probe_2m_ops16_fused` | HashMap | 4.019s | 124,409 | fallback get/put | `client_usecase_scalar_state_probe_2m_ops16_fused_hashmap_20260709_120815.json` |
| `scalar_state_probe_2m_ops16_batch` before unwrap | ForL0 | 8.249s | 60,614 | missed; wrapper not unwrapped | `client_usecase_scalar_state_probe_2m_ops16_batch_forl0_20260709_121240.json` |
| `scalar_state_probe_2m_ops16_batch` after unwrap | ForL0 | 4.013s | 124,587 | hit; `leftForL0=true/rightForL0=true` | `client_usecase_scalar_state_probe_2m_ops16_batch_forl0_20260709_121348.json` |
| `scalar_state_probe_2m_ops16_batch` | HashMap | 8.021s | 62,340 | fallback get/put | `client_usecase_scalar_state_probe_2m_ops16_batch_hashmap_20260709_121432.json` |
| `scalar_state_probe_2m_ops16` rerun | HashMap | 8.020s | 62,345 | normal MapState API | `client_usecase_scalar_state_probe_2m_ops16_hashmap_20260709_121452.json` |
| `scalar_state_probe_2m_ops16` rerun | ForL0 | 8.026s | 62,297 | normal MapState API | `client_usecase_scalar_state_probe_2m_ops16_forl0_20260709_121503.json` |

Interpretation:

- Single-operation fusion is not enough once Flink's user-facing wrapper and per-record loop overhead remain in the hot path.
- Batch fusion is enough to cut the ForL0 run from about 8.0s to about 4.0s in the current cluster state.
- This is not yet a production-transparent optimization, because normal Flink `MapState` has no batch increment API. It is strong evidence that JNI/API granularity is a real bottleneck for ForL0 on map-heavy scalar workloads.
- For the original customer operator, a similar gain would require either generated/operator-specific fused operations or a semantically richer native operation for the iterator/drain pattern. The generic `MapState` API cannot express that automatically.

## One-click workload retest attempt on 2026-07-09

I deployed the JNI batch fast-path build into the Flink lib/native directories and then started the contract apps run through the top-level one-click script:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 --backend all --apps-only --skip-docker-load --keep-going
```

Observed state and results:

| Step | Backend | Outcome | Result / note |
|---|---|---|---|
| WordCount contract run | HashMap | finished | `wordcount_hashmap_20260709_124855.json`: 100M records, 1432.674s, 69,799.55 rec/s, 34,899.77 rec/s/core |
| WordCount contract run | ForL0 | not accepted | the job kept running after the one-click process received SIGTERM; during the run one TaskManager disappeared and REST showed a TaskManager heartbeat timeout; I canceled the orphan job after 1947.8s |
| NexMark default apps run | HashMap q4 | canceled | default apps mode starts 80M-event q4; with the cluster degraded to 1 TM, it ran for about 414.6s without producing a result, so I canceled it |
| NexMark `forl0_tps_probe --query q18` | HashMap | finished | 206.43K events/s, 8.32K events/s/core, 1 TM |
| NexMark `forl0_tps_probe --query q18` | ForL0 | failed | Flink job failed after about 48s; NexMark metric reporter could not find the sink TPS metric and the Java launcher hung waiting, so I interrupted it |
| Client `scalar_state_probe_2m_ops16_batch` | HashMap / ForL0 | failed | both failed with `NoResourceAvailableException: Could not acquire the minimum required resources`; this is consistent with the degraded 1-TM cluster, not with a JNI exception |
| Benchset | HashMap | stopped | benchset starts with the same 100M WordCount contract run; I interrupted it to avoid repeating the already long/invalid single-TM run |
| Report generation | n/a | finished | `./docker/run_all_apps.sh --flink-home /home/shuhao/flink-1.20.3 --offline --report-only --no-profile` regenerated `benchmark/results/reports/benchmark_report.html` |

Important caveats:

- This machine still has no usable L0 hardware path: `libl0mempool.so` / device support is unavailable, so the runs exercise the native/JNI/state-backend path rather than true hardware L0 cache.
- The full apps retest is not a valid final performance comparison because the Docker/Flink cluster degraded from 2 TaskManagers to 1 TaskManager during ForL0 WordCount.
- The earlier successful batch-fusion result remains the cleanest signal from this run: `client_usecase_scalar_state_probe_2m_ops16_batch_forl0_20260709_121348.json` hit the unwrapped ForL0 fast path and improved the current-state ForL0 run from about 8.0s to about 4.0s.
- Normal WordCount/NexMark/client operators do not automatically call the new fused MapState API. Promoting this optimization transparently to all workloads would require changing the workload/operator hot paths or adding optimizer/runtime-recognized compound state operations.

## ForL0-only follow-up after boundary clarification

The fusion boundary was tightened: workload code must not be changed for this optimization. I reverted the temporary WordCount workload-side resolver experiment and kept the follow-up inside the ForL0 backend only.

ForL0-only change made:

- `ForL0ValueState` now has a transparent single-entry cache for primitive `ValueState<Long>` fast paths.
- `value()` on long/int-key and long-key TimeWindow paths checks `(key, keyGroup, namespace)` before issuing the JNI get.
- `update()`, `clear()`, and `addAndGetLong()` maintain or invalidate the cache so native state and the Java-side fast cache stay consistent.
- RowData/generic/string paths are not cached by this change.

This is not as strong as workload-side batch fusion, but it respects the boundary: existing WordCount/NexMark/client operators do not need to call ForL0-specific APIs. The expected upside is for hot-key primitive-long ValueState workloads where consecutive records revisit the same key or the same operator reads after a recent write.

Verification status:

- Backend Java compilation succeeded and the backend jar was updated in `target/`, `docker/deploy/`, and `/home/shuhao/flink-1.20.3/lib/`.
- No valid runtime retest was run after this change because the Flink cluster had degraded to zero registered TaskManagers. A container/cluster restart is required before measuring.

## One-click runner hardening on 2026-07-09

The earlier one-click run exposed three operational risks for the offline machine:

- REST readiness was treated as sufficient even when TaskManagers had disappeared.
- Interrupted benchmark commands could leave orphan Flink jobs running.
- Failure evidence could be lost if the terminal output was not preserved.

Fixes made:

- `docker/docker_run.sh start` now waits for at least 2 registered TaskManagers and 8 slots before declaring the cluster ready.
- `docker/run_all_apps.sh` now has experiment preflight checks for TaskManager count, total slots, and zero running jobs.
- `docker/run_all_apps.sh` now supports `--restart-cluster`, `--expected-taskmanagers N`, and `--expected-slots N`.
- `docker/run_all_apps.sh` now installs an exit trap: if a benchmark command fails or is interrupted, it writes a `FAILED_*.txt` marker under `benchmark/results/run_logs/` and cancels running/restarting Flink jobs through REST.
- `forl0-offline-app.sh` now passes the cluster health options through to the runner and supports `--restart-cluster`, `--expected-taskmanagers`, and `--expected-slots`.

Validation:

```bash
bash -n docker/run_all_apps.sh
bash -n docker/docker_run.sh
bash -n forl0-offline-app.sh
```

passed.

With the degraded cluster state, this command correctly failed before running experiments:

```bash
./docker/run_all_apps.sh --flink-home /home/shuhao/flink-1.20.3 \
  --offline --preflight-only --no-profile \
  --expected-taskmanagers 2 --expected-slots 8
```

Then this command restarted the Docker Flink cluster and verified 2 TaskManagers / 8 slots:

```bash
./docker/run_all_apps.sh --flink-home /home/shuhao/flink-1.20.3 \
  --offline --preflight-only --no-profile --restart-cluster \
  --expected-taskmanagers 2 --expected-slots 8
```

Finally, the top-level smoke path completed on the repaired cluster:

```bash
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --smoke-only --backend all --skip-docker-load --no-report \
  --expected-taskmanagers 2 --expected-slots 8
```

Smoke output files:

- `client_usecase_hashmap_20260709_165946.json`
- `client_usecase_forl0_20260709_165958.json`

Post-smoke cluster state: 2 TaskManagers, 0 running jobs.
