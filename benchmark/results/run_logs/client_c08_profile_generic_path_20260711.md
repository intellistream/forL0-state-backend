# Client C08 ForL0 profile and generic-path probe - 2026-07-11

Evidence label: real-online.

## Purpose

Follow up on the weak client usecase C08 speedup after the fair Ascend
reproduction run.  The goal was to determine whether the remaining bottleneck
is in ForL0 JNI/native state access or in the client workload's generic
serialization/runtime path.

## Profiler target fix

Initial C08 profiling captured `flink-taskmanager-1`, but the C08 job placed
the active `Co-Keyed-Process` subtasks and ForL0 keyed state backends on
`flink-taskmanager-2`.  The resulting flame graph contained mostly JVM/runtime
noise and no useful ForL0 symbols.

`docker/run_all_apps.sh` now preserves an explicit caller-provided
`FLINK_TASKMANAGER_CONTAINER` when it sources `docker/forl0-local.env`, whose
default value is `flink-taskmanager-1`.  The run provenance prints the selected
profile TaskManager only when profiling is enabled.

Validated command:

```bash
FLINK_TASKMANAGER_CONTAINER=flink-taskmanager-2 \
./forl0-offline-app.sh --flink-home /home/shuhao/flink-1.20.3 \
  --skip-docker-load --reproduce-ascend --workloads C08 \
  --profile cpu --no-report --restart-cluster
```

Outputs:

- Raw: `benchmark/results/raw/client_usecase_forl0_20260711_163541.json`
- Flame graph: `benchmark/results/profiles/flamegraph_cpu_forl0_client_usecase_20260711_163108.html`
- Throughput under profiler: `944.07 records/s/core`

## Profile findings

The TM2 profile contained valid client and ForL0 symbols.  Key max-width frame
ratios:

| Frame | Ratio |
| --- | ---: |
| `org/example/MvIncrementProcessFunction.processElement1` | 21.90% |
| `org/example/MvIncrementProcessFunction.processElement2` | 10.56% |
| `org/apache/flink/api/java/typeutils/runtime/PojoSerializer.deserialize` | 12.25% |
| `org/apache/flink/api/common/typeutils/base/ListSerializer.deserialize` | 10.52% |
| `org/apache/flink/state/forl0/ForL0MapState.get` | 11.04% |
| `org/apache/flink/state/forl0/ForL0MapState.deserializeUserValue` | 10.52% |
| `org/apache/flink/state/forl0/ForL0MapState.put` | 4.77% |
| `org/apache/flink/state/forl0/ForL0ValueState.value` | 4.63% |
| `org/apache/flink/state/forl0/NativeEngine.mapGetGeneric` | 0.47% |
| `Java_org_apache_flink_state_forl0_NativeEngine_mapGetGeneric` | 0.46% |
| `org/apache/flink/state/forl0/NativeEngine.mapPutGeneric` | 0.62% |

TaskManager logs also repeatedly showed:

```text
[ForL0] Unsupported TypeSerializer: org.apache.flink.api.java.typeutils.runtime.PojoSerializer. Using generic serialized path.
```

Interpretation: C08 is dominated by the client operator, Flink network
deserialization, Pojo/List serialization, and GC pressure.  ForL0's native
generic JNI calls are not the primary limiter.  This explains why C08 remains a
small positive result while WordCount and NexMark q18 show stronger gains.

## Generic deserializer reuse probe

Tried a ForL0-only Java probe that reused `DataInputDeserializer` inside
`ForL0MapState.deserializeUserValue` and `ForL0ValueState.deserializeValue`.
The probe was compiled locally into the backend JAR and installed into Flink.

No-profiler C08 result:

- Raw: `benchmark/results/raw/client_usecase_forl0_20260711_164335.json`
- Throughput: `944.64 records/s/core`

References:

- Previous C08 ForL0: `944.74 records/s/core`
  (`benchmark/results/raw/client_usecase_forl0_20260711_130105.json`)
- Previous C08 ForL0 rerun: `943.67 records/s/core`
  (`benchmark/results/raw/client_usecase_forl0_20260711_161431.json`)
- HashMap baseline: `930.41 records/s/core`
  (`benchmark/results/raw/client_usecase_hashmap_20260711_125615.json`)

Decision: revert the deserializer-reuse probe.  It did not improve C08 beyond
noise and should not be carried into the offline one-shot experiment.

## Current recommendation

Keep C08 as a compatibility/boundary workload with a small positive ForL0
delta.  Do not claim it as a headline speedup workload.  Further client-usecase
gains would require either changing the customer workload data shape to avoid
Pojo generic serialization, or a larger ForL0 feature that understands that
specific POJO layout; neither is a low-risk backend-only tuning change for the
offline reproduction.
