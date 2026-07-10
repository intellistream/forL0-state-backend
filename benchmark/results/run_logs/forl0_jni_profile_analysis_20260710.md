# ForL0 JNI/Profile Follow-up (2026-07-10)

Evidence label: real-online for the benchmark reruns below; derived-artifact for report regeneration and collapsed-profile summaries.

## Cleanup and Repro Stability

- Cleaned accidental local dirty files before new work.
- Fixed offline runner behavior: when `--offline` is set, Docker startup failure now fails fast instead of falling back to standalone Flink. This avoids mixing Docker and standalone results on the offline machine.
- Regenerated the HTML report after the new runs; report generation loaded 15 NexMark result directories.

## Profile Signal

The available CPU collapsed profile (`benchmark/results/profiles/forl0_cpu.collapsed`, older but still useful for hotspot direction) shows the ValueState scalar path dominated by JNI safe-get:

- `ForL0ValueState.value`: 19.68%
- `ForL0ValueState.valueForLongKey`: 19.09%
- `NativeEngine.valueGetLongLongSafe`: 18.80%
- `Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongLongSafe`: 14.75%
- `jni_SetLongArrayRegion`: 3.29%

Interpretation: for scalar get/update loops, one JNI call per operation and `long[]` out-parameter writes remain visible. The existing `addAndGetLong` path is therefore the right direction for counter-like workloads, because it avoids get+put and the safe-get array return.

## Changes Tested

1. Restored source/JAR consistency for `ForL0ValueState` by adding `LongValueStateAddAndGet` to source and implementing it.
2. Exposed existing native MapState fused methods through Java:
   - `NativeEngine.mapAddAndGetLongLong`
   - `NativeEngine.mapAddSequentialAndSumLongLong`
   - `ForL0MapState.addAndGetLong`
   - `ForL0MapState.addSequentialAndSumLong`
3. Fixed the client scalar benchmark harness to cache reflective `Method` objects during `open()` instead of resolving methods per state operation.
4. Added `scalar_state_probe_2m_ops64_batch` and mapped default client scalar batch workloads to batched ForL0 cases.

## Rerun Results

Recent Ascend/Docker reruns:

- `scalar_state_probe_2m_ops16_fused`
  - HashMap: `123,444 records/s/core` (`client_usecase_hashmap_20260710_194955.json`)
  - ForL0 before method-cache fix: `10,384 records/s/core` (`client_usecase_forl0_20260710_195105.json`)
  - ForL0 after method-cache fix: `61,953 records/s/core` (`client_usecase_forl0_20260710_195241.json`)
  - Conclusion: per-entry fused JNI is not enough here; even cached reflection plus one JNI per map entry does not beat in-JVM HashMap.

- `scalar_state_probe_2m_ops16_batch`
  - HashMap: `61,954 records/s/core` (`client_usecase_hashmap_20260710_195339.json`)
  - ForL0: `61,952 records/s/core` (`client_usecase_forl0_20260710_195405.json`)
  - TaskManager logs confirm `leftForL0=true, rightForL0=true`.
  - Conclusion: batch path is reached, but ops16 is still not enough to produce a stable visible win.

- `scalar_state_probe_2m_ops64_batch`
  - HashMap: `30,985 records/s/core` (`client_usecase_hashmap_20260710_195639.json`)
  - ForL0: `31,069 records/s/core` (`client_usecase_forl0_20260710_195718.json`)
  - Conclusion: larger batch pressure produces a small positive ForL0 delta, but the gain is not yet large.

## Next Optimization Hypotheses

1. Replace reflective invocation with a real public interface for MapState fused/batch calls, analogous to `LongValueStateAddAndGet`. This removes `Method.invoke` boxing/varargs overhead from the hot path.
2. Add a native batch API that combines both sides of the scalar join pattern: update right-side buckets and read left-side buckets in one JNI call. Current `processElement2` still performs a Java loop of `leftBuckets.get(...)`, which caps the benefit.
3. Add direct `long`/presence encoding APIs for known counter states only. Do not change general `ValueState.value()` null semantics; instead expose a ForL0-specific counter interface.
4. Stabilize client-usecase timing: several bounded runs differ depending on whether the harness observes `FINISHED` or cancels after source completion. The report should prefer repeated samples or a fixed post-source drain policy.
5. Install/package async-profiler in the offline bundle. Current run reported async-profiler missing, so the latest online reruns could not produce fresh flame graphs.
