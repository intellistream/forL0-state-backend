# NexMark q18 fixed-row2 JNI probe, 2026-07-11

Evidence label: real-online probes on the Ascend host.

## Question

After enforcing fair operator parallelism, the visible q18 percentage gain looked
smaller than earlier results. The suspected reasons were:

- earlier p4/p90s probes mixed in unstable or less fair resource placement;
- the stable p8/60s line raised HashMap as well as ForL0;
- q18 still paid per-operation JNI `long[]` access for the two-field
  `PARTITION BY bidder, auction` RowData key.

## ForL0-only change

Added an arity-2 FixedRow fast path inside ForL0 only:

- `RowDataKeyAccessor.extractFixedField(Object, int)` extracts one fixed field
  without materializing a JNI key array;
- `ForL0ValueState` dispatches RowData fixed keys with arity 2 to
  `ROWDATA_FIXED2_VOID`;
- `NativeEngine` and `jni_value_state.cpp` add `valueGet/Put/ClearFixedRow2*`
  methods that construct `FixedRow(2)` directly from two `long` JNI arguments;
- other FixedRow arities still use the previous `long[]` path.

This does not touch HashMap, NexMark SQL, Flink common code, or non-ForL0
backends.

## Build and deployment checks

Native build:

```text
make -C src/main/native clean all install
```

Result: success. The existing SwissTable `memset` warnings remain unchanged.

Java check without Maven:

```text
javac -source 8 -target 8 ... NativeEngine.java RowDataKeyAccessor.java ForL0ValueState.java
```

Result: success with the normal bootstrap-classpath warning.

Because `mvn` is not installed on this host, the updated classes and native
resource were injected into the existing backend JAR with `jar uf` for local
validation.

Deployed checksums:

```text
backend JAR: f55bcc30828697b031ecc26f98cbce0d4ed5b8328367dd6f2f3128d2f49ab29c
native .so: 0882b1c9e0f802599cd2ce6462a49da39a54362ec82d5e57715401430269824f
```

The new native symbols are exported:

```text
valueGetFixedRow2LongSafe
valueGetFixedRow2DoubleSafe
valueGetFixedRow2Generic
valueGetFixedRow2GenericPtr
valuePutFixedRow2Long
valuePutFixedRow2Double
valuePutFixedRow2Generic
valueClearFixedRow2
```

## Stable q18 results

All rows use q18, `parallelism=8`, `2 TM x 4 slots`, `metric_monitor_delay=15s`,
`metric_monitor_duration=60s`, and the `forl0_no_full_gc_lateq_deep` scenario.

| Run | Backend | Result file | Throughput | Per core | Health |
| --- | --- | --- | ---: | ---: | --- |
| Previous p8/60s | ForL0 | `benchmark/results/nexmark_20260711_182330/nexmark_results.json` | 835,490 events/s | 58,713/core | healthy |
| Previous p8/60s | HashMap | `benchmark/results/nexmark_20260711_182555/nexmark_results.json` | 652,170 events/s | 12,743/core | healthy |
| fixed-row2 | ForL0 | `benchmark/results/nexmark_20260711_183720/nexmark_results.json` | 904,630 events/s | 63,977/core | healthy |
| fixed-row2 + profiler | ForL0 | `benchmark/results/nexmark_20260711_184022/nexmark_results.json` | 913,250 events/s | 65,749/core | healthy |
| latest same deployment | HashMap | `benchmark/results/nexmark_20260711_184254/nexmark_results.json` | 575,830 events/s | 10,854/core | healthy |

Post-run health after both fixed-row2 ForL0 and latest HashMap:

```text
taskmanagers=2
slots-total=8
slots-available=8
flink-taskmanager-1 running oom=false
flink-taskmanager-2 running oom=false
```

## Interpretation

- The fixed-row2 path improved ForL0 q18 from 835,490 to 904,630 events/s:
  **+8.3% ForL0 self-improvement** under the same stable p8/60s setup.
- Against the higher previous HashMap p8/60s result, ForL0 is
  **+38.7%** on total throughput. This is the conservative headline delta.
- Against the latest same-deployment HashMap run, ForL0 is **+57.1%**. Treat
  this as a valid paired run but not the sole headline because HashMap showed
  noticeable run-to-run variance.
- The earlier apparent drop came from switching from p4/long-window or unstable
  probes to a fair p8/60s setup. After the arity-2 JNI fast path, the fair
  stable q18 result is stronger than the earlier +32% line.

## Profiler note

The CPU-profiled fixed-row2 run produced:

```text
benchmark/results/nexmark_20260711_184022/profiles/flamegraph_cpu_forl0_q18_20260711_184023.html
```

The profiler reported good quality (`idle=0.2%`). The top summary is dominated
by Flink task frames, and the short native FixedRow2 calls do not appear as
separate top-level symbols, but no samples were found for the old
`jlongarray_to_fixedrow` / `GetLongArrayRegion` path in the generated HTML.

## Decision

Keep the fixed-row2 JNI fast path and the p8/60s q18 default. Do not report
the previous p8/90s `1.22M` run as a default result because it ended with a TM
137/OOM failure.
