# ICDE numeric state-pressure suite: standard Flink API only

**Correction, 2026-11-03:** previous versions selected ForL0 fused/batch methods
inside the job while Heap used get/update or get/put loops. Those cross-backend
speedups are withdrawn as same-job comparisons. Historical output is preserved
with an erratum. The current version uses identical state access statements for
all backends; only `-Dstate.backend.type` chooses the implementation. The driver
backend label is not passed into the job.

Reference: Zhang et al., *Revisiting the Design of Data Stream Processing Systems
on Multi-Core Processors*, ICDE 2017, DOI 10.1109/ICDE.2017.119, Figure 5.
https://www.comp.nus.edu.sg/~hebs/pub/shuhaoICDE17a.pdf
Original application source commit: `770bfeeb4d4471092cf050d50052338d4ae38179`
at https://github.com/ShuhaoZhangTony/ProfilingStudy

## Preserved framework and adapted state

`reference/topology-contract.json` is the independent node/edge contract. Tests
compare the actual Flink StreamGraphs against it, including the two RCRF input
streams, LG's three sinks, and LR's direct dispatcher and cross-task edges.
No additional pressure operator is inserted. Operator chaining is disabled;
all original named tasks remain distinct. Parallelism is eight except the
single global ACD aggregation. Numeric Long entity partitioning is an explicit
adaptation; this is not an exact replay of the original 2017 datasets or physical
grouping settings.

| Workload | Numeric task behavior | Sink records per source record |
|---|---|---:|
| WC | Encoded numeric token split; per-key occurrence count | 1 |
| FD | Two-event transition counts and transition-probability anomaly score | 1 |
| SD | Ring-buffer moving average; 3% deviation detection | 1 |
| TM | Numeric road dictionary and per-road speed aggregation | 1 |
| LG | Numeric geo dictionary; geo/status/volume histories in three branches | 3 |
| VS | Caller/called-party rate features, global ACD, FoFIR/URL/ACD/Score tasks | 8 |
| LR | Vehicle speeds, accidents, counts, notifications, tolls and balances | 12 |

These are synthetic, state-heavy numeric adaptations. VS/LR implement distinct
numeric task roles with the reference streams, not full production telephony/
Linear Road semantics. Every input produces an output on each reference branch;
alerts are flags, not filters. No real text parsing, geospatial map matching or
original dataset ingestion is included. Do not describe results as reproducing
the original paper's absolute throughput.

Inside each stateful task, `MapState<Long,Long>` keeps a per-entity feature
history. Each record performs exactly `touches` standard `get`/`put` pairs,
adding the same 1,000,000,007 increment on both backends. Values therefore exercise
64-bit state outside the boxed Long cache. Repeated contiguous feature accesses
favor a backend with compact primitive storage without changing job code by backend.
Each Counter.add performs exactly one `ValueState.value()` and one
`ValueState.update()`. These Java statements and state descriptors are identical
for all backends. The job has no backend capability detection, casts to ForL0,
reflection of state methods, fused increments or batch extensions.

Long keys/values with VoidNamespace permit ForL0 to specialize its implementation
internally. This is a backend implementation detail; the job always calls the
same standard Flink API. Heap's normal get/put path also does not serialize each
state value. Flink POJO network serialization is common to both jobs.

Defaults: 524,288 entity keys, feature width 64,
128 entry increments per task input, moving window 16, uniform seeded access
after one complete key-space pass. Each ordinary task has up to 33,554,432
feature entries (256 MiB of raw Long values before keys/metadata). VS Global ACD
uses one global key and a four-entry history with one touch, preserving the
global aggregation without adding an artificial global feature hotspot.

The driver now uses longer default inputs to avoid timings that end after only
10–20 seconds. `--records` overrides these counts for every selected workload.

| Workload | Default source records |
|---|---:|
| WC | 48,000,000 |
| FD | 32,000,000 |
| SD | 24,000,000 |
| TM | 24,000,000 |
| LG | 12,000,000 |
| VS | 3,000,000 |
| LR | 3,000,000 |

## Run and validate

```bash
mvn -B -ntp -f benchmark/benchset/pom.xml package
python3 benchmark/scripts/run_benchset.py --rounds 2 --fresh-jvm
# Small paired execution, graph contract and independent source signature:
python3 benchmark/scripts/run_benchset.py --rounds 1 --keys 128 \
  --records 1024 --width 16 --touches 8 --validate
# Isolated profiling; excluded from timing medians:
python3 benchmark/scripts/run_benchset.py --backend all --benchmarks vs \
  --rounds 1 --keys 131072 --records 2000000 --profile --profile-event cpu
python3 benchmark/scripts/run_benchset.py --backend all --benchmarks vs \
  --rounds 1 --keys 131072 --records 2000000 --profile --profile-event alloc
```

Checkpoints are always disabled and nonzero checkpoint intervals are rejected.
L0 is always disabled. The driver verifies completed checkpoint count is zero,
two TMs/eight slots, original task nodes/edges, parallelism and output count.
It compares the final sink feature projection and source signature across paired
runs; `--validate` independently checks the input signature. This aggregate
projection is not a full per-key oracle for all intermediate task states.
Unit tests cover WC counts, moving-window eviction, transition probabilities,
spike threshold, numeric task semantics. Native and
real-JNI tests cover dense/hash fallback, wrap/overflow, iteration and isolation.

`--fresh-jvm` starts each measurement from a new cluster JVM, including state
construction and JIT while avoiding garbage left by the preceding backend.
The retained 2026-11-03 performance runs use this fresh-process protocol.

Outputs under `benchmark/results/runs/icde_*` retain every command, job/checkpoint/
exception payload, actual graph, sink row, CPU-time/RSS, GC counters and native/JAR
hashes. Alternate backend order each round. Use medians and ranges; do not select
best samples. Startup/JIT/network/GC are included in Flink job duration. Neither
CPU affinity nor Flink's process-memory budget is an exclusive host reservation.

See `docs/BriskState_Kunpeng_TopDown.md` for required future PMU collection and the
two stacked charts. The current container's PMU access is denied; CPU sampling
falls back to async-profiler ctimer and is not hardware Top-down measurement.
