# ForL0 Paper Story Outline

## Core Thesis

For in-memory, state-intensive stream processing workloads, the dominant bottleneck is often not operator computation but memory layout and memory management. A state backend that is cache-friendly, namespace-aware, and less dependent on JVM heap objects can improve throughput, tail latency, and stability without changing Flink state semantics or snapshot compatibility.

## 1. What is the problem?

Existing in-memory state backends such as HeapStateBackend become strongly memory-bound under state-intensive workloads. The main symptoms are high cache miss rate, long access chains, and poor spatial locality during frequent state reads and updates.

This problem is amplified in workloads with frequent per-record updates, window maintenance, and timer firing. Even when the full state fits in memory, the backend can still stall on the memory hierarchy because state access touches too many scattered objects and cache lines.

## 2. Why it matters?

This problem matters for latency-sensitive stateful stream processing scenarios, including financial risk control, market surveillance, fraud detection, real-time alerting, industrial monitoring, and complex event processing. In these settings, p99 latency and runtime stability are often as important as average throughput.

High cache miss rates and GC interference directly inflate tail latency and increase jitter, making it harder to satisfy strict SLA requirements. In addition, many real deployments run under limited memory budgets, so backend inefficiency translates into either lower performance or higher infrastructure cost.

## 3. Why existing works fail?

Existing in-memory backends are not primarily designed around cache locality and memory-layout efficiency. Their design priorities are usually generality, semantic correctness, JVM integration, and checkpoint/recovery support.

As a result, they often rely on pointer-heavy object graphs, heap-resident metadata, and conventional hash table organizations whose physical layouts are not cache-friendly. A single state access may traverse multiple Java objects and multiple cache lines, while the large number of heap objects also makes GC part of the steady-state cost.

## 4. What is the key idea?

The paper is built around three challenge-solution pairs.

### Challenge 1: Cache-unfriendly memory layout

In-memory state access suffers from high cache miss rates because state entries, metadata, and validation steps are physically scattered.

### Solution 1: Compact cache-friendly state layout

Use compact hash indexing, control-byte parallel matching, and inlined key-value layout so that probing, candidate filtering, and key validation touch fewer cache lines and become more cache-friendly.

### Challenge 2: Underexploited execution locality

Streaming state is naturally structured by state, key-group, and namespace, but traditional physical layouts do not fully translate this execution structure into locality.

### Solution 2: Namespace-aware hierarchical organization

Organize state as State -> KeyGroup -> Namespace -> small sub-table so that related accesses stay within smaller and more stable working sets, especially under window-heavy and timer-heavy workloads, while preserving snapshot/restore compatibility.

### Challenge 3: JVM heap constraints block the previous two designs

Even if we know the desired cache-friendly layout and hierarchy, a pure Java heap implementation still suffers from object-model constraints, long access chains, JVM heap pressure, and GC interference.

### Solution 3: Lightweight native state engine

Implement the core state storage in a lightweight native engine, while keeping a Java compatibility shell for Flink API integration, state registration, and snapshot orchestration. This allows precise control of memory layout, shortens the hot path, and reduces heap object count and GC disturbance.

## 5. What are the implementation details?

The system uses a split architecture.

Java is responsible for Flink API compatibility, state registration, serializer and type analysis, metadata management, and checkpoint/snapshot coordination. Native code is responsible for the hot-path state storage and access logic.

At the storage layer, state is organized around a compact hash-table-based layout with contiguous control metadata and cache-friendly entry placement. At the system layer, state is organized hierarchically by state, key-group, and namespace to align the physical organization with stream-processing execution context.

To preserve correctness, snapshot and restore remain compatible with Flink's existing semantics. The implementation uses a consistent snapshot strategy so that the optimized in-memory layout does not change the observable state model.

## 6. What is the experiment plan?

The evaluation should contain both microbenchmarks and end-to-end benchmarks.

Microbenchmarks should use flink-state-benchmark to isolate backend behavior under controlled get/update mixes, key distributions, namespace counts, and state sizes. End-to-end benchmarks should use benchset and state-intensive streaming jobs with window-heavy and timer-heavy behavior.

The baselines should include HeapStateBackend and HashMapStateBackend. The metrics should include throughput, average latency, tail latency, cache miss rate, memory-bound breakdown, GC time, GC count, GC pause behavior, and memory footprint.

The experiment matrix should explicitly include memory-constrained settings, because one core claim of the paper is that reducing heap pressure and GC interference matters most when memory is tight.

## 7. What are the experiment results?

This section is intentionally left open until the missing experiments are completed.

When filled in, it should answer the following questions:

1. Does the proposed backend improve throughput and tail latency on state-intensive workloads?
2. Does it reduce cache miss rate and memory-bound stall intensity?
3. Does it reduce GC disturbance, especially under constrained memory budgets?
4. Are the gains strongest on window-heavy, timer-heavy, and namespace-heavy workloads?
5. What are the costs or trade-offs, including snapshot overhead and implementation complexity?

## 8. What is the takeaway message?

For in-memory, state-intensive stream processing, backend performance is fundamentally a memory-layout and memory-management problem rather than just a compute problem. Cache-friendly physical layout, namespace-aware organization, and a native storage engine together provide a practical way to improve throughput, tail latency, and runtime stability without changing Flink semantics or checkpoint compatibility.

The intended message of the paper is not that we built a faster hash table in isolation. The intended message is that rethinking the physical organization of in-memory state is essential for state-intensive stream processing workloads.