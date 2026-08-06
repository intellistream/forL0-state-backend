# ICDE 2027 Round 2 go/no-go audit

Date: 2026-07-31

Target deadline: 2026-11-11 17:00 PT

Decision: **conditional go**

The repository is strong enough to justify a bounded ICDE Round 2 push, but
it is not submission-ready. The implementation, offline deployment artifact,
checkpoint path, benchmark harnesses, and prior ICPP/TKDE writing assets are
real. The current ICDE directory, however, is still an unadapted IEEE template,
and part of the latest benchmark evidence explicitly ran without a real L0
device. Those results may validate the backend/JNI/native-table path, but they
cannot support an L0-hardware speedup claim.

## Paper thesis

The paper should not be framed as "a faster hash table." The defensible
systems question is:

> Can a stream processor combine compact native state tables, an optional
> L0-backed hot-key cache, and copy-on-write snapshots to reduce state-access
> cost while preserving Flink checkpoint and recovery semantics?

The bounded contribution set is:

1. a Flink state-backend design with native Swiss-table group probing and
   explicit key-group/namespace routing;
2. an optional L0-backed HotCache above the StateTable source of truth, with an
   explicit simulation and evidence boundary;
3. checkpoint/recovery integration and correctness contracts across supported
   state types;
4. a reproducible offline artifact and a matched hardware evaluation.

The audited branch does **not** contain `ForL0StateMap` or incremental
extendible-hash splitting. Its `allocate_split` operation separates SwissTable
control bytes and slots into memory regions, while table growth performs a
whole-table rehash. Restoring incremental split as a core paper contribution
requires an explicit project-owner decision followed by implementation,
correctness tests, and mechanism ablations; wording changes alone are not
sufficient.

Zero-copy specializations and client-specific fast paths belong in the paper
only when their effect is isolated from the core layout and checkpoint
mechanisms.

## Current evidence boundary

- The offline bundle and one-command reproduction path are useful artifact
  evidence.
- The tracked Intel and Kunpeng sweeps provide workload exploration, not a
  single comparable final matrix by themselves.
- The July Ascend-host logs that report no real L0 device must be labelled as
  simulation/backend-path evidence.
- NexMark Q18 is a promising case, but a single favorable workload is not
  enough for the main claim.
- Weak or negative client and batch-JNI cases must remain in the evaluation;
  they define when the mechanism does not pay off.

## Required matched matrix

Each retained performance cell must bind:

- the exact commit, offline-bundle digest, Flink/JDK/native-library versions,
  CPU topology, L0 device and library identity;
- ForL0 real-L0 mode, ForL0 software-only path with HotCache inactive, Flink
  HashMapStateBackend, and a
  storage-oriented baseline such as RocksDB/ForSt when the workload permits;
- identical source rate, key distribution, state size, checkpoint interval,
  parallelism, task slots, warmup, measurement window, and failure policy;
- at least three independent job lifecycles with alternating arm order;
- throughput, throughput/core, P50/P95/P99 latency, CPU time, memory,
  checkpoint duration, recovery duration, and correctness/error counts.

Report both absolute values and paired deltas. Do not aggregate simulation and
real-L0 runs.

## Decision gates

### Gate A — 2026-08-14: provenance and correctness

- Freeze one machine-readable experiment manifest.
- Prove real L0 mode with device/library identity.
- Pass state, snapshot, restore, rescale, and failure-injection correctness.
- Separate simulation-only evidence from hardware evidence in all tables.

Failure disposition: stop performance writing and fix correctness/provenance.

### Gate B — 2026-09-11: mechanism evidence

- Complete the matched real-L0 matrix for at least WordCount, two NexMark
  queries with different state behavior, and one client/state-type workload.
- Include load-factor/whole-table-growth, HotCache, checkpoint, and state-type
  ablations. The ctrl/slots split allocator is currently a provisional,
  test-only extension point: production `DefaultAllocator` uses unified
  allocation, so split layout must not be reported as a production mechanism,
  ablation, or contribution unless production implementation and evidence are
  added first.
- Demonstrate that at least one win follows from the proposed mechanism rather
  than an unfair runtime or source-rate difference.

Failure disposition: no-go for the current ICDE performance thesis; retain an
artifact/demo or negative-study path.

### Gate C — 2026-10-09: paper-complete artifact

- Replace the stock ICDE template with the actual anonymous draft.
- Freeze figures and tables from checked-in raw data.
- Complete related work against Flink state backends, persistent-memory-aware
  state, cache-aware hashing, and streaming checkpoint/recovery systems.
- Run an independent artifact replay from the release bundle.

Failure disposition: move venue rather than submit a template-driven paper.

### Gate D — 2026-10-30: submission freeze

- Resolve all claim-to-artifact links.
- Complete anonymity, bibliography, ethics/disclosure, and PDF checks.
- Make no new performance claim after this date.

## Immediate work order

1. Build `research_paper/ICDE-2027-R2/` from the official current template,
   not from the untouched example.
2. Maintain the machine-readable evidence index and mechanism contract; both
   must pass before abstract or contribution claims are accepted.
3. Freeze the correctness and matched-matrix protocol before another benchmark
   run.
4. Draft the problem statement, design invariants, and limitations before
   writing performance claims.
