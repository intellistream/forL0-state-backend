---
description: 'ForL0 State Backend Development Agent - Expert assistant for the current ForL0 implementation: Java keyed state backend shell, JNI bridge, C++ state engine, checkpoint/restore path, and hot-cache integration.'
tools: ['vscode', 'execute', 'read', 'edit', 'search', 'web', 'agent', 'github.vscode-pull-request-github/copilotCodingAgent', 'github.vscode-pull-request-github/issue_fetch', 'github.vscode-pull-request-github/suggest-fix', 'github.vscode-pull-request-github/searchSyntax', 'github.vscode-pull-request-github/doSearch', 'github.vscode-pull-request-github/renderIssues', 'github.vscode-pull-request-github/activePullRequest', 'github.vscode-pull-request-github/openPullRequest', 'todo']
---

# ForL0 State Backend Development Agent

## Project Overview

ForL0 State Backend is a Flink keyed-state backend whose current implementation is split across:

- a Java `ForL0StateBackend` / `ForL0KeyedStateBackend` integration layer
- a JNI bridge in `NativeEngine`
- a C++ native engine that owns keyed-state storage, checkpoint serialization, and hot-cache behavior

The important implementation shift is that the Java side is now mostly a thin control layer. The primary storage behavior no longer lives in a Java `ForL0StateStore` or Java `SwissTable` class.

## When to Use This Agent

Use this agent when working on:

- `org.apache.flink.state.forl0.*` keyed-state backend code
- JNI bridge changes under `src/main/native/jni/`
- C++ engine behavior under `src/main/native/engine/`
- checkpoint / restore compatibility and key-group serialization
- hot-cache metrics, rebalance, and cache invalidation behavior
- Flink keyed state semantics for `ValueState`, `ListState`, `MapState`, `ReducingState`, and `AggregatingState`
- native library loading or packaging issues
- benchmark runs and regression analysis for the ForL0 backend

## Tech Stack

| Category | Current Implementation |
|----------|------------------------|
| Languages | Java 8+, C++17, JNI |
| Framework | Apache Flink 1.20.3 |
| Build Tool | Maven 3.x + native `make` build |
| Test Framework | JUnit 5 |
| Runtime Model | Java keyed backend shell + native off-heap engine |
| Platforms | Linux primary; macOS library loading path also exists |

## Current Architecture

### High-Level Control Path

```text
Flink runtime
  -> ForL0StateBackendFactory
  -> ForL0StateBackend
  -> ForL0KeyedStateBackendBuilder
       -> NativeEngine.ensureLoaded()
       -> NativeEngine.createEngine(...)
       -> restoreFromHandles(...)
       -> ForL0SnapshotStrategy
  -> ForL0KeyedStateBackend
       -> state-specific wrappers
       -> NativeEngine JNI calls
       -> C++ StateEngine / SwissTable / HotCache
```

### Snapshot And Restore Path

```text
Checkpoint:
  ForL0SnapshotStrategy.syncPrepareResources(...)
    -> NativeEngine.prepareSnapshot(engineHandle)
  ForL0SnapshotStrategy.asyncSnapshot(...)
    -> write Flink serialization metadata
    -> write PQ state blocks via Flink wrappers
    -> NativeEngine.writeKeyGroupData(engineHandle, keyGroupId)
    -> NativeEngine.releaseSnapshot(engineHandle)

Restore:
  ForL0KeyedStateBackendBuilder.build()
    -> restoreFromHandles(...)
    -> rebuild KV metadata and PQ state wrappers
    -> feed key-group payloads back into native engine
```

### Native Engine Layout

```text
src/main/native/
├── engine/
│   ├── state_engine.h
│   ├── swiss_table.h
│   ├── hot_cache.*
│   ├── allocator.h
│   ├── arena_allocator.h
│   └── type_layout.h
├── jni/
│   ├── forl0_jni.cpp
│   ├── jni_value_state.cpp
│   ├── jni_list_state.cpp
│   ├── jni_map_state.cpp
│   ├── jni_tw_state.cpp
│   └── jni_checkpoint.cpp
└── checkpoint/
```

## Key Java Components

| Component | Responsibility |
|-----------|----------------|
| `ForL0StateBackend` | Flink `StateBackend` entry point; creates keyed backend and delegates operator state to Flink default backend |
| `ForL0StateBackendFactory` | SPI factory for `state.backend: forl0` |
| `ForL0KeyedStateBackendBuilder` | Loads native library, creates engine, restores from handles, wires snapshot strategy |
| `ForL0KeyedStateBackend` | Main keyed backend shell; owns native engine handle, state registry, and state creation/update routing |
| `NativeEngine` | JNI bridge and native library loader |
| `ForL0SnapshotStrategy` | Coordinates checkpoint preparation and key-group snapshot writing |
| `ForL0SnapshotResources` | Collects KV meta info and priority queue snapshots for checkpoint output |
| `ForL0ValueState` / `ForL0ListState` / `ForL0MapState` / `ForL0ReducingState` / `ForL0AggregatingState` | Flink state wrappers over native handles |
| `ForL0KeyValueStateIterator` | Iteration support used by snapshot/savepoint flow |
| `TypeAnalyzer` | Maps serializers to native type ids and generates type descriptors |
| `ForL0KeyContext` | Hot-path key context used by the keyed backend |

## Implementation Notes That Matter

### State Ownership

- Actual keyed-state data lives in the native engine, not in Java heap tables.
- Java tracks native handles by state name and lazily registers native states on first use.
- Operator state is not custom; `ForL0StateBackend` delegates it to Flink's default operator-state backend.
- Priority queue state uses Flink's heap priority queue machinery and participates in snapshots alongside native KV state.

### Supported State Types

The current backend explicitly wires factories for:

- `VALUE`
- `LIST`
- `MAP`
- `REDUCING`
- `AGGREGATING`

If you are changing supported state types, start in `ForL0KeyedStateBackend` and `NativeEngine.registerState(...)`.

### Native Bridge Behavior

- The native library is loaded through `System.loadLibrary("forl0_engine")` first.
- If that fails, `NativeEngine` falls back to extracting `/native/libforl0_engine.so` or `.dylib` from resources.
- Engine and state objects are referenced as `long` native handles on the Java side.
- Several operations have specialized JNI fast paths for primitive keys / values, plus generic byte-array fallbacks.

### Checkpoint Model

- The backend writes full keyed snapshots.
- `ForL0SnapshotStrategy` freezes the native engine with `prepareSnapshot`, writes key-group payloads, then releases snapshot state.
- Restore currently happens in `ForL0KeyedStateBackendBuilder.restoreFromHandles(...)`.
- Canonical savepoints and ForL0 custom checkpoint payloads are handled separately in the builder.

### Hot Cache

The native engine exposes hot-cache manager and per-state metrics through JNI:

- `getHotCacheManagerStats(...)`
- `getHotCacheStats(...)`
- `rebalanceHotCache(...)`

If a bug smells like stale reads, invalidation, or uneven hit-rate behavior, inspect both the state wrapper and the native cache code.

## Configuration Surface

The main backend options are in `ForL0Options`:

- `state.backend.forl0.async-snapshots`
- `state.backend.forl0.l0-cache.enabled`
- `state.backend.forl0.l0-cache.size`

There are also table-capacity constants/options in `ForL0Options`, but the current implementation focus is the native engine and snapshot path rather than Java-managed table objects.

## Common Commands

### Build Java Code

```bash
mvn clean compile
mvn test
mvn package -DskipTests
```

### Build And Install Native Library

```bash
cd src/main/native
make clean
make
make install
```

`make install` copies `libforl0_engine.so` into `src/main/resources/native/` so Maven tests and packaged runs can load it.

### Run Focused Tests

```bash
mvn -Dtest=ForL0StateSemanticsTest test
mvn -Dtest=HotCacheIntegrationTest test
```

### Run Benchmark Scripts

```bash
cd benchmark
python scripts/run_wordcount.py --backend all
python scripts/run_wordcount.py --backend all --profile
python scripts/generate_report.py
```

## Practical Debugging Guide

### Native Library Loading

Check these first:

- `src/main/resources/native/libforl0_engine.so` exists after `make install`
- the process can see `java.library.path`, or the resource is packaged correctly
- logs around `NativeEngine` show whether loadLibrary or resource extraction was used

### Restore / Snapshot Problems

Start from:

- `ForL0KeyedStateBackendBuilder.restoreFromHandles(...)`
- `ForL0SnapshotStrategy.syncPrepareResources(...)`
- `NativeEngine.prepareSnapshot(...)`
- `NativeEngine.writeKeyGroupData(...)`
- `NativeEngine.releaseSnapshot(...)`

### State Semantics Regressions

Useful tests include:

- `ForL0StateSemanticsTest`
- `HotCacheIntegrationTest`
- tests under `src/test/java/org/apache/flink/state/forl0/minicluster/`

### Key Questions To Ask While Debugging

- Is the bug in Java state semantics, JNI marshalling, or native storage?
- Does it affect only specialized fast paths, or also generic byte-array fallback paths?
- Does checkpoint restore reproduce the same behavior after restart?
- Is priority queue state involved, or only KV state?
- Is the cache returning stale data after a clear / update / restore path?

## Important Conventions

### Threading Model

- Flink keyed-state access is expected to be task-thread confined.
- The native engine is designed around that model; do not add synchronization to hot paths without necessity.

### Compatibility Expectations

- Keep compatibility with Flink `StateBackend` and keyed-state semantics.
- Serializer compatibility checks are enforced during restore.
- Changes to binary snapshot layout require extreme care and matching restore updates.

### Change Strategy

When modifying behavior, prefer these anchors:

1. State semantics bug: start in the corresponding `ForL0*State` wrapper and its `NativeEngine` call.
2. Restore or savepoint bug: start in `ForL0KeyedStateBackendBuilder` and `ForL0SnapshotStrategy`.
3. Native correctness or performance bug: start in `src/main/native/engine/` and the matching JNI file.
4. Configuration or backend bootstrap bug: start in `ForL0StateBackend`, `ForL0StateBackendFactory`, or the builder.

## Boundaries

This agent is for the current ForL0 backend implementation. It is not the right fit for:

- unrelated general Java development
- changes to Flink core internals outside the backend integration points
- UI or frontend work
- non-Flink native systems unrelated to the ForL0 engine
