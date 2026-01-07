---
description: 'ForL0 State Backend Development Agent - Expert assistant for Apache Flink high-performance state backend with Swiss Tables architecture (Go 1.24 aligned), SWAR parallel matching, and Kunpeng L0 Cache optimization.'
tools: ['vscode', 'execute', 'read', 'edit', 'search', 'web', 'agent', 'github.vscode-pull-request-github/copilotCodingAgent', 'github.vscode-pull-request-github/issue_fetch', 'github.vscode-pull-request-github/suggest-fix', 'github.vscode-pull-request-github/searchSyntax', 'github.vscode-pull-request-github/doSearch', 'github.vscode-pull-request-github/renderIssues', 'github.vscode-pull-request-github/activePullRequest', 'github.vscode-pull-request-github/openPullRequest', 'todo']
---

# ForL0 State Backend Development Agent

## Project Overview

ForL0 State Backend is a high-performance state backend implementation for Apache Flink, featuring Swiss Tables architecture (aligned with Go 1.24) with SWAR parallel matching and Extendible Hashing for efficient state access.

## When to Use This Agent

- Developing or modifying ForL0StateBackend Java code
- Writing or debugging JNI native code (C language)
- Working with Swiss Tables architecture and SWAR algorithms
- Implementing Extendible Hashing operations (split, grow, rehash)
- Running and analyzing Benchmark performance tests
- Debugging Native library loading issues
- Optimizing for Kunpeng CPU L0 Cache

## Tech Stack

| Category | Technology |
|----------|------------|
| Languages | Java 8+, C (JNI native code) |
| Framework | Apache Flink 1.20.0 |
| Build Tool | Maven 3.6+ |
| Test Framework | JUnit 5 |
| Platforms | macOS (simulation mode), Linux with Kunpeng CPU (L0 mode) |

## Core Architecture

### Swiss Tables Architecture (Go 1.24 Aligned)

```
ForL0StateMap (StateMap interface + Directory routing)
├── directory[]         // Extendible Hashing routing table
├── tables: List        // Unique SwissTable list
└── SwissTable          // Storage layer
    ├── ctrl[]          // Control bytes (EMPTY=0x80, DELETED=0xFE, FULL=h2)
    ├── keysNs[]        // key/namespace storage (slot i → keysNs[2i], keysNs[2i+1])
    ├── values[]        // value storage
    └── hashes[]        // 64-bit hash storage (for rehash/grow/split)
```

### Hash Bit Allocation (Go 1.24 Aligned)

```
64-bit hash:
├── H1 = hash >>> 7     // Upper 57 bits, for probe start group
├── H2 = hash & 0x7F    // Lower 7 bits, stored in ctrl byte
└── Directory routing: hash >>> globalShift (upper globalDepth bits)
```

### Key Components

| Component | Responsibility | Location |
|-----------|----------------|----------|
| `ForL0StateBackend` | StateBackend entry point | `runtime/state/heap/` |
| `ForL0KeyedStateBackend` | KeyedStateBackend implementation | `runtime/state/heap/` |
| `ForL0StateMap` | StateMap interface + Directory routing | `runtime/state/heap/` |
| `SwissTable` | SWAR parallel matching hash table storage | `runtime/state/heap/` |
| `NativeL0Memory` | JNI bridge class (L0 Cache) | `runtime/state/heap/space/` |
| `forl0_native.c` | C implementation (L0/simulation mode) | `src/main/native/` |

### SwissTable Core Algorithms

```java
// SWAR parallel matching (8 slots simultaneous comparison)
static long matchH2(long ctrlWord, int h2) {
    long pattern = LSB * (h2 & 0xFFL);
    long x = ctrlWord ^ pattern;
    return (x - LSB) & ~x & MSB;
}

// put return value encoding
static final int NEW_FLAG = 1 << 16;   // New insertion flag
static final int SLOT_MASK = 0xFFFF;   // Slot mask
static final int NEED_SPLIT = -1;      // Needs split

// Usage example
int result = table.put(hash, key, namespace, MAX_CAPACITY);
if (result == NEED_SPLIT) {
    handleSplit(table);
    continue; // Retry
}
int slot = result & SLOT_MASK;
boolean isNew = (result & NEW_FLAG) != 0;
table.values[slot] = value;
```

### Extendible Hashing

- **globalDepth**: Number of hash bits used by Directory
- **localDepth**: Depth of each Table
- **split**: Triggered when Table is full and capacity = MAX_TABLE_CAPACITY
- **Go-style index deduplication**: `if (t.index == i) t.index = 2 * i`

## Code Conventions

### Java Code

1. **Package structure**: `org.apache.flink.runtime.state.heap.*`
2. **Naming conventions**:
   - Class names: `ForL0` prefix for project components
   - Constants: UPPER_SNAKE_CASE
   - 64B alignment constants use `*_SIZE`, `*_OFFSET` suffixes
3. **Logging**: Use SLF4J (`LoggerFactory.getLogger`), prefix `[ForL0]`
4. **Comments**: 
   - Javadoc for public APIs
   - Inline comments for complex logic
   - Mark performance critical paths with `// Hot path`

### Native Code (C)

1. **File location**: `src/main/native/`
2. **Conditional compilation**: 
   - `L0_NOT_SUPPORTED`: Defined on macOS, skips L0-specific code
   - `#ifndef L0_NOT_SUPPORTED ... #endif` wraps L0-specific code
3. **JNI naming**: `Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_*`
4. **Memory alignment**: Use `posix_memalign` for 64-byte alignment

## Memory Layout

- SwissTable ctrl[]: 1 byte control byte per slot
- SwissTable keysNs[]: slot i → keysNs[2*i] (key), keysNs[2*i+1] (namespace)
- SwissTable values[]: slot i → values[i]
- SwissTable hashes[]: slot i → 64-bit hash (for rehash/grow/split)
- Directory default size: 1 (globalDepth=0)
- Table capacity: INITIAL=64, MAX=1024, load factor 87.5%

## Common Commands

### Build Project
```bash
mvn clean compile
mvn test                    # Run tests
mvn package -DskipTests     # Package JAR
```

### Build Native Library
```bash
cd src/main/native
make clean && make          # macOS: .dylib, Linux: .so
make install                # Copy to resources/native/
```

### Run Benchmarks
```bash
cd benchmark/scripts
python run_wordcount.py --backend all
python run_wordcount.py --backend all --profile  # Collect flame graphs
python generate_report.py
```

## Debugging Guide

### Native Library Loading Issues
- Check `java.library.path` setting
- Verify `.dylib/.so` file exists
- Look for `[ForL0]` prefix in logs

### L0 Mode Detection
```java
NativeL0Memory.isL0Mode()           // Is L0 mode
NativeL0Memory.getModeDescription() // Mode description
```

### IDEA Test Configuration
- VM options: `-Djava.library.path=$ProjectFileDir$/src/main/resources/native`

## Important Documents

| Path | Description |
|------|-------------|
| `ForL0-State-Backend设计说明书.md` | Detailed design document |
| `dev_notes/` | Development notes and design decisions |
| `reference/` | Reference implementation (Flink HeapStateBackend) |
| `reference/go_maps/` | Go 1.24 Swiss Tables reference |
| `reference/l0_docs/` | L0 memory library API docs |
| `benchmark/docs/` | Benchmark design documents |

## Important Conventions

### Thread Safety
- Flink state access is **single-threaded**
- Allocator implementations don't need concurrency support
- Avoid synchronization primitives on hot paths

### Error Handling
- Native library load failure → throw exception, L0 Cache unavailable
- Memory allocation failure → throw `L0MemoryAllocationException`
- No fallback to heap memory option

### Compatibility
- Maintain full compatibility with Flink StateBackend API
- User code works without modification
- Support Flink Checkpoint/Savepoint mechanism

## Boundaries

This agent does NOT handle:
- General Java development unrelated to Flink state backend
- Hardware optimization for non-Kunpeng platforms
- Modifications to Flink core framework (only implements StateBackend API)
- Non-Swiss Tables hash table implementations