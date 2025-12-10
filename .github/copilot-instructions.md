# Copilot Instructions for ForL0 State Backend

## 项目概述

ForL0 State Backend 是一个为 Apache Flink 设计的高性能状态后端实现，旨在充分利用鲲鹏 CPU 的 L0 Cache 特性，通过缓存友好的数据结构和分层索引架构优化状态访问性能。

## 技术栈

- **语言**: Java 8+, C (JNI native code)
- **框架**: Apache Flink 1.20.0
- **构建工具**: Maven 3.6+
- **测试框架**: JUnit 5
- **平台**: 
  - 开发: macOS (模拟模式)
  - 生产: Linux with 鲲鹏 CPU (L0 模式)

## 核心架构

### 双层索引设计

```
ForL0StateMap
├── L0Table (热点缓存层, L0 Memory)
│   └── 使用 NativeL0MemoryAllocator (JNI)
└── MainTable (全量索引层, Flink MemoryManager)
    └── 使用 MemoryManagerAllocator
```

### 关键组件

| 组件 | 职责 | 位置 |
|------|------|------|
| `ForL0StateBackend` | StateBackend 入口 | `runtime/state/heap/` |
| `ForL0KeyedStateBackend` | KeyedStateBackend 实现 | `runtime/state/heap/` |
| `ForL0StateMap` | 核心状态存储,双层索引 | `runtime/state/heap/` |
| `L0Table` | 热点缓存,多种替换策略 | `runtime/state/heap/` |
| `MainTable` | 主索引表,支持局部扩展 | `runtime/state/heap/` |
| `EntryArena` | 键值存储区 | `runtime/state/heap/` |
| `NativeL0Memory` | JNI 桥接类 | `runtime/state/heap/space/` |
| `forl0_native.c` | C 实现 (L0/模拟模式) | `src/main/native/` |

### 内存管理

- **MainTable**: 使用 Flink `MemoryManager` 管理的堆外内存
- **L0Table**: 使用 JNI 分配的 native 内存
  - L0 模式: `libl0mempool.so` (鲲鹏 L0 硬件)
  - 模拟模式: 标准 `malloc/free`

## 代码规范

### Java 代码

1. **包结构**: `org.apache.flink.runtime.state.heap.*`
2. **命名约定**:
   - 类名: `ForL0` 前缀表示本项目组件
   - 常量: 全大写下划线分隔
   - 64B 对齐相关常量使用 `*_SIZE`, `*_OFFSET` 后缀
3. **日志**: 使用 SLF4J (`LoggerFactory.getLogger`)
4. **注释**: 
   - 公共 API 使用 Javadoc
   - 复杂逻辑添加行内注释
   - 性能关键路径标注 `// Hot path`

### Native 代码 (C)

1. **文件位置**: `src/main/native/`
2. **条件编译**: 
   - `L0_NOT_SUPPORTED`: macOS 下定义,跳过 L0 相关代码
   - `#ifndef L0_NOT_SUPPORTED ... #endif` 包裹 L0 专用代码
3. **JNI 命名**: `Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_*`
4. **内存对齐**: 使用 `posix_memalign` 保证 64 字节对齐

### 测试

1. **单元测试**: `src/test/java/` 下,使用 JUnit 5
2. **命名**: `*Test.java` 或 `*ITCase.java`
3. **Native 测试**: 需要 native 库可用,使用 `@EnabledIf("isNativeAvailable")`

## 常见任务

### 编译项目

```bash
mvn clean compile
mvn test                    # 运行测试
mvn package -DskipTests     # 打包 JAR
```

### 编译 Native 库

```bash
cd src/main/native
make clean && make          # macOS: .dylib, Linux: .so
make install                # 复制到 resources/native/
```

## 重要约定

### 线程安全

- Flink 状态访问是**单线程**的
- Allocator 实现不需要并发支持
- 避免在热路径使用同步原语

### 内存布局

- 所有索引结构 **64 字节对齐** (缓存行对齐)
- L0Table slot: 16 bytes (Tag + Valid + Extension + Pointer)
- MainTable slot: 10 bytes (Tag + Pointer)
- MainTable bucket: 64 bytes (6 slots + 4 expansion pointers)

### 错误处理

- Native 库加载失败 → 抛出异常,L0Table 不可用
- 内存分配失败 → 抛出 `L0MemoryAllocationException`
- 不提供降级到堆内存的选项

### 兼容性

- 保持与 Flink StateBackend API 完全兼容
- 用户代码无需修改即可使用
- 支持 Flink 的 Checkpoint/Savepoint 机制

## 文件说明

| 路径 | 说明 |
|------|------|
| `ForL0-State-Backend设计说明书.md` | 详细设计文档 |
| `dev_notes/` | 开发笔记和设计决策 |
| `reference/` | 参考实现 (Flink HeapStateBackend) |
| `reference/l0_docs/` | L0 内存库 API 文档 |
| `benchmark/` | 性能测试框架 |
| `benchmark/docs/Advanced_Metrics_Design.md` | 高级指标采集方案设计 |

## 调试提示

1. **Native 库加载问题**:
   - 检查 `java.library.path` 设置
   - 确认 `.dylib/.so` 文件存在
   - 查看日志中的 `[ForL0]` 前缀消息

2. **L0 模式检测**:
   ```java
   NativeL0Memory.isL0Mode()          // 是否 L0 模式
   NativeL0Memory.getModeDescription() // 模式描述
   ```

3. **IDEA 测试配置**:
   - VM options: `-Djava.library.path=$ProjectFileDir$/src/main/resources/native`

## Benchmark 测试

### 运行测试

```bash
cd benchmark/scripts

# 运行 WordCount 对比测试
python run_wordcount.py --backend all

# 运行 WordCount 并采集火焰图
export ASYNC_PROFILER_HOME=/path/to/async-profiler
python run_wordcount.py --backend all --profile

# 生成报告
python generate_report.py
```

### 采集的指标

| 指标 | 说明 | 平台支持 |
|------|------|----------|
| L0Table 命中率 | L0 热点缓存命中率 | macOS ✅ / Linux ✅ |
| 吞吐量/延迟 | 性能基准指标 | macOS ✅ / Linux ✅ |
| 火焰图 (CPU/Alloc) | Async Profiler | macOS ✅ / Linux ✅ |
| CPU Cache 统计 | cache-misses 等 | macOS ❌ / Linux ✅ |

> ⚠️ CPU Cache 统计 (cache-misses, L1-dcache-load-misses) 仅在 Linux 上可用，需要 perf_events 支持。

## 贡献指南

1. 修改前先理解双层索引架构
2. Native 代码修改需要重新编译库
3. 保持与 Flink 原生 API 的兼容性
4. 测试代码使用 `[BENCHMARK_TEST]` 注释标注
