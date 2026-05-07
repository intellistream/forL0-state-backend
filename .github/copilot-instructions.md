# Copilot Instructions for ForL0 State Backend

## 项目概述

ForL0 State Backend 是一个面向 Apache Flink 的高性能 Keyed State Backend。当前实现不是旧版纯 Java `ForL0StateStore` 架构，而是以下分层：

- Java 侧 `ForL0StateBackend` / `ForL0KeyedStateBackend` 作为 Flink 集成层
- `NativeEngine` 作为 JNI 桥接层
- `src/main/native/engine/` 下的 C++ 原生状态引擎负责真实 keyed state 存储、checkpoint 序列化和 hot-cache 逻辑

分析或修改代码时，应以“Java thin shell + JNI + C++ native engine”的现状为准，不要把旧设计文档里的 `ForL0StateStore` / Java `SwissTable` 当作当前源码事实。

## 技术栈

- **语言**: Java 8+, C++17, JNI
- **框架**: Apache Flink 1.20.3
- **构建工具**: Maven 3.x, GNU Make
- **测试框架**: JUnit 5
- **运行模型**:
  - Java keyed backend shell
  - Native off-heap engine
  - Flink heap priority queue wrappers for PQ state
- **平台**:
  - Linux 为主
  - macOS 保留 native library 加载与构建路径

## 核心架构

### 当前控制路径

```text
state.backend: forl0
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

### Snapshot / Restore 路径

```text
Checkpoint:
ForL0SnapshotStrategy.syncPrepareResources(...)
  -> NativeEngine.prepareSnapshot(engineHandle)
ForL0SnapshotStrategy.asyncSnapshot(...)
  -> 写 Flink serialization metadata
  -> 写 PQ state blocks
  -> NativeEngine.writeKeyGroupData(engineHandle, keyGroupId)
  -> NativeEngine.releaseSnapshot(engineHandle)

Restore:
ForL0KeyedStateBackendBuilder.build()
  -> restoreFromHandles(...)
  -> 重建 KV metadata / PQ wrappers
  -> 将 key-group payload 回灌到 native engine
```

### Native 目录结构

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

## 关键组件

| 组件 | 职责 | 位置 |
|------|------|------|
| `ForL0StateBackend` | Flink `StateBackend` 入口；创建 keyed backend；operator state 委托给 Flink 默认实现 | `src/main/java/org/apache/flink/state/forl0/` |
| `ForL0StateBackendFactory` | SPI 工厂，支持 `state.backend: forl0` | `src/main/java/org/apache/flink/state/forl0/` |
| `ForL0KeyedStateBackendBuilder` | native library 加载、engine 创建、restore、snapshot strategy 装配 | `src/main/java/org/apache/flink/state/forl0/` |
| `ForL0KeyedStateBackend` | keyed backend 主壳层；维护 native handle、state registry、serializer compatibility 和 state 创建逻辑 | `src/main/java/org/apache/flink/state/forl0/` |
| `NativeEngine` | JNI 桥接与 native library 加载入口 | `src/main/java/org/apache/flink/state/forl0/` |
| `ForL0SnapshotStrategy` | checkpoint 资源准备与 key-group snapshot 写出 | `src/main/java/org/apache/flink/state/forl0/` |
| `ForL0SnapshotResources` | checkpoint 所需的 KV meta info 与 PQ snapshot 资源 | `src/main/java/org/apache/flink/state/forl0/` |
| `ForL0ValueState` / `ListState` / `MapState` / `ReducingState` / `AggregatingState` | 具体 state wrapper，通过 native handle 执行读写 | `src/main/java/org/apache/flink/state/forl0/` |
| `ForL0KeyValueStateIterator` | snapshot/savepoint 迭代支持 | `src/main/java/org/apache/flink/state/forl0/` |
| `TypeAnalyzer` | serializer 到 native type id 的映射与 type descriptor 生成 | `src/main/java/org/apache/flink/state/forl0/` |

## 当前实现约束

### 状态所有权

- 真正的 keyed-state 数据存放在 native engine，不在 Java heap table 中。
- Java 侧按 state name 懒注册 native state handle。
- 当前支持的 keyed state 类型有 `VALUE`、`LIST`、`MAP`、`REDUCING`、`AGGREGATING`。
- Priority queue state 仍使用 Flink heap priority queue wrapper，而非 native 实现。
- Operator state 不是自研实现，直接委托给 Flink 默认 backend。

### Serializer 与恢复语义

- restore 入口以 `ForL0KeyedStateBackendBuilder.restoreFromHandles(...)` 为准。
- serializer compatibility 由 Flink 的 compatibility API 决定，不要自行假设“差不多兼容”。
- canonical savepoint 与当前 ForL0 自定义 checkpoint payload 的恢复路径不同，修改时必须同时检查。

### Hot Cache / L0

- Hot-cache 与 L0 相关能力通过 `NativeEngine` 暴露，而不是旧的 `NativeL0Memory`。
- 当前可直接观察或调用的 JNI 入口包括：
  - `getHotCacheManagerStats(...)`
  - `getHotCacheStats(...)`
  - `rebalanceHotCache(...)`
- 如果出现 stale read、invalidations、命中率异常，优先同时检查 state wrapper 和 native cache 逻辑。

## 代码规范

### Java 代码

1. **包结构**: `org.apache.flink.state.forl0.*`
2. **命名约定**:
   - 项目组件统一使用 `ForL0` 前缀
   - 常量使用全大写下划线风格
3. **热路径原则**:
   - Java wrapper 尽量保持薄
   - 数据读写和存储逻辑优先放在 `NativeEngine` / native engine
4. **兼容性原则**:
   - 使用 Flink serializer compatibility API
   - 修改 snapshot / restore 时必须同步考虑写入与恢复两侧
5. **日志**:
   - 使用 SLF4J
   - 延续 `[ForL0]` 前缀风格

### Native 代码

1. **语言现状**: 当前 native 主要是 C++，不是旧版 C 文件布局
2. **目录约定**:
   - `src/main/native/engine/`: 引擎内部实现
   - `src/main/native/jni/`: JNI 暴露层
   - `src/main/native/checkpoint/`: checkpoint 辅助代码
3. **构建方式**:
   - 使用 `src/main/native/Makefile`
   - 产物为 `libforl0_engine.so`
4. **平台差异**:
   - Makefile 已按 `aarch64`、`x86_64`、portable fallback 做编译选项切换

### 测试

1. **单元测试位置**: `src/test/java/`
2. **常用测试入口**:
   - `ForL0StateSemanticsTest`
   - `HotCacheIntegrationTest`
   - `src/test/java/org/apache/flink/state/forl0/minicluster/` 下的 MiniCluster 测试
3. **修改原则**:
   - 语义问题优先补或改语义测试
   - snapshot/restore 问题优先补恢复相关测试

### 文档

1. 除非用户明确要求，否则不要新增说明文档
2. 如果更新架构类说明，应优先同步 `.github/copilot-instructions.md` 与 `.github/agents/flinkAgent.agent.md`

## 常见任务

### 编译项目

```bash
mvn clean compile
mvn test
mvn package -DskipTests
```

### 编译 Native 库

```bash
cd src/main/native
make clean
make
make install
```

`make install` 会把 `libforl0_engine.so` 复制到 `src/main/resources/native/`。

### 运行聚焦测试

```bash
mvn -Dtest=ForL0StateSemanticsTest test
mvn -Dtest=HotCacheIntegrationTest test
```

### 运行 Benchmark

```bash
cd benchmark
python scripts/run_wordcount.py --backend all
python scripts/run_wordcount.py --backend all --profile
python scripts/generate_report.py
```

## 重要约定

### 线程模型

- Flink keyed state 访问默认是 task-thread confined
- native engine 以这一前提设计
- 不要在热路径随意引入同步原语

### 错误处理

- Native 库加载失败时，应从 `NativeEngine` 的加载流程和资源打包路径开始排查
- snapshot/restore 失败时，先区分 serializer compatibility、JNI marshalling、native payload 三类问题
- 不要默认提供“自动降级到 heap state”的修复思路，除非用户明确要求设计降级方案

### 兼容性

- 保持与 Flink `StateBackend` / keyed state 语义兼容
- 用户代码应无需修改即可接入 `state.backend: forl0`
- 修改二进制 checkpoint 布局时，必须同时维护 restore 路径

## 文件说明

| 路径 | 说明 |
|------|------|
| `src/main/java/org/apache/flink/state/forl0/` | Java backend 主实现 |
| `src/main/native/engine/` | native engine、SwissTable、HotCache |
| `src/main/native/jni/` | JNI 按状态类型拆分的桥接实现 |
| `src/test/java/org/apache/flink/state/forl0/` | 语义测试、集成测试、MiniCluster 测试 |
| `benchmark/` | benchmark 脚本、配置和报告 |
| `dev_notes/CPP_StateBackend_Rewrite_Design.md` | 当前 native-engine 重写设计背景 |
| `reference/` | Flink 参考实现与相关资料 |

## 调试提示

1. **Native 库加载问题**:
   - 检查 `src/main/resources/native/libforl0_engine.so` 是否存在
   - 检查 `java.library.path` 与资源打包是否正确
   - 查看 `NativeEngine` 日志，确认是 `System.loadLibrary` 成功，还是 fallback 到资源解压加载

2. **Checkpoint / Restore 问题**:
   - 从 `ForL0KeyedStateBackendBuilder.restoreFromHandles(...)` 入手看恢复
   - 从 `ForL0SnapshotStrategy.syncPrepareResources(...)` / `asyncSnapshot(...)` 入手看写出
   - 先验证 serializer compatibility，再怀疑 native 数据损坏

3. **State 语义问题**:
   - 从对应的 `ForL0*State` wrapper 和它调用的 `NativeEngine` 方法入手
   - 用 `ForL0StateSemanticsTest` 先确认不是 cache stale read / clear 语义回归

4. **测试运行配置**:
   - Surefire 已预置 `-Djava.library.path=${project.basedir}/src/main/resources/native`
   - 如在 IDE 单独运行测试，补同等 VM option

## Benchmark 测试

### 运行测试

```bash
cd benchmark

# 运行 WordCount 对比测试
python scripts/run_wordcount.py --backend all

# 运行 WordCount 并采集火焰图
export ASYNC_PROFILER_HOME=/path/to/async-profiler
python scripts/run_wordcount.py --backend all --profile

# 生成报告
python scripts/generate_report.py
```

### 采集指标

| 指标 | 说明 | 平台支持 |
|------|------|----------|
| 吞吐量 / 延迟 | 基准性能指标 | macOS ✅ / Linux ✅ |
| 火焰图 (CPU / Alloc) | Async Profiler | macOS ✅ / Linux ✅ |
| CPU Cache 统计 | `cache-misses` 等 | macOS ❌ / Linux ✅ |

> ⚠️ CPU Cache 统计仅在 Linux 可用，并依赖 perf_events 支持。

## 贡献指南

1. 修改前先确认当前源码是否走 `NativeEngine + C++ engine` 路径，而不是旧设计路径
2. Native 代码修改后通常需要重新 `make` / `make install`
3. 修改 checkpoint 布局或 serializer 行为时，必须同时考虑 restore 兼容性
4. 性能问题先区分语义回归、JNI 开销、native 存储问题，再谈优化
