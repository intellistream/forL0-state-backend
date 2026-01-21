# Copilot Instructions for ForL0 State Backend

## 项目概述

ForL0 State Backend 是一个为 Apache Flink 设计的高性能状态后端实现，采用 Swiss Tables 架构（对齐 Go 1.24），通过 SWAR 并行匹配和 Extendible Hashing 实现高效的状态访问。

## 技术栈

- **语言**: Java 8+, C (JNI native code)
- **框架**: Apache Flink 1.20.0
- **构建工具**: Maven 3.6+
- **测试框架**: JUnit 5
- **平台**: 
  - 开发: macOS (模拟模式)
  - 生产: Linux with 鲲鹏 CPU (L0 模式)

## 核心架构

### Swiss Tables 架构 (对齐 Go 1.24)

```
ForL0StateMap (StateMap 接口 + Directory 路由)
├── directory[]         // Extendible Hashing 路由表
├── tables: List        // 唯一 SwissTable 列表
└── SwissTable          // 存储层
    ├── ctrl[]          // 控制字节 (EMPTY=0x80, DELETED=0xFE, FULL=h2)
    ├── keysNs[]        // key/namespace 存储 (slot i → keysNs[2i], keysNs[2i+1])
    ├── values[]        // value 存储
    └── hashes[]        // 64位 hash 存储 (用于 rehash/grow/split)
```

### Hash 位分配 (对齐 Go 1.24)

```
64 位 hash:
├── H1 = hash >>> 7     // 高 57 位，用于探测起始 group
├── H2 = hash & 0x7F    // 低 7 位，存入 ctrl 字节
└── Directory 路由: hash >>> globalShift (高 globalDepth 位)
```

### 关键组件

| 组件 | 职责 | 位置 |
|------|------|------|
| `ForL0StateBackend` | StateBackend 入口 | `runtime/state/heap/` |
| `ForL0KeyedStateBackend` | KeyedStateBackend 实现 | `runtime/state/heap/` |
| `ForL0StateMap` | StateMap 接口 + Directory 路由 | `runtime/state/heap/` |
| `SwissTable` | SWAR 并行匹配的哈希表存储 | `runtime/state/heap/` |
| `NativeL0Memory` | JNI 桥接类 (L0 Cache) | `runtime/state/heap/space/` |
| `forl0_native.c` | C 实现 (L0/模拟模式) | `src/main/native/` |

### SwissTable 核心算法

```java
// SWAR 并行匹配 (8 slots 同时比较)
static long matchH2(long ctrlWord, int h2) {
    long pattern = LSB * (h2 & 0xFFL);
    long x = ctrlWord ^ pattern;
    return (x - LSB) & ~x & MSB;
}

// put 返回值编码
static final int NEW_FLAG = 1 << 16;   // 新插入标志
static final int SLOT_MASK = 0xFFFF;   // 槽位掩码
static final int NEED_SPLIT = -1;      // 需要 split

// 使用示例
int result = table.put(hash, key, namespace, MAX_CAPACITY);
if (result == NEED_SPLIT) {
    handleSplit(table);
    continue; // 重试
}
int slot = result & SLOT_MASK;
boolean isNew = (result & NEW_FLAG) != 0;
table.values[slot] = value;
```

### Extendible Hashing

- **globalDepth**: Directory 使用的 hash 位数
- **localDepth**: 每个 Table 的深度
- **split**: 当 Table 满载且 capacity = MAX_TABLE_CAPACITY 时触发
- **Go 风格 index 去重**: `if (t.index == i) t.index = 2 * i`

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

### 文档

1. 除非用户要求，否则禁止创建新的说明文档

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

- SwissTable ctrl[]: 每 slot 1 字节控制字节
- SwissTable keysNs[]: slot i → keysNs[2*i] (key), keysNs[2*i+1] (namespace)
- SwissTable values[]: slot i → values[i]
- SwissTable hashes[]: slot i → 64位 hash (用于 rehash/grow/split)
- Directory 默认大小: 1 (globalDepth=0)
- Table 容量: INITIAL=64, MAX=1024, 负载因子 87.5%

### 错误处理

- Native 库加载失败 → 抛出异常,L0 Cache 不可用
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
| 吞吐量/延迟 | 性能基准指标 | macOS ✅ / Linux ✅ |
| 火焰图 (CPU/Alloc) | Async Profiler | macOS ✅ / Linux ✅ |
| CPU Cache 统计 | cache-misses 等 | macOS ❌ / Linux ✅ |

> ⚠️ CPU Cache 统计 (cache-misses, L1-dcache-load-misses) 仅在 Linux 上可用，需要 perf_events 支持。

## 贡献指南

1. 修改前先理解 Swiss Tables 架构和 SWAR 并行匹配
2. Native 代码修改需要重新编译库
3. 保持与 Flink 原生 API 的兼容性
4. 测试代码使用 `[BENCHMARK_TEST]` 注释标注
