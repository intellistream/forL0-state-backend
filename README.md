# ForL0 State Backend

ForL0 State Backend 是一个为 Apache Flink 设计的高性能状态后端实现，旨在充分利用**鲲鹏 CPU 的 L0 Cache 特性**，通过缓存友好的数据结构和分层索引架构来优化状态访问性能。

## 项目概述

ForL0 State Backend 采用双层索引结构设计：
- **L0 Table**：热点缓存层，使用 JNI 分配的 L0 内存，为高频访问的键提供极低延迟的访问
- **Main Table**：主索引表，使用 Flink MemoryManager 管理的堆外内存，存储全量状态数据

### 运行模式

系统支持两种运行模式，**运行时自动检测**：

| 模式 | 条件 | 内存分配 | 适用场景 |
|------|------|----------|----------|
| **L0 模式** | `/dev/hisi_l0` 存在且 `libl0mempool.so` 可加载 | L0 内存池 | 鲲鹏服务器生产环境 |
| **模拟模式** | 其他情况 | 标准 malloc/free | 开发测试、无 L0 硬件环境 |

## 核心特性

### 🚀 性能优化
- **缓存友好设计**：64 字节对齐的桶结构，匹配 CPU 缓存行
- **多级索引**：L0 缓存 + 主表的分层架构减少访问延迟
- **局部扩展**：主表支持局部树状扩展，避免全局重哈希
- **多种替换策略**：支持 LRU、LFU、CLOCK、TinyLFU、Sampled-LRU 等缓存替换算法

### 🔧 架构特点
- **堆内对象存储**：状态对象直接存储在堆内，零序列化开销
- **堆外缓存友好索引**：L0Table 和 MainTable 使用堆外内存，64B 缓存行对齐
- **JNI Native 内存**：L0Table 使用 JNI 分配的原生内存，支持 L0 硬件加速
- **扩展桶池**：统一管理扩展桶，最多支持 255 个扩展桶/主桶
- **内存管理**：MainTable 基于 Flink MemoryManager，L0Table 使用独立的 NativeL0MemoryAllocator
- **状态快照**：完整支持 Flink 的检查点机制

### 📊 监控统计
- **多维度指标**：L0 缓存命中率、主表负载因子、扩展桶使用率
- **模式检测**：运行时可查询当前 L0/模拟模式状态
- **性能统计**：访问计数、驱逐率、内存使用量等

## 快速开始

### 环境要求

- Java 8+
- Apache Flink 1.20.0+
- Maven 3.6+
- **生产环境额外要求**：鲲鹏 CPU 服务器、L0 设备驱动、libl0mempool.so

### 编译构建

```bash
# 克隆项目
git clone https://github.com/intellistream/forL0-state-backend.git
cd forL0-state-backend

# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包 JAR
mvn package -DskipTests
```

### 编译 Native 库

```bash
# macOS (开发用)
cd src/main/native
make clean && make

# Linux (生产用，需在服务器上编译)
export JAVA_HOME=/path/to/jdk
make clean && make
```

### 部署到 Flink

#### 1. 添加依赖

```bash
# 将 JAR 包复制到 Flink lib 目录
cp target/flink-statebackend-forL0-1.0-SNAPSHOT.jar $FLINK_HOME/lib/

# Linux 服务器：将 native 库放到系统路径
sudo cp src/main/native/libforl0_native.so /usr/lib/
sudo ldconfig
```

#### 2. 配置 StateBackend

**配置文件方式（推荐）：**

在 `config.yaml` 中添加：

```yaml
state.backend: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory

# ========== ForL0 StateBackend 可选配置 ==========

# L0 缓存开关（默认 true）
state.backend.forl0.l0-cache.enabled: true

# 单个 L0Table 大小（2的幂次，默认10 = 1024 buckets = 64KB）
state.backend.forl0.l0-cache.size: 10

# L0 缓存替换策略：CLOCK, LRU, LFU, TINY_LFU, SAMPLED_LRU（默认 CLOCK）
state.backend.forl0.l0-cache.replacement-policy: CLOCK

# L0 内存池总容量（所有 L0Table 共享，默认 0 = 无限制）
state.backend.forl0.l0-memory.max-size: 256mb

# MainTable 初始大小（2的幂次，默认10 = 1024 buckets）
state.backend.forl0.main-table.initial-size: 10

# MainTable 负载因子阈值（默认 1.5）
state.backend.forl0.main-table.load-factor-threshold: 1.5
```

**编程方式配置：**

```java
import org.apache.flink.runtime.state.heap.ForL0StateBackend;
import org.apache.flink.runtime.state.heap.ForL0StateBackendConfig;
import org.apache.flink.runtime.state.heap.L0Table;

// 使用默认配置
ForL0StateBackend stateBackend = new ForL0StateBackend();

// 或使用自定义配置
ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
    .setL0CacheEnabled(true)
    .setL0CacheSize(12)  // 4096 buckets
    .setL0ReplacementPolicy(L0Table.ReplacementPolicy.CLOCK)
    .setL0MemoryMaxBytes(256 * 1024 * 1024L)  // 256MB
    .setMainTableLoadFactorThreshold(1.5)
    .build();
ForL0StateBackend stateBackend = new ForL0StateBackend(config);

env.setStateBackend(stateBackend);
```

#### 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `state.backend.forl0.l0-cache.enabled` | Boolean | `true` | 是否启用 L0 热点缓存 |
| `state.backend.forl0.l0-cache.size` | Integer | `10` | 单个 L0Table 大小（2的幂次，范围 1-20） |
| `state.backend.forl0.l0-cache.replacement-policy` | String | `CLOCK` | 缓存替换策略 |
| `state.backend.forl0.l0-memory.max-size` | MemorySize | `0` (无限制) | L0 内存池总容量 |
| `state.backend.forl0.main-table.load-factor-threshold` | Double | `1.5` | MainTable 扩容负载因子 |

#### 3. 验证部署

启动 Flink 后，查看 TaskManager 日志：

```bash
grep -E "ForL0|L0 mode" $FLINK_HOME/log/flink-*-taskexecutor-*.log

# L0 模式成功时显示：
# [ForL0] L0 device detected (/dev/hisi_l0)
# [ForL0] Running in L0 MODE

# 模拟模式显示：
# [ForL0] Running in SIMULATION MODE (malloc/free)
```

#### 4. 使用状态 API

ForL0 State Backend 完全兼容 Flink 的状态 API，无需修改用户代码：

```java
// ValueState 示例
ValueStateDescriptor<String> descriptor = 
    new ValueStateDescriptor<>("myState", String.class);
ValueState<String> valueState = getRuntimeContext().getState(descriptor);

// MapState 示例
MapStateDescriptor<String, Integer> mapDescriptor = 
    new MapStateDescriptor<>("myMapState", String.class, Integer.class);
MapState<String, Integer> mapState = getRuntimeContext().getMapState(mapDescriptor);
```


## 架构设计

### 内存分配架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        ForL0StateMap                            │
├────────────────────────────────┬────────────────────────────────┤
│         MainTable              │           L0Table              │
│    (MemoryManagerAllocator)    │    (NativeL0MemoryAllocator)   │
├────────────────────────────────┼────────────────────────────────┤
│    Flink MemoryManager         │        JNI Native Memory       │
│    (Off-heap managed memory)   │  (L0 mode / Simulation mode)   │
├────────────────────────────────┴────────────────────────────────┤
│                      HeapEntryStore (堆内)                       │
│              Object[] 数组存储 HeapStateEntry 对象               │
│                   (零序列化，直接对象引用)                        │
└─────────────────────────────────────────────────────────────────┘
```

### 数据结构布局

#### L0 Table 结构
```
桶大小: 64 字节 (缓存行对齐)
槽位数: 4 个槽位/桶
槽位结构 (16B):
├── Tag (2B): 键的哈希摘要
├── Valid (1B): 有效位标记
├── Extension (5B): 扩展字段 (LRU/LFU/CLOCK 数据)
└── Pointer (8B): Entry 指针
```

#### Main Table 结构
```
桶大小: 64 字节
槽位数: 6 个槽位/桶 + 4 个扩展指针
槽位结构 (10B):
├── Tag (2B): 键的哈希摘要
└── Pointer (8B): Entry 指针
扩展指针: 4 个扩展桶指针 (1B each)
```

### 核心组件

| 组件 | 职责 |
|------|------|
| `ForL0StateBackend` | StateBackend 工厂入口 |
| `ForL0KeyedStateBackend` | KeyedStateBackend 实现 |
| `ForL0StateMap` | 核心状态存储，组合双层索引 |
| `L0Table` | 热点缓存，支持多种替换策略 |
| `MainTable` | 主索引表，支持局部扩展 |
| `HeapEntryStore` | 堆内对象存储，零序列化 |
| `HeapStateEntry` | 状态条目，存储 key/namespace/state 对象引用 |
| `NativeL0Memory` | JNI 桥接，L0/模拟模式切换 |
| `NativeL0MemoryAllocator` | L0Table 内存分配器 |
| `MemoryManagerAllocator` | MainTable 内存分配器 |

## 性能特性

### 查找操作流程
1. 优先查询 L0 Table 缓存
2. L0 未命中时查询 Main Table
3. 主桶未命中时查询扩展桶
4. 成功查找时更新 L0 缓存（热点提升）

### 替换策略
| 策略 | 说明 | 适用场景 |
|------|------|----------|
| **CLOCK** | 时钟算法，1-bit 访问标记 | **默认推荐**，低开销高性能 |
| **LRU** | 最近最少使用 | 时间局部性强的工作负载 |
| **LFU** | 最不频繁使用 | 频率局部性强的工作负载 |
| **TinyLFU** | 带衰减的 TinyLFU | 混合访问模式 |
| **Sampled-LRU** | 随机采样 + LRU | 轻量级近似 LRU |

## 运行时 API

### 模式检测

```java
import org.apache.flink.runtime.state.heap.space.NativeL0Memory;

// 检查是否为 L0 模式
boolean isL0 = NativeL0Memory.isL0Mode();

// 获取模式描述
String mode = NativeL0Memory.getModeDescription();
// "L0 mode (libl0mempool.so)" 或 "Simulation mode (malloc/free)"

// 获取模式代码
int modeCode = NativeL0Memory.getMode();
// 0 = 未初始化, 1 = 模拟模式, 2 = L0 模式
```

### L0 缓存统计
```java
L0TableStats stats = l0Table.getStats();
double hitRate = stats.getHitRate();
double evictionRate = stats.getEvictionRate();
long accessCount = stats.getAccessCount();
```

### 主表统计
```java
MainTableStats stats = mainTable.getStats();
double loadFactor = stats.getLoadFactor();
int extensionBucketsUsed = stats.getExtensionBucketsUsed();
```

## 测试

### 运行单元测试

```bash
# 运行所有测试
mvn test

# 运行特定测试类
mvn test -Dtest=L0TableTest

# 运行 Native 内存测试
mvn test -Dtest=NativeL0MemoryTest
```

## 项目状态

### ✅ 已实现功能
- 双表架构 (L0Table + MainTable)
- 多种替换策略 (LRU/LFU/CLOCK/TinyLFU/Sampled-LRU)
- JNI Native 内存分配
- L0 硬件支持 (libl0mempool.so)
- 运行时模式自动检测
- 扩展桶池管理
- 完整的统计监控
- Flink StateBackend 集成
- Checkpoint/Savepoint 支持

### 🔄 待优化项
- 配置文件系统
- 动态配置调整
- 更多性能调优选项

## 文件结构

```
forL0-state-backend/
├── src/main/
│   ├── java/org/apache/flink/runtime/state/heap/
│   │   ├── ForL0StateBackend.java      # StateBackend 入口
│   │   ├── ForL0StateMap.java          # 核心双层索引实现
│   │   ├── L0Table.java                # 热点缓存
│   │   ├── MainTable.java              # 主索引表
│   │   ├── HeapEntryStore.java         # 堆内对象存储
│   │   ├── HeapStateEntry.java         # 状态条目 (key/ns/state)
│   │   └── space/                      # 内存分配器
│   │       ├── NativeL0Memory.java     # JNI 桥接
│   │       └── NativeL0MemoryAllocator.java
│   ├── native/
│   │   ├── forl0_native.c              # C 实现 (L0/模拟模式)
│   │   └── Makefile
│   └── resources/native/
│       └── libforl0_native.{dylib|so}  # 预编译库
├── dev_notes/                           # 开发笔记
├── reference/                           # 参考实现
│   └── l0_docs/                         # L0 内存库 API 文档
└── ForL0-State-Backend设计说明书.md     # 详细设计文档
```

## 文档

- 📖 [详细设计说明书](ForL0-State-Backend设计说明书.md)
- 📝 [L0 内存分配设计](dev_notes/L0_Memory_Allocation_Design.md)

## 联系方式

- 邮箱: yangjinyun@hust.edu.cn
- GitHub: [@Yang-YJY](https://github.com/Yang-YJY)

---

**注意**: 在没有 L0 硬件的环境下，系统会自动使用模拟模式运行，功能完全一致，仅性能有所差异。
