# ForL0 State Backend

ForL0 State Backend 是一个为 Apache Flink 设计的高性能状态后端实现，采用 **Swiss Tables 架构**（对齐 Go 1.24），通过 SWAR 并行匹配和 Extendible Hashing 实现高效的状态访问。

## 项目概述

ForL0 State Backend 采用 Swiss Tables + Extendible Hashing 架构设计：
- **SwissTable**：存储层，采用 SWAR 并行匹配（8 slots 同时比较），87.5% 负载因子
- **ForL0StateMap**：Directory 路由层，实现 Extendible Hashing 动态扩容，支持增量 split

### 运行模式

系统支持两种运行模式，**运行时自动检测**：

| 模式 | 条件 | 内存分配 | 适用场景 |
|------|------|----------|----------|
| **L0 模式** | `/dev/hisi_l0` 存在且 `libl0mempool.so` 可加载 | L0 内存池 | 鲲鹏服务器生产环境 |
| **模拟模式** | 其他情况 | 标准 malloc/free | 开发测试、无 L0 硬件环境 |

## 核心特性

### 🚀 性能优化
- **SWAR 并行匹配**：8 个 slot 同时比较，对齐 Go 1.24 Swiss Tables
- **Extendible Hashing**：增量 split 避免全局重哈希，仅迁移 50% 数据
- **Hash 存储优化**：存储完整 64 位 hash，split/grow 时无需重新计算
- **高负载因子**：87.5% 负载因子，空间利用率优于传统哈希表

### 🔧 架构特点
- **2 层架构**：ForL0StateMap (Directory) → SwissTable (存储)
- **堆内对象存储**：状态对象直接存储在堆内，零序列化开销
- **控制字节设计**：EMPTY=0x80, DELETED=0xFE, FULL=h2 (低 7 位)
- **JNI Native 内存**：支持 L0 硬件加速（鲲鹏 CPU）
- **状态快照**：完整支持 Flink 的检查点机制

### 📊 监控统计
- **Table 统计**：条目数、负载因子、split 次数
- **模式检测**：运行时可查询当前 L0/模拟模式状态

## 快速开始

### 离线机器一键 App（推荐）

推荐把**整个仓库目录**拷到离线服务器。仓库内应已经包含：

- `docker/deploy/`：ForL0 backend JAR、WordCount / NexMark / Client benchmark JAR；native 库可在 `docker/deploy/libforl0_engine.so` 或 `src/main/resources/native/libforl0_engine.so`
- `offline-packages/` 或 `benchmark/offline-packages/`：benchmark Python wheels，可选 async-profiler 压缩包
- `docker/images/eclipse-temurin-8-jre.tar.gz`：可选 Docker 镜像；如果目标机已经有 `eclipse-temurin:8-jre`，可不带

离线机器上只需要进入目录，执行顶层启动器：

```bash
set -euo pipefail

REPO_DIR="$HOME/forL0-state-backend"
FLINK_HOME="$HOME/flink-1.20.3"

cd "$REPO_DIR"
chmod +x ./forl0-offline-app.sh
./forl0-offline-app.sh --flink-home "$FLINK_HOME"
```

默认会依次完成：安装 ForL0 到 Flink、导入离线 Docker 镜像（如存在）、preflight、Client Usecase 冒烟、合同口径 apps 测试、HTML 报告生成。需要更完整的 NexMark no-Full-GC 压力复跑时执行：

```bash
cd "$REPO_DIR"
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --full
```

如果需要复现交付报告中的 Ascend 编号化性能清单，执行：

```bash
cd "$REPO_DIR"
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --skip-docker-load --reproduce-ascend --no-report
```

该清单按总吞吐优先组织，并保持公平执行拓扑：同一个 workload 内 HashMap 与 ForL0 使用相同的 query、输入比例、Flink/operator 并行度、slot 数和监控窗口；ForL0 只使用自身后端参数与实现优化。交付复现分为两类结果：WordCount fastpath 与 NexMark q18 TPS / lateq-deep 为主要收益场景；WordCount 默认恢复为 2 个 TM × 4 slots 对应的 p8 配置，沿用 2026-07-11 05:40 Ascend p8 复跑中 +20.0% 的历史有效设置，后续 p4 样本仅作为误调后的保守对照。NexMark q18 在 2026-07-11 12:30 Ascend 复跑中得到 +32.0% / +32.1% 总吞吐提升；NexMark q19/q20 与 Client state-pressure 为补充复核场景，同日 q19 / q20 分别为 +2.2% / +9.2%，Client 30 万 / 100 万状态压力分别为 +5.0% / +1.5%。清单不把仅体现 CPU/core 资源效率、需要提高 ForL0 operator 并行度或复跑波动为负的样本作为交付结论。

编号化复跑结果会写入 `benchmark/results/run_logs/ascend_reproduction_*.tsv`，原始 JSON 写入 `benchmark/results/raw/`，NexMark 明细写入 `benchmark/results/nexmark_*/nexmark_results.json`。如果运行时不加 `--no-report`，脚本还会基于已有结果生成 `benchmark/results/reports/benchmark_report.html`。

常用模式：

```bash
# 只做安装 + preflight + 最短冒烟，适合刚到离线机器时先验链路
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --smoke-only

# 只跑合同口径 apps，不跑冒烟
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --apps-only

# 只基于已有结果重新生成 HTML 报告
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --report-only

# 只跑 ForL0 或只跑 HashMap
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --backend forl0 --apps-only
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --backend hashmap --apps-only
```

运行完成后查看：

```bash
cd "$REPO_DIR"
ls -lh benchmark/results/reports/benchmark_report.html
find benchmark/results/raw -maxdepth 1 -type f | sort | tail
find benchmark/results -maxdepth 1 -type d -name 'nexmark_*' | sort | tail
```

结果目录说明：

- `benchmark/results/raw/`：WordCount、Client Usecase、NexMark 汇总 JSON
- `benchmark/results/nexmark_*/`：NexMark 每次运行的原始 summary、日志与监控数据
- `benchmark/results/figures/`：报告图表
- `benchmark/results/reports/benchmark_report.html`：HTML 汇总报告

#### 离线包安装路径（不拷整个仓库时使用）

如果你在联网/构建机器上先生成独立离线包，打包脚本会把 `forl0-offline-app.sh` 一起放到离线包根目录：

```bash
cd /path/to/forL0-state-backend
./docker/package_offline_bundle.sh --arch arm64 --output-dir /tmp/forl0-offline
tar -C /tmp -czf /tmp/forl0-offline.tar.gz forl0-offline
scp /tmp/forl0-offline.tar.gz user@server:/tmp/
```

在离线服务器上执行：

```bash
set -euo pipefail

FLINK_HOME="$HOME/flink-1.20.3"

cd /tmp
tar -xzf forl0-offline.tar.gz
cd /tmp/forl0-offline

chmod +x ./forl0-offline-app.sh
./forl0-offline-app.sh --flink-home "$FLINK_HOME" --install-dir "$HOME/forl0-runtime"
```

#### 常见问题快速定位

```bash
# 查看 ForL0 是否装入 Flink
ls -lh "$FLINK_HOME/lib"/flink-statebackend-forL0-*.jar "$FLINK_HOME/native"/libforl0_engine.so

# 查看 Flink/Docker 是否已经起来
curl -sf http://localhost:8081/overview || true
cd "$REPO_DIR/docker" 2>/dev/null || cd "$INSTALL_DIR/docker"
./docker_run.sh status || true

# 查看 L0 / 模拟模式日志
grep -R "ForL0\\|HotCache\\|SIMULATION\\|L0 MODE" "$FLINK_HOME/log" docker/*.log benchmark/results 2>/dev/null | tail -80
```

说明：

- 没有 `/dev/l0`、`/dev/hisi_l0` 或 `libl0mempool.so` 时，ForL0 会自动进入模拟模式；功能测试仍可完成，但不能作为真实 L0 性能验收。
- `run_all_apps.sh --offline` 不会联网安装依赖；如果 preflight 提示缺 wheel，需要在联网机器重新执行 `docker/package_offline_bundle.sh` 并把 `offline-packages/` 带到离线机。
- Docker 不可用时，`run_all_apps.sh` 会尝试回退到本机 Flink standalone；若你只想安装不启动集群，使用 `server_setup.sh --no-start`。

更详细的 benchmark 参数和场景说明见：[benchmark/README.md](benchmark/README.md)。

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

# ========== L0 Hot-Key Cache (可选) ==========
# 只有两个开关：enabled 与 size。容量过大会在 cache_tuner_init 阶段被内核
# 模块拒绝并自动降级为关闭（WARN 日志可查）。
state.backend.forl0.l0-cache.enabled: false
state.backend.forl0.l0-cache.size:    20mb
```

#### 多 slot 部署时的 l0-cache.size 公式

L0 的物理上限来自内核模块参数 `max_numa_capacity`（默认 20 MB / NUMA node）。
单台鲲鹏 920 双路有 8 个 NUMA node，整机实际可用 < 100 MB。
**同一 NUMA node 上所有 TaskManager slot 共享该节点的 L0 预算**，后启的 slot
一旦 `cache_tuner_init` 失败就会触发硬件门禁、走 WARN 降级路径。

推荐计算：

```
recommended_size = (max_numa_capacity - reserve) / slots_per_numa
                 ≈ (20 MB - 4 MB) / slots_per_numa
```

举例：单 NUMA 上 4 slots 共享 → 建议 `l0-cache.size: 4mb`；8 slots → `2mb`。
若超分，晚启 slot 会看到：

```
[ForL0-HotCache] WARN: L0 hardware not available (reason: cache_tuner_init ...);
cache forcibly disabled.
```

这不是错误；该 slot 上的 ValueState 仍然工作，只是没有 L0 加速。

**编程方式配置：**

```java
import org.apache.flink.runtime.state.heap.ForL0StateBackend;
import org.apache.flink.runtime.state.heap.ForL0StateBackendConfig;

// 使用默认配置
ForL0StateBackend stateBackend = new ForL0StateBackend();

// 或使用自定义配置
ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
    .build();
ForL0StateBackend stateBackend = new ForL0StateBackend(config);

env.setStateBackend(stateBackend);
```

#### 配置项说明

> 注意：SwissMap 架构使用自适应增量扩容，无需手动配置负载因子等参数。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `state.backend.forl0.l0-cache.enabled` | boolean | `false` | 是否请求开启 L0 Hot-Key Cache |
| `state.backend.forl0.l0-cache.size` | MemorySize | `20mb` | 请求的 L0 容量；受 `max_numa_capacity` 与同 NUMA slot 数约束，实际值见 metric `forl0.hotcache.bytesCapacity` |

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
| `ForL0StateMap` | StateMap 接口 + Directory 路由 + Extendible Hashing |
| `SwissTable` | SWAR 并行匹配的哈希表存储层 |
| `NativeL0Memory` | JNI 桥接，L0/模拟模式切换 |

## 性能特性

### 查找操作流程 (SWAR 并行匹配)
1. 计算 hash，通过 `hash >>> globalShift` 定位 SwissTable
2. 计算 H1 (`hash >>> 7`) 确定探测起始 group
3. 加载 8 字节 ctrl word，SWAR 并行匹配 H2 (`hash & 0x7F`)
4. 对匹配的 slot 进行 key/namespace equals 验证
5. 未找到则线性探测下一个 group

### SWAR 算法
```java
// 8 slots 同时比较 (Single Word, All Results)
static long matchH2(long ctrlWord, int h2) {
    long pattern = LSB * (h2 & 0xFFL);  // 0x0101010101010101L * h2
    long x = ctrlWord ^ pattern;
    return (x - LSB) & ~x & MSB;         // MSB = 0x8080808080808080L
}
```

### Extendible Hashing
- **增量扩容**：split 仅迁移 50% 数据到新表
- **Directory 翻倍**：当 localDepth > globalDepth 时 directory 翻倍
- **Go 风格去重**：`if (t.index == i) t.index = 2 * i` 避免重复处理

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

## 测试

### 运行单元测试

```bash
# 运行所有测试
mvn test

# 运行 SwissTable 测试
mvn test -Dtest=SwissTableTest

# 运行 ForL0StateMap 测试
mvn test -Dtest=ForL0StateMapTest

# 运行 Native 内存测试
mvn test -Dtest=NativeL0MemoryTest
```

## 项目状态

### ✅ 已实现功能
- Swiss Tables 架构 (对齐 Go 1.24)
- SWAR 并行匹配 (8 slots 同时比较)
- Extendible Hashing 增量扩容
- Hash 存储优化 (split/grow 无需重新计算)
- JNI Native 内存分配
- L0 硬件支持 (libl0mempool.so)
- 运行时模式自动检测
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
- 📝 [Swiss Tables 重构设计](dev_notes/SwissTable_Refactoring_Plan.md)
- 📝 [L0 内存分配设计](dev_notes/L0_Memory_Allocation_Design.md)

## 联系方式

- 邮箱: yangjinyun@hust.edu.cn
- GitHub: [@Yang-YJY](https://github.com/Yang-YJY)

---

**注意**: 在没有 L0 硬件的环境下，系统会自动使用模拟模式运行，功能完全一致，仅性能有所差异。
