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

### 离线服务器运行与测试（推荐）

推荐使用 GitHub Release 中已经构建好的离线包，不再要求 Windows 中转机执行 Bash、Maven 或 Docker。当前 ARM64 发布包为 `forl0-offline-linux-arm64-py310-20260721.tar.gz`，适用于 Linux ARM64（鲲鹏/aarch64）和 CPython 3.10。

#### 1. 离线前确认服务器前置条件

以下内容不包含在离线包内，必须提前装好：Flink 1.20.x、Java 8+、Python 3.10 及 venv 模块、Docker、L0 设备驱动、`libl0mempool.so` 和 `libnuma.so.1`。在目标服务器执行：

```bash
uname -m
python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
python3 -c 'import ensurepip, venv'
test -d "$HOME/flink_home"
test -e /dev/hisi_l0 || test -e /dev/l0
ldconfig -p | grep -E 'libl0mempool\.so|libnuma\.so\.1'
docker version >/dev/null 2>&1 || sudo -n docker version >/dev/null
```

预期架构为 `aarch64`，Python 为 `3.10`。Ubuntu/Debian 必须在断网前安装 `python3.10-venv`；只有 `python3` 而没有 venv/ensurepip 时，启动器无法创建 benchmark 环境。

`libl0mempool.so` 不一定安装在 `/usr/lib64` 等系统目录。解压离线包后可运行统一探测器，它依次检查显式配置、`LD_LIBRARY_PATH`、`ldconfig`、multiarch 目录、常见 L0 厂商目录，并在 `/opt`、`/usr/local` 和当前用户目录内进行有限深度搜索：

```bash
bash ./docker/lib/l0_detector.sh
```

输出会同时显示实际路径和命中来源。若厂商运行库位于其他挂载点，可明确指定，后续启动器和 Docker 脚本都会沿用该路径：

```bash
export L0_MEMPOOL_LIB_HOST_PATH=/path/to/libl0mempool.so
export L0_DEVICE_HOST_PATH=/dev/l0       # 或 /dev/hisi_l0
export NUMA_LIB_HOST_PATH=/path/to/libnuma.so.1  # 仅在自动探测失败时需要
bash ./docker/lib/l0_detector.sh
```

所有入口均应使用 `bash ./脚本名.sh`（或直接执行带可执行权限的脚本）；探测器和主启动器在误用 `sh` 时会自动切回 Bash，避免 `bad substitution`。

#### 2. Windows 下载、校验并中转

在 Windows 浏览器打开 [GitHub Releases](https://github.com/intellistream/forL0-state-backend/releases)，下载以下两个文件：

- `forl0-offline-linux-arm64-py310-20260721.tar.gz`
- `forl0-offline-linux-arm64-py310-20260721.tar.gz.sha256`

在下载目录打开 PowerShell，校验后上传到离线服务器：

```powershell
$Bundle = ".\forl0-offline-linux-arm64-py310-20260721.tar.gz"
$Expected = (Get-Content "$Bundle.sha256").Split()[0].ToLower()
$Actual = (Get-FileHash $Bundle -Algorithm SHA256).Hash.ToLower()
if ($Actual -ne $Expected) { throw "SHA256 mismatch" }

scp $Bundle "$Bundle.sha256" user@offline-server:/tmp/
```

#### 3. 离线服务器解压和双重校验

```bash
set -euo pipefail

cd /tmp
sha256sum -c forl0-offline-linux-arm64-py310-20260721.tar.gz.sha256
tar -xzf forl0-offline-linux-arm64-py310-20260721.tar.gz -C "$HOME"

BUNDLE_DIR="$HOME/forl0-offline-linux-arm64-py310-20260721"
cd "$BUNDLE_DIR"
sha256sum -c offline_bundle_sha256.txt
chmod +x ./forl0-offline-app.sh docker/*.sh
```

外层 `.sha256` 校验传输的压缩包，包内 `offline_bundle_sha256.txt` 校验每个 JAR、native 库、Python wheel、脚本和 Docker 镜像。

#### 4. 分阶段运行测试

不要首次运行就直接压测。先做最短 smoke，成功后再跑合同 apps：

```bash
set -euo pipefail

export FLINK_HOME="$HOME/flink_home"
BUNDLE_DIR="$HOME/forl0-offline-linux-arm64-py310-20260721"
INSTALL_DIR="$HOME/forl0-runtime"

cd "$BUNDLE_DIR"

# 阶段 1：安装 + L0/Flink/Python preflight + 最短 Client smoke
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" --smoke-only

# 阶段 2：WordCount、NexMark、Client 合同口径 apps
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" --apps-only

# 阶段 3（可选）：增加 NexMark throughput pressure 场景
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" --full

# 阶段 4（可选）：复现 Ascend 编号化性能清单
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" \
  --skip-docker-load --reproduce-ascend --no-report
```

默认要求 2 个 TaskManager、共 8 个 slot。若部署拓扑不同，可显式传入 `--expected-taskmanagers N --expected-slots N`。真实离线 L0 验证不要使用 `--allow-simulation`。

结果位于 `$HOME/forl0-runtime/benchmark/results/`：

- `raw/`：WordCount、Client Usecase、NexMark 汇总 JSON
- `nexmark_*/`：NexMark 原始 summary、日志和监控数据
- `run_logs/ascend_reproduction_*.tsv`：编号化复跑 manifest
- `reports/benchmark_report.html`：HTML 汇总报告

#### 5. 常见问题快速定位

```bash
# ForL0 是否装入 Flink
ls -lh "$FLINK_HOME/lib"/flink-statebackend-forL0-*.jar \
  "$FLINK_HOME/native"/libforl0_engine.so

# Flink/Docker 状态
curl -sf http://localhost:8081/overview || true
cd "$HOME/forl0-runtime/docker"
./docker_run.sh status || true

# L0 / 模拟模式日志
grep -R "ForL0\\|HotCache\\|SIMULATION\\|L0 MODE" \
  "$FLINK_HOME/log" "$HOME/forl0-runtime/benchmark/results" 2>/dev/null | tail -80

# 显示 L0 探测结果及命中来源
bash "$HOME/forl0-runtime/docker/lib/l0_detector.sh" || true
```

如果仍出现 `Could not find a version that satisfies the requirement pandas>=2.0.0`，先确认 Python 是 3.10 且包内存在 `benchmark/offline-packages/pandas-*-cp310-*-aarch64.whl`。目标 Python 不是 3.10 时，在 Windows 仓库根目录执行 `powershell -ExecutionPolicy Bypass -File .\docker\download_offline_python_wheels.ps1 -TargetArch arm64 -PythonVersion 3.11`（版本按实际值修改），再用生成的 `offline-packages/` 替换离线包中的 wheel 目录。

#### 6. 在联网 Linux 构建机重新打包

```bash
cd /path/to/forL0-state-backend
./docker/package_offline_bundle.sh \
  --arch arm64 \
  --python-version 3.10 \
  --output-dir "$PWD/docker/generated/forl0-offline-linux-arm64-py310-20260721"
```

打包脚本会重新编译 JAR/native 产物、下载指定 Linux 架构和 CPython ABI 的 wheels、导出 ARM64 Docker 镜像，并生成包内 manifest/SHA256。不要从旧的 `benchmark/offline-packages/` 手工复制 wheel；旧 wheel 可能属于其他 Python ABI。

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
