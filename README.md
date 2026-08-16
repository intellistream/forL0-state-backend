# ForL0 State Backend

Repository ownership and entry-point layout are documented in
[`docs/REPOSITORY_STRUCTURE.md`](docs/REPOSITORY_STRUCTURE.md).

ForL0 State Backend 是一个为 Apache Flink 设计的高性能状态后端实现。当前主线通过 JNI native StateEngine、SwissTable 风格的紧凑控制字节和分组匹配实现状态访问，并提供可选的 L0 HotCache 与 copy-on-write checkpoint 路径。

## 项目概述

当前实现的核心机制是：
- **Native StateEngine / SwissTable**：按 key-group 和 namespace 管理 native 状态表，采用紧凑控制字节、分组匹配和 87.5% 最大负载因子；扩容时执行全表 rehash。
- **L0 HotCache**：位于 StateTable 之上的可选热点键缓存；它不是 SwissTable 的底层 allocator。
- **CoW checkpoint**：snapshot 期间保存首次修改前的值，并支持 native checkpoint round trip。

`ForL0StateMap` directory router 与 incremental extendible-hash split **不在当前审计分支中**。若要重新把它们作为核心贡献，必须先由项目负责人确认方向，并补齐实现、正确性测试和机制消融证据。

### 运行模式

系统支持两种运行模式，**运行时自动检测**：

| 模式 | 条件 | HotCache 后端 | 适用场景 |
|------|------|----------|----------|
| **L0 模式** | `/dev/hisi_l0` 存在且 `libl0mempool.so` 可加载 | L0 内存池中的热点集合 | 鲲鹏服务器硬件验证 |
| **模拟/不可用模式** | 设备或运行库缺失 | HotCache 不得作为真实 L0 证据 | 开发测试、backend/native 路径验证 |

## 核心特性

### 🚀 性能优化
- **SWAR 并行匹配**：每个 control group 包含 16 个字节，具体匹配实现随构建架构选择
- **全表扩容**：达到增长阈值时容量翻倍并迁移所有 FULL entry；当前没有增量 hash split
- **分配器扩展点**：生产路径的 `DefaultAllocator` 使用统一分配；ctrl/slots 分离仅由测试分配器覆盖，不是生产贡献，也不等同于 hash-table split
- **高负载因子**：87.5% 负载因子，空间利用率优于传统哈希表

### 🔧 架构特点
- **Key-group/namespace 路由**：native StateEngine 将状态访问路由到对应 StateTable
- **Native 状态存储**：SwissTable slots 存储 typed 或 serialized native values
- **控制字节设计**：EMPTY=0x80, DELETED=0xFE, FULL=h2 (低 7 位)
- **可选 L0 HotCache**：仅缓存支持的热点键值类型，SwissTable 仍为 source of truth
- **状态快照**：native CoW 和 checkpoint round-trip 已有 fresh tests；Java/Flink 集成恢复证据需单独复跑

### 📊 监控统计
- **Table 统计**：条目数等 native state 指标
- **HotCache 统计**：active、容量、使用量、lookup、hit 和 invalidation

## 快速开始

### 离线服务器运行与测试（推荐）

推荐使用 GitHub Release 中已经构建好的离线包，不再要求 Windows 中转机执行 Bash、Maven 或 Docker。当前 ARM64 发布包为 `forl0-offline-linux-arm64-py310-20260721.tar.gz`，适用于 Linux ARM64（鲲鹏/aarch64）和 CPython 3.10。

如果目标服务器能访问 GitHub，最简单的方式是只克隆代码；一键脚本会在仓库根目录自动下载固定版本的 Release 压缩包及 SHA256，支持断点续传：

```bash
git clone https://github.com/intellistream/forL0-state-backend.git "$HOME/forL0-state-backend"
cd "$HOME/forL0-state-backend"
bash ./run-forl0-offline.sh
```

如果服务器真正断网，则在联网机器克隆仓库并下载 Release 三个资产后，将整个仓库传入服务器。可设置 `FORL0_OFFLINE_ONLY=true` 禁止脚本尝试联网。

使用 Windows IDEA Deployment 时，在 IDEA 同步完代码后，于仓库根目录运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\prepare-idea-offline-deployment.ps1
```

该脚本通过已认证的 GitHub CLI 自动下载私有 r7 Release 压缩包和 SHA256 到仓库根目录并完成校验。随后让 IDEA Deployment 上传整个仓库，确认没有把 `*.tar.gz` 排除。服务器端执行：

```bash
cd /root/forL0
FORL0_OFFLINE_ONLY=true bash ./run-forl0-offline.sh
```

#### 1. 离线前确认服务器前置条件

以下内容不包含在离线包内，必须提前装好：Docker、L0 设备驱动、`libl0mempool.so` 和 `libnuma.so.1`。完整离线包已经包含 Flink 1.20.3、ARM64 CPython 3.10、venv/pip、对应 wheels 和 Java Docker 镜像；目标服务器无需预装 Flink、Python 或宿主机 Java。在目标服务器执行：

```bash
uname -m
test -e /dev/hisi_l0 || test -e /dev/l0
ldconfig -p | grep -E 'libl0mempool\.so|libnuma\.so\.1'
docker version >/dev/null 2>&1 || sudo -n docker version >/dev/null
```

预期架构为 `aarch64`。如果 `$HOME/flink_home/bin/flink` 不存在，安装器会从包内完整分发包自动创建 `$HOME/flink_home`。解压后可用 `./tools/python/bin/python3 --version` 检查包内 Python；启动器会优先使用它，并创建独立的 `cp310` venv。

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

scp $Bundle "$Bundle.sha256" .\run-forl0-offline.sh `
  user@offline-server:~/forL0-state-backend/
```

`run-forl0-offline.sh` 可从同一个 Release 下载。上传完成后只需登录离线服务器执行：

```bash
cd "$HOME/forL0-state-backend"
bash ./run-forl0-offline.sh
```

它会自动校验压缩包、解压到当前代码仓库、校验包内全部文件，并使用默认的 `$HOME/flink_home` 完成安装和启动。根入口无参数时默认执行完整的 Ascend 编号化复现实验矩阵：W01--W02、N01--N14、C01--C08；单项失败会被记录，但不会阻止后续场景，全部场景结束后生成 HTML 报告。若只需较小的合同 apps，可显式使用 `--apps-only`；其他 `forl0-offline-app.sh` 参数也可以直接追加到该命令。

#### 3. 离线服务器解压和双重校验

```bash
set -euo pipefail

cd "$HOME/forL0-state-backend"
sha256sum -c forl0-offline-linux-arm64-py310-20260721.tar.gz.sha256
tar -xzf forl0-offline-linux-arm64-py310-20260721.tar.gz -C "$PWD"

BUNDLE_DIR="$PWD/forl0-offline-linux-arm64-py310-20260721"
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
BUNDLE_DIR="$HOME/forL0-state-backend/forl0-offline-linux-arm64-py310-20260721"
INSTALL_DIR="$HOME/forl0-runtime"

cd "$BUNDLE_DIR"

# 阶段 1：安装 + L0/Flink/Python preflight + 最短 Client smoke
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" --smoke-only

# 阶段 2：WordCount、NexMark、Client 合同口径 apps
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" --apps-only

# 阶段 3（可选）：增加 NexMark throughput pressure 场景
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" --full

# 阶段 4：复现完整 Ascend 编号化性能清单（只产出 raw/log 证据）
bash ./forl0-offline-app.sh --install-dir "$INSTALL_DIR" \
  --skip-docker-load --reproduce-ascend --keep-going --no-report
```

默认要求 2 个 TaskManager、共 8 个 slot。若部署拓扑不同，可显式传入 `--expected-taskmanagers N --expected-slots N`。真实离线 L0 验证不要使用 `--allow-simulation`。

`./reproduce-all` 运行时把本轮内容隔离在
`$HOME/forl0-runtime/benchmark/results/runs/<run_id>/`，其中 `.logs` 是完整持续日志。
smoke 和正式实验都成功后，本轮结果会发布到
`$HOME/forl0-runtime/benchmark/results/latest/`。

另外提供两个互不混淆的采集入口：

```bash
# 只采集硬件、NUMA、DRAM 与分级 L0 标定；不会启动正式 workload
./reproduce-all --profile

# 穷举 162 组参数；每组均执行 W01-W02、N01-N14、C01-C08 全部 24 个 workload
./reproduce-all --full

# 无 L0 的开发机使用最新真实 profile 驱动校准后的搜索模型
./reproduce-all --full --simulate
```

`--profile` 写入 `benchmark/results/profiles/<run_id>/`，`--full` 写入
`benchmark/results/tuning/<run_id>/`，两者的完整输出都在各自目录的 `.logs`。
真实标定会先自动停止遗留 Flink 容器，确保 L0 全局内存池处于空闲状态；如果
L0 不存在、所有分级探针失败或并行 TaskManager 形态探针失败，命令会明确失败，
不会再把 partial 数据打印成 `PROFILE COMPLETE`。真实 `--full` 不会复用失败的
标定结果，并在标定成功后执行 smoke 正确性门禁，通过后才开始搜索。单项失败后继续，重新
执行同一命令会跳过已完成 workload/trial；所有 24 个 workload
都成功的候选才有资格进入真实结果排名。可用
`FORL0_TUNING_MAX_TRIALS=N ./reproduce-all --full` 做有限验证，但这不再是完整穷举。
停止任意模式均使用 `./reproduce-all --stop`。

每轮还会在 Flink 启动前自动生成 `hardware_snapshot.json`、
`dram_calibration.json` 和 `l0_calibration.json`。前者保存 CPU/cache/NUMA、内存、
内核、L0 设备与运行库指纹；后两者测量目标机 DRAM/L0 的工作集延迟、带宽和
1/2/4 worker 扩展曲线。L0 以真实 TaskManager 的独立进程池形态创建至少 64 MiB
vendor tuner：密集延迟/带宽测量限制在已验证的 1 MiB，1/2/4/6 MiB 扩展区间采用与
生产 HotCache 相同的 192 字节 set 稀疏标签/键/值访问，不再整区清零。标定器将
vendor tuner 总容量、密集工作集和 HotSet 活跃容量分开；任一级失败便停止更大的请求。
厂商库崩溃只会形成带 signal/returncode、failure_stage 及最后阶段的诊断 JSON。
这些文件用于在开发机建立性能模型，不包含环境变量、网络配置或认证信息。

实验服务器默认不生成 figure、PDF 或 HTML。复制本轮 `results/runs/<run_id>/`
到分析工作站后，运行 `benchmark/scripts/generate_campaign_analysis.py`，派生物写入
已被 Git 忽略的 `output/`。

`latest/` 中全部是文件，没有子目录；原始层级用文件名中的 `__` 表示，
`UPLOAD_MANIFEST.tsv` 记录扁平文件名与原路径的映射。通过 GitHub 网页上传时，
直接选中 `latest/` 中的全部文件即可。新一轮成功结果会替换上一轮，失败结果不会
覆盖 `latest/`。

复制目标机的 `l0_calibration.json` 后，可在开发机生成本机曲线和校准模型：

```bash
./benchmark/scripts/calibrate-local-l0-model /tmp/local-calibration.json
python3 benchmark/scripts/compare_l0_calibrations.py \
  --target benchmark/results/latest/l0_calibration.json \
  --local /tmp/local-calibration.json \
  --output /tmp/l0-model.json
```

模型输出标记为 `simulation/model`。`./reproduce-all --full --simulate` 会自动选择
`benchmark/results/profiles/` 下最新可用的真实 L0 calibration，与本机 DRAM 曲线生成
`calibration_model.json` 后再搜索；也可用 `FORL0_TARGET_CALIBRATION=/path/to/file`
显式指定。模型适合筛选容量、并行度和热点策略，最终绝对吞吐仍需真实 L0 作业确认。

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

若 Python 与 wheel 不匹配，新版 preflight 会在调用 pip 前直接显示 host 架构、wheel ABI 和选中的解释器。当前 ARM64 发布包要求 Python 3.10，并优先使用 `python3.10`，不会再盲目使用指向其他版本的 `python3`。可在仓库根目录单独检查：

```bash
cd "$HOME/forL0-state-backend"
BUNDLE_DIR="$PWD/forl0-offline-linux-arm64-py310-20260721"
bash "$BUNDLE_DIR/docker/lib/python_wheel_detector.sh" \
  "$BUNDLE_DIR/benchmark/offline-packages"
```

完整 Release 会自动使用包内 `tools/python/bin/python3`，不依赖系统 Python。如需覆盖，可设置 `export FORL0_PYTHON_BIN=/path/to/python3.10`；该解释器仍必须与 cp310 wheels 匹配。

#### 6. 在联网 Linux 构建机重新打包

```bash
cd /path/to/forL0-state-backend
./docker/package_offline_bundle.sh \
  --arch arm64 \
  --python-version 3.10 \
  --output-dir "$PWD/docker/generated/forl0-offline-linux-arm64-py310-20260721"
```

打包脚本会重新编译 JAR/native 产物、下载指定 Linux 架构和 CPython ABI 的 wheels、加入同版本可携带 CPython、完整 Flink 1.20.3 分发包、ARM64 Docker 镜像，并生成包内 manifest/SHA256。不要从旧的 `benchmark/offline-packages/` 手工复制 wheel；旧 wheel可能属于其他 Python ABI。

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
sudo cp src/main/native/libforl0_engine.so /usr/lib/
sudo ldconfig
```

#### 2. 配置 StateBackend

**配置文件方式（推荐）：**

在 `config.yaml` 中添加：

```yaml
state.backend: org.apache.flink.state.forl0.ForL0StateBackendFactory

# ========== L0 Hot-Key Cache (可选) ==========
# total-size 是作业总预算。兼容键 expected-engines 现在表示会创建
# 进程级 L0 manager 的 TaskManager 数量，而不是 slot/算子实例数。
# 正式实验建议打开 strict-allocation，任何硬件降级都会直接使运行失败。
state.backend.forl0.l0-cache.enabled: false
state.backend.forl0.l0-cache.total-size: 20mb
state.backend.forl0.l0-cache.expected-engines: 2
state.backend.forl0.l0-cache.strict-allocation: true
state.backend.forl0.native-memory.max-size: 1gb
```

#### 多并行实例部署时的 L0 预算

L0 的物理上限来自内核模块参数 `max_numa_capacity`（默认 20 MB / NUMA node）。
单台鲲鹏 920 双路有 8 个 NUMA node，整机实际可用 < 100 MB。
**同一 TaskManager 内所有 StateEngine 共享一个 L0 manager/tuner**；不同
TaskManager 再共享设备总预算。这样并行 slot 不会重复占用 tuner 句柄。

runner 会把 `expected-engines` 设为实验拓扑中的 TaskManager 数。backend 使用：

```
per_process_size = total_size / expected_engines
```

手工部署时应按 TaskManager 进程数分配，而不是按 keyed StateBackend instance 数。
`l0-cache.size` 仍作为兼容的“每进程容量”选项保留；新实验应使用
`l0-cache.total-size`。若超分，严格模式会失败；非严格模式会看到：

```
[ForL0-HotCache] WARN: L0 hardware not available (reason: cache_tuner_init ...);
cache forcibly disabled.
```

非严格模式下 ValueState 仍然工作，但该样本不能用于 L0 硬件归因。

**编程方式配置：**

```java
import org.apache.flink.state.forl0.ForL0StateBackend;

// 使用默认配置
ForL0StateBackend stateBackend = new ForL0StateBackend();

env.setStateBackend(stateBackend);
```

自定义参数请通过上面的 Flink 配置项设置。

#### 配置项说明

> 注意：当前 SwissTable 达到增长阈值后执行全表 rehash；没有增量扩容路径。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `state.backend.forl0.l0-cache.enabled` | boolean | `false` | 是否请求开启 L0 Hot-Key Cache |
| `state.backend.forl0.l0-cache.total-size` | MemorySize | 无 | 作业级 L0 总预算，由进程级 manager 均分 |
| `state.backend.forl0.l0-cache.expected-engines` | int | `1` | 兼容键：共享总预算的 TaskManager/进程级 manager 数 |
| `state.backend.forl0.l0-cache.strict-allocation` | boolean | `false` | 分配缩小或硬件不可用时直接失败 |
| `state.backend.forl0.l0-cache.size` | MemorySize | `20mb` | 兼容选项：每个 TaskManager 进程的容量 |
| `state.backend.forl0.l0-cache.state-size` | MemorySize | `1mb` | 每个可缓存 scalar ValueState 的申请额度 |
| `state.backend.forl0.l0-cache.write-bypass-threshold` | long | `1048576` | 连续只写多少次后停止污染 L0；0 表示关闭 |
| `state.backend.forl0.native-memory.max-size` | MemorySize | `0` | 每个 StateEngine 的 SwissTable native 内存上限；0 表示不限 |
| `state.backend.forl0.max-table-capacity` | int | `0` | 每个 SwissTable 的最大容量；0 表示不限 |

#### 3. 验证部署

启动 Flink 后，查看 TaskManager 日志：

```bash
grep -E "ForL0|L0 mode" $FLINK_HOME/log/flink-*-taskexecutor-*.log

# L0 模式成功时显示：
# [ForL0] L0 device detected (/dev/hisi_l0)
# [ForL0] Running in L0 MODE

# L0 不可用或初始化失败时显示 WARN，HotCache 被禁用；StateTable 继续运行：
# [ForL0-HotCache] WARN: L0 hardware not available (...); cache forcibly disabled.
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

### 当前运行时架构

```
Flink state API
       │
       ▼
ForL0KeyedStateBackend ──JNI──▶ native StateEngine
                                      │
                                      ├── key-group / namespace StateTable
                                      │          └── SwissTable (source of truth)
                                      │
                                      ├── optional L0 HotCache (supported hot keys)
                                      │
                                      └── CoW snapshot + checkpoint reader/writer
```

`SwissTable` 通过 `Allocator::allocate_split` 保留分配器扩展点，但生产路径的
`DefaultAllocator` 返回统一分配；只有测试中的 `ForceSplitAllocator` 验证了
ctrl/slots 分离布局。因此该路径目前是 provisional、test-only，不构成生产
机制或论文贡献。当前增长路径会创建更大的表并迁移所有 FULL entries。
HotCache 是 StateTable 上方的独立缓存，不改变这条增长语义。

### 核心组件

| 组件 | 职责 |
|------|------|
| `ForL0StateBackend` | StateBackend 工厂入口 |
| `ForL0KeyedStateBackend` | KeyedStateBackend 实现 |
| `StateEngine` / `StateTable` | native key-group、namespace 路由和状态存储 |
| `SwissTable` | SWAR 并行匹配的哈希表存储层 |
| `HotCacheManager` | StateTable 上方的可选 L0 热点缓存 |
| checkpoint reader/writer | native CoW snapshot 的序列化与恢复 |

## 性能特性

### 查找操作流程 (SWAR 并行匹配)
1. StateEngine 依据 key-group 和 namespace 定位 StateTable。
2. 计算 H1 (`hash >> 7`) 确定 SwissTable 探测起始 group。
3. 加载 ctrl group，并行匹配 H2 (`hash & 0x7F`)。
4. 对匹配的 slot 进行完整 key equality 验证。
5. 未命中时按 triangular probe sequence 继续探测。

### SWAR 算法

当前 native `platform/simd.h` 为 aarch64、x86-64 和 portable fallback
提供 control-group matching，并由 `ProbeSeq` 实现 triangular probing。
论文中应使用 “group probing/matching” 描述；具体 lane 宽度应绑定构建架构，
不应把旧 Java 示例当作当前实现证据。

### 增长与 split 术语边界

- **SwissTable growth**：容量翻倍并全表 rehash。
- **Tombstone reclamation**：必要时按原容量全表 rehash。
- **Allocation split extension**：仅测试分配器覆盖 ctrl/slots 分离；生产
  `DefaultAllocator` 使用统一分配，也不是 hash bucket split。
- **Incremental extendible-hash split**：当前实现不存在，不能作为论文核心机制。

## 运行时 API

### HotCache 可观测性

启用 `forL0.metricsCollector.enabled` 后，backend 通过
`NativeEngine.getHotCacheManagerStats` 注册 manager-level gauges。真实 L0
实验仍必须同时归档 device、runtime library 和 detector 输出；配置项或
backend 名称本身不能证明 L0 硬件已激活。

## 测试

### 运行单元测试

```bash
# 运行 Java/Flink 测试（需要 JDK/Maven）
mvn test

# native mini-gtest 的独立 g++ 命令见：
# research_paper/evidence/native_tests_20260731.txt

# 运行论文 evidence/mechanism gate
python3 research_paper/validate_evidence_index.py
python3 -m unittest research_paper/test_validate_evidence_index.py
```

## 项目状态

### ✅ 已实现功能
- Swiss Tables 架构 (对齐 Go 1.24)
- native control-group 并行匹配与 triangular probing
- 全表 rehash/grow；生产 `DefaultAllocator` 使用统一分配
- JNI native StateEngine
- 可选 L0 HotCache（真实硬件性能仍需独立环境证据）
- Flink StateBackend 集成
- native CoW checkpoint round trip

### 🔄 待优化项
- 配置文件系统
- 动态配置调整
- 更多性能调优选项
- 若经负责人决策恢复：incremental extendible-hash split 与对应消融实验

## 文件结构

```
forL0-state-backend/
├── src/main/
│   ├── java/org/apache/flink/state/forl0/
│   │   ├── ForL0StateBackend.java       # StateBackend 入口
│   │   ├── ForL0KeyedStateBackend.java  # keyed backend 与 native handles
│   │   ├── ForL0SnapshotStrategy.java   # Flink snapshot 集成
│   │   └── NativeEngine.java            # JNI API
│   ├── native/
│   │   ├── engine/swiss_table.h         # native SwissTable
│   │   ├── engine/hot_cache.h           # optional L0 HotCache
│   │   ├── engine/state_engine.h        # StateEngine/StateTable + CoW
│   │   ├── checkpoint/                  # native checkpoint reader/writer
│   │   ├── jni/                         # JNI bridge
│   │   └── test/                        # native correctness tests
│   └── resources/native/
│       └── libforl0_engine.{dylib|so}   # native library
├── research_paper/
│   ├── evidence_index.json              # claim-to-artifact contract
│   └── mechanism_contract.json          # mechanism-to-code contract
├── dev_notes/                           # 开发笔记
├── reference/                           # 参考实现
│   └── go_maps/                          # 非当前主线的参考实现
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

**注意**：没有 L0 设备或运行库时，backend/native table 可以继续运行，但
HotCache 会被禁用。这类运行只能验证对应的软件路径，不能证明真实 L0 的
功能等价性或性能收益。
