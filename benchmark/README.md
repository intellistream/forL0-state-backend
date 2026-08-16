# ForL0 StateBackend Benchmark

本目录包含用于测试 ForL0 StateBackend 性能的自动化 benchmark 工具，支持：

- ✅ WordCount 滑动窗口基准测试
- ✅ NexMark 流处理标准基准测试
- ✅ Client usecase (`XX_6000c_Demo`) 状态密集型对比测试
- ✅ HashMapStateBackend vs ForL0StateBackend 自动对比
- ✅ 论文级别的图表和 HTML 报告生成
- ✅ 火焰图采集 (CPU、内存分配，需 Async Profiler)
- ✅ CPU Cache 统计 (仅 Linux，需 perf_events)

## 目录结构

```
benchmark/
├── wordcount/              # WordCount Benchmark (Maven 项目)
│   ├── pom.xml
│   └── src/main/java/...
├── scripts/                # Python 执行脚本
│   ├── run_benchmark.py    # 统一测试入口
│   ├── run_wordcount.py    # WordCount 测试
│   ├── run_nexmark.py      # NexMark 测试
│   ├── generate_report.py  # 报告和图表生成
│   └── utils/              # 工具模块
├── config/
│   └── benchmark.yaml      # 配置文件
├── lib/                    # 外部 JAR (需手动放置 NexMark)
├── results/                # 测试结果 (自动生成)
│   ├── raw/                # JSON 原始数据
│   ├── figures/            # PDF/PNG 图表
│   ├── reports/            # HTML 报告
│   ├── latency/            # 延迟采样数据
│   └── profiles/           # 火焰图 HTML 文件
├── tools/                  # 外部工具 (如 Async Profiler)
├── docs/                   # 设计文档
│   └── Advanced_Metrics_Design.md
└── requirements.txt        # Python 依赖
```

---

## 快速开始

### 离线服务器：scp 仓库后一键运行

推荐直接把整个仓库目录拷贝到离线服务器，然后在仓库内运行一键脚本。仓库需要随包携带以下内容：

- `docker/deploy/`：ForL0 backend JAR、native 依赖可从源码目录安装、WordCount / NexMark / Client usecase 预编译 JAR。
- `benchmark/offline-packages/`：Python 离线 wheels。
- `docker/run_all_apps.sh` 与 `docker/server_setup.sh`：部署与运行入口。
- `docker/images/eclipse-temurin-8-jre.tar.gz`：可选。若目标机已存在 `eclipse-temurin:8-jre` 镜像，或运行时切换到本机 Flink standalone，可不依赖该镜像包。

**1. 拷贝仓库**

```bash
scp -r /path/to/forL0-state-backend user@server:/path/to/
ssh user@server
cd /path/to/forL0-state-backend
```

**2. 指定 Flink**

```bash
export FLINK_HOME="$HOME/flink_home"
```

默认使用 `~/flink_home`。部署脚本也会兼容查找 `~/flink-1.20.3`、`~/flink`、`/opt/flink` 或 `/usr/local/flink`。

**3. 离线目标机一键完整实验**

默认入口是面向离线 L0 服务器的一键脚本。它会先安装并 preflight，然后完整运行两遍 `apps`：第一遍关闭 profiling 且不生成报告，第二遍开启 CPU profiling 并生成 HTML 报告，最后校验报告确实引用最新 profiled raw/profile 产物。脚本结束后不会停止 Flink 集群，便于继续查看 Web UI 和容器日志。

```bash
cd /path/to/forL0-state-backend/docker
./run_offline_l0_experiment.sh --flink-home "$FLINK_HOME"
```

注意：默认 `apps` 是合同级全量实验，包含 100M WordCount、8 个 NexMark stateful query（每个 40M--100M events）和 Client Usecase，并且会跑无 profile 与有 profile 两遍；在非真实 L0 硬件或 Docker fallback 环境中可能需要数小时。若目标机缺少 `/dev/l0`、`/dev/hisi_l0` 或 `libl0mempool.so`，结果只能用于离线链路/脚本验收，不应作为 L0 性能目标验收。若只做离线链路验收，可显式缩小范围，例如：

```bash
# 只验收 Client Usecase 两遍 + profile + report
./run_offline_l0_experiment.sh --flink-home "$FLINK_HOME" \
  --test client_usecase --scenario contract_baseline

# 只验收 NexMark q4 两遍 + profile + report
./run_offline_l0_experiment.sh --flink-home "$FLINK_HOME" \
  --test nexmark --query q4
```

`./reproduce-all` 的运行中产物位于
`benchmark/results/runs/<run_id>/`，完整持续日志是该目录下的 `.logs`。
成功后，所有产物会发布到 `benchmark/results/latest/`；该目录完全扁平，只有文件，
因此可直接从 GitHub 网页一次选中上传。文件名中的 `__` 表示原目录分隔，
`UPLOAD_MANIFEST.tsv` 给出原路径映射，完整日志会发布为 `campaign.log`。
`reproduce-all` 不在实验服务器生成 figure、PDF 或 HTML；服务器只负责 raw、
NexMark JSON、失败证据和日志。派生分析在复制结果后的工作站生成，并由根目录
`.gitignore` 排除：

```bash
python benchmark/scripts/generate_campaign_analysis.py \
  --campaign benchmark/results/runs/<run_id> --output output
```

新一轮成功结果会原子替换旧 `latest/` 并删除 staging；失败轮次不会污染
`latest/`，诊断信息会暂留在唯一的 `runs/<run_id>/` 中，下一次启动时清理。

L0 硬件归因使用三路一键消融（HashMap / ForL0-L0-off / ForL0-L0-on）：

```bash
./reproduce-l0-ablation
```

L0-on 路径强制严格分配，并将每个 TaskManager 的 `engine_start`、state/engine
summary、native 内存峰值保存在本轮 scoped results 中；缺少激活证据会使脚本失败。

如只想先做依赖检查，可运行：

```bash
cd /path/to/forL0-state-backend/docker
./server_setup.sh --flink-home "$FLINK_HOME" --no-start
./run_all_apps.sh --offline --preflight-only --test apps --backend all
```

`server_setup.sh --no-start` 会把 ForL0 backend JAR 安装到 `$FLINK_HOME/lib/`，把 native 库安装到 `$FLINK_HOME/native/`，并写出 `docker/forl0-local.env`。`preflight-only` 只检查依赖、JAR、native 库、Python 离线包和配置，不运行长实验。

**4. Smoke test**

```bash
cd /path/to/forL0-state-backend/docker
./run_all_apps.sh \
  --offline \
  --test client_usecase \
  --scenario contract_baseline \
  --backend all \
  --no-profile \
  --no-report
```

smoke test 用于确认 Flink 提交、StateBackend 加载和 raw 结果写入路径正常。

**5. 正式复跑**

完整应用套件：

```bash
cd /path/to/forL0-state-backend/docker
./run_all_apps.sh --offline --test apps --backend all --no-profile
```

NexMark 无 Full GC 压力复跑重点场景：

```bash
cd /path/to/forL0-state-backend/docker
./run_all_apps.sh \
  --offline \
  --test nexmark \
  --scenario forl0_no_full_gc_allq_pressure \
  --backend all \
  --query q4,q5,q8,q9,q11,q18,q19,q20 \
  --no-profile
```

合同 Base 用例带 Full GC 阈值保护：

```bash
cd /path/to/forL0-state-backend/docker
./run_all_apps.sh \
  --offline \
  --test nexmark \
  --scenario contract_baseline_gc_guard \
  --backend all \
  --query q4,q5,q8,q9,q11,q18,q19,q20 \
  --no-profile
```

只有显式请求 `--report-only` 时，低层工具才在当前机器生成旧版 HTML。正式
`./reproduce-all` 不调用它。只基于已有结果重新生成旧版 HTML：

```bash
cd /path/to/forL0-state-backend/docker
./run_all_apps.sh --offline --report-only --no-profile
```

### 可选：生成独立离线包

如果不想拷贝整个仓库，也可以在联网/构建机生成独立目录：

```bash
cd /path/to/forL0-state-backend
./docker/package_offline_bundle.sh --arch arm64 --output-dir /tmp/forl0-offline
```

然后将 `/tmp/forl0-offline/` 拷贝到离线服务器并执行：

```bash
cd /path/to/forl0-offline/docker
./install_offline_bundle.sh --flink-home "$HOME/flink_home" --install-dir ~/forl0-runtime
cd ~/forl0-runtime/docker
./run_all_apps.sh --offline --preflight-only --test apps --backend all
```

### 1. 安装依赖

```bash
cd benchmark

# 安装 Python 依赖
pip install -r requirements.txt
```

### 2. 编译 WordCount Benchmark

```bash
cd wordcount
mvn clean package -DskipTests
cd ..
```

> 💡 无论本地还是服务器，都通过 `flink run` 向集群提交作业，Flink 依赖由集群提供。

### 3. 启动 Flink 集群

```bash
# 启动本地 Flink 集群
$FLINK_HOME/bin/start-cluster.sh

# 检查集群状态
curl http://localhost:8081/overview
```

### 4. 运行测试

```bash
# 运行完整对比测试 (WordCount: hashmap vs forl0)
python scripts/run_benchmark.py --test wordcount --backend all

# 运行测试并采集火焰图。离线目标机推荐使用 docker/run_offline_l0_experiment.sh；
# 若手工运行，可使用随仓库携带的 async-profiler 4.4 Linux arm64 离线包。
mkdir -p tools
tar -xzf offline-packages/async-profiler-4.4-linux-arm64.tar.gz -C tools
export ASYNC_PROFILER_HOME=$PWD/tools/async-profiler-4.4-linux-arm64
python scripts/run_wordcount.py --backend all --profile cpu

# 生成报告和图表
python scripts/generate_report.py
```

报告会自动在浏览器中打开，位于 `results/reports/benchmark_report.html`。

### 5. 停止 Flink 集群

```bash
$FLINK_HOME/bin/stop-cluster.sh
```

---

## 命令行参数

### run_benchmark.py

```bash
python scripts/run_benchmark.py [OPTIONS]
```

| 参数 | 可选值 | 默认值 | 说明 |
|------|--------|--------|------|
| `--test` | `unittest`, `wordcount`, `nexmark`, `client_usecase`, `benchset`, `all` | `all` | 测试类型 |
| `--backend` | `hashmap`, `forl0`, `all` | `all` | State Backend |
| `--query` | `q4,q5,q8,...` | `all` | NexMark 查询 (仅 nexmark) |
| `--scenario` | WordCount: `contract_baseline`, `stateful_counter`, `high_cardinality`; NexMark: `contract_baseline`, `contract_baseline_gc_guard`, `forl0_optimized`, `forl0_tps_probe`, `forl0_q5_tps_probe`, `forl0_no_full_gc_promising`, `forl0_q4_no_full_gc_auction_heavy`, `forl0_no_full_gc_pressure`, `forl0_no_full_gc_allq_pressure`, `forl0_no_full_gc_q8_q11_tuned`, `forl0_no_full_gc_q8_q11_deep`, `forl0_no_full_gc_lateq_deep`, `forl0_no_full_gc_extra_sql`; Client: `contract_baseline`, `forl0_optimized`, `state_pressure_300k`, `state_pressure_1m`, `hotspot_drift_300k`, `hotspot_drift_1m`, `hotspot_state_left_2m`, `hotspot_state_join_2m` | 无 | 应用场景配置 |
| `--profile` | `cpu`, `cache`, `uarch`, `memory`, `hotspots` | 关闭 | 启用 profiling |

### Client usecase

```bash
# 运行 client usecase 基准
python scripts/run_benchmark.py --test client_usecase --backend all

# 单独运行 client usecase runner
python scripts/run_client_usecase.py --backend forl0

# 非合同项：30 万记录状态压力场景
../docker/run_all_apps.sh --test client_usecase --scenario state_pressure_300k --backend all --no-profile

# 非合同项：100 万记录状态压力场景
../docker/run_all_apps.sh --test client_usecase --scenario state_pressure_1m --backend all --no-profile

# 非合同项：30 万记录热点漂移状态压力场景
../docker/run_all_apps.sh --test client_usecase --scenario hotspot_drift_300k --backend all --no-profile

# 非合同项：100 万记录热点漂移状态压力场景
../docker/run_all_apps.sh --test client_usecase --scenario hotspot_drift_1m --backend all --no-profile

# 探索诊断项：轻量 payload + eventTimeStamp 热桶，直接观察客户 MapState 状态路径
../docker/run_all_apps.sh --test client_usecase --scenario hotspot_state_left_2m --backend all --no-profile
../docker/run_all_apps.sh --test client_usecase --scenario hotspot_state_join_2m --backend all --no-profile
```

在首次运行前需要先打包该 usecase：

```bash
cd client_usecase/XX_6000c_Demo
mvn clean package -DskipTests
```

如果服务器不能打包，可将生成的
`flink-keyedcoprocessfunction-example-*-jar-with-dependencies.jar`
放入仓库的 `docker/deploy/`，脚本会像 `wordcount` 一样优先使用该预构建产物。

`client_usecase` 只需要在 `benchmark.yaml` 里配置 `num_records`。
并行度和 checkpoint 继续复用全局 `runtime.parallelism` 与 `runtime.checkpoint_interval`。
`hotspot_drift_*` 场景会使用 `benchmark/client-drift` 中的非合同 driver，保留客户原有
`HuaweiTestFunction` 状态逻辑，只把 CSV 回放 source 替换为漂移热点 key 生成器；离线一键打包脚本会自动编译并携带
`client-drift-benchmark-*.jar`。

**示例**：

```bash
# 只测试 ForL0 的 WordCount
python scripts/run_benchmark.py --test wordcount --backend forl0

# 运行 NexMark Q5 和 Q8
python scripts/run_benchmark.py --test nexmark --query q5,q8 --backend all

# 非合同项：NexMark 逐查询稳态 TPS 模式，使用 sink 输入 TPS 观察端到端完成吞吐
# 为避免 cancel 残留影响，建议 HashMap / ForL0 分开跑，并在两次之间重启 Flink。
../docker/run_all_apps.sh --test nexmark --scenario forl0_tps_probe --backend hashmap --query q18 --no-profile
../docker/run_all_apps.sh --test nexmark --scenario forl0_tps_probe --backend forl0 --query q18 --no-profile
../docker/run_all_apps.sh --test nexmark --scenario forl0_tps_probe --backend hashmap --query q5 --no-profile
../docker/run_all_apps.sh --test nexmark --scenario forl0_tps_probe --backend forl0 --query q5 --no-profile

# 非合同项：NexMark Q18 漂移热点场景，用于观察 HotCache 指标与 sink TPS
../docker/run_all_apps.sh --test nexmark --scenario forl0_q18_l0_hotspot --backend hashmap --query q18 --no-profile
../docker/run_all_apps.sh --test nexmark --scenario forl0_q18_l0_hotspot --backend forl0 --query q18 --profile cpu

# 非合同项：NexMark promising 查询无 Full GC 复跑
../docker/run_all_apps.sh --test nexmark --scenario forl0_no_full_gc_promising --backend all --query q4,q18,q19,q20 --no-profile

# 非合同项：NexMark Q4 auction-heavy，无 Full GC 接受规则
../docker/run_all_apps.sh --test nexmark --scenario forl0_q4_no_full_gc_auction_heavy --backend all --query q4 --no-profile

# 非合同项：NexMark 无 Full GC 压力档，保持高状态压力并拒收 Full GC 样本
../docker/run_all_apps.sh --test nexmark --scenario forl0_no_full_gc_pressure --backend all --query q4,q18,q19 --no-profile

# 非合同项：NexMark 全查询无 Full GC 压力扫描
../docker/run_all_apps.sh --test nexmark --scenario forl0_no_full_gc_allq_pressure --backend all --query q4,q5,q8,q9,q11,q18,q19,q20 --no-profile

# 非合同项：NexMark q8/q11 边界查询重试配置
../docker/run_all_apps.sh --test nexmark --scenario forl0_no_full_gc_q8_q11_tuned --backend all --query q8,q11 --no-profile

# 非合同项：NexMark q8/q11 深调配置（最终 no-Full-GC 表使用 q8/q11 样本）
../docker/run_all_apps.sh --test nexmark --scenario forl0_no_full_gc_q8_q11_deep --backend all --query q8,q11 --no-profile

# 非合同项：NexMark q18/q19/q20 深调配置（最终 no-Full-GC 表使用 q18/q19/q20 样本）
../docker/run_all_apps.sh --test nexmark --scenario forl0_no_full_gc_lateq_deep --backend all --query q18,q19,q20 --no-profile

# 合同 Base 用例：带 Full GC 阈值保护，样本 Full GC delta 需 <= 5
../docker/run_all_apps.sh --test nexmark --scenario contract_baseline_gc_guard --backend all --query q4,q5,q8,q9,q11,q18,q19,q20 --no-profile

# 非合同项：NexMark 其他 SQL 扫描，严格拒收 Full GC 样本
../docker/run_all_apps.sh --test nexmark --scenario forl0_no_full_gc_extra_sql --backend all --query q0,q1,q2,q3,q6,q7,q10,q12,q13,q14,q15,q16,q17,q21,q22 --no-profile

# 运行测试并采集火焰图
python scripts/run_benchmark.py --test nexmark --scenario forl0_tps_probe --backend forl0 --query q18 --profile cpu
```

---

## 火焰图与 CPU Cache 统计

### Async Profiler 离线配置

离线目标机默认不需要联网安装 profiler。仓库随包携带：

```bash
benchmark/offline-packages/async-profiler-4.4-linux-arm64.tar.gz
```

`docker/run_offline_l0_experiment.sh` 会在 profile pass 前自动解压到 `tools/` 并设置 `ASYNC_PROFILER_HOME`。如果需要手工运行，可执行：

```bash
cd benchmark
mkdir -p ../tools
tar -xzf offline-packages/async-profiler-4.4-linux-arm64.tar.gz -C ../tools
export ASYNC_PROFILER_HOME=$(readlink -f ../tools/async-profiler-4.4-linux-arm64)
```

容器化 Flink 场景下，host attach 可能因权限失败；runner 会把 profiler 复制到 TaskManager 容器内并用容器 fallback 采集 flame graph。

### 平台支持

| 功能 | macOS | Linux | 说明 |
|------|-------|-------|------|
| CPU 火焰图 | ✅ (itimer) | ✅ (perf_events) | macOS 精度略低 |
| 内存分配火焰图 | ✅ | ✅ | alloc 事件 |
| Wall-clock 火焰图 | ✅ | ✅ | 包含阻塞时间 |
| cache-misses | ❌ | ✅ | 需要 perf_events |
| L1-dcache-load-misses | ❌ | ✅ | 需要 perf_events |
| LLC-load-misses | ❌ | ✅ | 需要 perf_events |

> ⚠️ **注意**：CPU Cache 统计功能仅在 Linux 上可用，macOS 不支持 perf_events 硬件计数器。

### 采集火焰图

```bash
# 运行测试时自动采集
python scripts/run_wordcount.py --backend all --profile

# 火焰图保存在 results/profiles/ 目录
ls results/profiles/
# flamegraph_itimer_hashmap_20251210_143000.html
# flamegraph_itimer_forl0_20251210_143100.html
# flamegraph_alloc_forl0_20251210_143100.html
```

---

## 配置文件详解

配置文件位于 `config/benchmark.yaml`，主要分为以下几个部分：

### 1. 运行模式

```yaml
# 切换运行模式：local (Mac 开发) 或 cluster (服务器生产)
mode: local
```

### 2. Flink 集群配置

```yaml
flink:
  home: ${FLINK_HOME}              # Flink 安装目录
  rest_url: "http://localhost:8081" # Flink REST API 地址
```

### 3. WordCount 配置

```yaml
local:
  parallelism: 2              # 并行度
  checkpoint_interval: 10000  # Checkpoint 间隔 (10s)
  
  wordcount:
    num_keys: 1000000         # Key 数量 (100万)
    num_records: 100000000    # 总记录数 (1亿)
    arrival_rate: 230000      # 数据到达率 (records/s, 0=无限制)
    skew_factor: 1.1          # Zipf 倾斜因子 (1.0~1.2)
    window_size: 5            # 窗口大小 (秒)
    slide_size: 200           # 滑动步长 (毫秒, 0.2s)

cluster:
  parallelism: 8              # 2 TM × 4 slots
  checkpoint_interval: 10000  # 10秒
  
  wordcount:
    num_keys: 1000000         # Key 数量 (100万)
    num_records: 100000000    # 总记录数 (1亿)
    arrival_rate: 230000      # 数据到达率 (230k records/s)
    skew_factor: 1.1          # Zipf 倾斜因子
    window_size: 5            # 窗口大小 (秒)
    slide_size: 200           # 滑动步长 (毫秒, 0.2s)
```

### 4. WordCount 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `num_keys` | 不同 Key 的数量 | 100万 |
| `num_records` | 总记录数上限 | 1亿 |
| `arrival_rate` | 数据到达率 (records/s)，0=无限制 | 230000 (230k/s) |
| `skew_factor` | Zipf 分布的 s 参数，越大越倾斜 | 1.1 (~25% 热点) |
| `window_size` | 滑动窗口大小 (秒) | 5 |
| `slide_size` | 滑动步长 (**毫秒**) | 200 (0.2s) |
| `checkpoint_interval` | Checkpoint 间隔 (毫秒) | 10000 (10s) |

> ⚠️ **注意**：`slide_size` 单位是**毫秒**，不是秒！设置 200 表示 0.2 秒。

### 5. NexMark 配置

```yaml
cluster:
  nexmark:
    q4_events: 80000000       # 8000万
    q5_events: 80000000       # 8000万
    q8_events: 100000000      # 1亿
    q9_events: 40000000       # 4000万
    q11_events: 80000000      # 8000万
    q18_events: 80000000      # 8000万
    q19_events: 80000000      # 8000万
    q20_events: 60000000      # 6000万
```

### 6. StateBackend 配置

```yaml
backends:
  - name: hashmap
    class: org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory
    description: "Flink 原生 HashMapStateBackend (基线)"
  
  - name: forl0
    class: org.apache.flink.state.forl0.ForL0StateBackendFactory
    description: "ForL0 StateBackend (SwissTable 架构)"
```

### 7. ForL0 StateBackend 配置参数

ForL0 StateBackend 使用 Go 1.24 风格的 SwissMap 架构，不再需要额外的配置参数。

主要特性：
- **SWAR 并行匹配**：使用 64 字节对齐的控制字节进行高效查找
- **87.5% 负载因子**：高空间利用率
- **渐进式扩容**：通过 Directory 分裂表，避免全量 rehash

---

## 测试场景

### WordCount Benchmark

**Sliding Time Window WordCount**：使用滑动时间窗口进行词频统计。

| 指标 | 说明 |
|------|------|
| 吞吐量 (Throughput) | records/s |
| 每核吞吐 (Throughput/Core) | records/s/core |
| 延迟 (Latency) | P50, P95, P99, Max (ms) |

**数据特点**：
- Zipf 分布生成倾斜数据
- 20%~30% 的数据集中在少数热点 Key

### NexMark Benchmark

NexMark 是流处理系统的标准基准测试，模拟在线拍卖场景。

| 查询 | 数据量 | 说明 | 状态特点 |
|------|--------|------|----------|
| Q4 | 8000万 | Average Selling Price | Keyed State |
| Q5 | 8000万 | Hot Items | Sliding Window |
| Q8 | 1亿 | Monitor New Users | Window Join |
| Q9 | 4000万 | Winning Bids | 复杂 Join |
| Q11 | 8000万 | User Sessions | Session Window |
| Q18 | 8000万 | Find Last Bid | Keyed State |
| Q19 | 8000万 | Auction Statistics | Multiple Aggregations |
| Q20 | 6000万 | Expand Bid | Flatmap + State |

Q18 的非合同项 L0 热点场景使用 `forl0_q18_l0_hotspot`。该场景将事件比例调为
Person:Auction:Bid = 1:9:90，并设置 `bid.hot-ratio.auctions=16`、
`bid.hot-ratio.bidders=16`、`auction.hot-ratio.sellers=8`。NexMark 生成器会把热
bidder/auction 绑定到最近一批 id，因此 Q18 的 `(bidder, auction)` 去重状态会形成
短期高复用、长期随时间漂移的热工作集，适合在目标 L0 服务器上观察
`forl0.hotcache.active/lookups/hits` 与端到端吞吐提升。验收时需要同时确认
`active=1` 且 `hits/lookups` 非零；如果命中为 0，说明该 SQL 的物理状态没有落到
当前 HotCache 支持的标量 `ValueState` 路径，应改用 Q18 的标量 last-bid 变体继续验证。

---

## 输出结果

### 目录结构

```
results/
├── raw/                    # JSON 原始数据
│   ├── wordcount_hashmap_local_20251205_114622.json
│   └── wordcount_forl0_local_20251205_114626.json
├── figures/                # 图表
│   ├── wordcount_throughput.pdf    # 吞吐量对比
│   ├── latency_comparison.pdf      # 延迟对比
│   ├── latency_cdf.pdf             # 延迟 CDF 分布
│   └── improvement_summary.pdf     # 提升汇总
├── latency/                # 延迟采样数据
│   └── latency_samples_*.csv
├── profiles/               # 火焰图 HTML
│   ├── flamegraph_itimer_hashmap_*.html
│   ├── flamegraph_itimer_forl0_*.html
│   └── flamegraph_alloc_forl0_*.html
└── reports/
    └── benchmark_report.html       # 完整 HTML 报告
```

### 报告内容

HTML 报告包含：

1. **Executive Summary** - 测试概要和通过/失败状态
2. **Quick Stats** - 关键指标卡片
3. **WordCount Benchmark** - 配置、吞吐量对比、延迟分布、CDF 图
4. **NexMark Benchmark** - 各查询性能对比表格
5. **L0Table Metrics** - 命中率、时序图、缓存对比图 (仅 ForL0)
6. **Performance Profiling** - 火焰图链接 (如果使用 --profile)
7. **Verification Results** - 是否达到 60% 提升目标

---

## 服务器部署

### 1. 准备 JAR

```bash
# 编译 JAR
cd wordcount
mvn clean package -DskipTests

# 将 JAR 上传到服务器
scp target/wordcount-benchmark-1.0-SNAPSHOT.jar user@server:/path/to/
```

### 2. 下载 NexMark

从 [NexMark GitHub](https://github.com/nexmark/nexmark) 下载 JAR，放入 `lib/` 目录。

### 3. 配置服务器

修改 `config/benchmark.yaml`：

```yaml
mode: cluster

flink:
  home: /opt/flink-1.20.0
  rest_url: "http://server-ip:8081"
```

### 4. 运行测试

```bash
# 在本地运行脚本，通过 REST API 提交作业到服务器
python scripts/run_benchmark.py --mode cluster --test all --backend all

# 生成报告
python scripts/generate_report.py
```

---

## 验收标准

| 指标 | 目标 |
|------|------|
| 每核吞吐提升 | ForL0 比 HashMapStateBackend **≥ 60%** |
| 测试完成 | 无报错，无 OOM |
| Checkpoint | 正常完成 |

> ⚠️ **注意**：在 Mac 本地运行时主要用于验证流程和报告生成；
> 性能结果请以鲲鹏服务器 + L0 Cache 环境为准。

---

## 常见问题

### Q: 本地运行报 ClassNotFoundException？

检查 ForL0 StateBackend JAR 是否已正确编译并放置在 Flink 的 lib 目录下：
```bash
# 在项目根目录编译
mvn clean package -DskipTests

# 复制到 Flink lib 目录
cp target/flink-statebackend-forL0-*.jar $FLINK_HOME/lib/
```

### Q: 延迟显示为 N/A？

检查测试是否正常完成，查看 `results/raw/` 下的 JSON 文件。

### Q: 如何只重新生成报告？

```bash
python scripts/generate_report.py
```
会读取 `results/raw/` 下的 JSON 文件重新生成报告和图表。

### Q: 如何清理历史结果？

```bash
rm -rf results/raw/*.json results/figures/* results/reports/*
```
