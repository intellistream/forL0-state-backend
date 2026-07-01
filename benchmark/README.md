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

### 离线一键运行完整实验并生成报告

联网/构建机先生成离线包：

```bash
cd /path/to/forL0-state-backend
./docker/package_offline_bundle.sh --arch arm64 --output-dir /tmp/forl0-offline
```

离线目标机安装并运行：

```bash
cd /path/to/forl0-offline/docker
./install_offline_bundle.sh --flink-home /path/to/flink-1.20.3 --install-dir ~/forl0-runtime

cd ~/forl0-runtime/docker
./run_all_apps.sh --offline --test apps --backend all --no-profile
```

运行完成后，HTML 报告位于：

```bash
~/forl0-runtime/benchmark/results/reports/benchmark_report.html
```

如果只需要基于已有结果重新生成报告：

```bash
cd ~/forl0-runtime/docker
./run_all_apps.sh --offline --report-only --no-profile
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

# 运行测试并采集火焰图 (需要 Async Profiler)
export ASYNC_PROFILER_HOME=$PWD/tools/async-profiler-4.2.1-macos  # 或 linux-x64/arm64
python scripts/run_wordcount.py --backend all --profile

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
| `--scenario` | WordCount: `contract_baseline`, `stateful_counter`, `high_cardinality`; NexMark: `contract_baseline`, `forl0_optimized`, `forl0_tps_probe`, `forl0_q5_tps_probe` | 无 | 应用 WordCount 或 NexMark 场景配置 |
| `--profile` | `cpu`, `cache`, `uarch`, `memory`, `hotspots` | 关闭 | 启用 profiling |

### Client usecase

```bash
# 运行 client usecase 基准
python scripts/run_benchmark.py --test client_usecase --backend all

# 单独运行 client usecase runner
python scripts/run_client_usecase.py --backend forl0
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

# 运行测试并采集火焰图
python scripts/run_benchmark.py --test nexmark --scenario forl0_tps_probe --backend forl0 --query q18 --profile cpu
```

---

## 火焰图与 CPU Cache 统计

### 安装 Async Profiler

```bash
cd benchmark/tools

# macOS (Apple Silicon / Intel)
curl -LO https://github.com/async-profiler/async-profiler/releases/download/v4.2.1/async-profiler-4.2.1-macos.zip
unzip async-profiler-4.2.1-macos.zip

# Linux x64
curl -LO https://github.com/async-profiler/async-profiler/releases/download/v4.2.1/async-profiler-4.2.1-linux-x64.tar.gz
tar xzf async-profiler-4.2.1-linux-x64.tar.gz

# Linux arm64 (鲲鹏)
curl -LO https://github.com/async-profiler/async-profiler/releases/download/v4.2.1/async-profiler-4.2.1-linux-arm64.tar.gz
tar xzf async-profiler-4.2.1-linux-arm64.tar.gz
```

### 配置环境变量

```bash
# 添加到 ~/.zshrc 或 ~/.bashrc
export ASYNC_PROFILER_HOME=/path/to/benchmark/tools/async-profiler-4.2.1-<platform>
```

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

> ⚠️ **注意**：在 Mac 本地运行时，ForL0 使用模拟模式（无真实 L0 硬件），
> 性能提升不明显甚至可能略低。60% 提升目标仅适用于鲲鹏服务器 + L0 Cache 环境。

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
