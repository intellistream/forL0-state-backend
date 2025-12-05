# ForL0 StateBackend Benchmark

本目录包含用于测试 ForL0 StateBackend 性能的自动化 benchmark 工具，支持：

- ✅ WordCount 滑动窗口基准测试
- ✅ NexMark 流处理标准基准测试
- ✅ HashMapStateBackend vs ForL0StateBackend 自动对比
- ✅ 论文级别的图表和 HTML 报告生成

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
│   └── reports/            # HTML 报告
└── requirements.txt        # Python 依赖
```

---

## 快速开始

### 1. 安装依赖

```bash
cd benchmark

# 安装 Python 依赖
pip install -r requirements.txt
```

### 2. 编译 WordCount Benchmark

```bash
cd wordcount

# 本地运行模式 (包含 Flink 依赖，用于 Mac 测试)
mvn clean package -Plocal -DskipTests

# 集群运行模式 (不包含 Flink 依赖，用于服务器部署)
mvn clean package -Pcluster -DskipTests

cd ..
```

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
| `--test` | `wordcount`, `nexmark`, `all` | `all` | 测试类型 |
| `--backend` | `hashmap`, `forl0`, `all` | `all` | State Backend |
| `--query` | `q4,q5,q8,...` | `all` | NexMark 查询 (仅 nexmark) |

**示例**：

```bash
# 只测试 ForL0 的 WordCount
python scripts/run_benchmark.py --test wordcount --backend forl0

# 运行 NexMark Q5 和 Q8
python scripts/run_benchmark.py --test nexmark --query q5,q8 --backend all
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
    class: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory
    description: "ForL0 StateBackend (L0 缓存优化)"
    # ForL0 专属配置参数
    config:
      l0_cache_enabled: true
      l0_cache_size: 10
      l0_cache_replacement_policy: CLOCK
      l0_memory_max_size: "256mb"
      main_table_initial_size: 10
      main_table_load_factor_threshold: 1.5
```

### 7. ForL0 StateBackend 配置参数

通过 `benchmark.yaml` 中的 `backends[forl0].config` 可以配置 ForL0 StateBackend 的运行参数：

| 配置项 (YAML) | Flink 配置键 | 类型 | 默认值 | 说明 |
|---------------|--------------|------|--------|------|
| `l0_cache_enabled` | `state.backend.forl0.l0-cache.enabled` | Boolean | `true` | 是否启用 L0 热点缓存 |
| `l0_cache_size` | `state.backend.forl0.l0-cache.size` | Integer | `10` | 单个 L0Table 大小（2的幂次，范围 1-20）<br>例如：10 表示 1024 buckets = 64KB |
| `l0_cache_replacement_policy` | `state.backend.forl0.l0-cache.replacement-policy` | String | `CLOCK` | 缓存替换策略 |
| `l0_memory_max_size` | `state.backend.forl0.l0-memory.max-size` | MemorySize | `0` | L0 内存池总容量（0=无限制）<br>例如：256mb, 1gb |
| `main_table_initial_size` | `state.backend.forl0.main-table.initial-size` | Integer | `10` | MainTable 初始大小（2的幂次） |
| `main_table_load_factor_threshold` | `state.backend.forl0.main-table.load-factor-threshold` | Double | `1.5` | MainTable 扩容负载因子阈值 |

**缓存替换策略说明**：

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| `CLOCK` | Clock 算法，1-bit 访问标记 | **推荐**，低开销 |
| `LRU` | Least Recently Used | 访问模式稳定 |
| `LFU` | Least Frequently Used | 长期热点明显 |
| `TINY_LFU` | TinyLFU with decay | 混合工作负载 |
| `SAMPLED_LRU` | 随机采样 + LRU | 轻量级实现 |

**配置示例**：

```yaml
# benchmark.yaml 中的 ForL0 配置
backends:
  - name: forl0
    class: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory
    config:
      # 禁用 L0 缓存（用于对比测试）
      l0_cache_enabled: false
      
      # 增大缓存以提高命中率
      l0_cache_size: 14          # 2^14 = 16384 buckets = 1MB
      
      # 限制 L0 内存使用
      l0_memory_max_size: "512mb"
      
      # 使用 LFU 策略
      l0_cache_replacement_policy: LFU
```

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
│   └── improvement_summary.pdf     # 提升汇总
└── reports/
    └── benchmark_report.html       # 完整 HTML 报告
```

### 报告内容

HTML 报告包含：

1. **Executive Summary** - 测试概要和通过/失败状态
2. **Quick Stats** - 关键指标卡片
3. **WordCount Benchmark** - 配置、吞吐量对比、延迟分布
4. **NexMark Benchmark** - 各查询性能对比表格
5. **Verification Results** - 是否达到 60% 提升目标

---

## 服务器部署

### 1. 准备 JAR

```bash
# 编译 cluster 模式 JAR (不含 Flink 依赖)
cd wordcount
mvn clean package -Pcluster -DskipTests

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

确保使用 `-Plocal` 编译：
```bash
mvn clean package -Plocal -DskipTests
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
