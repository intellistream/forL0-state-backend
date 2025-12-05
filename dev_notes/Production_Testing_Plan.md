# ForL0 State Backend 生产环境测试计划

## 文档信息

| 项目 | 说明 |
|------|------|
| **项目名称** | ForL0 State Backend |
| **文档版本** | v1.0 |
| **创建日期** | 2025年12月4日 |
| **目标环境** | 鲲鹏 CPU 服务器 (L0 模式) |
| **Flink 版本** | 1.20.0 |

---

## 1. 测试概述

### 1.1 测试目标

验证 ForL0 State Backend 在生产环境（鲲鹏服务器 + L0 硬件）中的：

1. **功能正确性**：状态操作的正确性和一致性
2. **性能表现**：延迟、吞吐、每核吞吐等关键指标
3. **稳定性**：长时间运行下的内存稳定性和可靠性
4. **安全性**：内存安全、数据安全等

### 1.2 测试范围

| 测试类型 | 测试内容 | 优先级 |
|----------|----------|--------|
| WordCount 测试 | Sliding Time Window 场景，亿级数据量 | P0 |
| NexMark 测试 | 8条标准 SQL 查询，多种数据规模 | P0 |
| 性能测试 | 微基准测试、压力测试、长稳测试 | P0 |
| 安全测试 | 内存安全、边界检查、故障注入 | P1 |

### 1.3 测试环境

#### 服务器配置

| 配置项 | 规格 |
|--------|------|
| CPU | 鲲鹏 920 高性能版，2路 160核 |
| L0 设备 | `/dev/hisi_l0` 可用 |
| JVM | 毕昇 JDK-8 |

#### 容器部署

| 容器角色 | 数量 | 规格 | 说明 |
|----------|------|------|------|
| JobManager | 1 | 2核 8G | 运行 Flink JM |
| TaskManager | 2 | 4核 16G | 每个容器运行 1 个 TM，并行度 4 |

**部署要求：**
- 所有容器绑定到同一个 NUMA 节点的核上
- 容器间网络通过网桥连接
- 总并行度：2 TM × 4 = 8

#### 软件环境

| 软件 | 版本 |
|------|------|
| 操作系统 | OpenEuler |
| JDK | 毕昇 JDK-8 |
| Flink | 1.20.0 |
| libl0mempool.so | 最新版本 |

---

## 2. WordCount 测试

### 2.1 测试场景

**Sliding Time Window WordCount**

使用滑动时间窗口进行词频统计，模拟真实流处理场景中的高频状态访问模式。

### 2.2 测试配置

| 参数 | 规格 |
|------|------|
| **Key 个数** | 100 万级 (1,000,000+) |
| **数据倾斜度** | 20% - 30% (Zipf 分布, s=1.0~1.2) |
| **数据总量** | 1 亿条 |
| **窗口大小** | 60 秒 |
| **滑动步长** | 10 秒 |
| **Checkpoint 间隔** | 10 秒 |
| **并行度** | 8 (2 TM × 4) |

### 2.3 对比测试

| State Backend | 说明 |
|---------------|------|
| ForL0StateBackend | 本项目实现 |
| HashMapStateBackend | Flink 原生实现（基线） |

### 2.4 测试指标

一次测试运行后，收集以下所有指标：

| 指标 | 说明 |
|------|------|
| **吞吐量 (Throughput)** | 每秒处理的记录数 (records/s) |
| **延迟 (Latency)** | 端到端处理延迟：P50, P95, P99, Max |
| **每核吞吐 (Throughput/Core)** | 吞吐量 / TM 核数 (8核) |
| **Checkpoint 耗时** | 平均/最大检查点完成时间 |
| **状态大小** | 状态存储占用 |
| **L0 命中率** | L0 Cache 命中比例 |

### 2.5 验收标准

| 指标 | 标准 |
|------|------|
| 测试完成 | 无报错，无 OOM |
| **每核吞吐提升** | **ForL0 比 HashMapStateBackend 高 60%** |
| Checkpoint | 正常完成，无失败 |

---

## 3. NexMark 测试

### 3.1 测试场景

NexMark 是流处理系统的标准基准测试，模拟在线拍卖场景。

### 3.2 公共配置

| 配置项 | 值 |
|--------|-----|
| **Checkpoint 间隔** | 10 秒 |
| **并行度** | 8 (2 TM × 4) |

### 3.3 对比测试

| State Backend | 说明 |
|---------------|------|
| ForL0StateBackend | 本项目实现 |
| HashMapStateBackend | Flink 原生实现（基线） |

### 3.4 SQL 查询与数据量

| 查询 | 数据量 | 描述 | 状态特点 |
|------|--------|------|----------|
| **Q8** | 1 亿条 | Monitor New Users | Window Join |
| **Q4** | 8000 万条 | Average Selling Price by Category | Keyed State |
| **Q5** | 8000 万条 | Hot Items | Sliding Window + Keyed State |
| **Q11** | 8000 万条 | User Sessions | Session Window |
| **Q18** | 8000 万条 | Find Last Bid | Keyed State |
| **Q19** | 8000 万条 | Auction Statistics | Multiple Aggregations |
| **Q9** | 4000 万条 | Winning Bids | 复杂 Join |
| **Q20** | 6000 万条 | Expand Bid | Flatmap + State |

### 3.5 测试指标

每个查询运行后，收集以下指标：

| 指标 | 说明 |
|------|------|
| **吞吐量** | records/s |
| **延迟** | P50, P95, P99, Max |
| **每核吞吐** | throughput / TM 核数 (8核) |
| **Checkpoint 耗时** | 平均/最大 |
| **状态大小** | 状态存储占用 |

### 3.6 验收标准

| 指标 | 标准 |
|------|------|
| 所有查询 | 完成无报错 |
| 结果正确性 | 与 HashMapStateBackend 结果一致 |
| **每核吞吐提升** | **ForL0 比 HashMapStateBackend 高 60%** |

---

## 4. 性能测试

### 4.1 微基准测试

使用 JMH 测试各状态操作的延迟和吞吐：

| 测试项 | 说明 |
|--------|------|
| ValueState 读写 | get/update 操作延迟 |
| MapState 读写 | get/put 操作延迟 |
| ListState 读写 | get/add 操作延迟 |
| 混合读写 | 7:3 读写比例 |

### 4.2 L0 Cache 性能测试

| 测试项 | 说明 |
|--------|------|
| 均匀随机访问 | 基线命中率 |
| Zipf 分布访问 | 热点场景命中率 |
| 替换策略对比 | LRU/LFU/CLOCK/TinyLFU/Sampled-LRU |

### 4.3 压力测试

| 测试项 | 配置 | 持续时间 |
|--------|------|----------|
| 高吞吐压力 | 逐步增加负载直到背压 | 1 小时 |
| 大状态压力 | 1000万 Key，每 Key 1KB，总计~10GB | 2 小时 |

### 4.4 长稳测试

| 测试项 | 负载 | 持续时间 |
|--------|------|----------|
| 24小时稳定性 | 70% 最大吞吐 | 24 小时 |
| 72小时极限稳定性 | 90% 最大吞吐 + 故障注入 | 72 小时 |

### 4.5 对比测试

| 对比项 | 测试场景 | 对比指标 |
|--------|----------|----------|
| vs HashMapStateBackend | WordCount, NexMark Q5/Q8 | 吞吐量、延迟、内存使用 |
| L0 模式 vs 模拟模式 | 热键访问、高吞吐场景 | 延迟差异、吞吐差异 |

---

## 5. 安全测试

### 5.1 内存安全测试

| 测试项 | 说明 |
|--------|------|
| 边界检查 | null key/value、超大 value、超长 key |
| 内存泄漏检测 | Native 内存 (Valgrind)、堆外内存 (NMT)、Java 对象 (MAT) |
| 内存越界检测 | AddressSanitizer 检测 |

### 5.2 数据安全测试

| 测试项 | 说明 |
|--------|------|
| 数据完整性 | 写入后读取校验 (CRC32) |
| Checkpoint 恢复 | 恢复后状态数据一致性 |
| 状态隔离 | 不同 key/namespace/state 间的数据隔离 |

### 5.3 故障注入测试

| 故障场景 | 预期行为 |
|----------|----------|
| 内存分配失败 | 抛出明确异常 |
| Native 库加载失败 | 报错或降级 |
| Checkpoint 失败 | 重试或报错 |
| TaskManager 崩溃 | 状态恢复正确 |
| L0 设备不可用 | 自动切换模拟模式 |
| L0 内存耗尽 | 触发驱逐或报错 |

---

## 6. 测试执行计划

### 6.1 测试阶段

| 阶段 | 时间 | 内容 |
|------|------|------|
| **第一阶段** | 第1-2周 | 环境搭建、冒烟测试 |
| **第二阶段** | 第3-4周 | WordCount 测试、NexMark 测试 |
| **第三阶段** | 第5-6周 | 性能测试、对比测试 |
| **第四阶段** | 第7周 | 安全测试 |
| **第五阶段** | 第8周 | 长稳测试、回归测试 |

### 6.2 测试里程碑

| 里程碑 | 时间点 | 交付物 |
|--------|--------|--------|
| M1: 环境就绪 | 第2周末 | 测试环境部署文档 |
| M2: 功能验证完成 | 第4周末 | WordCount + NexMark 测试报告 |
| M3: 性能基线建立 | 第6周末 | 性能测试报告、对比分析 |
| M4: 安全验证完成 | 第7周末 | 安全测试报告 |
| M5: 测试收官 | 第8周末 | 完整测试报告、问题清单 |

---

## 7. 测试报告模板

### 7.1 单项测试报告

| 项目 | 内容 |
|------|------|
| 测试信息 | 日期、人员、环境 |
| 测试配置 | 数据量、并行度、Checkpoint 间隔 |
| 性能指标 | 吞吐量、延迟、每核吞吐 |
| L0 统计 | 命中率、驱逐次数 |
| 问题发现 | 问题描述 |
| 结论 | PASS / FAIL / BLOCKED |

### 7.2 汇总报告

| 项目 | 内容 |
|------|------|
| 概述 | 测试周期、用例统计、通过率 |
| 性能总结 | vs HashMapStateBackend 对比数据 |
| 问题汇总 | ID、严重程度、描述、状态 |
| 风险评估 | 风险项及应对措施 |
| 建议 | 改进建议 |

---

## 8. 附录

### 8.1 监控指标

| 指标 | 说明 |
|------|------|
| `State.ForL0.L0CacheHitRate` | L0 缓存命中率 |
| `State.ForL0.MainTableLoadFactor` | 主表负载因子 |
| `State.ForL0.ExpansionBucketCount` | 扩展桶数量 |
| `State.ForL0.NativeMemoryUsed` | Native 内存使用量 |

### 8.2 问题排查检查清单

- [ ] L0 设备是否正常 (`ls -la /dev/hisi_l0`)
- [ ] Native 库是否加载
- [ ] JVM 参数是否正确
- [ ] Flink 配置是否正确
- [ ] 内存配额是否足够
- [ ] 日志中是否有异常

### 8.3 参考文档

- [ForL0 State Backend 设计说明书](../ForL0-State-Backend设计说明书.md)
- [L0 内存分配设计说明](./L0_Memory_Allocation_Design.md)
- [Apache Flink 官方文档](https://flink.apache.org/docs/)
- [NexMark Benchmark](https://github.com/nexmark/nexmark)

---

## 9. Benchmark 框架（已实现）

### 9.1 框架概述

已在 `benchmark/` 目录下实现完整的自动化测试框架，支持：
- 本地开发测试（Mac）和生产集群测试双模式
- WordCount 和 NexMark 两套基准测试
- 自动化执行和论文级报告生成

### 9.2 目录结构

```
benchmark/
├── config/
│   └── benchmark.yaml           # 配置文件（local/cluster 双模式）
├── lib/
│   └── .gitkeep                 # NexMark JAR 放置目录
├── results/
│   ├── raw/                     # 原始 JSON 结果
│   ├── reports/                 # 生成的报告
│   └── figures/                 # 生成的图表
├── scripts/
│   ├── run_benchmark.py         # 统一入口
│   ├── run_wordcount.py         # WordCount 执行脚本
│   ├── run_nexmark.py           # NexMark 执行脚本
│   ├── generate_report.py       # 报告和图表生成
│   └── utils/
│       ├── config.py            # 配置工具
│       └── flink_client.py      # Flink REST API 客户端
├── wordcount/
│   ├── pom.xml                  # Maven 配置
│   └── src/main/java/org/apache/flink/benchmark/wordcount/
│       ├── WordCountBenchmark.java   # 主程序入口
│       ├── SkewedWordSource.java     # Zipf 分布数据源
│       └── MetricsSink.java          # 指标收集 Sink
└── README.md                    # 使用说明
```

### 9.3 运行模式

| 模式 | 用途 | 数据量 | 运行位置 |
|------|------|--------|----------|
| `local` | Mac 开发测试 | 小规模 (1万 Key, 100万条) | 本地 JVM |
| `cluster` | 生产环境测试 | 全量 (100万 Key, 1亿条) | 鲲鹏服务器 Flink 集群 |

**脚本运行位置**：Python 脚本始终在本地运行，通过 Flink REST API 或 SSH 与集群交互。

### 9.4 WordCount Benchmark

**Java 实现特点**：
- 支持 Zipf 分布生成倾斜数据（20-30% 热点）
- Sliding Window (60s 窗口, 10s 滑动)
- 内置指标收集：吞吐量、延迟 P50/P95/P99
- 输出格式化 JSON，便于自动解析

**命令行参数**：
```
--numKeys           Key 数量
--numRecords        总记录数
--skewFactor        Zipf 倾斜因子 (1.0~1.2)
--windowSize        窗口大小（秒）
--slideSize         滑动步长（秒）
--parallelism       并行度
--checkpointInterval Checkpoint 间隔（毫秒）
```

### 9.5 NexMark Benchmark

使用官方 NexMark JAR，支持 8 条查询：
- Q4, Q5, Q8, Q9, Q11, Q18, Q19, Q20

每条查询的数据量在 `benchmark.yaml` 中独立配置。

### 9.6 报告生成

**论文级图表输出**：
- 吞吐量对比柱状图 (PDF + PNG)
- NexMark 多查询分组柱状图
- 延迟分布箱线图
- 自动计算提升百分比并标注

**使用 Python 库**：
- `matplotlib` + `seaborn`：绑定 serif 字体，300 DPI
- `pandas`：数据处理
- `jinja2`：HTML 报告模板

### 9.7 使用步骤

#### 1) 安装依赖
```bash
cd benchmark
pip install -r requirements.txt
```

#### 2) 编译 WordCount JAR
```bash
cd wordcount
mvn clean package
```

#### 3) 下载 NexMark JAR
```bash
# 下载到 benchmark/lib/nexmark-flink-*.jar
```

#### 4) 运行测试
```bash
# 本地快速验证
python scripts/run_benchmark.py --mode local --test all --backend all

# 生产环境测试（需配置 flink.home）
python scripts/run_benchmark.py --mode cluster --test all --backend all
```

#### 5) 生成报告
```bash
python scripts/generate_report.py
```

输出：
- `results/figures/*.pdf` - 论文图表
- `results/reports/benchmark_report_*.html` - HTML 报告

### 9.8 配置文件示例

`benchmark/config/benchmark.yaml`:

```yaml
mode: local  # 或 cluster

backends:
  - name: hashmap
    class: ""  # 默认
  - name: forl0
    class: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory

local:
  parallelism: 2
  checkpoint_interval: 5000
  wordcount:
    num_keys: 10000
    num_records: 1000000
    skew_factor: 1.1

cluster:
  parallelism: 8
  checkpoint_interval: 10000
  wordcount:
    num_keys: 1000000
    num_records: 100000000
    skew_factor: 1.1
  nexmark:
    q4_events: 80000000
    q5_events: 80000000
    q8_events: 100000000
    # ...
```

### 9.9 待办事项

- [ ] 在本地 Mac 验证 WordCount JAR 编译和运行
- [ ] 下载 NexMark 官方 JAR 到 `lib/`
- [ ] 配置服务器 Flink 环境变量
- [ ] 运行完整 Benchmark 并生成报告
