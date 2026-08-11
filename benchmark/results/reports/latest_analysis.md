# ForL0 最新性能实验分析

报告日期：2026-08-11

实验批次：2026-08-11 15:05–15:22（W01–W02、N01–N14、C01–C08）

报告类型：`derived-artifact`（从已提交原始结果重新核算，不代表重新运行实验）

## 结论摘要

本轮 24 个编号 workload 中，12 个产生了可分析结果，12 个 NexMark
workload 在 SQL 提交前失败。有效结果支持以下有限结论：

- WordCount 按脚本的 `best-of-3` 选择值计算，ForL0 比 HashMap 高
  **11.10%**；但三次样本中位数只高 **0.015%**，且 ForL0 样本波动明显，
  因此不能把 11.10% 当作稳定加速结论。
- NexMark q3 单次配对中，ForL0 总吞吐高 **12.50%**，驱动报告的
  throughput/core 高 **86.49%**，两端 Full GC delta 都为 0。该结果只有一对
  job，属于值得复验的信号。
- Client use case 的前三组 CSV workload 基本持平（**+0.39%**、
  **+0.13%**、**-0.04%**）。2M scalar-state/64-op 场景中 ForL0 为
  **1.996×**（+99.61%），但它是单次结果，且 4.03 s/8.05 s 完成时间受到
  约 4 秒轮询粒度影响，暂不能作为论文级 2× 加速结论。
- N01–N12 不是性能负结果。它们因旧 Java 8 驱动忽略 `--category forl0`
  而在 workload 查找阶段失败，未提交 SQL job，必须从性能统计中排除。
- 本批次没有记录足以证明真实 L0 硬件启用的 device/runtime identity。
  这些数据只能作为 ForL0 backend-path 观测，不能支持“L0 硬件加速”表述。

## 证据范围与有效性

| 项目 | 状态 | 说明 |
|---|---:|---|
| W01–W02 WordCount | 有效但不稳定 | 两端各 3 个样本，脚本采用 best-of-3 |
| N01–N12 NexMark q18/q19/q20/q9/q4 | 无效 | workload category 异常，SQL 提交前失败 |
| N13–N14 NexMark q3 | 有效、单次 | OA category 不受旧驱动缺陷影响 |
| C01–C08 Client use case | 有效、单次 | 场景名由 manifest、配置和运行顺序还原 |
| 真实 L0 归因 | 不满足 | 缺少设备、运行库和激活状态的同批次证明 |

工作负载定义来自
[ascend_reproduction_20260811_150507.tsv](../run_logs/ascend_reproduction_20260811_150507.tsv)，
完整执行信息在 [.logs](../.logs)。本轮发生在 run-ID 和 scenario-qualified
文件名修复生效之前，因此本报告显式列举输入文件，不使用 legacy-unscoped 的
自动聚合结果。

## WordCount：选中值有提升，中位数基本持平

场景：`stateful_counter_p4_probe`，2M Long keys、200M records、parallelism 4。

| Backend | 三次吞吐（records/s） | best-of-3 | 中位数 | 样本 CV |
|---|---:|---:|---:|---:|
| HashMap | 4,981,663 / 4,984,893 / 4,986,067 | 4,986,067 | 4,984,893 | 0.046% |
| ForL0 | 4,529,819 / 4,985,623 / 5,539,412 | 5,539,412 | 4,985,623 | 10.07% |

- best-of-3 delta：**+11.10%**。
- median delta：**+0.015%**。
- ForL0 三次结果持续上升，而 HashMap 已基本稳定。现有数据无法区分这是
  warm-up、缓存状态、运行顺序还是稳定性能差异。

因此，最新证据应表述为：**best-of-3 观察到 11.10% 的峰值优势，但稳健中心值
仍接近平局，需要交替顺序的独立 job 重复实验确认。**

原始数据：
[HashMap](../raw/wordcount_hashmap_20260811_150723.json)、
[ForL0](../raw/wordcount_forl0_20260811_150942.json)。

## NexMark：q3 有效，其余 12 项无效

### 有效配对 N13–N14

场景：`forl0_no_full_gc_extra_sql`，q3，configured TPS 1,000,000，
parallelism 4，单次 job。

| 指标 | HashMap | ForL0 | ForL0 相对变化 |
|---|---:|---:|---:|
| Throughput（events/s） | 567,380 | 638,310 | **+12.50%** |
| 驱动报告 CPU 指标 | 15.40 | 9.29 | **-39.68%** |
| Throughput/core | 36,843 | 68,709 | **+86.49%** |
| Full GC delta | 0 | 0 | 相同 |

这里的 CPU 和 throughput/core 是 NexMark 驱动报告值，不应扩展解释为物理核心
占用或硬件能效。由于只有一对 job，本结果适合用于提出“q3 可能降低 backend
CPU 开销”的待验证假设，不适合直接形成通用 NexMark 加速结论。

原始数据：
[HashMap](../nexmark_20260811_151301/nexmark_results.json)、
[ForL0](../nexmark_20260811_151442/nexmark_results.json)。

### 无效配对 N01–N12

| Workload | Query/场景 | 结果 |
|---|---|---|
| N01–N04 | q18 / TPS probe、lateq deep | workload 未定义，未提交 SQL |
| N05–N06 | q19 / TPS probe | workload 未定义，未提交 SQL |
| N07–N08 | q20 / lateq deep | workload 未定义，未提交 SQL |
| N09–N10 | q9 / all-query pressure | workload 未定义，未提交 SQL |
| N11–N12 | q4 / pressure | workload 未定义，未提交 SQL |

共同异常为 `IllegalArgumentException: The workload of query qX is not defined`。
根因已由提交 `1ec7838` 修复：运行器为 custom category 创建隔离的兼容目录视图，
同时保留 `queries-forl0` 专用 SQL。修复改变的是未来运行路径，不会让这些旧失败
结果变成有效数据。

## Client use case：三组持平，一组强信号

下表排除了正式批次之前 15:04 的 smoke pair；C01–C08 按 manifest 顺序、driver、
records、parallelism 和 scalar shape 配对。

| Workload | 配置 | HashMap（records/s） | ForL0（records/s） | Delta |
|---|---|---:|---:|---:|
| C01–C02 | contract, CSV, 300, p2 | 74.07 | 74.36 | **+0.39%** |
| C03–C04 | optimized, CSV, 3,000, p2 | 742.26 | 743.22 | **+0.13%** |
| C05–C06 | pressure, CSV, 300,000, p4 | 3,739.54 | 3,738.00 | **-0.04%** |
| C07–C08 | scalar-state, 2M, 64 ops/record, p4 | 248,352 | 495,742 | **+99.61%** |

前三组的差异都小于 0.4%，在没有多次独立重复和误差区间时应视为持平。
C07–C08 显示明确的候选优势，但只有一轮，而且 wall time 分别为 8.05 s 和
4.03 s，几乎落在轮询周期的整数倍上。下一轮应提高运行时长或记录 job 内部处理
时间，并至少进行 3 个交替顺序的独立 job。

原始数据：

- C01–C02：
  [HashMap](../raw/client_usecase_hashmap_20260811_151637.json)、
  [ForL0](../raw/client_usecase_forl0_20260811_151659.json)
- C03–C04：
  [HashMap](../raw/client_usecase_hashmap_20260811_151721.json)、
  [ForL0](../raw/client_usecase_forl0_20260811_151743.json)
- C05–C06：
  [HashMap](../raw/client_usecase_hashmap_20260811_151918.json)、
  [ForL0](../raw/client_usecase_forl0_20260811_152056.json)
- C07–C08：
  [HashMap](../raw/client_usecase_hashmap_20260811_152137.json)、
  [ForL0](../raw/client_usecase_forl0_20260811_152159.json)

## 对现有报告和论文分析的影响

当前 [benchmark_report.html](benchmark_report.html) 是本轮修复前生成的
legacy-unscoped 报告，不能代表最新批次：

- 它把本轮 q3 与历史 q4/q5 等 NexMark 结果混合；
- 它把 C07–C08 的 2M scalar-state 数据描述成 `contract_baseline`；
- 它引用的 figures 早于本轮数据。

因此，本文件取代该 HTML 作为 2026-08-11 批次的解释性报告。旧 HTML 和历史
结果继续保留作为审计证据，不删除。

本轮结果**不会改变**现有论文证据边界：仍然没有可归因到真实 L0 硬件的匹配
性能证据。需要调整的分析措辞是：

1. WordCount 的“+11.1%”只能称为 best-of-3 峰值，不能称为稳定平均加速；
2. q3 的 +12.5% 和 scalar-state 的 1.996× 是后续复验候选，不是跨 workload 结论；
3. CSV Client 场景目前显示 ForL0 与 HashMap 基本持平；
4. q4/q9/q18/q19/q20 本轮没有有效新结果，必须等待修复后的正式重跑。

## 下一轮验收条件

- 使用 `1ec7838` 或更新提交，通过 q18 smoke 后再进入全量运行；
- 每个 JSON 必须带相同 `run_id`，报告只加载该 run ID；
- workload manifest 记录成功/失败状态、git commit、bundle digest、硬件、L0 device
  和 runtime library identity；
- 关键 paired workload 至少 3 个独立 job，HashMap/ForL0 交替运行；
- WordCount 使用 median（并报告全部样本与离散度），不再用 best-of-3 作为唯一结论；
- 延长 scalar-state 测量窗口，避免 4 秒轮询量化主导结果；
- 全量结束后重新生成 scoped HTML，并确认只包含该批次的数据和图。
