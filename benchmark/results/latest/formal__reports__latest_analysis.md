# ForL0 最新离线实验分析

报告日期：2026-08-16

最新观测批次：`benchmark/results/runs/20260816_115143`

证据类型：`real-online`，批次状态为 `failed/partial`。由于失败批次不会覆盖
`benchmark/results/latest/` 的成功快照，本文件单独指向上述 campaign；分析只使用
配置一致且两端均成功的样本，失败单元不补零、不参与加速比。

## 执行摘要

- 计划矩阵共 24 个 backend 单元，16 个产生有效结果；12 组对比中只有 6 组完整。
- WordCount `stateful_counter_p4_probe` 中，ForL0 中位吞吐比 HashMap 低 **9.1%**，
  是本轮最明确的性能回退。
- NexMark q18 TPS probe 中，ForL0 吞吐提高 **17.2%**；q19 吞吐持平。
- q18/q19 报告的 ForL0 CPU 需求分别下降 **75.5%** 和 **45.8%**，但都只有单次
  backend 样本，必须复测后才能形成 CPU 效率或扩展性结论。
- Client contract、optimized、state-pressure 三组差异都小于 **0.1%**，目前只能
  判断为持平或受 source/bounded shutdown 限制。
- 当前证据支持“部分 workload 有 CPU 效率潜力”，不支持 ForL0 整体加速结论。

## 修复前实测性能

| 类别 | Workload | HashMap | ForL0 | ForL0 变化 | 样本说明 |
|---|---|---:|---:|---:|---|
| WordCount | stateful_counter_p4_probe | 4.986 M records/s | 4.532 M records/s | **-9.1%** | 每端 3 次，取中位数 |
| Client | contract_baseline | 74.28 records/s | 74.34 records/s | +0.1% | 每端单次 |
| Client | forl0_optimized | 743.10 records/s | 742.43 records/s | -0.1% | 每端单次 |
| Client | state_pressure_300k | 3.738 K records/s | 3.740 K records/s | +0.0% | 每端单次 |
| NexMark | q18 forl0_tps_probe | 274.09 K events/s | 321.25 K events/s | **+17.2%** | 每端单次 |
| NexMark | q19 forl0_tps_probe | 1.130 M events/s | 1.130 M events/s | 0.0% | 每端单次 |

WordCount 的三次直接测量如下：

| Backend | Repeat 1 | Repeat 2 | Repeat 3 | Median |
|---|---:|---:|---:|---:|
| HashMap | 4.529 M/s | 4.986 M/s | 4.988 M/s | 4.986 M/s |
| ForL0 | 4.529 M/s | 4.532 M/s | 4.535 M/s | 4.532 M/s |

HashMap 第一次存在明显冷启动慢样本，后两次稳定在约 4.99 M/s；ForL0 三次稳定在
约 4.53 M/s。因此不能把回退解释成单个异常点。

## CPU 效率与并行度

| Query | HashMap CPU | ForL0 CPU | CPU 变化 | Throughput/core 比率 |
|---|---:|---:|---:|---:|
| q18 | 22.48 cores | 5.50 cores | -75.5% | 4.79x |
| q19 | 9.74 cores | 5.28 cores | -45.8% | 1.84x |

较低 CPU 需求意味着 ForL0 **可能**拥有提高并行度或同机部署更多任务的余量，但
不能直接据此把正式矩阵改为 p8：本批次的 p8 q18/q20 已经导致 HashMap TaskManager
丢失，同时 ForL0 的共享 L0 分配也失败。下一轮应先完成稳定的 p4 配对，再单独做
p4→p8 scaling；每个点至少 3 次，并同时检查吞吐、CPU、L0 分配和 TaskManager 存活。

## 失败单元与根因

| Workload | Backend | 观测根因 |
|---|---|---|
| q18 lateq deep | HashMap | p8 压力下 TaskManager 丢失 |
| q18 lateq deep | ForL0 | `strict L0 allocation failed` |
| q20 lateq deep | HashMap | p8 压力下 TaskManager 丢失 |
| q20 lateq deep | ForL0 | `strict L0 allocation failed` |
| q9 pressure | ForL0 | L0 全局预算只按 parallelism 切分，低估 backend engine 数量 |
| q4 pressure | ForL0 | 多个 keyed operator 共享 L0，实际 engine 数超过 p4 |
| q3 extra SQL | ForL0 | L0 分配失败，随后达到 SwissTable max-table-capacity |
| Client scalar | ForL0 | 第四个约 128 MiB L0 分配失败；旧 runner 还漏报非零状态 |

q4 日志显示最多有三个 p4 keyed backend operator 组，即可能同时创建 12 个 backend
engine。旧配置仅按 pipeline parallelism=4 切分全局 256 MiB L0，导致每个 engine
请求约 64 MiB，多个实例启动时迅速耗尽全局设备池。

## WordCount 回退定位与调优

ForL0 WordCount 日志记录约 2 亿次 fused counter 写入，但 L0 cache 的
`lookups=0`、`hits=0`，每个 engine 都触发 write bypass；与此同时每轮仍分配总计
512 MiB L0。该 fused `addAndGetLong` 路径直接更新权威 SwissTable，当前 L0 cache
没有提供读取收益，却引入设备分配和初始化成本。

提交 `b0e686a` 已进行以下调整：

1. 仅对 `stateful_counter_p4_probe` 关闭无效 L0 cache，不改变记录数、key 分布、
   parallelism、聚合口径或 HashMap baseline。
2. 按每个 p4 subtask 约 500K distinct keys，将 primitive main table 预分配为
   1,048,576 slots，减少测量期间扩容。
3. 仍使用每端 3 次、median 聚合。调优后的数值必须由下一轮真实服务器重跑产生，
   本报告不把配置推测写成已经获得的提升。

## 已实施的失败修复

- q4/q9 pressure：L0 job-wide 预算按最多 16 个并发 engine share 切分。
- q18/q20 lateq：正式配对改为服务器已完成过的 p4 拓扑，并缩短到稳定测量窗口；
  两端使用完全相同的 workload 配置。
- q3：将持续保留输入的形状从 1M TPS 调整到可持续的 200K TPS，并将 table ceiling
  提高到 4,194,304。
- Client scalar：L0 预算改为 8 份 64 MiB share；Client 返回空结果时 runner 现在
  必须非零退出并生成 `FAILED_client_*` 证据。
- 一键 smoke/formal 默认显式 `--no-report`。实验服务器只生成 raw、NexMark JSON、
  `.logs` 和失败证据，不再生成 figure、PDF 或 HTML。
- 本地派生目录 `output/` 和 `tmp/` 已加入 `.gitignore`，图和 PDF 不推送远端。

## 下一轮验收标准

1. 运行更新后的 `./reproduce-all`，先通过 smoke，再执行完整正式矩阵。
2. WordCount 两端各 3 次；确认 ForL0 日志显示该场景 L0 cache disabled，并比较
   预分配后的稳定中位数。
3. q4/q9/q18/q20/q3/Client scalar 不得再出现 strict allocation、table capacity、
   TaskManager drop 或漏报失败。
4. q18/q19 CPU 效率至少重复 3 次；记录每次 CPU 序列、TaskManager 数和采样窗口。
5. 完整矩阵成功后再运行独立 p4/p8 scaling。若 p8 只降低 CPU 而吞吐不增长，或
   再次触发 L0/内存压力，则不能将 CPU/core 优势解释成可用扩展性。

## 派生报告

本地分析命令：

```bash
python benchmark/scripts/generate_campaign_analysis.py \
  --campaign benchmark/results/runs/20260816_115143 --output output
```

生成的 PDF、SVG、PNG、CSV 和 Markdown 仅保存在本地 `output/`。本文件是仓库内
持续更新的文字汇报，图表不进入 Git。
