# ForL0 最新离线实验分析

报告日期：2026-08-11

实验批次：2026-08-11 17:00–17:33（run manifest：
[`ascend_reproduction_20260811_170156.tsv`](formal__run_logs__ascend_reproduction_20260811_170156.tsv)）

报告类型：`derived-artifact`。本报告从已提交的 raw JSON、NexMark JSON 和 `.logs`
重新核算；没有在本机伪造或重跑 L0 硬件实验。

## 2026-08-11 18:42 批次校验（不作为有效性能批次）

远端提交 `2bd0bee` 中的新日志来自 18:42 开始的实验，而本轮 L0 归因、NexMark
cancel-409 和结果作用域修复直到 18:55 的 `3e190fe` 才提交。因此该实验实际运行的
是旧控制面：日志中 `GitCommit: unavailable`、`RunID: unassigned`，WordCount 仍显示
`best` 并使用旧的 `l0-cache.size` / `l0-memory.max-size` 属性，NexMark preflight 也没有
验证 cancel-409 修复。日志在 N04 启动中途结束，说明上传的是仍在运行的部分批次。

- q18 TPS 的 HashMap 作业在 116 ms 内进入 `FAILED`，没有产生可用 sample；ForL0 的
  601,870 events/s 因此没有匹配 baseline，不能计算提升。N03 HashMap 在完整 summary
  后 cancel 返回 409（1.12M events/s），属于旧 driver 的清理竞态；它也不能与缺失的
  N04 结果组成配对。
- HTML 中 q8 的 **+1492.5%** 不是本批次新结果。旧报告错误地把
  `forl0_no_full_gc_q8_q11_deep` 的 HashMap 10,400 events/s 与
  `contract_baseline` 的 ForL0 165,620 events/s 跨场景拼接。该数字无效。相同目录、
  相同 `contract_baseline` 的历史值实际是 165,690 vs 165,620，约 -0.04%，但同样不是
  本轮真实 L0 结论。
- WordCount 的 +0.1% 来自旧配置的 best-of-3。三次 HashMap 为
  4.981M / 4.156M / 4.535M records/s，ForL0 为 4.984M / 4.987M / 4.533M；样本呈现
  明显双峰。best 得到 +0.1%，median 会得到约 +9.9%，二者差异本身说明这组运行存在
  顺序/热身噪声，不能挑一个聚合方式当成稳定提升。

代码现已将报告限制为同一 `run_id`、同一有效 workload identity（场景、TPS、事件
比例、parallelism、monitor 配置）的完整 backend pair；无 `run_id` 的独立 raw 文件
不再跨文件比较。新结果还会记录 `control_revision`。因此上述 q8 拼接和旧 WordCount
best 选择不会进入下一轮正式报告。

## 结论

最新批次修复了先前的 NexMark category/path 问题，q3/q4/q9/q18/q19/q20 已经
真正提交并产生结果。单次配对显示 ForL0 在 q18、q20、q9、q4、q3 的吞吐分别
为 **+38.1%、+13.4%、+15.7%、+54.8%、+4.8%**，q19 为 **-1.7%**。
这些是有价值的系统级信号，但每个查询只有一对样本，仍不足以形成论文级稳定结论。

本批次依然不能证明上述提升来自 L0 硬件：launcher 日志没有归档 TaskManager 的
`[ForL0-HotCache]` 激活、容量、hit/miss 或销毁 summary。q18 使用复杂 Row state，
不是当前 scalar HotCache 的直接适用路径；它的提升更可能来自 native SwissTable、
fixed-row 与 JNI 路径。Client 的 batch scalar 提升同样主要属于 JNI batching，不能
写成 L0 加速。

## 有效结果

### WordCount

场景 `stateful_counter_p4_probe`，2M keys、200M records、parallelism 4，best-of-3：

| Backend | 三次吞吐（records/s） | 选中值 |
|---|---:|---:|
| HashMap | 4,981,463 / 4,532,995 / 4,533,909 | 4,981,463 |
| ForL0 | 4,982,396 / 4,155,123 / 4,987,044 | 4,987,044 |

选中值差异仅 **+0.11%**，应视为持平。两端都有明显慢样本，后续报告应使用交替
运行的独立 job 和 median/置信区间，不应只展示 best-of-3。该 fused add-and-get
路径始终更新权威 SwissTable；旧 HotCache 只做 cache put，并不提供读取加速。

原始数据：[`HashMap`](formal__raw__wordcount_stateful_counter_p4_probe_hashmap_20260811_170422.json)、
[`ForL0`](formal__raw__wordcount_stateful_counter_p4_probe_forl0_20260811_170646.json)。

### NexMark

| Query | HashMap events/s | ForL0 events/s | 吞吐变化 | 备注 |
|---|---:|---:|---:|---|
| q18 lateq | 1,050,000 | 1,450,000 | **+38.1%** | 两端 summary 完整；HashMap cancel 返回 409 |
| q19 | 957,640 | 941,190 | **-1.7%** | 近似持平 |
| q20 | 414,260 | 469,580 | **+13.4%** | 候选优势 |
| q9 | 96,540 | 111,710 | **+15.7%** | ForL0 的报告 CPU 更高，per-core 反而更低 |
| q4 | 172,480 | 266,930 | **+54.8%** | 最强候选，必须重复验证 |
| q3 | 538,860 | 564,570 | **+4.8%** | 小幅候选优势 |

q18 TPS probe 的 HashMap N01 在提交后很快失败，不能与 N02 配对；当时的 failure
marker 没有收集 TaskManager stderr，因此仅凭现有日志无法确定最终异常。另有两个
driver 在完整 summary 后 cancel 已结束 job 时收到 HTTP 409。409 是清理竞态，样本
可保留；其他非零退出即使含有局部 summary 也不应接纳。

### Client use case

| 场景 | HashMap records/s | ForL0 records/s | 变化 |
|---|---:|---:|---:|
| contract, 300 records | 74.34 | 74.30 | -0.05% |
| optimized CSV, 3K | 743.94 | 742.70 | -0.17% |
| state pressure, 300K | 3,737.08 | 3,738.09 | +0.03% |
| scalar batch, 2M × 64 ops | 248,273.88 | 248,639.85 | +0.15% |

四组均应视为持平。旧批次中接近 2× 的 scalar 结果没有在本轮复现；两端最新完成
时间都约 8.05 秒，说明此前差异受轮询量化或运行路径变化影响，旧结论必须撤回。

## 本轮代码校对发现与修复

- `l0_cache_size` 过去被每个并行 StateEngine 完整申请，导致 p4/p8 并行实例竞争
  同一硬件池。现在它被解释为 job-wide `l0-cache.total-size`，runner 自动按 workload
  parallelism 设置 `expected-engines` 并均分。
- 正式 L0-on 消融启用 strict allocation；硬件不可用或容量缩水不再悄悄降级。
- 每个 scalar state 的固定 64-set 配额改为显式 `l0-cache.state-size`，并输出请求量、
  实际量、state/engine counters 和 native memory peak。
- SwissTable 的 max capacity、load factor 和 native-memory limit 从“声明但未生效”改为
  强制执行；超限以明确异常失败，避免直接被容器 OOM killer 以 137 杀死。
- 长期只写 workload 达阈值后停止污染 L0；下一次读取强制 miss 并重新准入，保证正确性。
- NexMark 只接纳可证明的 post-summary cancel-409；其他 driver 异常一律重试/失败。
- `docker/run_all_apps.sh` 现在把 TaskManager 的 L0/memory/根因证据写入
  `benchmark/results/run_logs/`，同时进入 `benchmark/results/.logs`；每条 benchmark
  命令使用独立时间窗口，后续 workload 不会误用先前的 L0 激活记录。
- 新增 `./reproduce-l0-ablation`，一键运行 HashMap、ForL0-L0-off、ForL0-L0-on
  三路同场景比较；L0-on 缺少真实激活证据会失败。

## 下一轮验收

1. 先运行 `./reproduce-l0-ablation`，确认每个 L0-on engine 都有 `active=1` 记录且
   没有硬件 fallback；三路 JSON 的 `_metadata.variant` 必须不同。
2. 再运行 `./reproduce-all`。关键 query 至少进行 3 个独立、交替顺序的配对 job。
3. 报告 L0 hit rate、write bypass、requested/actual bytes、native peak 与 Full GC；没有
   同批证据时，只能表述为 ForL0 backend 提升，不能表述为 L0 硬件提升。
4. q4、q18 是优先复验对象；q19 是回归守门项；WordCount 和 Client batch 当前按持平处理。
