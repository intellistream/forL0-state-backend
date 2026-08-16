# ForL0 最新离线实验分析

报告日期：2026-08-16

实验批次：`20260816_135559`（13:55:59–14:26:47）

证据类型：`derived-artifact`，源数据是 L0 服务器上传的 `real-online` 日志和 JSON；本机只做代码构建、单测和汇总，没有伪造性能样本。

## 结论

本批次最终状态为 **failed**，因此没有发布为 `benchmark/results/latest` 的正式完整批次。它仍提供了两类明确证据：

1. 已成功的配对结果显示 ForL0 在 q18 TPS 和 q20 上有吞吐/CPU 效率信号，q19 在 source cap 下吞吐持平但 CPU 更低；WordCount 当前配置则回归 8.3%。
2. q18 lateq、q9、q4、q3 以及两个 Client 压力场景都在 StateBackend 构造期失败。完整 TaskManager 证据表明：同一进程内前两个 L0 manager 能创建，后续并行实例的 `l0_mem_alloc` 失败。这不是路径问题，也不是单纯容量除法问题，而是重复创建进程级 tuner。

因此，本批次不能用于 q9/q4/q3/Client 压力性能结论，也仍不能把成功的 ForL0 提升归因于 L0 HotCache。

证据入口：[`run manifest`](../../runs/20260816_135559/run_manifest.json)、[`完整日志`](../../runs/20260816_135559/.logs)、[`失败标记`](../../runs/20260816_135559/FAILED.txt)。

## 成功的配对结果

### WordCount

`stateful_counter_p4_probe`，2M keys、200M records、parallelism 4，median-of-3：

| Backend | 三次吞吐（records/s） | Median |
|---|---:|---:|
| HashMap | 4.153M / 4.532M / 4.534M | 4.532M |
| ForL0 | 4.530M / 4.154M / 4.157M | 4.157M |

ForL0 比 HashMap 低 **8.3%**。TaskManager 日志同时显示每个 ForL0 engine 预分配约 570 MiB native memory；1,048,576-slot 的预分配没有消除双峰，反而增加了内存压力。该配置已回退到 `initial=8192, max=262144`，必须重跑后才能判断回归是否消失。

### NexMark

| Query / 场景 | HashMap events/s | ForL0 events/s | 吞吐变化 | CPU cores（H / F） | 判断 |
|---|---:|---:|---:|---:|---|
| q18 `forl0_tps_probe` | 262,820 | 318,630 | **+21.2%** | 18.60 / 5.99 | 有效单次配对；L0 在该场景被配置关闭 |
| q19 `forl0_tps_probe` | 1,150,000 | 1,150,000 | 0.0% | 9.65 / 5.47 | source-capped；只支持 CPU 效率信号 |
| q20 `lateq_deep` | 23,560 | 25,690 | **+9.0%** | 12.24 / 6.01 | 有效单次配对；需要重复验证 |

q18 每核吞吐从 14.13K 提高到 53.19K，q19 从 119.17K 提高到 210.24K，q20 从 1.92K 提高到 4.27K events/s/core。CPU 指标是采样值，且每个场景只有一次配对，现阶段只能作为下一轮调优优先级，不能直接外推“可以提高几倍并行度”。

### Client use case

| 场景 | HashMap records/s | ForL0 records/s | 变化 |
|---|---:|---:|---:|
| contract baseline | 74.3369 | 74.3398 | +0.004% |
| `forl0_optimized` | 743.4802 | 743.4077 | -0.010% |

两组都应视为持平，只能作为兼容性/正确性证据。`state_pressure_300k` 和 `scalar_state_probe_2m_ops64_batch` 只有 HashMap 结果，ForL0 在 backend 初始化时失败，不能比较。

## 失败根因

失败 workload：N04 q18 lateq、N10 q9、N12 q4、N14 q3、C06 state pressure、C08 scalar batch。

所有失败日志都有相同结构：

- 前若干 `engine_start active=1` 成功；
- 同一 TaskManager 中后续 StateEngine 报 `strict L0 allocation failed: l0_mem_alloc probe failed`；
- 作业在约 0.4–1.3 秒内失败，尚未进入性能测量。

例如 q18 p4 每 engine 已正确拿到 32 MiB 配额，但一个 TaskManager 内只有两个 manager 成功，第三/第四个失败。Client p2 在同一进程内两个 64 MiB manager 均成功。这组对照说明主要限制是 **进程内 tuner/manager 实例数**，继续减小每实例字节数不能可靠解决。

q3 还有第二个独立错误：native peak 达到 528,482,816 bytes，紧贴 512 MiB 上限，随后 `std::bad_alloc`。因此 q3/extra-SQL 的 native 上限提高到 768 MiB，同时不改变输入 TPS、比例或测量窗口。

## 本轮代码修复

- TaskManager 进程内所有 `StateEngine` 改为共享一个 `HotCacheManager`、一个 tuner 和一块 L0 backing allocation。
- manager 的分配/free-list/统计/rebalance bookkeeping 加锁；每个 StateEngine 销毁时只释放自己的 state cache slices，最后一个 engine 退出后才销毁 tuner。
- 兼容配置键 `l0-cache.expected-engines` 在正式拓扑中设为 2，实际表示两个 TaskManager 进程级 manager；256 MiB job budget 因此按进程分为 128 MiB，而不是按 slot 重复申请。
- 新增进程共享与配置冲突 native 测试；native 总计 137/137 通过，Java/MiniCluster 测试 64/64 通过。
- 在 Kunpeng-920/aarch64 本机构建了新 production `.so` 并同步到 JAR、resources 和 deploy 目录。
- 一键脚本增加 native 源码指纹：完整仓库上传到离线 ARM 服务器后，如源码变化会在 smoke 前离线重建，避免继续运行旧二进制。
- WordCount 撤销 1,048,576-slot 预分配；q3 native limit 调整到 768 MiB。

上述修复尚未被新的 `real-online` 全量批次验证，所以它们是“代码/单测通过、等待硬件重跑”，不是新的性能结果。

## 下一轮验收与调优顺序

1. 运行 `./reproduce-all`；smoke 日志必须在每个 TaskManager 只出现一个 `engine_start`，其余并行实例应显示 `engine_reuse`，且不能再出现 strict allocation failure。
2. 优先检查 q18 lateq、q9、q4、q3、Client C06/C08 是否全部产生 ForL0 JSON；q3 peak 必须低于 768 MiB 且无 `std::bad_alloc`。
3. WordCount 重新采集 median-of-3。若回退预分配后仍低于 HashMap 5% 以上，再做独立 table-capacity sweep；不要在正式矩阵里继续猜容量。
4. 对 q18/q20 和 q19 CPU 效率至少做 3 轮交替配对，并记录 CPU、Full GC、L0 state_attach/hit rate。只有出现真实 `state_attach` 和非零 lookup/hit，才能讨论 L0 硬件贡献。
5. 在重复结果证明 CPU headroom 稳定之前，不提高正式 parallelism；CPU/core 效率提升只表示有扩并行潜力，不等于线性扩展保证。
