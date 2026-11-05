> **2026-11-03 校正：下文的跨后端加速比已撤回。** 旧 job 为 ForL0 调用 fused/batch 扩展，为 Heap 调用标准 API，不能作为同一 workload 下的公平后端比较。仅保留历史实验记录。当前 job 已统一为后端无感知的标准 Flink API；新结果见 `BriskState_Fair_Benchmark_Report.md`。

# ICDE 数值状态压力测试与性能归因（2026-11-02）

代码位于远端 `/root/forL0-state-backend`，分支 `briskstate_workspace`。本轮删除旧 suite 后重写七个应用；所有作业关闭 checkpoint 与 L0。修改未提交、未推送。

## 实验口径

- Flink 1.20.3、Java 11.0.32、GCC 12、Kunpeng-920 ARM64；1 JM（CPU 0–3，8 GiB）、2 TM（CPU 4–7 / 8–11，各 32 GiB、4 slots）。CPU affinity 与 Flink 内存预算不等于宿主机独占资源或硬 cgroup 内存上限。
- 每个应用 524,288 个实体 key、8,000,000 个输入，feature width=64、每次 task 输入更新 32 个 MapState 元素。普通 task 最多 33,554,432 个 feature 值；先遍历全 key 空间，再做固定种子的均匀随机访问。Global ACD 保留单全局聚合，feature width=4、touches=1。
- 源数据、state key/value 均为数值 Long；Flink 网络传输仍使用 POJO 序列化。ForL0 的状态访问使用 primitive/fused/batch 扩展；Heap 用标准 get/update 或 get/put 执行相同增量。比较明确偏向此类可批量访问的小范围 feature 字典，不代表任意 MapState、String 状态或通用序列化场景。
- 独立 task/edge 契约核对实际 StreamGraph；VS 两条 RCRF 输入、LG 三个 sink、LR 的直接与交叉边均保留。物理 key 分区和状态内容为适配项。VS/LR 为数值 task 框架实现，非完整生产业务或原论文数据集复现。
- REST 作业 duration 包含启动/JIT/网络/GC；后端顺序交替；下表保留两轮全部正式样本，profile 作业不参与中位数。两轮样本量较小，范围显示实际波动，未声称统计显著性。

## 正式测试

| 应用 | Heap 中位数 s（范围） | ForL0 中位数 s（范围） | 加速比 |
|---|---:|---:|---:|
| WC | 8.899 (8.750–9.047) | 4.867 (4.302–5.432) | 1.83× |
| FD | 16.579 (15.831–17.328) | 6.901 (6.711–7.090) | 2.40× |
| SD | 37.126 (31.205–43.046) | 8.880 (8.478–9.281) | 4.18× |
| TM | 32.696 (29.535–35.857) | 7.320 (6.345–8.295) | 4.47× |
| LG | 67.863 (67.501–68.224) | 14.409 (13.620–15.198) | 4.71× |
| VS | 385.297 (383.802–386.792) | 65.559 (65.529–65.590) | 5.88× |
| LR | 311.660 (310.106–313.215) | 54.323 (52.714–55.933) | 5.74× |

每次运行保存 job、exceptions、checkpoints、实际拓扑、sink JSON、命令、CPU/RSS 和二进制哈希。正式作业全部 FINISHED、checkpoint total=0；两后端的 sink 记录数、最终 feature projection、source signature 一致。聚合校验不是每个中间 task 的逐 key 完整业务结果 oracle。小规模执行另做独立 source signature 校验。

## 本轮后端改动与消融

1. MapState JNI 复用注册时缓存的稳定 StateTable 指针，消除每次通过 table id 查表。既有 ValueState 指针缓存保持。
2. 对显式批量 API 访问的、范围不超过 256 的非负 Long 字典，启用紧凑 value 数组和存在位图。普通稀疏 MapState 保持 hash；后续出现负数／越界 key 时转回 hash，保留已有条目。
3. 批量顺序更新只归一化一次取模，后续递增回绕；Long 加法保留 Java 的 64 位回绕行为。没有将 32 次更新替换成一次增大 delta。

下表为相同 benchmark JAR、相同 GC 日志配置、独立重启 JVM 后的旧库／新库对照；每个版本先做小规模运行，再保留两轮正式样本。此对照归因于“MapState 指针缓存＋紧凑布局＋循环”这一组改动，没有将各子改动的贡献单独拆开。

| 应用 | 旧 ForL0 中位数 s | 新 ForL0 中位数 s | 耗时下降 |
|---|---:|---:|---:|
| WC | 6.736 | 5.068 | 24.8% |
| SD | 10.744 | 7.780 | 27.6% |
| VS | 91.040 | 66.106 | 27.4% |

关闭 MapState batch 的 WC 中位数为 13.073 s，开启为 5.068 s（2.58×）。关闭 batch 时也不会启用该 dense 触发条件，所以该消融同时包含 JNI 批量化和布局效果，不能只归为 JNI 调用收益。

## CPU 火焰图

两台 TM 均使用 async-profiler 4.4；`-e cpu` 在当前 PMU 权限不足时自动回退到 ctimer，间隔 5 ms、native dwarf 栈。独立采样与正式计时分开。以下百分比的分母是两个 TM 的 CPU 样本总数；inclusive 分类可能重叠，不是可相加的组件耗时。

| VS 8M ForL0 | NativeEngine inclusive | Map batch inclusive | Hash 叶子 | 分配器叶子 |
|---|---:|---:|---:|---:|
| 优化前 | 60.74% | 42.79% | 6.71% | 3.30% |
| 优化后 | 43.52% | 24.96% | 0.08% | 0.11% |

热点变化支持“减少哈希查询、指针追逐与 native 小对象分配”的解释；单凭火焰图占比不能证明 cache miss 或 IPC 的变化。优化后网络 buffer、POJO 反射序列化和 ValueState JNI 的相对占比上升，是后续可继续优化的部分。

## GC 与分配

使用 Java 11 unified GC/safepoint 日志、REST GC/heap counter 快照及 2 秒采样 JSONL。GC pause 从 summary 行提取，避免累计 phase 行重复计数。下表选择第二轮 VS，日志覆盖完整运行；每个 TM 单独报告，不能把两台并发暂停的时间相加后当成作业耗时占比。

| 后端 | TM | pause 次数 | pause 总计 ms | 最长 pause ms | pause / 作业 wall | Full GC |
|---|---|---:|---:|---:|---:|---:|
| forl0 | tm1 | 27 | 1713.9 | 139.9 | 2.61% | 0 |
| forl0 | tm2 | 27 | 1717.5 | 186.2 | 2.62% | 0 |
| heap | tm1 | 61 | 24198.7 | 2709.8 | 6.30% | 0 |
| heap | tm2 | 65 | 29109.8 | 2961.3 | 7.58% | 0 |

第一轮前几个作业在动态开启 GC 日志之前运行，属于不完整覆盖，不能当作零 GC。新启动 JVM 已默认开启完整日志。GC counter 也有采样更新延迟，因此短作业以带时间戳的 GC 日志为准。当前测试会产生网络/POJO 等 JVM 分配，ForL0 状态搬到 native 不等于“没有 GC”。

另有 VS 2M（131,072 keys）两后端 CPU 与 allocation profile。allocation 按 `--alloc 1m` 间隔采样，导出的 HTML/collapsed 权重是分配样本数，不是字节数或 live heap。采样在 RUNNING 后 5 秒启动，两后端窗口处理的记录量不完全相同，不能用原始样本总数之比代表全作业分配量改善。分配栈分布用于识别仍存在的 JVM 分配来源。

- forl0：19,264 个分配样本；主要叶子：java.lang.Long_[i] 62.5%；org.apache.flink.benchmark.benchset.BenchsetEvent_[i] 28.7%；org.apache.flink.streaming.runtime.streamrecord.StreamRecord_[i] 7.2%；java.util.Optional_[i] 0.5%。
- heap：27,129 个分配样本；主要叶子：java.lang.Long_[i] 60.9%；org.apache.flink.benchmark.benchset.BenchsetEvent_[i] 26.4%；org.apache.flink.streaming.runtime.streamrecord.StreamRecord_[i] 6.7%；java.util.HashMap$Node_[i] 2.7%。

火焰图（每行包含两台 TM）：

- before-profile / forl0 / 8000000 records：[TM1](../benchmark/results/runs/icde_20261102_084354_32f9c4/01_vs_forl0_27f2cd/tm1-cpu.html)、[TM2](../benchmark/results/runs/icde_20261102_084354_32f9c4/01_vs_forl0_27f2cd/tm2-cpu.html)
- after-profile / forl0 / 8000000 records：[TM1](../benchmark/results/runs/icde_20261102_084829_382c5d/01_vs_forl0_d43e72/tm1-cpu.html)、[TM2](../benchmark/results/runs/icde_20261102_084829_382c5d/01_vs_forl0_d43e72/tm2-cpu.html)
- cpu-attribution / forl0 / 2000000 records：[TM1](../benchmark/results/runs/icde_20261102_093541_3f390f/01_vs_forl0_47c7f6/tm1-cpu.html)、[TM2](../benchmark/results/runs/icde_20261102_093541_3f390f/01_vs_forl0_47c7f6/tm2-cpu.html)
- cpu-attribution / heap / 2000000 records：[TM1](../benchmark/results/runs/icde_20261102_093541_3f390f/01_vs_heap_3375e5/tm1-cpu.html)、[TM2](../benchmark/results/runs/icde_20261102_093541_3f390f/01_vs_heap_3375e5/tm2-cpu.html)
- allocation-attribution / forl0 / 2000000 records：[TM1](../benchmark/results/runs/icde_20261102_093717_f22db0/01_vs_forl0_8d7486/tm1-alloc.html)、[TM2](../benchmark/results/runs/icde_20261102_093717_f22db0/01_vs_forl0_8d7486/tm2-alloc.html)
- allocation-attribution / heap / 2000000 records：[TM1](../benchmark/results/runs/icde_20261102_093717_f22db0/01_vs_heap_dbeb36/tm1-alloc.html)、[TM2](../benchmark/results/runs/icde_20261102_093717_f22db0/01_vs_heap_dbeb36/tm2-alloc.html)

## 微架构：本轮实测阻塞

已安装并运行官方 DevKit 26.2.RC1，正确识别 Kunpeng-920；`tuner top-down` 返回 `No permission to open PMU device`。用户态 cycles 的 perf_event_open 与 async-profiler cycles 也被拒绝。当前 host kernel=5.15、perf_event_paranoid=4、容器缺 CAP_PERFMON、/proc/sys 只读；服务账号无权 patch Deployment。

工作区位于 namespace `coder-workspaces`，Deployment `coder-user-yjy-flink-32`，节点 `host-192-168-0-2`（192.168.0.2）。提供的可登录 SSH 主机与该节点内核不同，经跳板机到该节点超时。详见 `docs/BriskState_Kunpeng_TopDown.md` 的管理员最小配置与证据文件。用户已允许本轮在此阻塞下先不测微架构。

未生成实际 PMU 堆叠图。`collect_topdown.sh` 和 `plot_topdown.py` 已准备：图一为 Retiring / Bad Speculation / Frontend / Backend；图二为 Backend 的 Core / L1 / L2 / L3-or-DRAM / Store，Memory Bound 作为父项标注。L1 Bound 不是 L1 miss rate，Kunpeng 的 L3-or-DRAM 不冒称独立 DRAM Bound。待管理员开放 PMU 后，必须采齐七个 workload × 两后端的真实计数并核对稳态采样窗口。硬件成功采集尚未验证。

## 验证和材料

- 141 项 native 测试；24 项 Java 配置/状态语义测试；10 项 benchset 测试通过。
- 小规模七应用 × 双后端执行通过；原始图节点/边、输出记录与 source signature 校验通过。
- 原始资料和代码说明：`benchmark/benchset/README.md`、`reference/topology-contract.json`；旧/新库二进制哈希及每次运行数据保留。
- `measurements.json/csv`、`medians.json`、`profiles.json` 为分析数据；`raw` 保留各 campaign；`evidence` 包含 GC、PMU 错误、构建和测试日志；`SHA256SUMS` 校验下载完整性。

原论文：[Zhang et al., ICDE 2017](https://www.comp.nus.edu.sg/~hebs/pub/shuhaoICDE17a.pdf)；[原应用仓库](https://github.com/ShuhaoZhangTony/ProfilingStudy/tree/770bfeeb4d4471092cf050d50052338d4ae38179)。微架构口径采用 [华为 DevKit 官方文档](https://www.hikunpeng.com/doc_center/source/en/kunpengdevps/profiler/profiler/KunpengDevKitCli_0033.html)，工具 API 另参考 [openEuler libkperf](https://github.com/openeuler-mirror/libkperf)。
