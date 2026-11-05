> **2026-11-03 校正：下文的跨后端加速比已撤回。** 旧 job 为 ForL0 调用 fused/batch 扩展，为 Heap 调用标准 API，不能作为同一 workload 下的公平后端比较。仅保留历史实验记录。当前 job 已统一为后端无感知的标准 Flink API；新结果见 `BriskState_Fair_Benchmark_Report.md`。

# Long 快路径大状态测试与优化记录

2026-11-02，远程分支 `briskstate_workspace`。Flink 1.20.3 / Java 11.0.32，
Kunpeng-920 ARM64。1 JM（4 核 / 8 GiB），2 TM（各 4 核 / 32 GiB），8 slots。
L0 全程关闭。下列结果只对应本次 Long counter 负载。

## 负载与计时

- 3200 万个 Long 键，1.28 亿条输入，均匀分布，不限速。
- 直接生成 Long，首次遍历铺满全部键空间；输入和输出均无 String 转换。
- 每条输入执行 16 次实际状态增量，总计 20.48 亿次；没有合并成一次大增量。
- ForL0 使用 fused increment；heap 使用 ValueState read/update 实现相同增量。
- 并行度 8，性能测试关闭 checkpoint。包含建表过程，不是仅测预热后的查询。
- 表中是 Flink REST 作业耗时，排除带 profiler 和正确性 oracle 的作业。
- 保留全部样本，使用中位数；包含旧版/新版二进制回切，未挑选最佳结果。

| 后端 / 实现 | 无 profiler 样本数 | 中位耗时 | 最小–最大 | 中位状态更新吞吐 |
|---|---:|---:|---:|---:|
| Heap | 5 | 56.425 s | 49.266–65.413 s | 36.30 M/s |
| ForL0 优化前 | 3 | 44.166 s | 43.336–45.529 s | 46.37 M/s |
| ForL0 优化后 | 5 | 39.176 s | 36.991–41.727 s | 52.28 M/s |

ForL0 中位耗时下降 **11.3%**，状态更新吞吐提高 **12.7%**。
相对本轮 heap 中位数，优化后 ForL0 吞吐高 **44.0%**。
heap 波动较大，样本数也有限；这不是对所有 Flink 负载的性能承诺。

ForL0 原始样本（秒）：优化前 `[44.166, 43.336, 45.529]`；优化后 `[40.631, 39.176, 36.991, 41.727, 38.015]`。
Heap 原始样本（秒）：`[65.413, 52.958, 62.623, 56.425, 49.266]`。

## 保留的优化

在状态注册时解析并保存稳定的 native StateTable 指针。ValueState JNI 不再为每次
状态访问重新查找 StateEngine 的状态 ID 注册表。StateTable 对象由 unique_ptr 持有，
注册表扩容与 SwissTable 内部重分配不会移动该对象；原有 CoW 和快照逻辑不变。

两台 TM 的 CPU profile：优化前 40993 个样本，注册表查询占
2.95%；优化后 37414 个样本，
该查询路径占 0.00%。完整分配与 CPU 火焰图见 `profiles/`。
这轮没有进行语法式 Java 11 重写。

另做了整数 hash 混合候选微基准：查找有所改善，但建表从约 0.37 秒增至约 1.20 秒，
因此没有把该候选用于生产路径。微基准不计入上述 Flink 成绩，记录见 `hash-probe.log`。

## 正确性

- 138 个 native 测试通过，新增 cached pointer 的注册表扩容、rehash、snapshot 测试。
- 23 个 Java 配置 / 状态语义测试通过，使用本次重新编译的 native 库。
- 5 个 benchmark 回归测试通过，覆盖源键空间、两种增量实现和独立计数 oracle。
- heap / ForL0 各跑 100 万条、每条 16 次增量，逐键对照独立 HashMap 计数，全通过。
- 单独的 ForL0 作业启用 3 秒 checkpoint，成功完成 checkpoint；不把其时间用于性能比较。
- native round-trip 覆盖了 checkpoint 编解码；未把 Flink checkpoint 创建等同于故障恢复验证。

## 复现与证据

容器默认开发命令（已固定 Long 大状态负载）：

```bash
cd /root/forL0-state-backend
bash docker/briskstate-dev.sh wordcount --backend heap
bash docker/briskstate-dev.sh wordcount --backend forl0
# 单独采样，不纳入计时中位数
bash docker/briskstate-dev.sh wordcount --backend forl0 --profile
```

完整命令、Job ID、原始指标、全部计时样本和验证文件保存在本目录 `raw/`、
`correctness/`、`tests/`；`summary.json` 可以机器读取。回切脚本和日志记录在
`/root/briskstate-setup/`。最终安装的是 cached-table 版本，集群保持运行。

TM RSS 包含 JVM 在此前作业后保留的内存，不应作为两个后端的 live state 内存比较。
采样作业与正式计时分开，CPU/内存采样只是辅助证据。多次同键更新的 workload
有意突出状态快路径；不能据此外推到 String、泛型序列化或 SQL 的全部场景。
