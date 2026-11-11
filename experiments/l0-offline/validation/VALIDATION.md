# 离线流程验收记录（2026-11-11）

目标是验证离线包可执行和结果正确，不替合作方重跑完整性能实验。

裸机：Kunpeng 920 / aarch64，Ubuntu 22.04，Linux 5.15.0-25。使用独立network/mount namespace，只保留loopback、无外部路由；屏蔽宿主机Python、Java/Maven和原有Flink/JDK目录。所有运行依赖来自校验后的离线包。**本机没有L0硬件，所有开启组明确标记为 `DRAM_VALIDATION_ONLY`。**

| 验证项 | 覆盖 | 结果 |
|---|---|---|
| 原生状态语义测试 | 148项既有native测试 | 通过 |
| 真实MapState | RowCacheProbe：get/put、remove、Long.MIN_VALUE、putAll、类型提升、迭代、clear、快照 | 通过 |
| 共享且已满的驻留池 | SharedRowProbe：两个实际MapState共享单set，驻留满后的访问与快照 | 通过 |
| WC短验证 | 4096keys；Heap、无L0、三档L0；模拟共享容量12MiB，对应3/6/9MiB | 五组通过 |
| VS | Heap短验证通过；无L0和三档L0的已完成运行通过独立oracle | 通过 |
| FD/SD/TM/LG/LR短验证 | 4096keys，2s预热/5s计数，每项一次；三档分配预检，作业使用75%档 | 五项通过 |
| 根目录入口 | `bash ./run-l0-benchset` 路由，自动解包/校验、启动/关闭、汇总、生成tar.gz | 通过 |
| CPU限制 | 逐进程/线程检查绑定掩码，包含Java/GC/JIT/native线程 | 未发现越界 |
| 作业正确性 | source/sink记录数、签名、源前缀重放与feature投影、拓扑/并行度、CP=0 | 成功运行均通过 |

验收过程中修复了L0统计目录未创建的问题；同时分开收集stdout/stderr，避免Java/native并发日志交错造成文本启用证明误判。正式判断采用与实际TM PID对应的结构化模式/容量/驻留池统计。

最初误将验收扩大为完整计时矩阵；按用户纠正已主动停止，不再继续。此前完成的VS运行和中止记录保留在诊断包内，**不充当真实L0实验结果或最终论文数据**。补齐剩余workload后仅做短流程验证。

`validation-diagnostics.tar.gz` 保存上述原始日志、命令、配置、oracle及结果。`native-tests-final.log`、`resident-probe.log`、`resident-shared.log` 保存native/真实状态验证。

真实硬件的SDK、驱动容量元数据和净吞吐收益仍需目标机器确认。脚本支持main分支文档中的容量参数；未知驱动若不暴露可信容量信息，将自动保存诊断并停止，不申请满容量来猜测，也不冒充已得到硬件加速。
