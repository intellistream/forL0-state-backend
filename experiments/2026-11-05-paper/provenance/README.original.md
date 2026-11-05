# 实验材料入口

完整分析见 [REPORT.md](REPORT.md)。本轮包括210次主实验、56次独立CPU归因采集，以及13项操作×3后端×2fork的状态微基准。Heap/ForL0微基准复用冻结R30数据，RocksDB为补测；RocksDB不进入端到端主矩阵。

- [35个条件及置信区间](analysis/conditions.json)、[210次主运行分析](analysis/formal-runs.json)、[逐次CSV](analysis/runs.csv)。
- [56次归因分析](analysis/profiles-runs.json)，原始JFR、collapsed及HTML火焰图在 `profiles/` 各运行目录。
- [三后端微基准](analysis/micro-results.json)、[78个入选fork](analysis/micro-forks.csv)。完整原读取/长写/长Map读取结果及进程绑定证据分别保存在对应micro目录；早期短窗口写操作仅为稳定性诊断，未与最终长窗口分数混合。
- [实际驻留内存](analysis/memory-resident-summary.csv)、[GC/Java堆驻留归因](analysis/memory-mechanism.json)。
- [执行协议](PROTOCOL.md)、[随机顺序和参数](plan.json)、[交付核查](QUALITY_REVIEW.json)、[文件哈希清单](MANIFEST.sha256)。

主实验均为相同作业JAR与标准状态API；每次运行保存command/config/VM.flags、GC日志、源/终端统计、状态增量投影校验和CPU/内存采样。`formal/` 与 `profiles/` 分别为主计时和独立诊断，不能合并为五次主重复。

图表同时提供PNG、SVG、PDF。RSS/PSS/USS衡量进程实际驻留页；GC后Java heap-used不包括ForL0原生状态。报告明确列出部分Heap窗口的下降趋势、连续REST JVM指标缺口、共享宿主机和合成数值负载的适用范围，不将它们掩盖为已经证明的严格稳态或通用性能结论。

`reproduction/` 保存冻结的后端、native二进制、公共benchmark JAR和相关源码/采集支持；`steady-job/` 保存本轮共同测量作业与provenance。脚本中的绝对路径对应原容器环境；在新环境复现需要相同Flink/JDK/ARM依赖并调整部署路径。现有运行目录会被控制器识别为已完成，不应把直接再次启动脚本视为一次新的独立重复。

校验全部材料可在本目录执行 `sha256sum -c MANIFEST.sha256`。完整交付包保留原始采样工件；不需要额外改变年轻代配置来读取或复现本报告的原始主矩阵。
