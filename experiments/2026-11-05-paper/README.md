# 2026-11-04—05 论文实验材料

[完整分析报告](REPORT.md) · [主实验摘要](MAIN_RESULTS.md) · [内存增长归因](MEMORY_EXPLANATION.md)

本目录收纳本轮的报告、图表、配置、统计、可读逐次原始数据和完整采样工件。主实验为7项workload×5档内存×Heap/ForL0×3次重复，共210次；另有56次独立CPU归因采集。微基准为13项操作×3后端×2fork，Heap/ForL0复用R30，RocksDB仅补微基准，不进入端到端主矩阵。

## 目录

- `REPORT.md`：环境、配置、参数、workload、全部条件结果、Memory Footprint和详细归因。
- `analysis/`：CSV/JSON、吞吐与内存曲线、GC/状态/其他CPU堆叠图、三后端微基准；图提供PNG/SVG/PDF。
- `formal/`、`profiles/`：逐次测量、源/终端统计、配置、VM.flags、GC日志、校验和HTML火焰图。
- `micro-reused/`、`micro-rocksdb*`：复用/补测的JMH原始结果及观察证据，最终入选文件列于 `analysis/micro-forks.csv`。
- `steady-job/`、`source-oracle/` 和分析脚本：实际测量作业源码、来源校验及报告生成代码；原始执行环境和绝对路径保留于记录中。
- `data/full-raw-data.tar.gz`：完整归档，包括JFR、线程采样、进程观测以及冻结源码和测量二进制，由Git LFS管理。
- `data/ARTIFACTS.json`、`data/SHA256SUMS`：完整包的大小、SHA-256及内容说明。

## 获取完整采样工件

在仓库根目录执行：

```sh
git lfs install
git lfs pull --include="experiments/2026-11-05-paper/data/full-raw-data.tar.gz"
cd experiments/2026-11-05-paper/data
sha256sum -c SHA256SUMS
```

可将完整归档解压到单独目录，然后在解压目录执行 `sha256sum -c MANIFEST.sha256`。本目录根部的 `MANIFEST.sha256` 是**完整归档内部**的清单，不能用它要求当前可读快照包含所有大体积二进制采样；当前目录由 `PACKAGE_SHA256SUMS` 校验。

`provenance/` 保留原始阅读包清单和说明。`READER_README.txt` 中的容器路径是采集/交付时的位置；仓库内的权威完整数据入口为 `data/full-raw-data.tar.gz`。

## 解释范围

报告明确列出22次Heap运行存在窗口内下降趋势、连续REST JVM指标采集缺口、共享宿主机以及合成Long状态压力适配等限制。RSS/PSS/USS反映进程驻留页，不等于有效状态数据大小。原始数据全部保留；自行追加后停止的年轻代/长窗口对照没有并入本轮报告数据。

本轮后端JAR和native库与已验证R30版本一致。归档用于复核既有观测，不应把有现成结果的控制器再次启动当成新的独立重复；在新部署路径复现时，需要调整采集脚本中的原容器绝对路径。
