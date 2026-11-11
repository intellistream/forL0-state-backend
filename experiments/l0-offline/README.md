# L0 硬件离线 benchmark set

在仓库根目录运行：

```bash
bash ./run-l0-benchset
```

无需安装 Java、Flink、Python、Maven、CMake、Docker，也不会访问网络。运行包使用普通 Git 分片保存；不从 Release 或 LFS 下载运行时。适用环境为 Linux ARM64 / Ubuntu 22.04，与验收裸机一致。系统需已有可用的 L0 内核驱动、用户态 `libl0mempool.so`，以及访问设备的权限；脚本不会修改 BIOS、内核或驱动参数。

脚本依次完成硬件识别、容量读取及验证、105次正式运行、结果汇总和打包。终端会打印结果目录和可直接回传的 `.tar.gz` 路径。结果目录默认位于本目录的 `results/时间戳/`。每次运行新建目录，不覆盖历史数据。

## 五组配置

| 组别 | 状态后端 | L0 |
|---|---|---|
| `heap` | Heap / HashMapStateBackend | 关闭 |
| `brisk_off` | BriskState / ForL0StateBackend | 关闭 |
| `brisk_l0_25` | 同一 BriskState 二进制 | 自动确定预算的25% |
| `brisk_l0_50` | 同一 BriskState 二进制 | 自动确定预算的50% |
| `brisk_l0_75` | 同一 BriskState 二进制 | 自动确定预算的75% |

容量均为**两个TM共享总量**，每TM一半。**不分配或探测100%容量，也不假定一定有70MiB。**

容量来源优先为厂商文档中的驱动参数 `/sys/module/hisi_l0_mem_pool/parameters/max_numa_capacity`（每NUMA节点上限，文档单位MB，按MiB读取）。两个TM位于同一NUMA节点，不把整机各NUMA节点容量相加。该参数是驱动配置上限，不能冒充芯片物理容量。另支持驱动直接暴露的共享 `capacity_bytes` / `total_capacity_bytes` 字段。无法读取可信容量时会保存诊断并退出，不通过申请满容量来猜测。

脚本最多以该上限的75%同时建立两个TM大小的SDK内存池，并逐缓存行读写全部申请区域。若当前可用空间不足，会缩小实验预算，再按25/50/75%确定三档。三档会在开始主实验前逐一验证；之后全套实验使用固定容量。请求字节数、驱动容量原文、预算缩小过程、实际池容量均保存，汇总表直接标注实际MiB，避免把预算百分比误写成物理容量百分比。

SDK自动搜索方式沿用 `main` 分支的 `docker/lib/l0_detector.sh`：`/dev/l0` 或 `/dev/hisi_l0`；`LD_LIBRARY_PATH`、ldconfig、常见库目录以及 `/opt`、`/usr/local`、HOME 下的有界搜索。native使用检测到的SDK绝对路径。不会用普通内存伪装硬件启用，也不会在启用失败后继续记为开启L0。

## 与主 benchmark set 相同的实验协议

| 项目 | 参数 |
|---|---|
| Flink / Java | Flink 1.20.3 / BiSheng OpenJDK 11.0.32+9 |
| 集群 | 1 JM，2 TM；同一NUMA节点内选取12个可用CPU |
| JM | 4核，8GiB process memory |
| 每TM | 4核，32GiB process memory，4 slots |
| CPU约束 | JM、TM1、TM2分别绑定不同的4核；进程内Java、GC、JIT、C++线程全部继承并被检查 |
| 内存配置 | Heap task off-heap=0；BriskState=8GiB；managed=0；network=512MiB |
| 并行度 | 8；原拓扑Global ACD保持1 |
| 状态规模 | 524,288个Long实体key；均匀分布 |
| 状态访问 | feature width=64，touches=128，window=16；标准MapState.get/put；Long增量1,000,000,007 |
| 拓扑 | WC、FD、SD、TM、LG、VS、LR；保持主实验任务/边及输出倍数 |
| 时序 | 全key覆盖初始化及排空 → 60s预热 → 120s计数 → 排空、正确性核对 |
| 重复 | 每workload每组3次，独立JVM；打乱workload顺序并轮换五组顺序 |
| CP / 重启 | checkpoint关闭；失败不自动重启 |
| 吞吐 | 终端Sink窗口计数 ÷ 拓扑输出倍数 ÷ 120s，单位input-equivalent records/s |
| 正确性 | 记录数/签名、独立源前缀重放和feature投影、拓扑契约、CP=0 |

五组使用**同一个后端无感知Job JAR**。Job与原主实验相同，SHA-256为 `7f6929397ca2187d14b26ceb52ce1dd1a7ab7e276738b9606e45a65f3d337c15`；仅Flink状态后端及L0配置不同。初始化耗时单独报告。32GiB为Flink的process预算，沿用主实验口径，不冒充OS对整个进程的硬RSS限制。

宿主机需至少同一NUMA节点内12个可用CPU、约76GiB可用内存，以及解包与运行所需磁盘空间。CPU不要求与原机器编号相同，脚本自动选择并保存实际编号。采用私有动态加载器和依赖库，不替换系统glibc。

## L0 驻留策略

此离线版本在已有共享L0Cache内增加可选的 `resident` 行驻留策略。它适用于VoidNamespace下的Long/Long MapState，优先收纳完整的64元素dense行；普通MapState API、其他宽度、稀疏/通用状态继续使用原有路径。

驻留池填满后停止准入，不在均匀大状态流中反复换入换出。MapState的原生目录头直接指向驻留行，后续标准get/put访问该地址。准入关闭后，Java解析恢复为与关闭L0相同的目录路径，目录地址指向L0或DDR；未驻留状态没有逐记录JNI换入、复制或替换开销。clear、结构变化、类型提升和快照继续受原有epoch/快照协议约束；快照会将驻留值写回DDR。checkpoint关闭是本实验设置，不是“忽略快照正确性”。

原有替换策略保留；离线实验通过 `FORL0_L0_ROW_POLICY=resident` 显式选择驻留策略。它属于共享L0Cache机制，仍由每TM的同一个SDK池及总配额管理。统计日志主要记录准入阶段，准入关闭后的计数**不能解释为稳态状态访问命中率或硬件命中率**。

此驻留策略只填一轮；已关闭准入的池在快照写回后不重新填充。因此本次收益测试限定为checkpoint关闭的benchmark set，不能推广为checkpoint场景的性能结论。

该策略减少了旧行缓存的未命中开销，并保留L0直接访问机会。真实L0的净吞吐收益仍必须由目标硬件上的本实验确认；原始结果全部保存，脚本不会为了得到正加速而更改Job、重跑挑选或过滤慢样本。

## 返回结果

| 文件 | 内容 |
|---|---|
| `RESULTS.md` | 七项workload、五组吞吐均值，以及三档L0相对无L0的比值 |
| `results.csv` / `results.json` | 每轮原始吞吐、初始化时间、实际容量、PSS/RSS和核验状态 |
| `plan.json` / `status.json` | 实际运行顺序和整体完成状态 |
| `environment/` | CPU、系统、JDK、配置协议、二进制哈希 |
| `l0-preflight/` | SDK/驱动检测、容量依据、双进程读写证明及失败记录 |
| `raw/每条件/` | 最终Flink配置、命令、计时窗口、逐秒记录数、日志、GC日志、进程采样、独立oracle、L0启用证明 |
| `结果目录名.tar.gz` | 自动打包，回传此一个文件即可 |

个别条件失败时保留现场并继续其他条件，最终以非零状态退出，不把缺失项写成0吞吐。启动前的硬件问题会产生 `fatal-error.json` 和结果包。每个条件的五组有相同状态规模和访问算法；更快的组在相同时间内处理更长的输入前缀，最终oracle按实际前缀核验。

PSS/RSS字段为**两个TM进程的合计**，不含JM。PSS在计时窗口内取样本均值，RSS报告窗口采样峰值；原始逐进程采样另行保留。

## 可选操作

默认命令已经完成正式全套测试。以下仅供验收或定位问题，无需合作方预先设置：

```bash
# 先在真L0机器上短测七项，仍验证三档及Heap/无L0
bash ./run-l0-benchset --smoke

# 只运行一次完整矩阵；其余仍为正式状态规模与60s/120s窗口
bash ./run-l0-benchset --repeats 1

# 在无L0机器上验收离线流程；所有结果明确标为DRAM_VALIDATION_ONLY
bash ./run-l0-benchset --validate-dram --validation-capacity-mib 12 --smoke
```

普通内存验收不代表真实L0硬件实验，不能用其结果宣称L0加速。
