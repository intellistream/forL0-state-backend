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
| 根目录入口 | `bash ./run-l0-benchset` 路由，自动解包/校验、启动/关闭和文本结果汇总 | 通过 |
| CPU限制 | 逐进程/线程检查绑定掩码，包含Java/GC/JIT/native线程 | 未发现越界 |
| 作业正确性 | source/sink记录数、签名、源前缀重放与feature投影、拓扑/并行度、CP=0 | 成功运行均通过 |

验收过程中修复了L0统计目录未创建的问题；同时分开收集stdout/stderr，避免Java/native并发日志交错造成文本启用证明误判。正式判断采用与实际TM PID对应的结构化模式/容量/驻留池统计。

最初误将验收扩大为完整计时矩阵；按用户纠正已主动停止，不再继续。此前完成的VS运行和中止记录保留在文本记录内，**不充当真实L0实验结果或最终论文数据**。补齐剩余workload后仅做短流程验证。

`records/` 保存上述流程验收的原始文本结果、状态和oracle记录；不保留诊断压缩包。`native-tests-final.log`、`resident-probe.log`、`resident-shared.log` 保存native/真实状态验证。

真实硬件的SDK和净吞吐收益仍需目标机器确认。2026-11-12脚本改为固定共享2/4/8/16MiB四档，原表中的三档验收保留为历史记录。驱动容量元数据现在只作参考，缺失不会阻止测试；各档分配失败独立记录，其余档位及基线继续。

## 四档与部分成功流程验证（2026-11-12）

只做两个WC短流程，未运行正式矩阵。每个实际启动的Job使用4096keys、2s预热、5s测量；启动仍为JM4核8GiB、两个TM各4核32GiB。

| 场景 | 结果 |
|---|---|
| DRAM验收，显式模拟4MiB可用容量，选择2/16MiB两档 | 2MiB双进程读写预检和实际Job通过；16MiB预检失败、跳过；报告含1个成功条件及1个跳过条件 |
| 实际无L0设备/SDK且无容量元数据，选择无L0及全部四档 | 无L0实际Job通过；四个L0档位逐项记录跳过；报告含1个成功条件及4个跳过条件 |

两个场景均正常生成 `RESULTS.md`、`results.csv`、`results.json`，最终状态为 `partial_results`，退出码为1以提示存在未完成条件。原始文本报告及终端日志在 `fixed-tiers-20261112/`。这些结果验证失败隔离与汇总流程，不是L0硬件性能数据。

## 自动后台与单JSON输出（2026-11-12）

当前入口已改为 `./run-l0-benchset` 自动后台提交，六档共享2/4/8/16/24/32MiB，默认168个条件。上面的目录式报告属于历史验证；新结果目录每次只有一个JSON，中间记录转入私有运行缓存。Job、后端及运行时二进制保持不变。

本轮仅验证运行流程，没有执行完整性能矩阵：

| 场景 | 已观察结果 |
|---|---|
| 真正通过根入口后台提交，启动SSH会话结束，独立网络namespace仅loopback；WC4096keys，预热2s、计数5s；DRAM模拟可用4MiB，选择2/24/32MiB | 后台完成；2MiB实际Job和oracle通过，24/32MiB跳过；只生成一个JSON，status为partial_results，1成功/2跳过 |
| 已有任务时立即再次提交同一请求 | 中文提示已有任务，显示准备阶段及已有结果路径，没有重复启动 |
| keys=3的启动参数错误 | 不启动JVM；生成单JSON，status为failed，中文说明keys必须为8的正倍数；--status显示同一原因 |
| --stop | 保存stopped状态及当前interrupted条件，停止本次JM/TM；检查未发现本次运行残留Flink进程 |

重复启动检查首次碰上前一短测已结束，误提交了默认矩阵；立即通过--stop停止在首个VS条件初始化阶段，没有运行完整矩阵。中止JSON保留，不能作为性能数据。之后用无效参数请求完成受控的重复启动验证。

`background-20261112/`仅保存上述三个JSON数据文件。显示中的中止计数随后补充到进度文案；仅改变显示和summary字段，没有再次启动作业。

## 可操作错误诊断（2026-11-12）

补充错误编号、原因/不确定性、处理步骤、应由谁处理和最近问题；预检失败将设备/SDK检测与两个探测进程日志末尾纳入同一JSON，运行失败补充提交日志及可取得的Flink异常。资源错误显示实际可用值，SDK检测分别核对设备和库并保留检测返回码。

本轮未启动Flink作业，仅检查错误路径：

| 验证 | 结果 |
|---|---|
| 通过根入口提交keys=3 | INVALID_ARGUMENT；--status显示修改参数/默认入口两条建议；JSON含guidance |
| 裸机实际缺少L0设备及SDK，仅选择L0 2MiB | L0_ENVIRONMENT_MISSING；区分管理员处理的驱动/设备及SDK路径问题；JSON含检测原始输出和last_issue |
| 独立临时空目录触发离线文件缺失，不改变真实payload | OFFLINE_FILES_INVALID；显示缺失文件真实路径和复制完整分支的建议；启动错误JSON可正常解析 |
| 用现有native源码中的四类原始错误文本核对分类 | dlopen→SDK_LOAD_FAILED；dlsym→SDK_ABI_MISMATCH；cache_tuner_init→L0_SDK_INIT_FAILED；l0_mem_alloc→L0_ALLOCATION_FAILED |

`error-diagnostics-20261112/`保存三份实际错误JSON。后两种SDK调用失败明确不把根因直接认定为容量不足。此分类是排查线索，不承诺能从厂商简略错误中推导出唯一根因，也不承诺合作方可以自行修复驱动或代码问题。
