# L0硬件离线测试

合作方取得 briskstate_workspace 分支的完整仓库后，在仓库根目录执行：

```bash
./run-l0-benchset
```

脚本自动在后台运行，启动后立即返回，可以退出SSH；无需手动使用nohup、重定向或安装运行环境。启动信息给出本次结果JSON的完整路径。每次测试只在结果目录生成一个JSON文件，不再生成CSV、Markdown报告、压缩包或独立原始数据目录。

查看当前阶段、已完成数量和上一轮结果：

```bash
./run-l0-benchset --status
```

需要停止时：

```bash
./run-l0-benchset --stop
```

停止后关闭本次JM/TM，保留已完成数据。已有后台任务时再次执行入口只显示其进度，不会重复启动。机器重启后不会自动恢复实验；没有断点续跑功能。

## 八组配置

| 组别参数 | 后端 | 两TM共享L0容量 | 每TM容量 |
|---|---|---:|---:|
| heap | Heap | 关闭 | — |
| brisk_off | BriskState | 关闭 | — |
| brisk_l0_2 | BriskState | 2 MiB | 1 MiB |
| brisk_l0_4 | BriskState | 4 MiB | 2 MiB |
| brisk_l0_8 | BriskState | 8 MiB | 4 MiB |
| brisk_l0_16 | BriskState | 16 MiB | 8 MiB |
| brisk_l0_24 | BriskState | 24 MiB | 12 MiB |
| brisk_l0_32 | BriskState | 32 MiB | 16 MiB |

六档逐档独立验证，不搜索硬件最大容量，也不把失败档缩小后冒充原容量。大档失败不影响小档、Heap和无L0组。驱动容量信息只作参考，缺失不会阻止固定档位验证。

设备/SDK缺失时，L0组记录跳过原因，基线继续。预检同时建立两个进程的SDK内存池，逐缓存行读写后释放；实际作业还会核对真实TM的L0模式、容量和驻留池，不能把普通内存回退记为硬件启用。

旧main脚本列出的容量为0/2/4/8/12/16/20/24 MiB；当前按实验要求扩为2/4/8/16/24/32 MiB。这些数值是测试请求量，不是对实际可用硬件容量的保证。

## 机器要求和实验协议

需要Linux ARM64 / Ubuntu22.04，与验收裸机一致；需要同一NUMA节点内至少12个可用CPU、约76GiB可用内存、可写的仓库结果目录和数GiB运行空间。硬件测试还需已安装并有权限使用L0驱动和libl0mempool.so；脚本不会修改驱动、BIOS或内核。

Java、Flink、Python及其依赖和编译产物使用仓库内普通Git分片提供，无需Release/LFS下载，不访问外网。SDK查找沿用main的l0_detector.sh：设备/dev/l0或/dev/hisi_l0；库从LD_LIBRARY_PATH、ldconfig、常见目录及/opt、/usr/local、HOME有界搜索。只使用loopback上的Flink通信。

| 项目 | 参数 |
|---|---|
| 运行方式 | 直接启动1个JM JVM和2个TM JVM，不使用Docker；每个条件重新启动 |
| Flink / Java | Flink1.20.3 / BiSheng OpenJDK11.0.32+9 |
| CPU | JM、TM1、TM2分别绑定不同的4核，包含GC/JIT/native线程 |
| 内存 | JM8GiB；每TM32GiB process memory，4 slots |
| Heap / BriskState | task off-heap分别为0 / 8GiB；managed0；network512MiB |
| workload | WC、FD、SD、TM、LG、VS、LR |
| 状态与访问 | 均匀524288实体key，feature width64、touches128、window16 |
| 并行度 / CP | p8，Global ACD保持1；checkpoint关闭 |
| 单轮 | 全key初始化及排空 → 预热60s → 测量120s → 排空、正确性核验 |
| 重复 | 每条件3次；打乱workload、轮换后端顺序 |
| 吞吐 | Sink窗口记录数 ÷ 拓扑输出倍数 ÷ 测量秒数，单位输入记录/秒 |

默认计划7×8×3=168个条件。全部运行时仅预热与计数共8小时24分钟，另有初始化和启动等耗时。预检失败的条件记录为skipped，不消耗对应的完整测量窗口。

八组使用同一个后端无感知Job，标准MapState.get/put。Job SHA-256为7f6929397ca2187d14b26ceb52ce1dd1a7ab7e276738b9606e45a65f3d337c15。32GiB是Flink process预算，CPU通过进程affinity绑定；不把它解释为OS硬RSS限制。

## 进度与结果

启动后显示后台进程号、六档容量、进度/停止命令和JSON路径。--status用中文显示当前workload、后端、重复编号、初始化/预热/测量/核验阶段及已完成数量。预热和测量阶段显示本阶段剩余秒数，约每5秒更新；不估算整套实验的剩余时间。

默认结果路径：

```text
experiments/l0-offline/results/时间戳-进程号.json
```

JSON启动时即建立，每完成一个条件原子更新一次；程序结束自动写入最终状态。结果只包含结构化数据，不含报告文字或图表：

| 字段 | 数据 |
|---|---|
| schema_version / status / mode | 格式版本、整体状态、硬件实验或DRAM验收 |
| started_ms / finished_ms | 开始与结束时间，Unix毫秒 |
| counts / plan | 成功、失败、跳过数量和实验计划 |
| environment / artifacts | 环境、实验参数、CPU分配、二进制哈希 |
| l0_sdk / l0_capacity / l0_preflight | SDK、容量依据、各档读写预检及失败原因 |
| runs | 每轮吞吐、计时、状态规模、逐秒计数、内存、正确性、实际配置和L0证明 |
| summary | 各条件有效重复数、平均吞吐、相对无L0加速比 |
| error | 无法继续或手动停止的原因 |

status为complete表示全部成功；partial_results表示部分成功，失败和跳过项仍完整保留；failed表示无法完成；stopped表示用户停止。缺失吞吐为null，不写成0。只有通过核验的轮次进入均值，valid_repeats明确实际样本数。

PSS/RSS为两个TM的合计，不含JM。PSS为窗口采样均值，RSS为窗口采样峰值。失败轮次包含错误信息、堆栈和相关日志末尾，合作方只需返回这一份JSON。完整诊断日志和中间文件保存在内部运行缓存，不进入结果目录；--status显示其位置。

入口返回0只表示后台任务已经提交，最终成败以JSON的status为准。依赖损坏、CPU/内存不足等全局问题会写入失败状态；L0单档问题只影响对应条件。若结果目录不可写或磁盘已满，需按启动/诊断提示处理。

## 出错后如何处理

--status会持续显示最近一次问题、影响范围、错误编号、原始提示、处理建议和应由谁处理；进入下一个条件后不会把上一个问题隐藏。结果JSON的error、l0_preflight或失败run含guidance：error_code、cause、next_steps、responsible_party。预检失败附设备/SDK检测及双进程日志；作业失败附提交日志、JM/TM日志末尾及可取得的Flink异常。启动环境失败也把错误末尾和处理建议写入同一JSON。

| 情况 | 合作方如何处理 |
|---|---|
| INVALID_ARGUMENT | 按提示改参数，或直接使用默认入口 |
| L0_DEVICE_MISSING / L0_ENVIRONMENT_MISSING | 管理员检查驱动、设备节点及账号权限；若同时缺SDK，提示会一并说明 |
| L0_SDK_MISSING | 确认SDK已安装；可用export L0_MEMPOOL_LIB_HOST_PATH=/实际路径/libl0mempool.so指定 |
| ACCESS_DENIED | 管理员检查提示路径的访问权限，不会自动修改权限或驱动 |
| SDK_LOAD_FAILED / SDK_ABI_MISMATCH | 按原始错误核对依赖、库路径、架构及符号；接口不兼容需SDK提供方或开发者协助 |
| L0_ALLOCATION_FAILED / L0_SDK_INIT_FAILED | 单档自动跳过；若最小档也失败，管理员检查同NUMA域占用、容量、权限和驱动；不把所有失败都认定为容量不足 |
| HOST_MEMORY_LOW / CPU_ALLOCATION_INSUFFICIENT / DISK_SPACE_LOW | 显示实际资源或路径，释放其他占用或请管理员分配足够资源；保持正式实验配置 |
| VALIDATION_FAILED / UNCLASSIFIED_FAILURE | 返回本次JSON给开发者定位 |

常见参数、路径和资源问题可按提示处理；驱动、SDK内部错误及后端正确性问题不能保证由合作方自行修复。无法可靠判断时明确说明不确定，并保留原始证据。修复后重新执行会建立新JSON，原失败记录保留；如仅需补测某组，可使用--arms筛选。

## 可选参数

```bash
# 短测WC，仍为后台运行；4096keys、预热2s、测量5s、每组一次
./run-l0-benchset --smoke --workloads wc

# 正式规模，每条件一次
./run-l0-benchset --repeats 1

# 仅运行指定条件
./run-l0-benchset --workloads wc,fd --arms heap,brisk_off,brisk_l0_2

# 指定一个尚不存在的JSON结果文件
./run-l0-benchset --results /path/my-l0-results.json

# 无L0机器验收，明确标为DRAM_VALIDATION_ONLY
./run-l0-benchset --validate-dram --smoke --workloads wc

# 模拟容量不足，用于验证失败隔离；不是真实硬件容量或性能
./run-l0-benchset --validate-dram --validation-capacity-mib 4 --smoke --workloads wc
```

## L0驻留策略与适用范围

同一共享L0Cache中的resident策略收纳VoidNamespace下的Long/Long MapState完整64元素dense行。池满后停止准入，目录头直接指向L0或DDR，避免均匀大状态流的反复换入换出；其他状态继续原路径。六档使用相同后端二进制，只改变容量。

该策略只填一轮；快照会写回DDR，封闭的池不会重新填充，性能结论限定为本次CP关闭场景。准入关闭后的统计不能解释为稳态或硬件命中率。真实L0净吞吐收益仍须目标硬件确认，脚本不根据加速比筛选原始数据。
