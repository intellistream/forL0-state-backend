# Benchmark set：Heap / BriskState 无L0 / 四档L0

**模式：DRAM_VALIDATION_ONLY**

配置：JM4核8GiB；两个TM各4核32GiB；p8；均匀分布；feature width64/touches128；CP关闭。

吞吐单位为 input-equivalent records/s，按终端Sink窗口计数除以拓扑输出倍数；初始化不计入吞吐。

固定共享容量为2、4、8、16 MiB，每TM分配一半。预检逐档独立进行，失败档跳过，成功档及基线继续。

已处理2组：成功1，运行失败0，预检失败而跳过1。均值仅使用成功运行；未完成项不记为0。

| L0档位 | 共享MiB | 每TM MiB | 预检 |
|---|---:|---:|---|
| brisk_l0_2 | 2 | 1 | passed |
| brisk_l0_16 | 16 | 8 | failed |

| Workload | L0 2 MiB | L0 16 MiB |
|---|---:|---:|
| WC | 823,594.40 (n=1) | — |

## 失败及跳过记录

| Workload | 组别 | 重复 | 状态 | 原因 |
|---|---|---:|---|---|
| WC | brisk_l0_16 | 1 | skipped | RuntimeError: 仅DRAM验收：固定档位超过显式设置的模拟可用容量 4 MiB。 |

所有成功和失败运行均保留；不根据加速比筛除数据。原始逐轮数据见 results.csv/results.json，日志和配置见 raw/。

CPU限制为整个进程及其Java/GC/JIT/native线程的affinity；32GiB为Flink process预算，非OS硬RSS上限。

**此结果仅为普通内存下的离线流程验收，开启列实际使用DRAM驻留池，不能作为真实L0加速结果。**
