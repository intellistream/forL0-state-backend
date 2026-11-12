# Benchmark set：Heap / BriskState 无L0 / 四档L0

**模式：REAL_L0_HARDWARE**

配置：JM4核8GiB；两个TM各4核32GiB；p8；均匀分布；feature width64/touches128；CP关闭。

吞吐单位为 input-equivalent records/s，按终端Sink窗口计数除以拓扑输出倍数；初始化不计入吞吐。

固定共享容量为2、4、8、16 MiB，每TM分配一半。预检逐档独立进行，失败档跳过，成功档及基线继续。

已处理5组：成功1，运行失败0，预检失败而跳过4。均值仅使用成功运行；未完成项不记为0。

| L0档位 | 共享MiB | 每TM MiB | 预检 |
|---|---:|---:|---|
| brisk_l0_2 | 2 | 1 | failed |
| brisk_l0_4 | 4 | 2 | failed |
| brisk_l0_8 | 8 | 4 | failed |
| brisk_l0_16 | 16 | 8 | failed |

| Workload | BriskState 无L0 | L0 2 MiB | L0 4 MiB | L0 8 MiB | L0 16 MiB |
|---|---:|---:|---:|---:|---:|
| WC | 767,467.00 (n=1) | — | — | — | — |

| Workload | L0 2 MiB / 无L0 | L0 4 MiB / 无L0 | L0 8 MiB / 无L0 | L0 16 MiB / 无L0 |
|---|---:|---:|---:|---:|
| WC | — | — | — | — |

## 失败及跳过记录

| Workload | 组别 | 重复 | 状态 | 原因 |
|---|---|---:|---|---|
| WC | brisk_l0_2 | 1 | skipped | RuntimeError: 找不到已安装的L0设备/SDK；见 l0-preflight/detection.log。 |
| WC | brisk_l0_4 | 1 | skipped | RuntimeError: 找不到已安装的L0设备/SDK；见 l0-preflight/detection.log。 |
| WC | brisk_l0_8 | 1 | skipped | RuntimeError: 找不到已安装的L0设备/SDK；见 l0-preflight/detection.log。 |
| WC | brisk_l0_16 | 1 | skipped | RuntimeError: 找不到已安装的L0设备/SDK；见 l0-preflight/detection.log。 |

所有成功和失败运行均保留；不根据加速比筛除数据。原始逐轮数据见 results.csv/results.json，日志和配置见 raw/。

CPU限制为整个进程及其Java/GC/JIT/native线程的affinity；32GiB为Flink process预算，非OS硬RSS上限。
