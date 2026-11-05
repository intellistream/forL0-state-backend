#!/usr/bin/env python3
import argparse,json,statistics
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--base',type=Path,required=True);p.add_argument('--optimization',type=Path);a=p.parse_args();b=a.base
rows=json.loads((b/'summary.json').read_text());profiles=json.loads((b/'analysis/decomposition.json').read_text())['runs'];names=['wc','fd','sd','tm','lg','vs','lr']
def get(w,backend):return next(r for r in profiles if r['workload']==w and r['backend']==backend)
def timing(w,backend):return [r['seconds'] for r in rows if r['workload']==w and r['backend']==backend and not r['profiled']]
def fmt(values):return f"{statistics.median(values):.3f} ({min(values):.3f}–{max(values):.3f}; n={len(values)})"
formal_table=[]
formal_file=b/'analysis/formal-counters.json'
if formal_file.exists():
 formal=json.loads(formal_file.read_text())['runs']
 formal_table=['## 无 profiler 的 GC 核对','','下表直接使用正式计时区间的进程/线程 CPU 计数，每项为两次测量的中位数；不依赖火焰图。GC 仅指 GC 工作线程，状态路径另由后面的 profile 估计。','','| Workload | 总 CPU Heap / ForL0 s | GC CPU Heap / ForL0 s | GC 占减少 CPU 的比例 | 平均使用核数 Heap / ForL0 |','|---|---:|---:|---:|---:|']
 for w in names:
  d={backend:{k:statistics.median(r[k]for r in formal if r['workload']==w and r['backend']==backend)for k in ['total_cpu_seconds','gc_worker_cpu_seconds','average_cores']}for backend in ['heap','forl0']}
  h,f=d['heap'],d['forl0'];share=(h['gc_worker_cpu_seconds']-f['gc_worker_cpu_seconds'])/(h['total_cpu_seconds']-f['total_cpu_seconds'])
  formal_table.append(f"| {w.upper()} | {h['total_cpu_seconds']:.2f} / {f['total_cpu_seconds']:.2f} | {h['gc_worker_cpu_seconds']:.2f} / {f['gc_worker_cpu_seconds']:.2f} | {share:.1%} | {h['average_cores']:.2f} / {f['average_cores']:.2f} |")
 formal_table += ['','两台 TM 的平均 CPU 使用合计均低于八核，ForL0 没有通过多用 CPU 获取这些收益。','']

lines=['# 同一标准 API job 的公平对比与后端优化（2026-11-03）','',
'Job 对 backend 无感知：同一 JAR、源数据、拓扑、状态描述符、访问语句和参数，只由 Flink 配置选择 Heap 或 ForL0。此前 job 分别调用扩展 API 和标准 API 的跨后端加速比已撤回；本报告只采用重新运行的标准 API 数据。','',
'## 正式耗时','',
'以下为不带 profiler 的 Flink REST duration，包含建表、JIT、网络和 GC。N 和全部样本范围随表给出；按用户要求提高输入量，使更快的 ForL0 也运行约一分钟或更长；两个后端的成对输入量始终相同。','',
'| Workload | 输入记录 | Heap 中位数 s（范围；N） | ForL0 中位数 s（范围；N） | 比值 |', '|---|---:|---:|---:|---:|']
for w in names:
 h=timing(w,'heap');f=timing(w,'forl0');n=get(w,'heap')['records']
 lines.append(f'| {w.upper()} | {n:,} | {fmt(h)} | {fmt(f)} | {statistics.median(h)/statistics.median(f):.2f}× |')
lines += ['', '![无 profiler 正式计时](analysis/unprofiled-timings.png)', '', '样本数量有限，不能把这些比值外推为任意 Flink 作业的性能保证。完整运行顺序和每次命令保留在 raw 数据中。','',
'## 共同 workload 与内部实现','',
'保留 [ICDE17 原文 Figure 5](https://www.comp.nus.edu.sg/~hebs/pub/shuhaoICDE17a.pdf) 七个应用的任务与边。使用数值状态适配原任务框架，非原始生产算法或原始数据集的完整复现。每个普通有状态算子整体使用至多 524288 个实体 key，每个 key 有 64 个 Long feature，并分布到 8 个并行子任务；每次输入执行 128 对标准 MapState.get/put，并有该任务角色所需的标准 ValueState.value/update。Global ACD 保留单全局聚合，feature width=4、touches=1。没有新增压力算子。','',
'feature 为连续小整数键，增量统一为 1,000,000,007，使值超出 boxed Long 缓存。这是两个后端共同执行的有利于 primitive 连续存储的访问设计。没有 ForL0 类型判断、反射、扩展 API 或分别选取状态表示。[Flink HeapMapState 标准路径](https://github.com/apache/flink/blob/release-1.20/flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/HeapMapState.java) 也不逐值序列化，收益不能归为省去了 Heap 本来就没有的序列化。','',
'实际共同循环见 [LongStateAccess.java](source/benchmark/benchset/src/main/java/org/apache/flink/benchmark/benchset/LongStateAccess.java)：','', '```java','Long old = state.get(slot);','long next = (old == null ? 0 : old) + 1_000_000_007L;','state.put(slot, next);','```','',
'ForL0 的优化全部位于后端：Long 读取减少 JNI 数组回写；小而密集的整数 Map 自动转为连续布局；完整连续行使用后端内部的直接内存访问，减少逐元素 JNI 往返。原生变更和 snapshot prepare 通过 epoch 使旧视图失效，snapshot 活跃时不提供直接写视图。checkpoint 本轮未启用，CP 性能留待单独测试。','',
'复测正式计时可使用仓库脚本，`--fresh-jvm` 保持每次从新 JVM 开始：','',
'```bash',
'python3 benchmark/scripts/run_benchset.py --backend all --touches 128 --rounds 2 --fresh-jvm',
'```','',
'本次实际 JAR、版本库、源码、逐次命令与采集脚本一并保留，复核原始结果使用 reproduction/benchmark.jar 与 runs/*/command.json。','',
*formal_table,
'## CPU 与 GC 归因','',
'下表来自单独的 CPU profiler 作业，不与上面的正式计时样本混合。单位为两台 TM 合计的 CPU 秒；状态路径是基于每线程调用栈比例的采样估计，GC 工作线程 CPU 来自 /proc 计数，其他是剩余项。状态路径包括访问循环、更新算术、分配及 mutator 写屏障等开销。这三类是互斥的 CPU 记账项，不是相互独立的因果机制。','',
'| Workload | Heap 状态 CPU | ForL0 状态 CPU | Heap GC CPU | ForL0 GC CPU | Heap 其他 CPU | ForL0 其他 CPU |', '|---|---:|---:|---:|---:|---:|---:|']
for w in names:
 h=get(w,'heap')['cpu'];f=get(w,'forl0')['cpu'];v=[h['state_estimated'],f['state_estimated'],h['gc_workers_measured'],f['gc_workers_measured'],h['other_estimated'],f['other_estimated']]
 lines.append('| '+w.upper()+' | '+' | '.join(f'{x:.2f}' for x in v)+' |')
lines += ['', '![CPU 工作量分解](analysis/cpu-work.png)','',
'这里的 CPU 占比不等于并行流水线中串行临界路径的墙钟占比。不能直接用“状态 CPU 节约秒数”减作业耗时。下式按 8 核归一化，作为 CPU 工作量与墙钟时间的记账关系，而非单因素因果干预：','',
'`T = (state_CPU + GC_CPU + other_CPU) / 8 + unused_capacity_equivalent`。最后一项包含启动、等待、负载不均和其他未使用容量。CPU 边界按 REST 开始/结束时间对 0.5 秒线程计数插值。','',
'| Workload | 减少总 CPU s | 其中 GC CPU s（占减少量） | 其中状态 CPU s（占减少量） | 减少 CPU / 8 s | profile 墙钟减少 s |', '|---|---:|---:|---:|---:|---:|']
for w in names:
 h=get(w,'heap');f=get(w,'forl0');total=h['cpu']['total']-f['cpu']['total'];gc=h['cpu']['gc_workers_measured']-f['cpu']['gc_workers_measured'];state=h['cpu']['state_estimated']-f['cpu']['state_estimated']
 lines.append(f"| {w.upper()} | {total:.2f} | {gc:.2f} ({gc/total:.1%}) | {state:.2f} ({state/total:.1%}) | {total/8:.2f} | {h['seconds']-f['seconds']:.2f} |")
h=get('vs','heap');f=get('vs','forl0');c=h['cpu']
lines += ['', f"以 VS 为例，Heap 的状态路径占总 CPU {c['state_fraction']:.1%}，GC 工作线程占 {c['gc_fraction']:.1%}。若只理想化消除状态工作、保持其他工作与并行效率不变，CPU 工作量模型的上限约 {c['state_only_amdahl_upper_bound']:.2f}×；同时理想化消除状态与 GC 的上限约 {c['state_and_gc_amdahl_upper_bound']:.2f}×。这两个值不是对真实并行墙钟时间的保证。实际观测是两方面共同减少，不能解释为仅靠状态查询快了数倍。",'',
f"VS 的 CPU 工作量比为 {h['cpu']['total']/f['cpu']['total']:.2f}×，平均使用核数从 {h['cpu']['average_cores']:.2f} 到 {f['cpu']['average_cores']:.2f}。墙钟比满足 `CPU 工作量比 × 平均使用核数比`，得到 {(h['cpu']['total']/f['cpu']['total'])*(f['cpu']['average_cores']/h['cpu']['average_cores']):.2f}×。状态路径 CPU 减少 {1-f['cpu']['state_estimated']/h['cpu']['state_estimated']:.1%}，GC 工作线程 CPU 减少 {1-f['cpu']['gc_workers_measured']/h['cpu']['gc_workers_measured']:.2%}。这说明收益可以由相同八核上的实际工作减少解释。",'',
f"若固定 GC 和其他 CPU 工作，仅代入已测得的状态路径减少量，CPU 工作量模型约为 {h['cpu']['total']/(h['cpu']['total']-h['cpu']['state_estimated']+f['cpu']['state_estimated']):.2f}×；若固定状态和其他工作，仅代入 GC 工作线程减少量，则约为 {h['cpu']['total']/(h['cpu']['total']-h['cpu']['gc_workers_measured']+f['cpu']['gc_workers_measured']):.2f}×。这是记账模型，不是两次独立因果实验，不能把两个加速比相加。",'',
f"按 8 核折算，VS 的状态路径、GC、其他工作分别减少 {(h['cpu']['state_estimated']-f['cpu']['state_estimated'])/8:.2f}、{(h['cpu']['gc_workers_measured']-f['cpu']['gc_workers_measured'])/8:.2f}、{(h['cpu']['other_estimated']-f['cpu']['other_estimated'])/8:.2f} 等效秒，未用容量项减少 {h['cpu_equivalent_seconds_at_8_cores']['unused_capacity_equivalent']-f['cpu_equivalent_seconds_at_8_cores']['unused_capacity_equivalent']:.2f} 秒，合计对应 profile 作业墙钟减少 {h['seconds']-f['seconds']:.2f} 秒。这是归一化的时间记账，不是四个独立实验测出的墙钟因果效应。",'',
'## GC 暂停','',
'暂停单位为每个 JVM 的 STW 秒数。两个 TM 可以同时暂停，因此不能把它们直接相加作为作业总暂停。','',
'| Workload | 后端 | TM1 暂停 s | TM2 暂停 s | 两 TM 暂停时间并集 s |', '|---|---|---:|---:|---:|']
for w in names:
 for backend in ['heap','forl0']:
  r=get(w,backend);lines.append(f"| {w.upper()} | {backend} | {r['roles']['tm1']['gc_pauses']['pause_seconds']:.3f} | {r['roles']['tm2']['gc_pauses']['pause_seconds']:.3f} | {r['gc_pause_union_seconds']:.3f} |")
allocation=b/'allocation/summary.json'
if allocation.exists():
 lines += ['', '## Java 分配 profile','', '独立 WC 分配 profile 使用相同的 WC 长作业输入、128 次访问。下面是 async-profiler 分配采样估计的字节权重，包含建表与作业启动；它不统计 C++ malloc。不能把这些数字理解成两后端的总内存用量。','', '| 后端 | Java 分配采样权重 GiB | 含 Long 栈的权重 GiB |', '|---|---:|---:|']
 for r in json.loads(allocation.read_text()):lines.append(f"| {r['backend']} | {r['allocation_profile_weight_bytes']/2**30:.3f} | {r['long_allocation_profile_weight_bytes']/2**30:.3f} |")
gc_file=b/'analysis/gc-heap-summary.json'
if gc_file.exists():
 gcrows=json.loads(gc_file.read_text())['runs'];h=next(r for r in gcrows if r['backend']=='heap');f=next(r for r in gcrows if r['backend']=='forl0')
 lines += ['',f"本次分配 profile 的 ForL0 Java 分配字节权重高于 Heap，不能据此声称分配更少。GC 改善应结合状态存储位置和引用更新分析：独立 WC CPU-profile 作业中，两台 TM 合并的各次 GC 后堆占用中位数为 Heap {h['pooled_post_gc_heap_mib_median']:.0f} MiB、ForL0 {f['pooled_post_gc_heap_mib_median']:.0f} MiB；G1 refinement 线程 CPU 为 {h['refine_cpu_seconds']:.2f} / {f['refine_cpu_seconds']:.2f} 秒，平行 GC worker CPU 为 {h['parallel_gc_cpu_seconds']:.2f} / {f['parallel_gc_cpu_seconds']:.2f} 秒。",'',
 'GC 后堆占用包含尚未回收的老年代垃圾，不能当作精确存活集，也不包含 C++ 状态内存。结合后端代码，这支持“移除 Java 堆中的长期状态对象图和大量堆引用更新，减少扫描与 remembered-set 工作”的解释；不是把分配总量减少作为收益来源，也没有单独隔离各 GC 机制的因果贡献。[G1 refinement 源码](https://github.com/openjdk/jdk11u/blob/master/src/hotspot/share/gc/g1/g1ConcurrentRefineThread.cpp)。','',
 '分配火焰图也显示 Long 装箱仍是后续可优化点；当前实现没有做到零分配。']
if a.optimization:
 opt=json.loads((a.optimization/'analysis/decomposition.json').read_text())['runs']
 lines += ['', '## 同一个 VS job 上的后端优化过程','', '这组诊断保持 2M 输入、524288 keys、width=64、touches=128 与同一 JAR 不变。Heap 是固定实现；R0–R4 均为 ForL0 内部变化。','', '| 实现 | profile 耗时 s | 状态 CPU s（估计） | GC CPU s |', '|---|---:|---:|---:|']
 for r in opt:
  name='Heap' if r['backend']=='heap' else r.get('backend_revision','unknown')
  lines.append(f"| {name} | {r['seconds']:.3f} | {r['cpu']['state_estimated']:.2f} | {r['cpu']['gc_workers_measured']:.2f} |")
 lines += ['', 'R0：本轮公平基线；R1：缩短 Long Java 热路径并用原始 JNI 返回值；R2：标准 put 自动选择连续布局；R3：完整连续行直接内存访问；R4：在同一实体的连续访问期间重新尝试采用已形成的连续行。各版原生库和后端 JAR 的哈希保留在 measurement 中。']
violations=[v for r in rows for role in r['roles'].values() for v in role['affinity_violations']]
assert not violations
lines += ['', '## 资源与可复核性','',
'Flink 1.20.3、BiSheng Java 11.0.32、Kunpeng 920。JM 4 核/8 GiB；两台 TM 各 4 核/32 GiB、4 slots；两个后端采用相同的 21.875 GiB JVM heap 和 8 GiB task off-heap 配置，其余用于 Flink/JVM 开销。TM1 使用 CPU 4–7，TM2 使用 8–11，taskset 在启动 Java 前生效。[JNI](https://docs.oracle.com/en/java/javase/11/docs/specs/jni/design.html) 与 C++ 原生线程在相同 OS 线程/[继承 affinity](https://man7.org/linux/man-pages/man3/pthread_create.3.html) 范围内执行；本轮每 0.5 秒检查全部 JVM 线程，未发现越界 affinity。CPU 计数包括 Java、C++ 和 GC。','',
'CPU affinity 不是独占 CPU，也不是不可修改的 cpuset；Flink 的 32 GiB process memory 是配置预算，不是每 TM 的硬 cgroup 限额。本轮没有通过后端代码扩大 affinity。','',
'所有作业关闭 L0 和 checkpoint；拓扑、sink 记录数、source signature 与 feature projection 成对一致。汇总校验不声称逐 key 复现原始业务算法。编译和现有原生/真实 JNI 状态语义检查通过。','',
'CPU 火焰图与 Java 分配火焰图分别位于 runs 与 allocation/runs；GC 原始日志位于 runtime-evidence；完整命令、图、sink、CPU/affinity 样本、JAR/原生库哈希均保留。所有更改在远端 briskstate_workspace，未提交、未推送。','',
'PMU 仍被宿主机权限阻止，perf_event_paranoid=4，缺少 CAP_PERFMON，/proc/sys 只读；DevKit/kperf 不能绕过该限制。因此本报告没有 Retiring、Backend/Frontend Stall 或 DRAM Bound 的实测比例，CPU 图也不是微架构 top-down 图。']
(b/'REPORT.md').write_text('\n'.join(lines)+'\n')
print(b/'REPORT.md')
