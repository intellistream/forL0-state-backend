from pathlib import Path
from collections import defaultdict
import datetime,hashlib,json,math,re,statistics

B=Path(__file__).resolve().parent
L=lambda p:json.loads((B/p).read_text())
conditions=L('analysis/conditions.json') if (B/'analysis/conditions.json').exists() else []
formal=L('analysis/formal-runs.json') if (B/'analysis/formal-runs.json').exists() else []
profiles=L('analysis/profiles-runs.json') if (B/'analysis/profiles-runs.json').exists() else []
micro=L('analysis/micro-results.json') if (B/'analysis/micro-results.json').exists() else []
mechanism=L('analysis/memory-mechanism.json') if (B/'analysis/memory-mechanism.json').exists() else {}
complete=len(formal)==210 and len(profiles)==56 and len(micro)==13 and all(len(r.get('rocksdb',{}).get('fork_scores',[]))==2 for r in micro)
lines=[]
def add(s=''):lines.append(s)
def table(headers,rows):
    add('| '+' | '.join(headers)+' |');add('|'+'|'.join('---' for _ in headers)+'|')
    for row in rows:add('| '+' | '.join(str(x) for x in row)+' |')
    add()
def fmt(s,digits=1):
    if s is None:return '—'
    ci=s.get('ci95');return f"{s['mean']:,.{digits}f}"+(f" [{ci[0]:,.{digits}f}, {ci[1]:,.{digits}f}]" if ci else '')
def rel(row):return str(Path(row['directory']).relative_to('/root/briskstate-setup/paper-experiments-20261104'))
def artifact(path,title):
    if (B/path).exists():add(f'![{title}]({path})\n')
def histogram(row):
    values=defaultdict(lambda:[0,0])
    for role in ['tm1','tm2']:
        p=B/rel(row)/(role+'-retained-class-histogram.txt')
        for line in p.read_text().splitlines():
            m=re.match(r'^\s*\d+:\s+(\d+)\s+(\d+)\s+(\S+)',line)
            if m:values[m[3]][0]+=int(m[1]);values[m[3]][1]+=int(m[2])
    return values

add('# ForL0 R30 实验报告：七项 benchmark set 吞吐、内存梯度与三后端状态微基准\n')
add('**状态：'+('全部正式实验完成。' if complete else f'实验进行中；当前主实验 {len(formal)}/210、归因 {len(profiles)}/56，本文不能作为完成版使用。')+'**\n')
add('后端范围按最终要求执行：benchmark set 比较 Heap 与 ForL0；RocksDB 仅出现在状态微基准。时间单位秒；内存采用二进制 GiB。本文区分原始实测、采样估计、统计推断及适用范围。\n')
if formal:
    tz=datetime.timezone(datetime.timedelta(hours=8))
    start=min(r['measurement_start_ms'] for r in formal);end=max(r['measurement_end_ms'] for r in formal+profiles)
    date=lambda t:datetime.datetime.fromtimestamp(t/1000,tz).strftime('%Y-%m-%d %H:%M:%S')
    add(f'正式宏实验采集区间：{date(start)} 至 {date(end)}（Asia/Shanghai）；初始化和准备早于该首个计时窗口，RocksDB微基准采集时间另见原始文件。\n')
    paired=[c for c in conditions if 'paired_speedup' in c]
    if paired:
        ratios=[c['paired_speedup']['geomean'] for c in paired]
        strong=sum(c['paired_speedup'].get('ci95') is not None and c['paired_speedup']['ci95'][0]>1 for c in paired)
        add(f"在当前有成对数据的{len(paired)}个条件中，ForL0/Heap吞吐比点估计范围为{min(ratios):.2f}–{max(ratios):.2f}×；其中{strong}个条件的配对95%区间下界超过1。逐条件结果、区间和未确定差异完整列出，不用单个最好结果代表全部负载。\n")
        overall=math.exp(statistics.mean(math.log(x) for x in ratios))
        add(f'35个workload/内存条件等权的描述性几何均值为{overall:.2f}×。该数值概括本数值状态压力适配集合，不代表所有Flink作业；逐条件区间未作多重比较校正。\n')
    drift_count=sum(abs((r['last_first_30s_ratio'] or 1)-1)>.15 for r in formal)
    if drift_count:add(f'**解释范围：本轮是60s预热后的120s固定窗口吞吐；{drift_count}次主运行存在首尾30s显著趋势，不能将全部条件称为已经证明严格平稳。所有原数据保留，具体诊断列于第4节。**\n')

add('## 1. 研究问题与实验协议\n')
add('RQ1：固定 CPU 和每 TM 总内存预算下，ForL0 对七项状态密集数值化 workload 的稳态吞吐有何影响？RQ2：内存预算从32到64GiB变化时，吞吐、GC和运行时/保留内存如何变化？RQ3：同一标准 Flink API 的操作级代价及其与端到端收益的关系是什么？\n')
add('主实验以预热后固定窗口吞吐衡量持续运行性能，目标是稳态比较；实际是否平稳另行检查。它不是固定输入完成时间，也不与先前 R4/R18 的短作业或有限输入耗时拼接。完整协议及执行顺序分别见 [PROTOCOL.md](PROTOCOL.md)、[plan.json](plan.json)。\n')
table(['项目','设计'],[
 ['主矩阵','7 workloads × 5内存点 × 2后端 × 3独立JVM = 210次'],
 ['内存点','每TM 32/40/48/56/64 GiB'],
 ['每次流程','全key覆盖并排空 → 至少60s预热 → 120s固定计数窗口 → 排空/保留内存诊断 → 正常完成'],
 ['归因矩阵','7 workloads × 32/64GiB × 2后端 × 2独立JVM = 56次；独立于主计时'],
 ['执行顺序','seed20261104；每轮打乱35个workload/内存条件，同条件两后端相邻，交替先后顺序'],
 ['统计单位','独立JVM；每秒计数和30s子窗口只用于平稳性诊断，不充当额外重复'],
 ['数据筛选','所有成功完成的正式运行均保留；基础设施或正确性失败保留原始记录并注明原因，不按性能删慢样本']])
add('吞吐定义为 `sum(终端sink在窗口内完成的记录数) / sink输出倍数 / 120s`，单位 input-equivalent records/s。LG/VS/LR 的倍数分别为3/8/12；它消除拓扑输出数量不同造成的计数混淆。它仍然是本数值适配作业的吞吐，不是原始生产数据集吞吐或端到端延迟。\n')

add('## 2. 完整实验环境与资源配置\n')
table(['层次','环境'],[
 ['处理器','HiSilicon Kunpeng-920，aarch64，小端；192个在线逻辑CPU，每核1线程；lscpu报告4个48核cluster、8个NUMA节点，socket字段不可用，不据此猜测物理插槽数'],
 ['缓存','每核L1d/L1i各64KiB、L2 512KiB；每个L3实例24MiB，8实例。原始lscpu与sysfs输出随附'],
 ['宿主机与容器','宿主机/proc/meminfo显示约2TiB；容器memory.max=192GiB，cpu.max=32 CPU配额。宿主机共享，核心未独占'],
 ['OS','容器openEuler24.03 LTS-SP3；宿主机kernel 5.15.0-25-generic；完整版本见environment目录'],
 ['Java','BiSheng OpenJDK11.0.32+9，64-bit Server VM，G1；每角色ActiveProcessorCount=4'],
 ['Flink','1.20.3，发行版commit01e3a6d，Scala2.12发行包'],
 ['JM','1个，CPU0–3，8GiB process memory'],
 ['TM','2个，分别CPU4–7/8–11，各4slot，默认并行度8；所有这些CPU位于NUMA node0。未设置内存页NUMA硬绑定'],
 ['编译与实现','ForL0 Java11 / 原生Release构建；R30源码、JAR、native二进制与精确SHA-256随附；无测量中代码调优'],
 ['存储','/root位于40GiB ext4 Longhorn PVC；/tmp位于容器overlay。不是已验证的独占本地NVMe；仅RocksDB微基准使用其本地DB文件'],
 ['采集','/proc进程/线程CPU和RSS约0.5s；smaps_rollup/PSS约2s；Flink REST指标请求约5s（本轮返回空列表，未作为JVM指标结果）；async-profiler4.4 ARM64 CPU采样5ms，精确裁剪JFR窗口']])
add('原始硬件、软件、cgroup、挂载、NUMA、编译器、git差异及二进制哈希见 [environment](environment/artifacts.json)。每次运行另保存最终config.yaml、JVM命令和VM.flags，实际Xmx与压缩引用变化以这些记录为准。\n')
flagrows=[]
for m in [32,40,48,56,64]:
    for backend in ['heap','forl0']:
        candidates=[r for r in formal if r['memory_gib']==m and r['backend']==backend]
        if not candidates:continue
        flags=candidates[0]['jvm_flags']['tm1']
        flagrows.append([m,backend,f"{int(flags['MaxHeapSize'])/2**30:.3f}",flags.get('UseCompressedOops'),flags.get('UseCompressedClassPointers'),f"{int(flags['G1HeapRegionSize'])/2**20:.0f}"])
if flagrows:table(['GiB/TM','后端','实际Xmx GiB','CompressedOops','CompressedClassPointers','G1 region MiB'],flagrows)
table(['配置','Heap','ForL0'],[
 ['taskmanager.memory.process.size','32–64GiB','相同'],['taskmanager.memory.task.off-heap.size','0GiB','8GiB'],
 ['taskmanager.memory.managed.size','0','0'],['network min/max','512MiB / 512MiB','相同'],
 ['checkpoint / restart','关闭 / none','相同'],['ForL0 L0','不适用','关闭'],['operator chaining','关闭','相同'],['任务并行度','8；Global ACD为1','相同']])
add('采用相同总process预算、按后端用途分配：Heap可将状态放入Java堆；ForL0预留8GiB task off-heap。Flink预算不是OS硬RSS限额，父容器192GiB是实际硬限制。本文提供真实PSS/RSS，不声称Java/C++总RSS都被分别限制为配置预算。CPU affinity约束整个TM及其Java、GC、JIT、Cleaner和原生线程，C++不获得额外核；逐线程允许掩码随原始数据保存。\n')
add('ForL0还保留`native-memory.max-size=1GiB`的每StateEngine表分配器配置；它不是完整原生RSS上限，嵌套标准库容器等分配不能据此全部计账。原生C++17构建为Release、`-O3 -DNDEBUG -march=native`；R30后端Java release11，共同benchmark类target8，均运行于上述Java11。RocksDB使用发行版预编译JNI，不假定其编译目标与ForL0一致。[Flink官方内存说明](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/mem_setup_tm/)与完整配置文件一并提供。\n')

add('## 3. Workload、状态与初始化\n')
table(['workload','角色框架','每源记录终端输出'],[
 ['WC','Split → Count','1'],['FD','Predict：二事件转移计数/异常分数','1'],['SD','Moving Average → Spike Detection','1'],
 ['TM','Map Match → Speed Calculate','1'],['LG','Geo Finder→Geo Status、Status Counter、Volume Counter三分支','3'],
 ['VS','Voice Dispatcher；RCRF/ECRF/ENCR/CT24/ECR24/Global ACD；FoFIR/URL/ACD；Score','8'],
 ['LR','速度、事故、车辆计数、事故通知、每日费用、收费通知、账户余额及原汇聚边','12']])
add('源首先覆盖524,288个Long实体key，此后使用每个源子任务固定seed `20261102+subtask` 的均匀随机流。普通有状态任务使用64个连续Long feature，每输入执行128对标准 `MapState.get/put`，增量1,000,000,007；另执行角色所需的标准ValueState和历史MapState访问。Global ACD保持单全局任务、width4/touches1，移动窗口长度16。实体域由8个并行子任务分担，不是每子任务各524,288实体。\n')
add('共同初始化阶段将Predict的272个历史槽和Moving Average的16个环形槽通过标准API置零；之前缺失值在对应计算中同样解释为零。这样把有界字典先材料化，避免较快后端因预热处理更多记录而拥有更多逻辑条目。feature在首次覆盖中填齐。所有源到达屏障且终端计数达到期望倍数后，才释放稳态输入。初始化单独计时。\n')
table(['workload','普通有状态角色数','每源记录的普通角色处理次数','每源记录feature get/put对数','feature value理论有效载荷'],[
 ['WC',1,1,128,'256MiB'],['FD',1,1,128,'256MiB；另有272槽历史字典'],['SD',2,2,256,'512MiB；另有16槽环形历史'],
 ['TM',2,2,256,'512MiB'],['LG',4,4,512,'1GiB'],['VS','9+单全局角色','22+全局1','2816+全局1','2.25GiB+全局32B'],['LR',8,16,2048,'2GiB']])
add('理论载荷只计算`实体数×feature数×8B`，不是实测footprint：没有计入Map键、哈希桶、节点、存在位、外层索引、对象头、容量余量和JVM。相同角色收到更多分支记录会提高访问量，但不会重复增加同一实体的64个feature槽。\n')
add('Job没有后端类型判断或扩展API。相同源码/JAR只通过Flink配置选择后端；共同访问循环和事件表示与原冻结JAR字节码一致，见 [provenance.json](steady-job/provenance.json)。拓扑与真实有限输入结果的核验见 [oracle/summary.json](oracle/summary.json)；短验证运行不纳入性能统计。\n')
add('稳态模式的命令中`numRecords=524288`用于覆盖阶段及参数有效性检查；连续输入由控制文件终止，实际总量保存在每次运行的source统计。不同后端在固定时间内完成不同长度的确定性输入前缀；比较的是相同生成规则、分布、状态规模和访问算法下的吞吐，不能要求不同长度流的原始checksum相等。\n')
add('每次正常完成后，在控制器CPU上独立重放各source实际前缀，重建全key计数直方图，核对终端feature投影的闭式期望、记录倍数和源签名；[历史七项长作业验证](oracle-replay-prior/summary.json)也已验证该公式。该校验覆盖终端状态增量投影，并非所有中间状态或业务字段的逐条等价证明。\n')
add('这是保留Zhang等ICDE2017 Figure5任务/边的**合成数值状态压力适配**，没有原始文本解析、真实地图匹配、生产电话数据或完整Linear Road语义。连续小整数和Long表示有意选择了ForL0擅长的访问模式；比较公平性来自两个后端执行相同作业，不代表通用类型和稀疏访问也有相同收益。\n')
add('任务图来源为[Zhang等原文Figure5](https://www.comp.nus.edu.sg/~hebs/pub/shuhaoICDE17a.pdf)；实际节点、边（含重复边）、并行度及完整状态循环以随附源码/拓扑契约为准。\n')

add('## 4. 完整 benchmark set：吞吐与内存敏感性\n')
artifact('analysis/throughput.png','七项稳态吞吐及95%区间')
add('每个后端报告独立JVM均值和95% Student-t区间（n=3，df=2）；加速比采用同轮配对吞吐比的几何均值及log空间t区间。n较小，区间可能很宽；阴影不是每秒采样产生的伪重复置信区间。\n')
rows=[]
for c in conditions:
    h=c.get('heap');f=c.get('forl0');s=c.get('paired_speedup')
    ratio=f"{s['geomean']:.2f}×"+(f" [{s['ci95'][0]:.2f}, {s['ci95'][1]:.2f}]" if s['ci95'] else '') if s else '—'
    rows.append([c['workload'].upper(),c['memory_gib'],fmt(h['throughput']) if h else '—',fmt(f['throughput']) if f else '—',ratio])
table(['workload','GiB/TM','Heap records/s [95%CI]','ForL0 records/s [95%CI]','配对F/H [95%CI]'],rows)
geomeans=[]
for memory in [32,40,48,56,64]:
    cc=[c for c in conditions if c['memory_gib']==memory and 'paired_speedup' in c]
    if len(cc)==7:geomeans.append([memory,f"{math.exp(statistics.mean(math.log(c['paired_speedup']['geomean']) for c in cc)):.2f}×",'7 workloads等权描述性几何均值'])
if geomeans:table(['GiB/TM','集合几何均值F/H','解释'],geomeans)
endpoints=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    cc={c['memory_gib']:c for c in conditions if c['workload']==w and 'heap' in c and 'forl0' in c}
    if 32 not in cc or 64 not in cc:continue
    h=cc[64]['heap']['throughput']['mean']/cc[32]['heap']['throughput']['mean']-1
    f=cc[64]['forl0']['throughput']['mean']/cc[32]['forl0']['throughput']['mean']-1
    endpoints.append([w.upper(),f"{h:+.1%}",f"{f:+.1%}",f"{cc[32]['paired_speedup']['geomean']:.2f}×",f"{cc[64]['paired_speedup']['geomean']:.2f}×"])
if endpoints:
    table(['workload','Heap32→64吞吐变化','ForL0 32→64变化','32GiB F/H','64GiB F/H'],endpoints)
    lookup={(c['workload'],c['memory_gib']):c for c in conditions}
    changes={w:lookup[w,64]['heap']['throughput']['mean']/lookup[w,32]['heap']['throughput']['mean']-1 for w in ['lg','vs','lr'] if (w,32) in lookup and (w,64) in lookup}
    if len(changes)==3:add(f"增加预算使Heap在VS/LR/LG的端点吞吐分别增加{changes['vs']:.1%}/{changes['lr']:.1%}/{changes['lg']:.1%}。这些高角色访问次数负载对内存更敏感，但提升没有消除与ForL0的差距。ForL0各中间点并不单调，因此不能将单次波动表述为增加内存必然加速。\n")
if formal:
    drift=[r for r in formal if abs((r['last_first_30s_ratio'] or 1)-1)>.15]
    add(f'平稳性诊断：{len(drift)}/{len(formal)}次运行的末30s/首30s吞吐偏离1超过15%。这些运行仍保留；逐秒计数、30s窗口、跨运行CV均可复核。阈值用于提示时间趋势，不用于筛除低分。\n')
    for backend in ['heap','forl0']:
        rr=[r for r in formal if r['backend']==backend]
        flagged=[r for r in rr if abs((r['last_first_30s_ratio'] or 1)-1)>.15]
        cv=[c[backend]['throughput']['cv'] for c in conditions if backend in c]
        if cv:add(f"{backend}：{len(flagged)}/{len(rr)}次出现上述趋势；各条件三次重复的吞吐CV中位数{statistics.median(cv):.2%}、最大值{max(cv):.2%}。\n")
    if drift:add('因此本文报告的是明确的60s预热后120s固定窗口吞吐；全部数据保留。对出现下降趋势的条件，本轮不能给出已经达到严格稳态的结论；论文若依赖该主张，需要另行验证，不能通过删去低分样本掩盖趋势。\n')
    if drift:
        table(['条件','末/首30s','窗口CV','原始数据'],[[f"{r['workload']} {r['memory_gib']}GiB {r['backend']} r{r['repeat']}",f"{r['last_first_30s_ratio']:.3f}",f"{r['throughput_30s_window_cv']:.1%}",f"[JSON]({rel(r)}/measurement.json)"] for r in drift])

add('## 5. Memory Footprint\n')
add('运行时footprint以同一轮采样的两TM之和统计；RSS包含共享页重复计数，PSS按共享比例分摊。分析JSON提供窗口均值/P95/峰值及初始化至窗口结束峰值，不把两个不同时间的单TM峰值简单相加。计时后的Full GC诊断另列，不能用其造成的额外驻留页充当运行时峰值。GC后Java heap-used及Xms/Xmx另列，它们不等于总进程内存；本轮连续heap-used采集缺口如下。\n')
add('采集完整性：`jvm-metrics.jsonl.gz`中的Flink REST查询返回了空`values`列表，未获得有效的约5s连续JVM heap-used/direct-buffer时间序列。本文不据此计算或声称连续JVM内存均值；运行时footprint来自独立的/proc与smaps采样，GC后Java heap-used来自真实jcmd输出，Xms/Xmx来自VM.flags，GC日志可补充收集事件时的堆占用。该缺口不影响sink吞吐、线程CPU或PSS/RSS的原始采集。\n')
add('RSS（Resident Set Size）是进程当前驻留RAM的页总量，共享页在各进程RSS中分别计数。PSS（Proportional Set Size）将共享页按映射份额分摊，更适合汇总多个TM。USS按smaps的Private_Clean+Private_Dirty计算，表示该口径下的独占驻留页。三者均描述实际驻留页，包含Java堆、原生状态、运行时及分配器保留的空闲页；不等于只保存有效状态的空间。定义见[Linux内核/proc文档](https://docs.kernel.org/filesystems/proc.html)。\n')
resident_rows=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        for backend in ['heap','forl0']:
            rr=[r for r in formal if (r['workload'],r['memory_gib'],r['backend'])==(w,m,backend)]
            if not rr:continue
            avg=lambda fn:statistics.mean(fn(r)/2**30 for r in rr)
            resident_rows.append([w.upper(),m,backend,f"{avg(lambda r:r['memory']['VmRSS']['mean']):.2f}",f"{avg(lambda r:r['memory']['Pss']['mean']):.2f}",f"{avg(lambda r:r['memory']['Private_Clean']['mean']+r['memory']['Private_Dirty']['mean']):.2f}"])
if resident_rows:table(['workload','GiB/TM','后端','两TM平均RSS GiB','两TM平均PSS GiB','两TM平均USS GiB'],resident_rows)
add('RSS来自较高频率的status采样，PSS/USS来自较低频率的smaps_rollup；独立采样均值不强制满足同一瞬间的大小关系。PSS与USS接近表明驻留页绝大部分为独占，不能将较高ForL0 PSS解释为共享库重复计数。即使空闲页可以被JVM/分配器复用，只要仍驻留就仍计入物理占用。\n')
artifact('analysis/memory-pss.png','运行时两TM平均PSS')
artifact('analysis/memory-rss.png','运行时两TM峰值RSS')
add('正式窗口结束后暂停所有源并排空管道，再在两TM执行Full GC，记录保留PSS/RSS和GC.heap_info。此时状态仍存活；该诊断完全处于性能/GC统计窗口之外。它描述静止保留进程占用，包含JIT、Flink、网络池及原生allocator保留页，仍不是精确的状态数据结构字节数。\n')
artifact('analysis/memory-retained.png','窗口结束后GC保留PSS')
table(['workload','GiB/TM','运行时平均PSS H/F GiB','运行时峰值RSS H/F GiB','GC后保留PSS H/F GiB'],[
 [c['workload'].upper(),c['memory_gib'],f"{c['heap']['pss_mean_gib']['mean']:.2f} / {c['forl0']['pss_mean_gib']['mean']:.2f}",f"{c['heap']['rss_peak_gib']['mean']:.2f} / {c['forl0']['rss_peak_gib']['mean']:.2f}",f"{c['heap']['retained_pss_gib']['mean']:.2f} / {c['forl0']['retained_pss_gib']['mean']:.2f}"] for c in conditions if 'heap' in c and 'forl0' in c])
add('更大Xmx可能允许更多短命对象留在已触碰的堆页中，因此运行时RSS增加不等于状态变大。压缩引用、类指针和G1 region可能随堆尺寸改变；内存梯度是这些JVM自动决策共同作用下的实际敏感性实验，不能只归因于GC容量。ForL0页、primitive布局和读缓存也有空间成本；本组端到端作业不使用ListState，不能用List缓存收益解释其footprint。\n')
add('GC后保留进程占用仍包含空闲但已提交/驻留的Java堆页，尤其Xms=Xmx时不能把它当作“只有活对象”的内存量。另保存的GC.heap_info used更接近GC后Java堆内的存活占用，但仍含Flink运行时，且不包含ForL0原生状态。\n')
heaprows=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        for backend in ['heap','forl0']:
            rr=[r for r in formal if r['workload']==w and r['memory_gib']==m and r['backend']==backend]
            if not rr:continue
            heap=statistics.mean(sum(r['memory']['post_gc_retained_heap_used_bytes'].values())/2**30 for r in rr)
            jm=statistics.mean(r['memory']['jobmanager_rss']['mean']/2**30 for r in rr)
            full=statistics.mean(r['memory']['three_process_pss']['mean']/2**30 for r in rr)
            heaprows.append([w.upper(),m,backend,f'{heap:.3f}',f'{jm:.3f}',f'{full:.2f}'])
if heaprows:table(['workload','GiB/TM','后端','两TM GC后heap-used GiB','JM运行时RSS GiB','JM+2TM平均PSS GiB'],heaprows)
add('计时后`retained-class-histogram.txt`还记录了真实Java对象图（包括Long、HashMap节点/桶数组、Flink状态条目及运行时对象）。该诊断发生在保留内存快照之后，不进入稳态计时。对象图的减少可解释GC存活扫描/引用维护的变化，但不能单凭Java histogram推导ForL0的完整原生字节数。\n')
histrows=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        for backend in ['heap','forl0']:
            rr=[r for r in formal if r['workload']==w and r['memory_gib']==m and r['backend']==backend]
            if not rr:continue
            hh=[histogram(r) for r in rr]
            avgbytes=lambda cls:statistics.mean(h[cls][1] for h in hh)/2**30
            share=statistics.mean(sum(h[c][1] for c in ['java.lang.Long','java.util.HashMap$Node','[Ljava.util.HashMap$Node;'])/sum(r['memory']['post_gc_retained_heap_used_bytes'].values()) for h,r in zip(hh,rr))
            histrows.append([w.upper(),m,backend,f"{avgbytes('java.lang.Long'):.3f}",f"{avgbytes('java.util.HashMap$Node'):.3f}",f"{avgbytes('[Ljava.util.HashMap$Node;'):.3f}",f'{share:.1%}'])
if histrows:table(['workload','GiB/TM','后端','Long对象GiB','HashMap节点GiB','HashMap桶数组GiB','三类/GC后heap used'],histrows)
add('Histogram统计的是这些类的浅对象字节总和，包含相同类的少量框架对象，不是严格的状态对象所有权划分。对Long feature，Heap的值对象、Map节点与桶数组构成主要保留图；ForL0的primitive数据位于原生内存，所以不能把其较少的Java对象直接表述为相同倍数的总内存节省。\n')
add('### 5.1 为什么ForL0的PSS随配置预算近似线性增长\n')
add('GC日志与实际VM.flags显示，默认G1年轻代上限为Java堆的60%；ForL0计时窗口内的Eden目标接近该上限。随着Flink进程预算增大，Xms=Xmx也增大，Eden因而按近似固定比例增长。处理过程填满这些年轻代区域后，页成为真实物理驻留；收集掉临时对象不等于将驻留页归还OS。状态实体/feature数量没有随预算变化。[Java11参数定义](https://docs.oracle.com/en/java/javase/11/tools/java.html)、原始flags与GC日志共同支持该解释。\n')
if mechanism:
    mm=[r for r in mechanism.get('formal_conditions',[]) if r['workload']=='wc' and r['backend']=='forl0']
    table(['WC ForL0 GiB/TM','Xmx/TM GiB','计时内两TM Eden目标GiB','运行时两TM平均PSS GiB','诊断时Java堆驻留RSS界限GiB','GC后Java heap-used GiB'],[
        [r['memory_gib'],f"{r['xmx_per_tm_gib']:.3f}",f"{r['two_tm_eden_target_gib']:.2f}",f"{r['two_tm_mean_pss_gib']:.2f}",f"[{r['two_tm_java_heap_resident_lower_gib']:.2f}, {r['two_tm_java_heap_resident_upper_gib']:.2f}]",f"{r['two_tm_post_gc_heap_used_gib']:.3f}"] for r in mm])
    endpoints_memory={r['memory_gib']:r for r in mm}
    if 32 in endpoints_memory and 64 in endpoints_memory:
        lo,hi=endpoints_memory[32],endpoints_memory[64]
        total_growth=hi['two_tm_diagnostic_resident_gib']-lo['two_tm_diagnostic_resident_gib']
        java_growth=hi['two_tm_java_heap_resident_lower_gib']-lo['two_tm_java_heap_resident_upper_gib']
        add(f'WC的GC后诊断RSS由{lo["two_tm_diagnostic_resident_gib"]:.2f}增加至{hi["two_tm_diagnostic_resident_gib"]:.2f}GiB；增加的{total_growth:.2f}GiB中，Java堆驻留增量下界为{java_growth:.2f}GiB，即约{java_growth/total_growth:.1%}。这一结果直接定位大部分额外驻留页在Java堆中，不能用状态实体增加解释，也不是完整原生状态字节数的测量。\n')
add('Java堆驻留界限来自另外保存的retained-numa-maps.gz和GC.heap_info地址范围：统计每个VMA的驻留页；与Java堆边界合并的VMA按地址跨度给出保守上下界，避免把整段匿名映射误算为某一方。该快照在暂停/排空、Full GC和histogram之后，和计时窗口PSS不是完全同一时刻；界限是RSS而非PSS，不能直接相减得到原生状态字节数。[逐次诊断与算法说明](analysis/memory-mechanism.json)。\n')
add('ForL0较少的长期Java对象图及较轻的收集工作，使G1可以选择更大的年轻代；Heap则受到暂停目标和存活/引用图处理成本约束，计时内Eden通常更小。这不是原生状态按Xmx增长的证据，也不能把高PSS从实际物理成本中删掉。剩余非Java堆驻留部分还包含JVM的GC元数据、代码、线程栈、Flink缓冲和原生分配器，不能全部归属ForL0状态。\n')
add('## 6. 详细归因：GC、状态访问与其他工作\n')
add('GC工作线程CPU由/proc计数直接获得。状态CPU按每个非GC线程在精确窗口内的CPU栈占比乘该线程CPU时间估计；严格backend-frame分类与包含共同LongStateAccess循环的分类均保留。主图采用后者，包含循环算术、装箱及mutator写屏障。其他CPU为过程总CPU减前两项，包括网络、序列化、调度、JIT和未归入状态的工作。JFR只裁剪目标120s窗口，线程标题不参与状态frame匹配。\n')
add('三项互斥是**CPU记账定义**，不是互相独立的墙钟因果效应。GC写屏障可能落在mutator状态路径中；状态布局改变又会影响GC，两项存在机制联系。不能把采样状态比例乘作业墙钟、也不能将GC CPU秒当作暂停秒。\n')
add('GC专用线程以名称识别；VM Thread等非专用线程中的部分GC工作可能落在“其他”，因此GC worker CPU不是全部GC相关开销。主分解统计两TM；JM的配置、CPU计数、亲和性与内存另行保存，未混入八核worker分解。\n')
artifact('analysis/gc-memory.png','五档内存下完成相同工作量的GC CPU开销')
for memory in [32,64]:
    artifact(f'analysis/cpu-decomposition-m{memory}.png',f'{memory}GiB下每百万输入的CPU工作量分解')
    artifact(f'analysis/cpu-relative-m{memory}.png',f'{memory}GiB下相对Heap的单位输入CPU工作量分解')
    artifact(f'analysis/cpu-time-m{memory}.png',f'{memory}GiB下120秒窗口内实际CPU秒分解')
add('原始CPU秒图中ForL0的状态CPU可能更大，因为同一120s内完成更多记录；访问效率应结合单位输入图判断。相对图以同workload的Heap单位输入总CPU为100%，各组件仍按同一分母归一化。八核CPU等效时间及未用容量项也保存在分析JSON，不能视为独立测出的串行因果时间。\n')
rows=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        for backend in ['heap','forl0']:
            pp=[r for r in profiles if r['workload']==w and r['memory_gib']==m and r['backend']==backend]
            if not pp:continue
            avg=lambda k:statistics.mean(r[k] for r in pp)
            norm=lambda k:statistics.mean(r['cpu_seconds_per_million_inputs'][k] for r in pp)
            rows.append([w.upper(),m,backend,f"{norm('gc'):.2f}",f"{norm('state'):.2f}",f"{norm('other'):.2f}",f"{avg('average_cores'):.2f}",f"{avg('gc_pause_union_seconds'):.3f}"])
table(['workload','GiB/TM','后端','GC CPU s/M','状态CPU s/M（估计）','其他CPU s/M（估计）','平均使用核数','120s内STW并集s'],rows)
gcrows=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        for backend in ['heap','forl0']:
            rr=[r for r in formal if r['workload']==w and r['memory_gib']==m and r['backend']==backend]
            if not rr:continue
            avg=lambda k:statistics.mean(r['gc_components_cpu_seconds'][k]*1e6/r['input_equivalent_records'] for r in rr)
            gcrows.append([w.upper(),m,backend,f"{avg('refinement'):.2f}",f"{avg('parallel_workers'):.2f}",f"{avg('concurrent_mark'):.2f}",f"{avg('remaining_workers'):.2f}"])
if gcrows:table(['workload','GiB/TM','后端','refinement CPU s/M','并行GC worker s/M','并发标记CPU s/M','其余GC worker s/M'],gcrows)
add('以上GC子类来自无profiler主运行的线程计数。refinement反映G1记忆集维护工作；并行GC worker可参与不同收集阶段，不能仅凭线程名称将其全部解释为某一种暂停。若Heap的节点/Long保留图、refinement与暂停同时较大，而ForL0减少这些工作，证据支持减少托管堆引用维护这一机制；准确份额仍按上述CPU记账定义解释。\n')
add('对相同工作量的CPU节省，采用每百万输入CPU差值分解；GC/状态/其他三项之和严格等于总CPU差值。占比可为负值或超过100%，应原样解释，不能强制得到漂亮的正贡献。吞吐比还受平均使用核数影响：`TP_F/TP_H = (CPU_per_input_H/CPU_per_input_F) × (cores_F/cores_H)`。它是测量恒等式，不是独立优化的消融实验。\n')
savings=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        h=[r for r in profiles if r['workload']==w and r['memory_gib']==m and r['backend']=='heap'];f=[r for r in profiles if r['workload']==w and r['memory_gib']==m and r['backend']=='forl0']
        if not h or not f:continue
        hmean={k:statistics.mean(r['cpu_seconds_per_million_inputs'][k] for r in h) for k in ['gc','state','other','total']}
        fmean={k:statistics.mean(r['cpu_seconds_per_million_inputs'][k] for r in f) for k in hmean}
        d={k:hmean[k]-fmean[k] for k in hmean};frac=lambda k:f"{d[k]/d['total']:.1%}" if d['total'] else '—'
        savings.append([w.upper(),m,f"{d['total']:.2f}",f"{d['gc']:.2f} ({frac('gc')})",f"{d['state']:.2f} ({frac('state')})",f"{d['other']:.2f} ({frac('other')})"])
table(['workload','GiB/TM','总CPU节省s/M','GC节省及占比','状态节省及占比','其他节省及占比'],savings)
state_ratios=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        h=[r['cpu_seconds_per_million_inputs']['state'] for r in profiles if (r['workload'],r['memory_gib'],r['backend'])==(w,m,'heap')]
        f=[r['cpu_seconds_per_million_inputs']['state'] for r in profiles if (r['workload'],r['memory_gib'],r['backend'])==(w,m,'forl0')]
        if h and f:state_ratios.append(statistics.mean(h)/statistics.mean(f))
if state_ratios:add(f'在14个归因条件中，包含共同访问循环的单位输入状态路径CPU代价，Heap/ForL0估计比为{min(state_ratios):.2f}–{max(state_ratios):.2f}。这提供了GC worker之外的状态路径成本下降证据；其中仍含mutator屏障和装箱等成本，不应全部解释为单次原生查表指令的加速。\n')
for w in ['wc','vs']:
    h=[r for r in profiles if r['workload']==w and r['memory_gib']==32 and r['backend']=='heap']
    f=[r for r in profiles if r['workload']==w and r['memory_gib']==32 and r['backend']=='forl0']
    if not h or not f:continue
    hm={k:statistics.mean(r['cpu_seconds_per_million_inputs'][k] for r in h) for k in ['gc','state','other','total']}
    fm={k:statistics.mean(r['cpu_seconds_per_million_inputs'][k] for r in f) for k in hm}
    diff=hm['total']-fm['total'];hc=statistics.mean(r['average_cores'] for r in h);fc=statistics.mean(r['average_cores'] for r in f)
    if diff>0:
        add(f"以{w.upper()}、32GiB/TM为例，每百万输入的CPU工作量为Heap {hm['total']:.2f}s、ForL0 {fm['total']:.2f}s。节省的{diff:.2f}CPU秒中，GC worker占{(hm['gc']-fm['gc'])/diff:.1%}，状态路径占{(hm['state']-fm['state'])/diff:.1%}，其他占{(hm['other']-fm['other'])/diff:.1%}。两者平均使用{hc:.2f}/{fc:.2f}核，可用上限均为8；这些百分比描述CPU节省来源，不是墙钟提速的独立因果份额。\n")
add('每个归因条件仅两个独立运行，完整样本、CPU校准因子、采样覆盖率、严格状态分类和火焰图均保留；不把二次采样误差包装成确定因果贡献。与主计时的吞吐差异用于检查profiler干扰，诊断数据不并入主图。\n')
checks=[]
for w in ['wc','fd','sd','tm','lg','vs','lr']:
    for m in [32,64]:
        for backend in ['heap','forl0']:
            pp=[r for r in profiles if r['workload']==w and r['memory_gib']==m and r['backend']==backend]
            rr=[r for r in formal if r['workload']==w and r['memory_gib']==m and r['backend']==backend]
            if not pp:continue
            strict=statistics.mean(r['strict_state_cpu_seconds_estimated'] for r in pp)
            incl=statistics.mean(r['state_cpu_seconds_estimated'] for r in pp)
            coverage=min(r['roles_window'][role]['sampled_nongc_cpu_coverage'] for r in pp for role in ['tm1','tm2'])
            overhead=statistics.mean(r['throughput_input_records_s'] for r in pp)/statistics.mean(r['throughput_input_records_s'] for r in rr)-1 if rr else None
            checks.append([w.upper(),m,backend,f"{coverage:.1%}",f"{strict:.2f} / {incl:.2f}",f"{overhead:+.1%}" if overhead is not None else '—'])
if checks:table(['workload','GiB/TM','后端','最小非GC CPU采样覆盖','严格/包含循环状态CPU s','profile/无profile吞吐变化'],checks)
table(['归因样本','CPU火焰图TM1','CPU火焰图TM2','校准后原始分析'],[[f"{r['workload']} {r['memory_gib']}GiB {r['backend']} r{r['repeat']}",f"[HTML]({rel(r)}/tm1-cpu.html)",f"[HTML]({rel(r)}/tm2-cpu.html)",f"[JSON]({rel(r)}/analysis.json)"] for r in profiles])

add('## 7. 状态微基准：Heap / ForL0 R30 / RocksDB\n')
add('Heap/ForL0直接复用已冻结R30的52个独立fork结果，不重跑、不重新挑选分数。RocksDB先完成26个短窗口fork，再统一追加18个加长写操作fork和2个充分预热的Map读取fork。最终图采用6个原读取fork、18个长写fork和2个长Map读fork；原短窗口完整保留为诊断。所有计时和fixture类来自同一个SHA-256为 `bc258cea2877431005b499210096217e0ca6adcd24ddcb142976c87c26ec6ad6` 的应用；新增覆盖层只含工厂/枚举及RocksDB构造helper。\n')
table(['参数','设置'],[['CPU/JVM','整个JVM CPU4–7、ActiveProcessorCount4、Xms=Xmx8GiB、1 benchmark线程'],['数据','1,048,576实体；Long Value、16元素Long List、64对Long→Long Map；Map get/update每实体连续64次'],['调用','批次1024次标准API，OperationsPerInvocation1024；bulk按一次API调用计数'],['重复/时间','每后端2独立fork；普通项3×3s预热、5×3s测量；Value add为10×5s预热、5×5s测量'],['RocksDB','Flink1.20.3 EmbeddedRocksDB，frocksdbjni6.20.3-ververica-2.0；DEFAULT options；共享native预算4GiB、write-buffer ratio0.5、high-priority pool0.1'],['内存口径','8GiB仅Java堆；ForL0和RocksDB另占原生内存，不声称总RSS同为8GiB'],['采集时间','Heap/ForL0为先前R30数据，RocksDB为本次补测；不是同时段三后端交错试验，较小差异需谨慎解释']])
add('上表原时间参数适用于Heap/ForL0及RocksDB的Value get、List get/iterate。RocksDB的9种写操作最终统一为24×5s预热、12×5s测量；Map get含67,108,864对键值，按初测速率完整访问约15.5分钟，因此另用240×5s预热、12×5s测量，以减少块缓存冷启动影响。每种最终配置均2fork。\n')
add('微基准namespace为Void，backend覆盖key group0–1。初始Value为`BASE+k`，List第j项为`BASE+16k+j`，Map第j项为`BASE+64k+j`，BASE=1,000,000,007；批量输入为共同不可变的16元素/键值集合。实体访问使用乘65537后的二次幂域排列。Value add和Map add使用偏移keyCount的新key域，并在计时外clear当前批次；稳定循环中的总驻留实体域可达到初始域加插入域约2×keyCount，其余操作使用初始域。这些规则在三后端相同，完整fixture与命令随附。\n')
add('最初putAll两fork的五次测量分别约24/7.5/7.0/7.0/6.1与54/17.9/10.9/10.7/7.4ops/ms，原始序列见micro-rocksdb目录；最终没有从短序列中挑选较快或较慢区间。较长窗口仍可能包含LSM维护/共享存储波动，完整迭代和fork范围均保留，不将该配置称为最佳RocksDB调优。\n')
artifact('analysis/micro-three-backends.png','三后端微基准：线性坐标')
artifact('analysis/micro-three-backends-log.png','三后端微基准：对数坐标')
table(['标准操作','Heap ops/ms','ForL0 ops/ms','RocksDB ops/ms','F/H','F/R'],[[r['operation'],f"{r['heap']['score']:,.2f}",f"{r['forl0']['score']:,.2f}",f"{r['rocksdb']['score']:,.2f}" if 'rocksdb' in r else '—',f"{r['forl0']['score']/r['heap']['score']:.2f}×",f"{r['forl0']['score']/r['rocksdb']['score']:.2f}×" if 'rocksdb' in r else '—'] for r in micro])
if micro:
    ratios=[r['forl0']['score']/r['heap']['score'] for r in micro]
    add(f'复用的R30微基准中，13项操作的ForL0/Heap均值比为{min(ratios):.2f}–{max(ratios):.2f}×。这是相同API/fixture下的操作级结果；主作业只使用Value/Map，List的提升不能解释端到端收益。RocksDB的写维护和共享PVC会影响其操作成本，尤其putAll两fork差异较大，不能用最小吞吐值代表其一般能力。\n')
add('List get只取得Iterable；iterate读取并求和全部16值；addAll/update为16元素，Map putAll为16对。插入、命中删除及固定列表长度由共同计时外fixture复位，GC/缓存影响可能延续。特别是Heap List add的大部分分配来自fixture的列表重建，不能全部归给计时append。Java分配量和GC MXBean collection time也不等于原生分配或GC线程CPU。\n')
add('Heap本来就不逐值序列化；ForL0相对Heap的收益不能称为省去其不存在的逐值序列化。RocksDB标准状态调用包含其原有序列化/JNI/KV访问代价，结果对应该API和配置。没有由微基准倍数推断RocksDB端到端表现，本轮也未进行其benchmark set正式比较。\n')

add('## 8. 适用范围与论文表述边界\n')
add('- 连续Long实体和feature、无checkpoint、无ForL0 L0的数值状态密集负载是本实验范围；通用序列化、稀疏大key、checkpoint、恢复、TTL、真实生产算法与多机网络均未由此验证。\n- R30分页和直接primitive访问减少查找、JNI及对象图更新；Small/dense Map作用于本套作业，List读取缓存只影响List微基准。不能把所有操作级收益叠乘为端到端增益。\n- CPU绑定限制可用核，但不提供物理核心独占；宿主机负载与跨日变化仍是效度限制。记录负载、容器CPU throttling及每秒曲线以供复核。\n- 原论文重点包含instruction footprint和微架构瓶颈；RSS/PSS是进程内存占用，二者不能混用。未获得PMU实测，本文没有虚构Retiring、Backend Stall或Cache-miss归因。\n- 一套有利于ForL0的公平workload能展示目标路径优势，不能代替更广类型/访问模式的外部有效性实验。\n')
add('分页索引还取决于**单个后端实例**内的key密度：微基准的一个实例管理全部测试key group，端到端则由8个并行实例分担key域。其密度准入规则可能使部分实体回退Swiss表。因此不能仅凭全局key连续就假定端到端全部命中分页路径；实际状态/JNI栈和内存结果用于解释主实验。\n')

add('## 9. 原始数据、复现与校验\n')
add('- [执行协议](PROTOCOL.md)、[计划/seed](plan.json)、[逐次CSV](analysis/runs.csv)、[逐条件统计JSON](analysis/conditions.json)。\n- [主实验完整分析](analysis/formal-runs.json)、[归因完整分析](analysis/profiles-runs.json)、[微基准fork CSV](analysis/micro-forks.csv)、[微基准完整JSON](analysis/micro-results.json)。\n- 每次运行目录包含command、config、flags、job/CP/拓扑、sink/source统计、gzip线程和JVM内存样本、GC日志；profile另含精确窗口JFR、collapsed及HTML火焰图。\n- 脚本：`campaign.py`执行，`analyze.py`做窗口对齐与图表，`micro_analysis.py`复用/补充微基准，`build_report.py`生成本文；图同时提供PNG/SVG/PDF。\n- [SHA-256清单](MANIFEST.sha256)覆盖交付文件，重跑以冻结工件和显式参数为准，不依赖未提交git工作区的隐含状态。\n')
table(['workload','GiB/TM','后端','重复','records/s','平均PSS GiB','GC CPU s/120s','原始数据'],[[r['workload'].upper(),r['memory_gib'],r['backend'],r['repeat'],f"{r['throughput_input_records_s']:.2f}",f"{r['memory']['Pss']['mean']/2**30:.3f}",f"{r['gc_cpu_seconds']:.3f}",f"[JSON]({rel(r)}/measurement.json)"] for r in formal])
add('## 10. 参考资料\n')
add('1. Zhang et al. *Revisiting the Design of Data Stream Processing Systems on Multi-Core Processors*. ICDE2017，DOI10.1109/ICDE.2017.119。[原文PDF](https://www.comp.nus.edu.sg/~hebs/pub/shuhaoICDE17a.pdf)。\n2. Apache Flink1.20.3 [State Backends](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/state_backends/)。\n3. Apache Flink1.20.3 [TaskManager Memory](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/mem_setup_tm/)。\n')
(B/'REPORT.md').write_text('\n'.join(lines)+'\n')
print('REPORT_READY',{'complete':complete,'formal':len(formal),'profiles':len(profiles),'micro':len(micro)},flush=True)
