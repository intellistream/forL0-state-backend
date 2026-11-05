# 论文实验协议（执行前冻结）

- 后端：benchmark set 仅 Heap、ForL0 R30；RocksDB 只补微基准。
- 主指标：稳态端到端吞吐，以终端 sink 每秒完成记录数除以拓扑输出倍数，报告 input-equivalent records/s。不是固定输入完成时间。
- 七项：WC、FD、SD、TM、LG、VS、LR。原任务图、分支与汇聚保持一致，标准 ValueState、MapState 循环保持字节码一致。
- 所有后端相同 Long 实体域 524,288、64 feature、每输入 128 对标准 get/put、增量 1,000,000,007；逻辑窗口16。Global ACD 保持单任务、width4/touches1。
- 为避免材料化条目数随预热吞吐不同，Predict 的272个历史槽、Moving Average的16个历史槽在每实体首次访问时通过标准 API 置零。缺失值此前同样被解释为零；其余角色计算不改。一次共同 key 覆盖阶段排空后，才释放连续的均匀随机输入。
- 每 TM process memory：32、40、48、56、64 GiB；每条件3个独立 JVM，共210次主实验。共同初始化后至少60s预热，120s测量；每秒计数保留用于查看平稳性。子窗口不是独立重复。
- 每轮将35个 workload/内存条件用seed20261104打乱，每个条件内两后端相邻、交替先后顺序。不按测量结果挑选顺序或删除慢样本。
- 1 JM：4 CPU/8 GiB；2 TM：各4 CPU/4slot，合计8个作业并行子任务。JM CPU0–3、TM1 CPU4–7、TM2 CPU8–11；逐线程采样亲和性。
- 相同总 process memory 预算下按后端用途分配：Heap task off-heap0；ForL0 task off-heap8GiB；两者 managed0，network固定512MiB。记录实际Xmx、压缩指针和G1 region。预算不是硬RSS cgroup限制。
- Java11、G1；checkpoint关闭；ForL0 L0关闭。R30后端和native SHA冻结，不在采集过程中继续优化。
- 归因：32/64GiB两个端点，每条件2个独立 CPU-profile 运行，共56次。性能主图只使用无profiler运行；诊断吞吐单独列示以检查开销。
- CPU分解：GC线程CPU来自/proc计数；状态访问CPU来自精确测量窗口内JFR栈样本、逐线程CPU校准；其他为剩余。三项是CPU记账，不能当作互斥的墙钟因果贡献。另报GC STW时间并集。
- 堆叠图主要采用CPU秒/百万input-equivalent records，另提供原始CPU秒、比例及按8核归一化的CPU等效时间。状态访问含共同访问循环，严格backend-frame分类作为敏感性检查。
- Memory Footprint：共同时间点两TM RSS/PSS之和的均值、P95与峰值；单独记录RssAnon/RssFile、Java heap used/committed/max及空集群基线。RSS减heap-used不解释为精确原生状态大小。不强制GC来制造较低峰值。
- 保留内存另测：正式计时结束后暂停全部source、排空所有管道，再在两TM执行一次Full GC，记录quiescent RSS/PSS和GC.heap_info。此诊断不计入稳态性能/GC分解，单列为post-GC retained process footprint，不能替代运行时RSS峰值或当作精确状态字节数。
- 初始化耗时、完整job运行时间、输入和输出总量、源签名、输出倍数、CP计数、亲和性、配置、JAR/native哈希及主机负载均保留。
- 实际输出校验：job结束后在控制器CPU上重放各source实际处理的确定性前缀，得到全key访问直方图；按已知分支倍数计算feature投影的闭式期望并与sink核对，保存直方图与结果。它不在计时窗口内执行，不给Flink作业添加额外状态或逐条校验开销，也不声称校验了所有业务字段。
- 统计单位是独立JVM，不把每秒样本当作n。报告均值、SD、CV、范围及基于独立重复的95%区间；n=3时区间可能很宽，如实显示。
- 微基准沿用R30的Heap/ForL0原始数据和冻结应用；新增RocksDB工厂覆盖层不含计时/fixture类。RocksDB与旧结果同CPU4–7、8GiB Java heap、1线程、1,048,576实体key、2fork，参数与各原操作一致；额外原生共享预算4GiB明确单列。三组并非同时段交错采集，报告注明。
- RocksDB稳定性修订（主矩阵启动前）：原短窗口putAll的5次计时从24→6与54→7ops/ms持续下降。因此所有9种写操作统一改为24×5s预热、12×5s测量、2独立fork。4种读操作保留原参数。原26次短窗口结果全部保留为诊断，写操作正式图只采用统一加长后的18次结果；不选择较快fork或混合两种时间配置。
- RocksDB Map get补充：Map域含67,108,864对键值，按短窗约72k get/s估计一次完整访问约15.5分钟。为减少块缓存未充分预热的影响，该项另做240×5s预热、12×5s测量、2fork；最终采用此数据，短窗数据保留。其余3种读操作的全域访问周期短得多，维持原参数。最终RocksDB图共26个fork均值样本，来源为6个原读fork、18个长写fork和2个长Map读fork。
- 不将旧R4/R18数据拼为R30正式结果，不将合成数值适配描述成原论文完整业务/数据集复现，不报告未实测的PMU比例。

参考：Zhang等《Revisiting the Design of Data Stream Processing Systems on Multi-Core Processors》，ICDE2017；Flink1.20.3官方状态后端与TM内存文档。论文中的instruction footprint与本报告RSS/PSS不同，不能混用术语。
