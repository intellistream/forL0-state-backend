一、Flink 基础与架构

1. 基础认知
	1.	什么是 Flink？它解决什么问题？
	2.	Flink 和 Spark Streaming / Structured Streaming 的区别是什么？
	3.	Flink 为什么适合实时计算？
	4.	Flink 的“批流一体”怎么理解？
	5.	有界流和无界流分别是什么？
	6.	Flink 的四大基石是什么？
	7.	Flink 常用 API 有哪些？DataStream、Table API、SQL 怎么选？
	8.	你们项目里为什么选 Flink，而不是 Spark 或 Kafka Streams？  ￼

2. 运行架构
	9.	JobManager、TaskManager、Dispatcher、ResourceManager 分别负责什么？
	10.	Client 提交任务到 Flink 集群的流程是什么？
	11.	一个 Flink 作业从提交到运行，经历了哪些阶段？
	12.	Flink 的并行度是怎么生效的？
	13.	slot 是什么？parallelism 和 slot 的关系是什么？
	14.	一个 TaskManager 可以有多个 slot，这意味着什么？
	15.	Operator Chain 是什么？为什么要做链式优化？
	16.	Flink 的执行图（JobGraph、ExecutionGraph）怎么理解？  ￼

⸻

二、时间语义、Watermark、窗口

3. 时间语义
	17.	Event Time、Processing Time、Ingestion Time 有什么区别？
	18.	实时计算为什么通常更推荐 Event Time？
	19.	什么时候可以用 Processing Time？
	20.	你们项目中的时间字段来自哪里？怎么处理脏时间戳？  ￼

4. Watermark
	21.	Watermark 是什么？本质上解决什么问题？
	22.	Watermark 和窗口的关系是什么？
	23.	有序流和乱序流的 Watermark 生成策略有什么不同？
	24.	bounded out-of-orderness 是什么？
	25.	为什么 Watermark 变慢会导致结果延迟？
	26.	多并行度下 Watermark 是怎么对齐/传播的？
	27.	空闲分区（idle partition）为什么会卡住 Watermark？
	28.	迟到数据怎么处理？allowed lateness 是什么？
	29.	late data 走侧输出流怎么做？
	30.	线上发现窗口一直不出结果，你会先查什么？  ￼

5. Window
	31.	Flink 支持哪些窗口？滚动、滑动、会话、全局窗口分别适合什么场景？
	32.	count window 和 time window 的区别是什么？
	33.	window assigner、trigger、evictor、window function 各自做什么？
	34.	ProcessWindowFunction 和 AggregateFunction / ReduceFunction 怎么选？
	35.	会话窗口的合并机制是什么？
	36.	窗口计算为什么容易出状态膨胀问题？  ￼

⸻

三、状态管理、Checkpoint、Exactly-once

6. State
	37.	Flink 中 state 是什么？为什么说 state 是实时计算核心？
	38.	Keyed State 和 Operator State 的区别是什么？
	39.	ValueState、ListState、MapState、ReducingState、AggregatingState 分别适合什么场景？
	40.	Broadcast State 是什么？用来解决什么问题？
	41.	状态 TTL 怎么用？为什么要设置 TTL？
	42.	状态过大会带来什么问题？  ￼

7. Checkpoint 与容错
	43.	Checkpoint 是什么？和 savepoint 有什么区别？
	44.	Flink Checkpoint 的核心原理是什么？
	45.	barrier 是什么？barrier alignment 是什么？
	46.	为什么多输入流会发生 barrier 对齐？
	47.	非对齐 checkpoint（unaligned checkpoint）是什么？什么时候用？
	48.	Checkpoint 为什么可以保证状态一致性？
	49.	Checkpoint 失败常见原因有哪些？
	50.	Checkpoint 间隔、超时时间、最小间隔怎么配置？
	51.	externalized checkpoint 有什么用？
	52.	savepoint 更适合哪些场景？升级、迁移、回滚怎么做？  ￼

8. Exactly-once
	53.	Flink 的 exactly-once 是怎么实现的？
	54.	exactly-once、at-least-once、at-most-once 的区别是什么？
	55.	“Flink 支持 exactly-once”具体是算子级，还是端到端？
	56.	端到端 exactly-once 需要 source 和 sink 满足什么条件？
	57.	如果下游不支持事务，怎么尽量接近 exactly-once？
	58.	两阶段提交（2PC）在 Flink Sink 里怎么落地？
	59.	Kafka source + Flink + Kafka sink 如何保证一致性？
	60.	Kafka source + Flink + MySQL / Doris / Hudi / Iceberg，语义分别怎么回答？  ￼

⸻

四、状态后端、容灾恢复、内存

9. 状态后端与恢复
	61.	HashMapStateBackend 和 RocksDBStateBackend 的区别是什么？
	62.	什么时候用内存状态，什么时候用 RocksDB？
	63.	incremental checkpoint 是什么？为什么能降低 checkpoint 成本？
	64.	checkpoint 数据一般存哪里？本地磁盘、HDFS、对象存储的区别是什么？
	65.	故障恢复时，状态和 source offset 是怎么一起恢复的？
	66.	rescale 之后状态如何重新分配？  ￼

10. 内存与序列化
	67.	Flink 内存模型了解吗？
	68.	managed memory 是干什么的？
	69.	堆内存、堆外内存、网络内存各自有什么作用？
	70.	序列化为什么会影响性能？
	71.	Kryo 什么时候会出现？为什么很多人不希望回退到 Kryo？  ￼

⸻

五、Source / Sink / Kafka / CDC

11. Source 与 Sink
	72.	Flink 常见 source 和 sink 有哪些？
	73.	KafkaSource 相比旧版 FlinkKafkaConsumer 有什么变化？
	74.	Kafka 分区数和 Flink 并行度怎么匹配？
	75.	source 并行度大于 Kafka 分区数会怎样？
	76.	sink 并行写出时如何避免热点和乱序？  ￼

12. Kafka 相关
	77.	Flink 消费 Kafka 时 offset 什么时候提交？
	78.	Kafka 数据重复消费的常见原因有哪些？
	79.	Kafka exactly-once 和 Flink exactly-once 的边界在哪里？
	80.	Kafka 分区倾斜会对 Flink 造成什么影响？
	81.	你们线上 Kafka 积压时，Flink 侧怎么排查？  ￼

13. CDC
	82.	Flink CDC 原理了解吗？
	83.	Flink CDC 做全量 + 增量同步时要注意什么？
	84.	binlog 断点续传是怎么做的？
	85.	CDC 场景下 schema 变更怎么处理？
	86.	用 Flink CDC 做维表同步、库表同步、实时数仓入湖时，难点有哪些？
这些题经常出现在“实时数仓/湖仓一体”岗位的延伸追问里，尤其当岗位描述里出现 Kafka、MySQL、Doris、Hudi、Iceberg、Paimon 时更容易问。 ￼

⸻

六、Join、维表、SQL、Table API

14. Join
	87.	Flink 里常见 Join 有哪些？window join、interval join、regular join 有什么区别？
	88.	双流 join 为什么容易出状态爆炸？
	89.	interval join 适合什么场景？
	90.	流表 join 和双流 join 的区别是什么？
	91.	维表 join 有哪些实现方式？广播维表、异步 I/O、lookup join 各自适合什么场景？
	92.	维表数据更新频繁时怎么处理一致性？
	93.	热 key join 怎么解决？  ￼

15. Flink SQL / Table API
	94.	Table API 和 SQL 的底层关系是什么？
	95.	Flink SQL 的执行流程是什么？SQL -> 逻辑计划 -> 物理计划，大概怎么走？
	96.	retract stream、upsert stream 是什么？
	97.	changelog 流怎么理解？
	98.	append-only 表和 upsert 表的区别是什么？
	99.	Flink SQL 做聚合为什么经常会产生更新流？
	100.	mini-batch 优化是什么？
	101.	state retention 在 SQL 里为什么重要？
	102.	TopN、去重、group by、窗口聚合常见的状态风险是什么？  ￼

⸻

七、部署、运维、监控、调优

16. 运行模式
	103.	Flink 常见部署模式有哪些？Standalone、Yarn、Kubernetes 各自优缺点是什么？
	104.	Session、Per-Job、Application 模式有什么区别？
	105.	为什么现在很多公司把 Flink 部署在 K8s 上？
	106.	高可用（HA）怎么做？  ￼

17. 调优与排障
	107.	Flink 反压是什么？怎么看？
	108.	反压会影响哪些指标？
	109.	线上延迟变高，你怎么定位是 source、计算、状态还是 sink 的问题？
	110.	数据倾斜在 Flink 里怎么识别和处理？
	111.	checkpoint 变慢通常从哪几方面排查？
	112.	为什么状态越大，checkpoint 往往越慢？
	113.	CPU 高、GC 频繁、网络打满、sink 写入慢，各自会有什么现象？
	114.	如何看 Flink Web UI 里的 busy/backPressured/idle？
	115.	如何做并行度调优？
	116.	operator chaining 什么时候要断开？
	117.	异步 I/O 能解决什么问题？它的风险是什么？
	118.	你做过哪些线上优化？比如减少状态、减少 shuffle、提升 checkpoint 成功率、处理反压。