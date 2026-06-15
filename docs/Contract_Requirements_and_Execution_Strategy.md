# 合同约束记录与执行策略（ForL0）

更新时间：2026-06-12

## 1. 合同原文记录

（1）测试环境：
1) 服务器：鲲鹏 920 高性能版，2 路 CPU 160 核；JVM：毕昇 JDK-8
2) 启动三个容器进行测试。两个运行 TM 的容器，单个容器规格为 4 核 16G，每个容器里运行一个 TM，每个 TM 的并发度为 4。一个运行 JM 的容器，容器规格为 2 核 8G。容器启动时绑到同一个 NUMA 的核上，容器之间的网络通过网桥连接。
3) 数据盘：ES3000 V5 SSD

（2）测试场景
1）Nexmark 测试场景：Checkpoint 间隔 10s；8 条 SQL 使用的数据量分别为，Q8 一亿条，Q4、Q5、Q11、Q18、Q19 八千万条，Q9 四千万条，Q20 六千万条。
2）Wordcount 测试场景：Sliding Time Window Wordcount；测试数据部分，key 个数为 100 万级，数据 Skew 为 20%-30%，数据量为亿量级；Checkpoint 间隔 3s 等。
3）客户 A 测试用例：双流 Join 加聚合
4）性能指标：throughput/cores，计算公式：（优化后的吞吐-优化前的吞吐）/优化前的吞吐；

（3）验收基础指标：
1）Nexmark 有状态的 8 条 SQL：Q4、Q5、Q8、Q9、Q11、Q18、Q19、Q20 平均端到端性能提升 60%
2）Wordcount 用例端到端性能提升 60%
3）客户 A 测试用例提升 40%
4）Nexmark 中 8 条 SQL 进行故障恢复测试，故障恢复耗时不低于开源基于 Heap State 耗时
5）以非侵入式提供插件代码
6）专利一篇通过甲方内部评审，提交交底书

（4）挑战目标：
1）Nexmark 运行时 TM 内存使用率峰值降低 30%
2）Wordcount 用例端到端性能提升 70%
3）客户 A 测试用例提升 50%
4）专利挑战通过潜高

## 2. 红线清单（不可改）

以下内容属于合同口径，作为正式交付结果时不允许改动：

1. 测试硬件与软件环境（鲲鹏 920、毕昇 JDK-8、ES3000 V5 SSD）。
2. 容器拓扑和资源规格（2 个 TM 容器 + 1 个 JM 容器，TM 4C16G、JM 2C8G）。
3. TM 并发度约束（每个 TM 并发度为 4）。
4. NUMA 绑核与容器网桥连接方式。
5. NexMark 的 8 条 SQL 集合与各 Query 数据量。
6. NexMark Checkpoint 间隔 10s。
7. WordCount 场景类型（Sliding Time Window）、key 规模、Skew 范围、亿级数据量、Checkpoint 间隔 3s。
8. 客户 A 场景定义（双流 Join + 聚合）。
9. 评估指标定义（throughput/cores 及提升率公式）。

## 3. 可优化清单（不改变合同口径）

在不触碰红线前提下，可做且建议做的优化方向：

1. ForL0 插件内部实现优化：JNI、native engine、序列化和 cache 路径优化。
2. ForL0 专属配置调优：例如 L0 cache 策略/容量、内部阈值（仅插件参数，不改场景口径）。
3. 执行稳定性优化：减少无效重试、改进 profiling 采样有效性、保证结果可复现。
4. 构建与部署优化：保持非侵入式插件方式，完善一键部署/运行与离线依赖自动化。
5. 故障恢复链路优化：在相同场景和参数下优化恢复耗时与稳定性。

## 4. 当前仓库与合同项对齐状态

当前 benchmark 配置已对齐关键合同项：

1. NexMark Checkpoint：10s（nexmark_checkpoint_interval=10000）。
2. WordCount Checkpoint：3s（wordcount_checkpoint_interval=3000）。
3. NexMark 8 条 SQL 与各 Query 事件量已按合同值配置。
4. WordCount 为 Sliding Time Window，key/数据量/Skew 已按合同区间配置。

## 5. 执行策略（保证合规且尽量有利于 ForL0）

1. 正式交付只使用合同参数运行，并单独出具“合同口径结果”。
2. 如需内部诊断，可增加“诊断报告”字段，但不得替代或污染合同结果。
3. 所有 ForL0 优化动作优先放在插件内部与插件参数，不改 benchmark 合同场景定义。
4. 每次提交结果时附带环境指纹（容器规格、JDK、Flink 版本、配置快照）确保可追溯。
5. 故障恢复测试按合同 8 条 SQL 全量执行，输出恢复时间与对照基线。

## 6. Query 级参数策略（合同内可用）

在不改变合同 workload 的前提下，可对 ForL0 插件内部参数做 query-specific 调优。

建议配置方式（已在 benchmark/config/benchmark.yaml 支持）：

```yaml
backends:
	- name: forl0
		config:
			l0_cache_enabled: true
			l0_cache_size: 64mb
			initial_table_capacity: 256
			max_table_capacity: 4096
			main_table_load_factor_threshold: 0.75
			query_overrides:
				q9:
					l0_cache_enabled: false
					initial_table_capacity: 64
					max_table_capacity: 1024
				q11:
					l0_cache_enabled: true
					l0_cache_size: 64mb
					initial_table_capacity: 256
					max_table_capacity: 4096
				q18:
					l0_cache_enabled: true
					l0_cache_size: 64mb
					initial_table_capacity: 256
					max_table_capacity: 4096
```

说明：

1. 该策略不改 SQL 集合、事件量、checkpoint 间隔，合同口径保持不变。
2. 仅影响 `-Dstate.backend.forl0.*` 内部参数，属于插件内优化。
3. 运行日志里的 Java Command 可直接看到 query 对应的生效参数。

## 7. 变更审批规则（建议执行）

1. 任何涉及红线项的改动都必须标记为“非合同实验”，且不得进入正式验收报告。
2. 任何正式验收前的参数调整，需做“是否触碰合同口径”的逐条核对。
3. 评审汇报时先展示合同口径结果，再展示可选挑战目标结果。
