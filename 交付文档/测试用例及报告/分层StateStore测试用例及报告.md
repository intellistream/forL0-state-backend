# 分层 StateStore 测试用例及报告

| 版本 | 日期 | 作者 | 说明 |
|------|------|------|------|
| v1.0 | 2026-04-14 | ForL0 Team | 初始版本 |

---

## 1. 概述

本文档为 ForL0StateBackend 分层 StateStore 的测试用例与测试报告，涵盖功能测试（单元测试与集成测试）和性能基准测试（WordCount Benchmark 与 NexMark Benchmark）。所有测试均面向鲲鹏服务器（ARM64/AArch64）平台。

### 1.1 测试目标

- 验证 ForL0StateBackend 在鲲鹏服务器上的功能正确性
- 验证所有 Flink KeyedState 类型（ValueState、ListState、MapState、ReducingState、AggregatingState）的正确性
- 验证 Checkpoint/Savepoint 机制的正确性和数据一致性
- 验证窗口聚合（Window Aggregate）场景下的状态管理正确性
- 通过 WordCount 和 NexMark 基准测试评估性能，与 HashMapStateBackend 进行对比

### 1.2 测试环境

| 项目 | 规格 |
|------|------|
| 服务器 | 鲲鹏（Kunpeng）ARM64 服务器 |
| CPU | 鲲鹏 920 / 鲲鹏 930 |
| 操作系统 | Linux (openEuler / CentOS ARM64) |
| JDK | JDK 1.8+ (aarch64) |
| Apache Flink | 1.20.3 |
| ForL0StateBackend | 1.0-SNAPSHOT |
| Native 库 | libforl0_engine.so (aarch64, NEON 启用) |

---

## 2. 功能测试用例

### 2.1 状态类型正确性测试 (ForL0StateTypesITCase)

基于 Flink MiniCluster 的集成测试，验证所有 KeyedState 类型的基本读写正确性。

| 编号 | 测试用例 | 测试方法 | 测试说明 | 预期结果 |
|------|----------|----------|----------|----------|
| FT-01 | ValueState 正确性 | `testValueState()` | 6 条记录、3 个 key (a:3, b:2, c:1)，验证 ValueState 计数累加 | 各 key 计数值正确 |
| FT-02 | ListState 正确性 | `testListState()` | 6 条 String 类型记录、3 个 key，验证 ListState 追加操作 | List 内容与顺序正确 |
| FT-03 | ReducingState 正确性 | `testReducingState()` | 6 条 Integer 记录，验证 ReducingState 求和归约 | 各 key 归约值正确 |
| FT-04 | AggregatingState 正确性 | `testAggregatingState()` | 自定义聚合函数，6 条记录 | 聚合结果正确 |
| FT-05 | MapState (String→Integer) 正确性 | `testMapState()` | 5 条组合 Tuple 记录，验证 MapState 读写 | Map 键值对正确 |
| FT-06 | MapState (Long→Long) 特化正确性 | `testMapStateLongLong()` | Long→Long 类型特化路径测试 | 特化路径结果正确 |
| FT-07 | ListState (Long) 正确性 | `testListStateLong()` | 5 条 Long 类型记录、2 个 key | List 内容与长度正确 |

### 2.2 Checkpoint/Savepoint 正确性测试 (ForL0CheckpointSavepointITCase)

验证 ForL0StateBackend 的检查点和保存点机制在鲲鹏服务器上的正确性。

| 编号 | 测试用例 | 测试方法 | 测试说明 | 预期结果 |
|------|----------|----------|----------|----------|
| CP-01 | 周期性 Checkpoint | `testPeriodicCheckpointCompletes()` | 200ms 间隔，ExactlyOnce 模式，验证 chk-* 目录生成 | Checkpoint 目录正确生成 |
| CP-02 | Savepoint 与恢复 | `testSavepointAndRestore()` | 两阶段：生成 1000 条记录 → 触发 Savepoint → 恢复并验证 | 恢复后数据一致 |
| CP-03 | Savepoint 精确状态验证 | `testSavepointAndRestore_ExactState()` | Savepoint 后恢复，验证 count 值 == N | 状态值精确匹配 |
| CP-04 | 外部化 Checkpoint | `testExternalizedCheckpoint_ExactState()` | RETAIN_ON_CANCELLATION 模式，取消后验证外部 Checkpoint | Checkpoint 文件保留且状态正确 |
| CP-05 | 多状态类型 Savepoint 兼容性 | `testSavepointCompatibilityMultiStateTypes()` | Value + List + Map 多状态类型，多 Namespace 模式 | 所有状态类型恢复正确 |

### 2.3 Checkpoint 优化测试 (ForL0CheckpointOptimizationITCase)

验证优化后的序列化路径在 Checkpoint/Savepoint 场景下的正确性。

| 编号 | 测试用例 | 测试方法 | 测试说明 | 预期结果 |
|------|----------|----------|----------|----------|
| CO-01 | 多状态类型恢复 | `testSavepointRestoreWithMultipleStateTypes()` | 500 条记录，Value/List/Map 混合状态操作 | 恢复后所有状态类型数据正确 |
| CO-02 | 大规模 Key 恢复 | `testSavepointRestoreWithManyKeys()` | 大量分布式 Key 的 Savepoint 恢复 | 所有 Key 状态正确恢复 |

### 2.4 窗口聚合测试 (ForL0WindowAggregateITCase)

验证 ForL0StateBackend 在 EventTime 滑动窗口场景下的正确性。

| 编号 | 测试用例 | 测试方法 | 测试说明 | 预期结果 |
|------|----------|----------|----------|----------|
| WA-01 | 滑动窗口聚合 | `testSlidingWindowAggregateWithTuple2LongLong()` | 1000ms 窗口 / 200ms 滑动，Tuple2<Long,Long> 累加器 | 窗口聚合结果正确 |
| WA-02 | 高负载窗口聚合 | `testHighLoadWindowAggregate()` | 10,000 条记录、1,000 个 key，高压力测试 | 所有窗口结果正确 |
| WA-03 | 窗口 Checkpoint 恢复 | `testWindowAggregateCheckpointRestore()` | 无界数据源 + Checkpoint + 重启 | 恢复后窗口状态正确 |

### 2.5 MiniCluster 集成测试 (ForL0MiniClusterITCase)

端到端集成测试，包含高负载和倾斜分布场景。

| 编号 | 测试用例 | 测试方法 | 测试说明 | 预期结果 |
|------|----------|----------|----------|----------|
| MC-01 | SPI 加载 ValueState | `testValueStateWordCountWithSPI()` | 通过 SPI 机制自动发现并加载 ForL0StateBackend | 作业正常完成，计数正确 |
| MC-02 | 高负载倾斜 ValueState | `testSkewedHighLoadValueState()` | 20M 记录、1M key、80% 流量集中于 1% 热点 key | 所有 key 状态正确 |
| MC-03 | 滑动窗口 WordCount | `testSlidingEventTimeWindowWordCount()` | EventTime 滑动窗口结合 ValueState | 窗口与状态结果正确 |

### 2.6 单元测试 (ListAppendDebugTest)

| 编号 | 测试用例 | 测试方法 | 测试说明 | 预期结果 |
|------|----------|----------|----------|----------|
| UT-01 | ListState 追加行为 | `testListAppendDoesNotCreateNewArrayList()` | 验证 ListState append 不创建新 ArrayList | 内存分配行为正确 |

### 2.7 C++ 原生引擎单元测试

通过 CMake 构建的 C++ 单元测试，验证底层数据结构和算法的正确性。

| 编号 | 测试用例 | 测试文件 | 测试说明 |
|------|----------|----------|----------|
| NT-01 | SwissTable 核心操作 | `swiss_table_test.cpp` | 插入、查找、删除、rehash、grow 操作 |
| NT-02 | COW 快照一致性 | `state_table_cow_test.cpp` | Copy-on-Write 机制正确性 |
| NT-03 | Checkpoint 序列化往返 | `checkpoint_round_trip_test.cpp` | 序列化→反序列化数据一致性 |
| NT-04 | 类型布局解析 | `type_layout_parser_test.cpp` | Java 类型到 C++ 布局映射 |
| NT-05 | TimeWindow Namespace | `time_window_namespace_test.cpp` | 时间窗口 Namespace 管理 |
| NT-06 | TypedInnerMap | `typed_inner_map_test.cpp` | 类型化内部 Map 操作 |

---

## 3. 性能基准测试用例

### 3.1 WordCount Benchmark

#### 3.1.1 测试目的

以纯状态访问（ValueState 读-改-写）为核心工作负载，排除窗口/定时器等开销干扰，最大化状态后端对吞吐量的影响，对比 ForL0StateBackend 与 HashMapStateBackend 的性能差异。

#### 3.1.2 测试架构

```
SkewedWordSource → KeyedProcessFunction (ValueState: count++) → MetricsSink (BlackHole)
```

- **数据源**: `SkewedWordSource`，产生 `Tuple2<Long, Long>` (key, 1L)
- **状态操作**: 每条记录执行 `value()` → 累加 → `update()` (1 次读 + 1 次写)
- **数据汇**: `MetricsSink`，统计吞吐量后丢弃数据

#### 3.1.3 测试参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `num_keys` | 1,000,000 | 高基数，压力测试 SwissTable |
| `num_records` | 2,000,000,000 | 20 亿条记录 |
| `arrival_rate` | 0 | 无限速，全速运行 |
| `skew_factor` | 0 | 均匀分布 (也可设为 1.1 进行 Zipf 倾斜测试) |
| `parallelism` | 8 | 并行度 8 |
| `checkpoint_interval` | 0 | 关闭 Checkpoint，最大吞吐量 |

#### 3.1.4 测试执行

```bash
cd benchmark/scripts
# 运行所有后端对比测试
python run_benchmark.py --test wordcount --backend all

# 单独运行 ForL0 后端
python run_benchmark.py --test wordcount --backend forl0

# 附带 CPU 火焰图采集
python run_benchmark.py --test wordcount --backend all --profile cpu
```

#### 3.1.5 测试指标

| 指标 | 单位 | 说明 |
|------|------|------|
| `throughput` | records/sec | 总吞吐量 |
| `throughput_per_core` | records/sec/core | 单核吞吐量 |
| `total_time_seconds` | sec | 总运行时间 |
| `total_records` | - | 处理的总记录数 |

#### 3.1.6 输出格式

测试结果以 JSON 格式存储于 `benchmark/results/raw/` 目录：

```json
{
  "benchmark": "wordcount",
  "backend": "forl0",
  "total_records": 2000000000,
  "total_time_seconds": 65.658,
  "throughput": 15230436.50,
  "throughput_per_core": 3807609.13,
  "parallelism": 8
}
```

### 3.2 NexMark Benchmark

#### 3.2.1 测试目的

使用 NexMark 流处理基准测试套件中的有状态查询（q4-q20），评估 ForL0StateBackend 在真实流处理场景下的性能表现。NexMark 模拟在线拍卖系统，涵盖窗口聚合、会话窗口、TopN、去重等多种有状态计算模式。

#### 3.2.2 测试数据模型

NexMark 包含三种实体类型，默认比例为 Person : Auction : Bid = 1 : 3 : 46：

| 实体 | 说明 | 比例 |
|------|------|------|
| Person | 参与拍卖的用户 | 1 |
| Auction | 拍卖商品 | 3 |
| Bid | 出价记录 | 46 |

#### 3.2.3 测试查询及参数

| 查询 | 事件数量 | 说明 | 状态类型 |
|------|----------|------|----------|
| q4 | 1.6×10⁸ | 分类平均价格 | 窗口聚合 |
| q5 | 1.6×10⁸ | 热门商品 | 滑动窗口 |
| q7 | 1.6×10⁸ | 最高出价 | 窗口聚合 |
| q8 | 2×10⁸ | 新用户监控 | 会话窗口 |
| q9 | 8×10⁷ | 中标结果 | 双流 Join |
| q11 | 1.6×10⁸ | 用户会话 | 会话窗口 |
| q12 | 1.6×10⁸ | 处理时间窗口 | ProcessingTime 窗口 |
| q18 | 1.6×10⁸ | 最近关闭拍卖 | 去重 |
| q19 | 1.6×10⁸ | 拍卖 TOP-10 价格 | TopN |
| q20 | 8×10⁷ | 拍卖价格分段 | 分组聚合 |

#### 3.2.4 测试执行

```bash
cd benchmark/scripts
# 运行所有有状态查询
python run_benchmark.py --test nexmark --backend all

# 运行指定查询
python run_benchmark.py --test nexmark --backend all --query q4,q5,q8

# 附带火焰图采集
python run_benchmark.py --test nexmark --backend forl0 --profile cpu
```

#### 3.2.5 测试指标

| 指标 | 单位 | 说明 |
|------|------|------|
| `throughput` | events/sec | 总吞吐量 |
| `throughput_per_core` | events/sec/core | 单核吞吐量 |
| 查询执行时间 | sec | 各查询的运行时间 |

### 3.3 可选性能分析

除基础吞吐量指标外，可选采集以下高级性能数据（仅 Linux 鲲鹏平台）：

| 分析类型 | 工具 | 命令参数 | 输出 |
|----------|------|----------|------|
| CPU 火焰图 | Async Profiler (v3.0+ ARM64) | `--profile cpu` | SVG 火焰图 |
| 内存分配火焰图 | Async Profiler | `--profile memory` | SVG 火焰图 |
| CPU Cache 命中率 | Linux perf_events | `--profile cache` | Cache miss 统计 |

火焰图颜色约定：
- **HashMapStateBackend**: 蓝色 (#4C72B0)
- **ForL0StateBackend**: 绿色 (#55A868)

---

## 4. 功能测试报告

### 4.1 测试执行方式

```bash
# 在鲲鹏服务器上执行全量功能测试
mvn test -Djava.library.path=src/main/resources/native
```

### 4.2 测试结果汇总

| 测试套件 | 用例数 | 通过 | 失败 | 通过率 |
|----------|--------|------|------|--------|
| ForL0StateTypesITCase | 7 | 7 | 0 | 100% |
| ForL0CheckpointSavepointITCase | 5 | 5 | 0 | 100% |
| ForL0CheckpointOptimizationITCase | 2 | 2 | 0 | 100% |
| ForL0WindowAggregateITCase | 3 | 3 | 0 | 100% |
| ForL0MiniClusterITCase | 3 | 3 | 0 | 100% |
| ListAppendDebugTest | 1 | 1 | 0 | 100% |
| C++ 原生引擎测试 | 6 | 6 | 0 | 100% |
| **合计** | **27** | **27** | **0** | **100%** |

### 4.3 功能测试覆盖矩阵

| 功能特性 | 覆盖的测试用例 |
|----------|----------------|
| ValueState 读写 | FT-01, MC-01, MC-02, MC-03 |
| ListState 读写 | FT-02, FT-07, UT-01 |
| MapState 读写 | FT-05, FT-06 |
| ReducingState 读写 | FT-03 |
| AggregatingState 读写 | FT-04 |
| Checkpoint 生成 | CP-01, CP-04 |
| Savepoint 触发与恢复 | CP-02, CP-03, CP-05, CO-01, CO-02 |
| 多状态类型混合 | CP-05, CO-01 |
| 时间窗口聚合 | WA-01, WA-02, WA-03 |
| 窗口 + Checkpoint | WA-03 |
| 高负载/热点 Key | MC-02 |
| SPI 自动发现 | MC-01 |
| NEON SIMD (SwissTable) | NT-01 |
| COW 快照一致性 | NT-02 |
| 序列化往返 | NT-03 |

---

## 5. 性能基准测试报告

### 5.1 WordCount Benchmark 测试报告

#### 5.1.1 测试配置

| 配置项 | 值 |
|--------|----|
| 平台 | 鲲鹏 ARM64 服务器 |
| 并行度 | 8 |
| Key 空间 | 1,000,000 |
| 记录数 | 2,000,000,000 |
| 数据分布 | 均匀分布 |
| Checkpoint | 关闭 |

#### 5.1.2 测试结果

| 后端 | 吞吐量 (records/sec) | 单核吞吐量 (records/sec/core) | 运行时间 (sec) |
|------|----------------------|-------------------------------|----------------|
| HashMapStateBackend | 待实测填入 | 待实测填入 | 待实测填入 |
| ForL0StateBackend | 待实测填入 | 待实测填入 | 待实测填入 |

> **说明**: 以上性能数据需在目标鲲鹏服务器上实际运行后填入。开发环境参考数据：ForL0 吞吐量约 15.2M records/sec (4 并行度)。

#### 5.1.3 报告生成

```bash
cd benchmark/scripts
python generate_report.py
```

生成内容包括：
- 吞吐量对比柱状图 (PDF 格式)
- HTML 格式详细报告
- 原始 JSON 数据

### 5.2 NexMark Benchmark 测试报告

#### 5.2.1 测试配置

| 配置项 | 值 |
|--------|----|
| 平台 | 鲲鹏 ARM64 服务器 |
| 并行度 | 8 |
| 查询集 | q4, q5, q7, q8, q9, q11, q12, q18, q19, q20 |
| 数据比例 | Person:Auction:Bid = 1:3:46 |
| 流速 | 无限速 (0 = full throughput) |

#### 5.2.2 测试结果

| 查询 | HashMap 吞吐量 | ForL0 吞吐量 | 提升比例 |
|------|----------------|--------------|----------|
| q4 | 待实测填入 | 待实测填入 | - |
| q5 | 待实测填入 | 待实测填入 | - |
| q7 | 待实测填入 | 待实测填入 | - |
| q8 | 待实测填入 | 待实测填入 | - |
| q9 | 待实测填入 | 待实测填入 | - |
| q11 | 待实测填入 | 待实测填入 | - |
| q12 | 待实测填入 | 待实测填入 | - |
| q18 | 待实测填入 | 待实测填入 | - |
| q19 | 待实测填入 | 待实测填入 | - |
| q20 | 待实测填入 | 待实测填入 | - |

> **说明**: 以上性能数据需在目标鲲鹏服务器上实际运行后填入。

---

## 6. 测试结论

1. **功能正确性**: ForL0StateBackend 全部 27 项功能测试用例通过，覆盖 5 种 KeyedState 类型、Checkpoint/Savepoint 机制、窗口聚合、高负载倾斜等场景，功能正确性验证完备。

2. **Flink API 兼容性**: ForL0StateBackend 完全兼容 Flink 1.20.3 StateBackend API，用户代码无需修改即可使用，支持 SPI 自动发现加载。

3. **Checkpoint/Savepoint 可靠性**: Savepoint 触发、恢复、多状态类型兼容、外部化 Checkpoint 等场景均验证通过，数据一致性满足 ExactlyOnce 语义。

4. **原生引擎稳定性**: C++ SwissTable 引擎的 6 项原生测试全部通过，序列化往返、COW 快照一致性等关键机制验证完备。

5. **性能基准**: WordCount 和 NexMark 两套基准测试体系完整，支持自动化执行和报告生成，待在目标鲲鹏服务器上完成最终性能数据采集。
