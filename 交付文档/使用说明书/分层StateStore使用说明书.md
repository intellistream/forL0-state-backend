# 分层 StateStore 使用说明书

| 版本 | 日期 | 作者 | 说明 |
|------|------|------|------|
| v1.0 | 2026-04-14 | ForL0 Team | 初始版本 |

---

## 1. 概述

### 1.1 产品简介

ForL0StateBackend 是一个为 Apache Flink 设计的高性能分层状态后端，采用 Swiss Tables 架构和 SWAR (SIMD Within A Register) 并行匹配技术，专为鲲鹏（Kunpeng）服务器优化。其核心特性包括：

- **Swiss Tables 哈希表**: 基于 Go 1.24 SwissMap 设计，8 个 slot 同时比较，87.5% 负载因子
- **NEON SIMD 加速**: 利用鲲鹏 ARM64 NEON 指令集进行并行匹配
- **C++ 原生引擎**: 核心状态存储和操作由 C++ 实现，零 GC 压力
- **L0 Cache 集成**: 可选启用鲲鹏 CPU 的 L0 Cache 加速（需硬件支持）
- **完全兼容 Flink API**: 用户代码无需修改即可使用

### 1.2 适用范围

- **目标平台**: 鲲鹏 ARM64 服务器（openEuler / CentOS ARM64）
- **Flink 版本**: Apache Flink 1.20.3
- **JDK 版本**: JDK 1.8+（aarch64）
- **运行模式**:
  - **L0 模式**: 鲲鹏服务器上具备 L0 内存设备（`/dev/hisi_l0`）和 `libl0mempool.so` 库
  - **模拟模式**: 标准 malloc/free，无 L0 硬件时自动回退

### 1.3 支持的状态类型

| 状态类型 | 说明 | 对应类 |
|----------|------|--------|
| ValueState | 单值状态 | `ForL0ValueState` |
| ListState | 列表状态 | `ForL0ListState` |
| MapState | 映射状态 | `ForL0MapState` |
| ReducingState | 归约状态 | `ForL0ReducingState` |
| AggregatingState | 聚合状态 | `ForL0AggregatingState` |

---

## 2. 快速开始

### 2.1 前置条件

1. 鲲鹏 ARM64 服务器已部署 Apache Flink 1.20.3 Standalone 集群
2. JDK 1.8+ (aarch64) 已安装
3. ForL0 JAR 包 (`flink-statebackend-forL0-1.0-SNAPSHOT.jar`) 已构建
4. Native 库 (`libforl0_engine.so`) 已编译（参见《编译构建指导书》）

### 2.2 安装部署

#### 步骤 1：部署 JAR 包

将 ForL0 JAR 包复制到 Flink 的 `lib` 目录：

```bash
cp flink-statebackend-forL0-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
```

#### 步骤 2：部署 Native 库

将编译好的 native 库复制到 Flink 可访问的目录：

```bash
mkdir -p $FLINK_HOME/native
cp libforl0_engine.so $FLINK_HOME/native/
```

#### 步骤 3：配置 Flink

编辑 `$FLINK_HOME/conf/config.yaml`（或 `flink-conf.yaml`），添加以下配置：

```yaml
# 设置 ForL0 为默认状态后端
state:
  backend:
    type: forl0

# 配置 Native 库路径（所有 TaskManager 节点）
env:
  java:
    opts:
      all: -Djava.library.path=/opt/flink/native

# Checkpoint 配置（按需）
state:
  checkpoints:
    dir: file:///data/flink/checkpoints
  checkpoint-storage: filesystem
```

#### 步骤 4：重启 Flink 集群

```bash
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

#### 步骤 5：验证加载

检查 TaskManager 日志，确认 ForL0StateBackend 正确加载：

```bash
grep "\[ForL0\]" $FLINK_HOME/log/flink-*-taskmanager-*.log
```

预期输出：
```
[ForL0] Running in L0 mode       # 如果鲲鹏 L0 硬件可用
[ForL0] Running in simulation mode  # 如果无 L0 硬件
```

---

## 3. 配置参考

### 3.1 状态后端配置

ForL0StateBackend 支持通过 Flink 配置文件或代码方式配置。

#### 3.1.1 配置文件方式

在 `$FLINK_HOME/conf/config.yaml` 中配置：

```yaml
state:
  backend:
    type: forl0    # 或使用全限定类名: org.apache.flink.state.forl0.ForL0StateBackendFactory
```

#### 3.1.2 代码方式

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new ForL0StateBackend());
```

或通过 Factory 方式：

```java
env.setStateBackend(new ForL0StateBackendFactory().createFromConfig(
    new Configuration(), Thread.currentThread().getContextClassLoader()));
```

### 3.2 配置参数详情

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `state.backend.type` | String | - | 设为 `forl0` 启用 ForL0StateBackend |
| `state.backend.forl0.async-snapshots` | boolean | `true` | 启用异步快照 |
| `state.backend.forl0.l0-cache.enabled` | boolean | `false` | 启用 L0 Cache 内存（需鲲鹏硬件支持） |
| `state.backend.forl0.l0-cache.size` | long | 268435456 (256MB) | L0 Cache 内存池大小（字节） |
| `state.backend.forl0.l0-cache.max-per-alloc` | long | 65536 (64KB) | 单次最大 L0 分配（超出则回退堆内存） |

### 3.3 SwissTable 内部参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 初始容量 | 64 | 每个 SwissTable 的初始 slot 数（2 的幂次，≥8） |
| 最大容量 | 1024 | 单表最大容量（达到后触发分裂） |
| 负载因子 | 87.5% | 触发 rehash/grow 的负载阈值 |

### 3.4 L0 Cache 配置

当鲲鹏服务器具备 L0 Cache 硬件时，可启用 L0 加速：

```yaml
state:
  backend:
    type: forl0
    forl0:
      l0-cache:
        enabled: true
        size: 268435456       # 256MB L0 内存池
        max-per-alloc: 65536  # 单次最大 64KB
```

**L0 模式检测条件**：
1. L0 设备文件存在: `/dev/hisi_l0`
2. L0 内存库可加载: `libl0mempool.so`
3. 配置项 `l0-cache.enabled` 为 `true`

如果条件不满足，ForL0StateBackend 将自动回退到模拟模式（使用标准堆内存），功能不受影响。

---

## 4. 使用方式

### 4.1 DataStream API 使用

ForL0StateBackend 完全兼容 Flink DataStream API，用户代码无需修改：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 方式一：通过配置文件自动加载（推荐）
// 在 flink-conf.yaml 中设置 state.backend.type: forl0

// 方式二：代码中显式设置
env.setStateBackend(new ForL0StateBackend());

// 正常使用 KeyedState
DataStream<Tuple2<String, Long>> input = ...;

input.keyBy(t -> t.f0)
     .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
         private ValueState<Long> countState;

         @Override
         public void open(Configuration parameters) {
             countState = getRuntimeContext().getState(
                 new ValueStateDescriptor<>("count", Long.class));
         }

         @Override
         public void processElement(Tuple2<String, Long> value, Context ctx, 
                                    Collector<String> out) throws Exception {
             Long count = countState.value();
             count = (count == null) ? 1L : count + 1;
             countState.update(count);
             out.collect(value.f0 + ": " + count);
         }
     });
```

### 4.2 Flink SQL 使用

Flink SQL 作业通过配置文件方式使用 ForL0StateBackend 即可，无需额外代码：

```sql
-- 在 SQL Client 或 Table API 中正常编写查询
-- ForL0StateBackend 自动管理底层状态

CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DOUBLE,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (...);

-- 窗口聚合（使用 TimeWindow namespace）
SELECT user_id, 
       TUMBLE_START(order_time, INTERVAL '1' MINUTE) AS window_start,
       SUM(amount) AS total_amount
FROM orders
GROUP BY user_id, TUMBLE(order_time, INTERVAL '1' MINUTE);
```

### 4.3 Checkpoint 与 Savepoint

ForL0StateBackend 完全支持 Flink 的 Checkpoint 和 Savepoint 机制：

```yaml
# Checkpoint 配置
execution:
  checkpointing:
    interval: 60000           # 60 秒间隔
    mode: EXACTLY_ONCE
    externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

state:
  checkpoints:
    dir: file:///data/flink/checkpoints
  savepoints:
    dir: file:///data/flink/savepoints
```

**触发 Savepoint**：

```bash
# 通过 CLI 触发
$FLINK_HOME/bin/flink savepoint <jobId> file:///data/flink/savepoints

# 从 Savepoint 恢复
$FLINK_HOME/bin/flink run -s file:///data/flink/savepoints/savepoint-xxx -d your-job.jar
```

### 4.4 作业提交

#### 通过 CLI 提交

```bash
$FLINK_HOME/bin/flink run -d \
  -Dstate.backend.type=forl0 \
  -p 8 \
  your-application.jar
```

#### 通过 REST API 提交

```bash
# 上传 JAR
curl -X POST -H "Expect:" \
  -F "jarfile=@your-application.jar" \
  http://<jobmanager>:8081/jars/upload

# 提交作业
curl -X POST \
  http://<jobmanager>:8081/jars/<jar-id>/run \
  -d '{"programArgs": "--parallelism 8", 
       "flinkConfiguration": {"state.backend.type": "forl0"}}'
```

---

## 5. Namespace 行为说明

ForL0StateBackend 采用分层 StateStore 架构，对不同 Namespace 类型有特化处理：

### 5.1 VoidNamespace 特化

对于使用 `VoidNamespace` 的状态（如 KeyedProcessFunction 中的 ValueState），ForL0 自动检测并跳过 Namespace HashMap 层，直接访问 SwissTable，实现零额外开销：

```
KeyGroup[i] → SwissTable<K, S>   （直接访问，无 HashMap 开销）
```

适用场景：
- `KeyedProcessFunction` 中的 `ValueState`、`ListState`、`MapState` 等
- 非窗口算子的 KeyedState

### 5.2 TimeWindow Namespace

对于使用 `TimeWindow` 作为 Namespace 的状态（如窗口聚合），ForL0 使用 Namespace Map 进行隔离：

```
KeyGroup[i] → Map<TimeWindow, SwissTable<K, S>>
```

适用场景：
- `WindowFunction` / `ProcessWindowFunction`
- `AggregateFunction` 配合时间窗口

### 5.3 Namespace 自动清理

当某个 Namespace 下的 SwissTable 变为空时（所有 key 被删除），ForL0 自动从 Namespace Map 中移除该 SwissTable，释放内存。

---

## 6. 性能调优建议

### 6.1 内存配置

ForL0StateBackend 的状态数据存储在 JVM 堆外（C++ 管理），因此：

- **Task Manager 内存**: 根据状态规模适当增加 `taskmanager.memory.process.size`
- **Managed Memory**: ForL0 不依赖 Flink Managed Memory，可适当降低 `taskmanager.memory.managed.size`

### 6.2 并行度设置

- 并行度直接影响 KeyGroup 分配，建议与 CPU 核心数对齐
- 鲲鹏服务器建议设置：`parallelism = TaskManager 数量 × 每 TM slot 数`

### 6.3 L0 Cache 调优

启用 L0 Cache 时的建议配置：

| 状态规模 | L0 Cache Size | Max Per Alloc |
|----------|---------------|---------------|
| 小 (< 100MB 状态) | 64MB | 32KB |
| 中 (100MB - 1GB) | 256MB (默认) | 64KB (默认) |
| 大 (> 1GB 状态) | 512MB+ | 128KB |

### 6.4 Checkpoint 调优

- **异步快照**: 保持 `async-snapshots: true`（默认），避免阻塞数据处理
- **Checkpoint 间隔**: 根据容错需求设置，间隔越长吞吐量越高
- **Checkpoint 存储**: 生产环境建议使用 HDFS 或共享文件系统

---

## 7. 运维监控

### 7.1 日志

ForL0StateBackend 使用 `[ForL0]` 前缀输出日志，可通过以下方式查看：

```bash
# 查看 ForL0 相关日志
grep "\[ForL0\]" $FLINK_HOME/log/flink-*-taskmanager-*.log

# 关键日志信息
[ForL0] Running in L0 mode              # L0 硬件模式
[ForL0] Running in simulation mode      # 模拟模式
[ForL0] Native engine loaded            # 原生引擎加载成功
[ForL0] State backend initialized       # 状态后端初始化完成
```

### 7.2 模式检测 API

在 Java 代码中可以通过以下 API 检测当前运行模式：

```java
NativeEngine.isL0Mode();          // 是否 L0 模式
NativeEngine.getModeDescription(); // 模式描述字符串
```

### 7.3 常见问题排查

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| `UnsatisfiedLinkError` | Native 库路径未配置 | 检查 `java.library.path` 包含 `libforl0_engine.so` 所在目录 |
| `ClassNotFoundException: ForL0StateBackendFactory` | JAR 未部署到 lib | 确认 JAR 在 `$FLINK_HOME/lib/` 下 |
| 启动后显示 simulation mode | 无 L0 硬件或配置未启用 | 检查 `/dev/hisi_l0` 和 `l0-cache.enabled` |
| Checkpoint 失败 | Checkpoint 目录不可写 | 检查 `state.checkpoints.dir` 路径权限 |
| 内存不足 | 状态规模超出预期 | 增加 TaskManager 内存或减少状态 TTL |

---

## 8. 限制与注意事项

1. **鲲鹏平台优化**: ForL0StateBackend 的 NEON SIMD 优化仅在 ARM64（鲲鹏）架构上生效
2. **线程安全**: Flink 状态访问为单线程模型，ForL0 遵循此约定，不提供并发支持
3. **无降级模式**: Native 库加载失败将直接抛出异常，不会降级到纯 Java 模式
4. **状态后端迁移**: 从 HashMapStateBackend 迁移到 ForL0StateBackend 需要从新的 Savepoint 开始（两者 Checkpoint 格式兼容）
5. **L0 Cache 限制**: L0 Cache 总容量有限（通常 20MB 级别），适合热点状态加速，大规模状态仍存储在堆外内存
