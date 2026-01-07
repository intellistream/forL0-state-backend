# ForL0 State Backend设计说明书

## 整体架构设计

ForL0 StateBackend 设计的目标是为 Apache Flink 提供高性能的状态后端实现。系统采用 **Swiss Tables 架构**（对齐 Go 1.24），通过 SWAR 并行匹配和 Extendible Hashing 实现高效的状态访问。

整体采用两层架构：**ForL0StateMap** 作为 StateMap 接口实现并负责 Directory 路由，**SwissTable** 作为底层存储层提供 SWAR 并行匹配能力。状态对象直接存储在 SwissTable 的堆内数组中，实现热路径零序列化。该架构支持 Flink 原生的 Checkpoint 和故障恢复机制，保证高兼容性与易部署性。

> 图 1 整体架构图

本系统采用 Swiss Tables + Extendible Hashing 设计，主要具有以下特点：

- **SWAR 并行匹配**：8 个 slot 同时比较，单次操作可检查 8 个候选位置，对齐 Go 1.24 Swiss Tables 实现；
- **Extendible Hashing**：增量 split 仅迁移 50% 数据，避免全局重哈希开销；
- **高负载因子**：87.5% (7/8) 负载因子，空间利用率优于传统哈希表；
- **Hash 存储优化**：存储完整 64 位 hash 值，split/grow 时无需重新计算；
- **堆内对象存储**：状态对象直接存储在 SwissTable 的 Object[] 数组中，热路径零序列化，仅在 Checkpoint 时序列化；
- **控制字节设计**：EMPTY=0x80, DELETED=0xFE, FULL=h2，支持高效的 SWAR 匹配操作；
- **检查点与容错**：与 Flink 原生快照机制兼容，支持 Savepoint/Restore 功能；
- **易集成与部署**：面向 Flink 1.20+，Java 8+ 构建，将生成 JAR 放入 Flink lib 目录或通过代码方式设置 StateBackend 即可集成。

**ForL0StateBackend** 通过 Swiss Tables 架构、SWAR 并行匹配与 Extendible Hashing，在保持与 Flink 原生接口兼容的同时，显著提升状态访问的性能和效率。

### SwissTable 存储层

SwissTable 是底层存储实现，采用对齐 Go 1.24 的 Swiss Tables 设计。每个 SwissTable 包含：

- **ctrl[]**：控制字节数组，每个 slot 1 字节，存储 EMPTY(0x80)、DELETED(0xFE) 或 h2(低 7 位)
- **keysNs[]**：key/namespace 存储，slot i 对应 keysNs[2*i] (key) 和 keysNs[2*i+1] (namespace)
- **values[]**：state 对象存储，slot i 对应 values[i]
- **hashes[]**：64 位完整 hash 存储，用于 rehash/grow/split 时避免重新计算

**表 1 SwissTable 参数**

| 参数 | 值 | 说明 |
|------|-----|------|
| INITIAL_TABLE_CAPACITY | 64 | 初始容量 |
| MAX_TABLE_CAPACITY | 1024 | 最大容量，超过触发 split |
| 负载因子 | 87.5% | 7/8，与 Go 1.24 一致 |
| EMPTY | 0x80 | 空槽标记 |
| DELETED | 0xFE | 已删除标记 |

### ForL0StateMap (Directory 路由层)

ForL0StateMap 实现 StateMap 接口，并负责 Extendible Hashing 的 Directory 路由：

- **directory[]**：SwissTable 引用数组，大小为 2^globalDepth
- **tables: List**：唯一 SwissTable 列表（去重）
- **globalDepth/globalShift**：Directory 深度和位移量

通过 `hash >>> globalShift` 计算 directory 索引，定位到目标 SwissTable。当 SwissTable 达到 MAX_TABLE_CAPACITY 且满载时，触发 split 操作，仅迁移约 50% 的数据到新表。

---

## 架构实现细节

ForL0 State Backend 基于 Flink 原生 HashMapStateBackend 架构进行改造，在保留原有核心接口和部分可复用组件的基础上，引入了 Swiss Tables 架构以支持高性能状态访问。这些新增组件与原有 Flink StateBackend 紧密协作，既保持了 API 兼容性，又实现了存储结构的深度优化。

> 图 4 ForL0StateBackend 组件图

### 2.1 可复用组件与扩展组件

在 ForL0 State Backend 中，以下 Flink 原生组件可直接复用。

#### 2.1.2 StateTable / StateMap

ForL0 在接口层面复用 Flink 的 StateTable/StateMap 抽象，但用 ForL0 专用实现替换了原有的 CopyOnWriteStateMap。**ForL0StateTable / ForL0StateMap** 承担对外统一状态表接口（增删改查、迭代、快照/恢复等），对上层 KeyedStateBackend 保持 API 兼容，同时在内部采用 Swiss Tables + Extendible Hashing 实现高效的状态存储。StateTable 抽象仍作为外部契约，便于与 Flink 的 Snapshot/Restore 流程对接。

#### 2.1.3 PriorityQueueManager / HeapPriorityQueue

定时器/优先队列相关功能直接复用或兼容 Flink 自身的堆实现（HeapPriorityQueue 等），ForL0 不改变定时器语义，PriorityQueue 的管理器按原有接口工作，保证与现有事件时间/处理时间定时器逻辑兼容。

#### 2.1.4 ForL0KeyedStateBackend

**ForL0KeyedStateBackend** 是与 Flink KeyedStateBackend 的适配扩展点，负责在任务初始化时组装 ForL0StateTable、初始化 Directory 和 SwissTable 等，并在 Snapshot/Restore 路径中插入 ForL0StateMap 的序列化/反序列化逻辑。该类承担生命周期管理、checkpoints 的协调以及把 ForL0 的内部结构映射到 Flink 的 KeyedState 接口上。

### 2.2 关键新增组件

#### 2.2.1 SwissTable

**SwissTable** 是核心存储组件，实现了对齐 Go 1.24 的 Swiss Tables 算法：

- **SWAR 并行匹配**：通过 `matchH2()` 方法，8 个 slot 同时比较
- **控制字节设计**：EMPTY=0x80, DELETED=0xFE, FULL=h2
- **Hash 位分配**：H1 = hash >>> 7（探测起始），H2 = hash & 0x7F（ctrl 字节）
- **存储数组**：ctrl[], keysNs[], values[], hashes[]
- **自动扩容**：负载达到 87.5% 时 grow() 翻倍，达到 MAX_CAPACITY 时返回 NEED_SPLIT

#### 2.2.2 ForL0StateMap

**ForL0StateMap** 实现 StateMap 接口并负责 Extendible Hashing 的 Directory 路由：

- **Directory 路由**：通过 `hash >>> globalShift` 定位 SwissTable
- **Split 操作**：当 SwissTable 返回 NEED_SPLIT 时，分裂为两个表
- **Go 风格去重**：`if (t.index == i) t.index = 2 * i` 避免重复处理
- **迭代器**：使用 SWAR matchFull 快速定位有效 slot

#### 2.2.3 NativeL0Memory

**NativeL0Memory** 提供 JNI 桥接，支持两种运行模式：

- **L0 模式**：鲲鹏 CPU 服务器上使用 libl0mempool.so
- **模拟模式**：其他环境使用标准 malloc/free

#### 2.2.4 ForL0State

在 State 层面，为不同类型的 State 提供了对应的 ForL0 实现（如 ValueState、ListState、MapState、ReducingState 等），这些类在 ForL0KeyedStateBackend 中注册被调用创建与更新。每个 ForL0State 实现将状态操作翻译为对 ForL0StateMap 的具体增删改查操作，从而向上层暴露与 Flink API 完全兼容的行为。

#### 2.2.5 Snapshot & Restore

快照与恢复流程沿用 Flink HashMapStateBackend 的 Snapshot/Restore 语义：ForL0StateMap 为快照的数据来源，快照过程使用 SWAR matchFull 遍历所有有效条目并序列化，恢复时重建 Directory 和 SwissTable 结构。

### 2.3 State Map 实现

ForL0StateMap 是 ForL0 State Backend 的核心实现，采用 Swiss Tables + Extendible Hashing 架构实现高效的状态存储与访问。

#### 2.3.1 结构组成

ForL0StateMap 由 Directory 路由层和 SwissTable 存储层组成：

- **Directory**：SwissTable 引用数组，通过 `hash >>> globalShift` 路由到目标表
- **SwissTable**：SWAR 并行匹配的哈希表，存储 key/namespace/value 对象引用

> 图 5 ForL0StateMap 结构

```
ForL0StateMap
├── directory[]: SwissTable[]  // 大小 = 2^globalDepth
├── tables: List<SwissTable>   // 唯一表列表
├── globalDepth: int           // Directory 深度
├── globalShift: int           // 64 - globalDepth
└── size: int                  // 总条目数

SwissTable
├── ctrl[]: byte[]             // 控制字节 (EMPTY/DELETED/h2)
├── keysNs[]: Object[]         // key/namespace 存储
├── values[]: Object[]         // state 存储
├── hashes[]: long[]           // 64位 hash 存储
├── localDepth: int            // 表深度
├── index: int                 // Directory 中的起始索引
└── used/tomb: int             // 使用/墓碑计数
```

#### 2.3.2 SwissTable 实现

SwissTable 的核心是 SWAR 并行匹配算法：

```java
// H1/H2 计算 (对齐 Go 1.24)
static long h1(long hash) { return hash >>> 7; }
static int h2(long hash) { return (int)(hash & 0x7F); }

// SWAR 并行匹配 - 8 slots 同时比较
static long matchH2(long ctrlWord, int h2) {
    long pattern = LSB * (h2 & 0xFFL);  // 0x0101010101010101L * h2
    long x = ctrlWord ^ pattern;
    return (x - LSB) & ~x & MSB;         // MSB = 0x8080808080808080L
}
```

**控制字节设计**：
- EMPTY = 0x80 (最高位为 1)
- DELETED = 0xFE (特殊标记)
- FULL = h2 (0x00-0x7F，最高位为 0)

#### 2.3.3 主要操作流程

**查询（Get）**

1. 计算 64 位 hash
2. 通过 `hash >>> globalShift` 定位 SwissTable
3. 计算 H1 确定探测起始 group
4. 加载 8 字节 ctrl word，SWAR 并行匹配 H2
5. 对匹配的 slot 进行 key/namespace equals 验证
6. 未找到则线性探测下一个 group，直到遇到 EMPTY

**插入（Put）**

```java
int result = table.put(hash, key, namespace, MAX_CAPACITY);
if (result == NEED_SPLIT) {
    handleSplit(table);
    continue; // 重试
}
int slot = result & SLOT_MASK;
boolean isNew = (result & NEW_FLAG) != 0;
table.values[slot] = value;
if (isNew) size++;
```

**删除（Remove）**

1. 查找目标 slot
2. 将 ctrl[slot] 设为 DELETED (0xFE)
3. 清空 keysNs/values/hashes 对应位置
4. 更新 used 和 tomb 计数

**Split 操作**

当 SwissTable 达到 MAX_CAPACITY (1024) 且满载时触发：

1. 创建新表，容量 = 旧表容量
2. 新表 localDepth = 旧表 localDepth + 1
3. 遍历旧表，根据 `hash >>> (globalShift - 1) & 1` 决定去向
4. 使用存储的 hashes[] 避免重新计算 hash
5. 更新 directory，Go 风格去重：`if (t.index == i) t.index = 2 * i`

### 2.4 ForL0State 实现

ForL0StateBackend 提供与 Flink State API 一致的五类状态实现：**ForL0ValueState、ForL0ListState、ForL0MapState、ForL0ReducingState 和 ForL0AggregatingState**。

#### 2.4.1 ForL0ValueState

表示每个 key/namespace 下的单一值。其主要操作与 State Map 映射关系如下：

- `value()`：调用底层 `ForL0StateMap.get` 读取当前 key/namespace 下的值；如果为 `null` 则返回默认值。  
- `update(value)`：当 value 不为 `null` 时调用 `ForL0StateMap.put` 写回；当 value 为 `null` 时底层调用 `ForL0StateMap.remove`。

#### 2.4.2 ForL0ListState

表示可追加/枚举的序列集合。其主要操作与 State Map 映射关系如下：

- `get()`：通过 `ForL0StateMap.get` 读取当前 key/namespace 下的 List 值，若为 `null` 则返回空或默认值。  
- `add(value)`：先 `get` 取出当前 List；若为 `null` 则新建 List，向 List 添加元素后显式调用 `ForL0StateMap.put` 写回，确保底层可见。  
- `update(values)`：将新的 List 完整序列化后通过 `ForL0StateMap.put` 覆盖写入；当 values 为空时调用 `ForL0StateMap.remove`。  
- `addAll(values)`：使用 `ForL0StateMap.transform` 将传入集合合并到已有状态，底层由 transform 操作实现 read-modify-write 并处理可能的内存重分配。

#### 2.4.3 ForL0MapState

表示键值映射集合。其主要操作与 State Map 映射关系如下：

- `get(userKey)`：通过 `ForL0StateMap.get` 获取 Map 对象，再在该 Map 上查询 `userKey` 并返回结果。  
- `put(userKey, userValue)` / `putAll(map)`：先 `get` 当前 Map（若 `null` 则新建），在内存上修改后通过 `ForL0StateMap.put` 显式写回以持久化变更。  
- `remove(userKey)`：读取并修改 Map；若修改后 Map 为空则调用 `ForL0StateMap.remove` 整条条目，否则显式 `put` 回写以保持一致性。  
- `contains(userKey)` / `entries()` / `keys()` / `values()`：均基于 `ForL0StateMap.get` 返回的 Map 在用户程序中进行判断或迭代。

#### 2.4.4 ForL0ReducingState

用于增量合并的聚合状态（reduce 聚合函数）。其主要操作与 State Map 映射关系如下：

- `get()`：通过 `ForL0StateMap.get` 读取当前累积值。  
- `add(value)`：调用 `ForL0StateMap.transform`，将新增值与已有状态通过用户提供的聚合函数合并；transform 负责读取旧值、执行聚合转换、并将结果序列化写回。  
- `merge()`：合并逻辑由聚合函数提供，最终写回仍通过 `put/transform` 完成。

#### 2.4.5 ForL0AggregatingState

支持更复杂的聚合语义（带 accumulator 的聚合）。其主要操作与 State Map 映射关系如下：

- `get()`：通过 `ForL0StateMap.get` 读取累加器，若非 `null` 则调用累加器的 `getResult()` 得到输出值并返回。  
- `add(value)`：通过 `ForL0StateMap.transform` 将输入值通过累加器的 `add` 方法添加到累加器中；底层通过一次 read-modify-write 完成序列化与堆外存管理。  
- `merge()`：由累加器的 `merge` 方法定义合并语义，底层负责将合并后的累加器持久化回 Entry Store 并更新索引。

---

## 容错机制设计

ForL0StateBackend 的容错设计基于 Flink 原生的快照和恢复框架，确保与 HeapStateBackend 格式的兼容性。ForL0StateMap 为快照的数据来源，状态对象直接存储在 SwissTable 的堆内数组中。

在写操作（如 put、transform、update）中，数据直接写入 SwissTable 的 keysNs[] 和 values[] 数组。在快照和恢复流程中，使用 SWAR matchFull 快速遍历所有有效条目进行序列化。由于热路径无序列化开销，仅在 Checkpoint 时才需要序列化对象，大大提高快照过程效率。

### 快照与恢复流程

**快照发起**  

在 Flink 调 `snapshot` 方法后，快照操作交由 `SnapshotStrategyRunner` 执行。`stateSnapshot` 方法为每个 key-group 生成状态快照，`ForL0StateMap` 快照以与 HeapStateBackend 相同的格式提供序列化接口。

**快照采集**  

`ForL0StateMapSnapshot` 使用 SwissTableIterator（基于 SWAR matchFull）遍历所有有效条目，从 keysNs[] 和 values[] 获取 key、namespace 和 value 对象。快照数据按 Flink 要求的顺序序列化写入 Checkpoint 输出流。

**恢复（Restore）**  

`ForL0RestoreOperation` 从快照流中反序列化对象，重建 Directory 和 SwissTable 结构。恢复后立即可用，无需额外初始化。

---

## 总结

**ForL0StateBackend** 采用 Swiss Tables + Extendible Hashing 架构，实现高效的状态访问。系统由 **ForL0StateMap（Directory 路由层）** 与 **SwissTable（存储层）** 构成：

- **SwissTable** 采用 SWAR 并行匹配（8 slots 同时比较），对齐 Go 1.24 实现
- **Extendible Hashing** 实现增量 split，仅迁移 50% 数据，避免全局重哈希
- **87.5% 负载因子** 提供优于传统哈希表的空间利用率
- **Hash 存储优化** 避免 split/grow 时重新计算 hash
- **堆内对象存储** 实现热路径零序列化

容错方面与 Flink 原生快照机制兼容：快照基于 SWAR matchFull 快速遍历有效条目，支持 Savepoint/Restore 功能。总体上，设计在保持与 Flink 快照格式和运行时集成兼容性的同时，实现了**高性能、高空间利用率**的状态后端。