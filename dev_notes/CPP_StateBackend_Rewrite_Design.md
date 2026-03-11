# ForL0 C++ State Backend 重写设计说明书

## 1. 背景与动机

### 1.1 问题陈述

当前 ForL0 State Backend 完全使用 Java 实现。尽管已通过 SwissTable+SWAR 并行匹配、VoidNamespace 特化、公有字段直接访问等手段进行了深度优化，但 Java 语言本身的局限性仍然制约了进一步的性能提升：

- **对象头开销**：每个 Java 对象携带 12-16 字节的对象头（Mark Word + Klass Pointer），在大规模状态存储场景下，元数据占比不可忽视。
- **GC 压力**：状态对象存活于整个 checkpoint 周期，长期存活对象给 G1/ZGC 的 Old Generation 带来持续的标记扫描负担。当状态量达到 GB 级别时，GC 暂停显著影响尾延迟。
- **内存布局不可控**：JVM 的对象布局由 GC 决定，无法保证 SwissTable 的 ctrl 数组与 entries 数组在物理内存上紧邻，降低了 prefetch 效率。
- **SIMD 不可用**：Java 的 Vector API 尚未稳定（仍为 incubator），无法利用 NEON/SVE 指令进行真正的 SIMD 并行匹配，当前仅能使用 SWAR 模拟。
- **间接引用链过深**：`State → StateStore → SwissTable → entries[] → Object` 的引用链导致多次 cache miss。

### 1.2 设计目标

将整个 ForL0 State Backend 的核心逻辑重写为 C++，在保持 Flink StateBackend API 兼容性的前提下，实现以下目标：

1. **零序列化热路径**：用户算子内的状态访问（get/update/add 等）不产生任何 Java↔C++ 的序列化/反序列化开销。状态操作的语义应被视为一种"声明"，由 C++ 侧"编译执行"。
2. **全 C++ 状态存储**：SwissTable、Namespace 路由、KeyGroup 分区等核心数据结构全部在 C++ off-heap 内存中实现。
3. **全 C++ Checkpoint**：快照写入与恢复逻辑全部由 C++ 完成，直接从 off-heap 内存序列化到 checkpoint 流，无需回调 Java TypeSerializer。
4. **高性能数据结构**：SwissTable 实现参考 abseil-cpp 的 `absl::flat_hash_map`，利用 SSE2/NEON 进行真正的 SIMD 并行匹配。
5. **内存布局可控**：所有状态数据采用紧凑的 flat 布局，消除对象头和引用链开销，最大化 cache 友好性。
6. **彻底重写，清理旧代码**：这不是在现有 Java 实现旁边另建一套 C++ 实现。这是一次**完全替换**——用 C++ 重写全部核心逻辑后，现有的纯 Java 实现代码（SwissTable.java、ForL0StateStore*.java、快照内部实现等）将被**删除**，仅保留必须存在的 Java 薄壳文件（Flink 接口实现类）。最终交付物中不存在两套并行的实现。

### 1.3 非目标（本期）

- L0 Cache 集成（后续独立设计）
- Queryable State 支持
- State TTL 支持（可在 C++ 侧后续扩展）
- State Migration / Schema Evolution（要求类型不变）

---

## 2. 整体架构

### 2.1 分层架构

整个系统分为三层：**Java 薄壳层**、**JNI 桥接层**、**C++ 引擎层**。

```
┌─────────────────────────────────────────────────────────────┐
│                    Flink Runtime (Java)                       │
│  StreamTask → KeyedProcessOperator → setCurrentKey()         │
│                                    → processElement()        │
└────────────────────────┬────────────────────────────────────┘
                         │ Flink API 契约（不可修改）
┌────────────────────────▼────────────────────────────────────┐
│              Java 薄壳层 (Thin Shell)                        │
│                                                              │
│  ForL0StateBackend          ForL0KeyedStateBackend           │
│  ForL0ValueState            ForL0ListState                   │
│  ForL0MapState              ForL0ReducingState               │
│  ForL0AggregatingState      ForL0SnapshotStrategy            │
│                                                              │
│  职责：实现 Flink 接口签名，所有方法体仅为 JNI 调用的          │
│       一行转发，不含任何业务逻辑。                             │
└────────────────────────┬────────────────────────────────────┘
                         │ JNI 调用（极薄）
┌────────────────────────▼────────────────────────────────────┐
│                JNI 桥接层 (Bridge)                            │
│                                                              │
│  forl0_jni.cpp — JNI 函数注册与类型分派                       │
│                                                              │
│  职责：                                                      │
│   · Java Object → C++ native handle 的映射                   │
│   · 类型 ID 分派（根据初始化时确定的类型走不同模板实例）         │
│   · 异常转换（C++ exception → Java Exception）                │
└────────────────────────┬────────────────────────────────────┘
                         │ C++ 内部调用
┌────────────────────────▼────────────────────────────────────┐
│                C++ 引擎层 (Engine)                            │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐    │
│  │ StateEngine  │  │ SwissTable   │  │ CheckpointEngine │    │
│  │ <K,N,S>     │  │ (abseil对齐) │  │                  │    │
│  └──────┬──────┘  └──────┬───────┘  └────────┬─────────┘    │
│         │                │                    │              │
│  ┌──────▼──────┐                    ┌────────▼─────────┐    │
│  │ TypeLayout  │                    │ CheckpointCodec  │    │
│  │ (类型映射)  │                    │ (Flink格式读写)  │    │
│  └─────────────┘                    └──────────────────┘    │
│                                                              │
│  职责：                                                      │
│   · 全部状态存储与访问逻辑                                    │
│   · SwissTable SIMD 并行匹配（对象内联存储于 slot）           │
│   · Checkpoint 二进制读写                                     │
│   · 内存管理（C++ 对象生命周期，RAII）                        │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 核心设计原则

**原则一：Java 薄壳，C++ 厚核**

Java 层仅实现 Flink 接口签名，方法体只做 JNI 转发。所有计算、存储、序列化逻辑均在 C++ 中完成。Java 层不持有任何状态数据，仅持有指向 C++ 对象的 `long nativeHandle`。

**原则二：类型一次翻译，终身 C++ 执行**

初始化时（`createOrUpdateInternalState`），根据 Java TypeSerializer 推断出 C++ 侧的类型描述符（TypeLayout），后续所有操作直接以 C++ 原生类型执行，不再涉及 Java 对象。

**原则三：类型翻译而非序列化**

热路径上绝不存在序列化/反序列化。"序列化"指的是 Object → DataOutputView → byte stream 这条路径，涉及虚方法调度、缓冲区管理、格式编码。我们需要的是"类型翻译"——在初始化时分析 Java 类型的结构，确定其对应的 C++ 类型（如 `int64_t`、`std::string`、自动生成的 C++ struct）。后续所有状态操作在 JNI 边界将 Java 对象转换为 C++ 对象，C++ 对象直接内联存储于 SwissTable 的 slot 中，与 abseil `flat_hash_map` 完全一致。全程不经过 `TypeSerializer`，不产生 byte stream，不做格式编码。

**原则四：Checkpoint 全 C++ 闭环**

数据以 C++ 对象的形式存储于 SwissTable 的 slot 中，checkpoint 时 C++ 遍历 SwissTable，直接从 slot 中的 C++ 对象按 Flink 兼容格式写出二进制流，无需回调 Java TypeSerializer。恢复时同理，C++ 从二进制流读取数据并构造 C++ 对象存入 SwissTable。

### 2.3 旧 Java 代码清理

这是一次彻底的重写，不是在现有 Java 代码旁边增加一套 C++ 实现。重写完成后，现有的纯 Java 实现代码将被**全部删除**，仅保留必要的 Java 薄壳（Flink 接口实现层）。

**删除的 Java 文件**（核心逻辑已完全由 C++ 替代）：

| 文件 | 原有职责 | C++ 替代 |
|------|---------|---------|
| `SwissTable.java` | Java SwissTable 哈希表 | C++ `swiss_table.h`（abseil 对齐，SIMD 16路） |
| `SwissTableInt.java` / `SwissTableLong.java` | 特化版 SwissTable | C++ 模板特化 |
| `ForL0StateStore.java` | 状态存储（KeyGroup → Namespace → SwissTable） | C++ `StateEngine` |
| `ForL0StateStoreInt.java` / `ForL0StateStoreLong.java` | 特化版状态存储 | C++ 模板特化 |
| `ForL0StateStoreSnapshot.java` | 快照表示 | C++ `CheckpointEngine` |
| `ForL0StateStoreKeyGroupReader.java` | KeyGroup 读取器 | C++ `checkpoint_reader.h` |
| `ForL0SnapshotResources.java` | 快照资源管理 | C++ COW 机制 |
| `ForL0SavepointRestoreOperation.java` | Savepoint 恢复 | C++ `checkpoint_reader.h` |
| `ForL0RestoreOperation.java` | 通用恢复操作 | C++ `checkpoint_reader.h` |
| `ForL0KeyContext.java` | currentKey 上下文 | Key 随操作传递到 C++ 侧 |
| `ForL0KeyValueStateIterator.java` | KV 迭代器 | C++ SwissTable 迭代 |
| `RowDataBinaryConverter.java` | RowData 二进制转换 | C++ `type_layout.h` |
| `space/NativeL0Memory.java` | L0 内存 JNI 桥接 | C++ `allocator.h`（统一分配器） |
| `space/L0MemoryAllocator.java` | 分配器接口 | C++ `allocator.h` |
| `space/NativeL0MemoryAllocator.java` | 分配器实现 | C++ `allocator.h` |
| `utils/UnsafeAccess.java` | Unsafe 工具类 | 仅保留字段提取器需要的部分，其余删除 |

**重写为 JNI 转发薄壳的 Java 文件**（保留，但方法体清空为单行 JNI 调用）：

| 文件 | 保留原因 |
|------|---------|
| `ForL0StateBackend.java` | Flink `StateBackend` 接口实现 |
| `ForL0StateBackendFactory.java` | Flink SPI 服务发现入口 |
| `ForL0KeyedStateBackend.java` | Flink `AbstractKeyedStateBackend` 实现 |
| `ForL0KeyedStateBackendBuilder.java` | Builder 模式入口 |
| `ForL0ValueState.java` | Flink `InternalValueState` 接口实现 |
| `ForL0ListState.java` | Flink `InternalListState` 接口实现 |
| `ForL0MapState.java` | Flink `InternalMapState` 接口实现 |
| `ForL0ReducingState.java` | Flink `InternalReducingState` 接口实现 |
| `ForL0AggregatingState.java` | Flink `InternalAggregatingState` 接口实现 |
| `ForL0SnapshotStrategy.java` | Flink snapshot 外层控制流 |
| `ForL0Options.java` | 配置选项（用户可调参数） |
| `NativeEngine.java` | native 库加载与 JNI 方法声明 |
| `TypeAnalyzer.java` | Java 类型分析（生成 TypeLayout 描述符传给 C++） |
| `StateUID.java` | 状态唯一标识（轻量工具类） |

**不受影响的文件**（benchmark、测试等外围代码不在清理范围内）。

---

## 3. 类型系统设计

### 3.1 核心思想：编译而非序列化

这是整个设计的核心。问题的本质是：**如何将 Java 的动态类型系统映射为 C++ 的静态类型系统，使得状态以 C++ 原生对象的形式存储在 SwissTable 中？**

答案是：**将类型映射视为编译过程**。在状态注册时（冷路径），分析 Java TypeSerializer 对应的类型，确定其 C++ 对应类型（如 `int64_t`、`std::string`、自动生成的 struct），然后以该 C++ 类型作为 SwissTable 的模板参数进行实例化。后续所有热路径操作直接以 C++ 原生对象执行——对象内联存储在 SwissTable 的 slot 中，与 abseil `flat_hash_map` 的存储方式完全一致。

需要严格区分两种完全不同的操作：

| | 序列化（禁止出现在热路径） | 类型转换（热路径使用） |
|---|---|---|
| 本质 | 将对象编码为自描述字节流 | 将 Java 对象转换为 C++ 对象（或反向） |
| 经过的路径 | Object → TypeSerializer.serialize() → DataOutputView → byte[] | Java int → JNI jint → C++ int32_t；Java String → JNI GetStringUTFRegion → std::string |
| 虚方法调度 | 有（TypeSerializer 是多态的） | 无（类型在注册时已确定，走模板特化路径） |
| 格式编码 | 有（VarInt、UTF-8 编码等） | 无（原生类型直传，字符串直拷） |
| 内存分配 | 有（DataOutputView 缓冲区） | C++ 对象内联在 SwissTable slot 中，由 SwissTable 管理 |

#### 3.1.1 类型分类（按 C++ 对应类型）

| 类别 | Java 类型 | C++ 类型 | JNI 转换方式 |
|------|----------|---------|-------------|
| **原始类型** | `int`, `long`, `float`, `double`, `boolean` | `int32_t`, `int64_t`, `float`, `double`, `bool` | JNI 直传原始值，零开销 |
| **字符串类型** | `String`, `byte[]` | `std::string` | JNI GetStringUTFRegion / GetByteArrayRegion |
| **POJO/Tuple 结构类型** | `POJO`, `Tuple` | 自动生成的 C++ struct（成员为上述类型的组合） | Unsafe 逐字段提取 → 逐字段构造 C++ struct |
| **RowData 类型** | `RowData` (SQL/Table API 内部类型) | 单字段原始类型时**展开为原始类型**（如 `int64_t`）；多字段全定长时为 `FixedLengthRow<N>` C++ struct；含变长字段时为通用 C++ struct | RowData 类型化 getter 逐字段提取（`getLong(pos)`/`getString(pos)` 等，**非 Unsafe**） |
| **容器类型** | `List<T>`, `Map<K,V>` | `std::vector<T>`, `absl::flat_hash_map<K,V>` | 遍历 Java 容器，逐元素转换并 push_back / insert |

关键点：**不存在"fallback 到序列化"的路径**。所有 Java 类型最终都被映射为 C++ 原生对象，存储在 SwissTable slot 中。

#### 3.1.2 TypeLayout 描述符

TypeLayout 是"编译"的产物，描述一个 Java 类型对应的 C++ 类型信息：

- **PrimitiveLayout**：原始类型。记录 C++ 类型（int32_t/int64_t/float/double/bool）。SwissTable 直接以该类型为模板参数，对象内联在 slot 中。
- **StringLayout**：字符串类型。C++ 侧为 `std::string`，内联在 slot 中。`std::string` 自身通过 SSO（Small String Optimization）处理短字符串，长字符串由 `std::string` 内部管理堆内存。
- **StructLayout**：结构类型（POJO/Tuple）。记录各字段的名称、Java 端 Unsafe 偏移量、C++ struct 中的字段偏移量和子 TypeLayout。C++ 侧生成对应的 struct 定义，该 struct 整体内联在 slot 中。
- **RowDataLayout**：RowData 类型（SQL/Table API）。记录各字段的 LogicalType 和位置索引。根据字段构成分为三种子策略：单字段原始类型展开、全定长字段 FixedLengthRow、含变长字段的通用 struct。详见 §3.1.5。
- **ContainerLayout**：容器类型。记录元素的 TypeLayout。C++ 侧为 `std::vector<ElementType>` 或 `absl::flat_hash_map<K,V>`，对象内联在 slot 中（容器本身的元素数据由容器自行管理堆内存，与 `std::string` 同理）。

#### 3.1.3 类型映射的生成流程

状态注册时（`createOrUpdateInternalState` 调用链，冷路径）：

1. Java 薄壳从 `StateDescriptor` 获取 K/N/S 的 `TypeSerializer`
2. **类型分析器**（TypeAnalyzer）检查 TypeSerializer 的具体类：
   - `IntSerializer` / `LongSerializer` 等 → PrimitiveLayout
   - `StringSerializer` / `BytePrimitiveArraySerializer` → StringLayout
   - `RowDataSerializer` → **RowData 专用分析流程**（详见 §3.1.5）：
     1. 提取 `LogicalType[]` 字段类型列表
     2. **单字段原始类型展开**：若 RowData 仅含一个原始类型字段（如 `RowData[BIGINT]`），直接展开为该原始类型的 PrimitiveLayout（如 `int64_t`），使用与 `LongSerializer` 完全相同的快速路径
     3. **全定长字段**：若所有字段均为定长类型（BIGINT/INT/FLOAT/DOUBLE/BOOLEAN/TIMESTAMP(p≤3)），生成 `FixedLengthRow<N>` 特化的 StructLayout
     4. **含变长字段**：生成通用 StructLayout，含变长字段（VARCHAR 等）的子 TypeLayout
   - `PojoSerializer` → 通过反射获取字段列表和 Unsafe 偏移量，递归生成 StructLayout
   - `TupleSerializer` → 按位置生成 StructLayout
   - `ListSerializer<T>` → ContainerLayout，元素为 T 的 TypeLayout
   - `MapSerializer<K,V>` → ContainerLayout，元素为 (K, V) pair 的 TypeLayout
3. 将 TypeLayout 序列化为描述符字节流，通过 JNI 传递给 C++
4. C++ 侧根据 TypeLayout 选择对应的模板实例化（高频组合）或使用 interpreted 路径（通用组合）
5. Java 侧根据 TypeLayout 生成对应的**字段提取器**：
   - POJO/Tuple：封装 Unsafe 偏移量
   - RowData：封装 `RowData.FieldGetter[]`（通过 `RowData.createFieldGetter(logicalType, pos)` 创建）

#### 3.1.4 POJO/Tuple 结构类型的处理详解

以一个 POJO 类型为例说明类型映射的工作方式：

假设用户状态类型为 `UserProfile { long userId; String name; double score; }`

**注册时（冷路径，一次性）**：类型分析器通过反射发现三个字段，确定对应的 C++ struct 布局——`struct UserProfileSlot { int64_t userId; std::string name; double score; }`。同时用 `Unsafe.objectFieldOffset()` 记录各字段在 Java 对象中的偏移量，生成字段提取器。SwissTable 以 `UserProfileSlot` 为 value 类型实例化（或在 interpreted 路径中以等价的运行时布局处理）。

**update(profile) 时（热路径）**：字段提取器通过 Unsafe 从 Java 对象逐字段提取——`Unsafe.getLong(profile, 16)` 得到 userId，`Unsafe.getObject(profile, 24)` 取得 String 引用通过 JNI 转为 C++ `std::string`，`Unsafe.getDouble(profile, 32)` 得到 score。这些值传入 C++ 侧构造 `UserProfileSlot` 对象，直接存入 SwissTable 的 slot 中。全程无 TypeSerializer 调用、无 DataOutputView、无格式编码。

**value() 时（热路径）**：反向操作——C++ 从 SwissTable slot 读取 `UserProfileSlot` 对象的各字段，通过 JNI 传回 Java 侧，字段提取器用 `Unsafe.allocateInstance()` 创建 UserProfile 实例，用 `Unsafe.putLong/putObject/putDouble` 填充字段。全程同样无反序列化。

#### 3.1.5 RowData 类型的处理详解（SQL/Table API 核心路径）

RowData 是 Flink SQL/Table API 的内部数据表示，是 Nexmark SQL 基准测试中所有 key 和 value 的类型。RowData 的处理与 POJO 有本质区别，需要专门的优化策略。

##### 3.1.5.1 BinaryRowData 内部格式

Flink 的 `RowDataSerializer` 在序列化时将所有 RowData 转换为 `BinaryRowData`——一种基于 `MemorySegment`（本质是 byte[]）的紧凑二进制格式：

```
BinaryRowData 内存布局:
┌──────────────────────────────────────────────────────┐
│ Fixed-length part                                     │
│ ┌─────────┬───────────────┬─────────────────────────┐ │
│ │ Header  │ Null bit set  │ Field values            │ │
│ │ 1 byte  │ ((arity+71)/64)│ 8 bytes × arity        │ │
│ │ RowKind │  ×8 bytes     │ [f0][f1]...[fN-1]      │ │
│ └─────────┴───────────────┴─────────────────────────┘ │
├──────────────────────────────────────────────────────┤
│ Variable-length part (仅当存在变长字段时)              │
│ String/byte[] 的实际数据                              │
└──────────────────────────────────────────────────────┘
```

**关键特性：**
- 每个字段在 fixed-length part 占固定 8 字节 slot
- 定长类型（BIGINT、INT、DOUBLE、BOOLEAN、TIMESTAMP(p≤3)）：值直接存储在 8 字节 slot 中
- 变长类型（VARCHAR、VARBINARY）：8 字节 slot 存储 offset+length，实际数据在 variable-length part
- BinaryRowData **不是 Java 对象字段**——数据存储在 MemorySegment 中，不能用 Unsafe 提取

##### 3.1.5.2 RowData 字段提取机制（非 Unsafe）

与 POJO 使用 `Unsafe.getLong(obj, offset)` 不同，RowData 的字段提取通过 **RowData 接口的类型化 getter**：

```java
// RowData 接口方法（BinaryRowData 和 GenericRowData 均实现）
rowData.getLong(pos)      // → long（BIGINT、TIMESTAMP(p≤3)）
rowData.getInt(pos)       // → int（INTEGER、DATE）
rowData.getDouble(pos)    // → double
rowData.getFloat(pos)     // → float
rowData.getBoolean(pos)   // → boolean
rowData.getString(pos)    // → StringData（VARCHAR）
rowData.isNullAt(pos)     // → boolean（null 检查）
```

这些 getter 在 BinaryRowData 上是直接内存读取（`segment.getLong(offset + nullBitsSize + pos * 8)`），开销极低。在 GenericRowData 上是数组索引（`fields[pos]`），同样极低。**不涉及 TypeSerializer、不涉及格式编码、不涉及 DataOutputView。**

##### 3.1.5.3 单字段原始类型展开（最关键优化）

Nexmark SQL 中大量查询的 key 仅含一个字段（如 `GROUP BY bidder` → `RowData[BIGINT]`）。此时：

**TypeAnalyzer 检测到 RowDataSerializer 仅含 1 个 BIGINT 字段** → 直接展开为 `int64_t`，使用与 `LongSerializer` 完全相同的快速路径。

```java
// Java 薄壳中的 setCurrentKey 处理
RowData keyRow = ...;  // Flink runtime 设置的 RowData key
long actualKey = keyRow.getLong(0);  // 一次 getter 调用，零序列化
// 后续所有操作使用 actualKey（long）走 int64_t 快速路径
```

调用链：
```
Java: state.value()
  → long key = currentKeyRow.getLong(0)     // RowData getter（直接内存读取）
  → JNI: nativeGetValueLongLong(handle, key, keyGroup)  // jlong 直传
  → C++: SwissTable<int64_t, int64_t>::get(key)          // 原生 int64_t 查找
  → JNI return jlong
  → Java: reconstruct RowData from long value
```

**与 DataStream Long key 的性能差异：仅多一次 `rowData.getLong(0)` 调用（约 1-2 ns），其余完全相同。** 这使得 SQL/Table API 的状态访问性能几乎等同于 DataStream API。

##### 3.1.5.4 多字段全定长 RowData（Nexmark 窗口查询）

Nexmark SQL 的窗口查询 key 包含多个字段，如 `RowData[bidder: BIGINT, window_start: TIMESTAMP(3), window_end: TIMESTAMP(3)]`。其中 TIMESTAMP(3) 是 compact 格式，存储为 `long` 毫秒值。

当 RowData 的**所有字段均为定长类型**时（这覆盖了 Nexmark SQL 的全部查询），使用 `FixedLengthRow<N>` 特化：

```cpp
// C++ 侧：全定长 RowData 的存储类型
template <size_t N>
struct FixedLengthRow {
    int64_t fields[N];  // 每个定长字段统一存储为 int64_t
    
    bool operator==(const FixedLengthRow& o) const {
        return std::memcmp(fields, o.fields, N * 8) == 0;
    }
};

// Hash：对 N 个 int64_t 做 combine
template <size_t N>
struct FixedLengthRowHash {
    size_t operator()(const FixedLengthRow<N>& row) const {
        size_t h = 0;
        for (size_t i = 0; i < N; ++i) {
            h = h * 0x9e3779b97f4a7c15ULL + std::hash<int64_t>{}(row.fields[i]);
        }
        return h;
    }
};
```

**JNI 传输**：Java 侧逐字段提取为 long 数组，通过 JNI 传递：

```java
// Java 薄壳：多字段 RowData key 提取
long[] keyFields = new long[arity];  // 可复用的数组
for (int i = 0; i < arity; i++) {
    keyFields[i] = rowData.getLong(i);  // 或根据 LogicalType 调用对应 getter
}
// JNI: nativePutFixedRow(handle, keyGroup, keyFields, value)
```

```cpp
// C++ JNI 入口
void nativePutFixedRow(jlong handle, jint keyGroup, jlongArray keyFields, ...) {
    jlong* fields = env->GetLongArrayElements(keyFields, nullptr);
    FixedLengthRow<3> key;
    std::memcpy(key.fields, fields, 3 * 8);  // 单次 memcpy
    env->ReleaseLongArrayElements(keyFields, fields, JNI_ABORT);
    table->put(keyGroup, key, value);
}
```

**性能分析**：
- Java 侧：N 次 `rowData.getLong(pos)`（每次约 1-2 ns，直接内存读取）
- JNI 传输：单次 `GetLongArrayElements`（pin 或 copy，N × 8 字节）
- C++ 侧：`FixedLengthRow<N>` 内联在 SwissTable slot 中，hash 为 N 个 int64_t combine
- **全程零 TypeSerializer 调用、零 DataOutputView、零格式编码**

对比当前 generic path（RowDataSerializer.serialize → 36 字节 binary → hash 36 bytes → SwissTable<string, string>），性能提升来自：
1. 消除 `RowDataSerializer.serialize()` 的 `toBinaryRow()` + `writeInt` + `copyToView` 开销
2. 消除 `RowDataSerializer.deserialize()` 的 `readInt` + `readFully` + `new byte[]` + `wrap` 开销
3. hash(N × int64_t) 远优于 hash(36 bytes binary blob)（更少字节、更好的 hash 分布）
4. `SwissTable<FixedLengthRow<3>, V>` 的 slot 大小（24 bytes + V）远小于 `SwissTable<string, string>` 的 slot 大小（≥64 bytes per string object）

##### 3.1.5.5 含变长字段的 RowData

当 RowData 存在 VARCHAR/VARBINARY 等变长字段时（Nexmark SQL 中较少见），使用通用 struct 路径：

```java
// Java 侧：按字段类型分别提取
long f0 = rowData.getLong(0);          // BIGINT → long
StringData f1 = rowData.getString(1);  // VARCHAR → StringData
byte[] f1Bytes = f1.toBytes();         // 转为字节数组
double f2 = rowData.getDouble(2);      // DOUBLE → double
// JNI: 打包为 packed buffer [8:f0][4:f1Len][f1Bytes][8:f2]
```

C++ 侧根据 StructLayout 描述符从 packed buffer 中按已知偏移读取各字段，构造 C++ struct。这仍然是**已知布局的内存读写**，不是序列化。

##### 3.1.5.6 RowData 值的重建（value() 返回路径）

从 C++ 读取状态值后，需要重建 Java 侧的 RowData：

- **单字段展开**：C++ 返回原始值（如 `jlong`），Java 创建 `GenericRowData(1)` 并 `setField(0, value)`
- **多字段定长**：C++ 返回 `long[]`，Java 创建 `GenericRowData(N)` 并逐字段 `setField(i, fields[i])`
- **含变长字段**：C++ 返回 packed buffer，Java 按 TypeLayout 逐字段解析并填充 `GenericRowData`

注意：返回的是 `GenericRowData` 而非 `BinaryRowData`。Flink 的 codegen 算子代码可以直接使用 `GenericRowData`（它实现了 `RowData` 接口），无需转为 `BinaryRowData`。

### 3.2 模板特化策略

C++ 引擎层对高频类型组合进行模板特化，生成最优代码：

| Key 类型 | Namespace | State 类型 | 场景 |
|----------|-----------|-----------|------|
| `int64_t` | VoidNamespace | `int64_t` | DataStream 数值聚合 / SQL 单字段 BIGINT 聚合（最常见） |
| `int64_t` | VoidNamespace | `double` | 浮点值状态 |
| `int64_t` | VoidNamespace | `std::string` | 字符串值状态 |
| `int64_t` | `int64_t` (Window) | `int64_t` | DataStream 窗口聚合 |
| `int32_t` | VoidNamespace | `int64_t` | 整数 key 场景 |
| `FixedLengthRow<2>` | VoidNamespace | `int64_t` | SQL 二字段 key 聚合（如 GROUP BY id, category） |
| `FixedLengthRow<3>` | VoidNamespace | `int64_t` | SQL 窗口聚合（如 GROUP BY bidder, window_start, window_end） |
| `FixedLengthRow<3>` | VoidNamespace | `FixedLengthRow<2>` | SQL 窗口聚合 多累加器 |
| `std::string` | VoidNamespace | `std::string` | 通用 fallback（复杂类型） |

对于未预实例化的类型组合，C++ 侧使用 interpreted 路径——根据 TypeLayout 描述符在运行时确定偏移量和读写方式，功能等价但比模板特化版多一层间接寻址。注意这仍然不是序列化——依然是已知偏移处的直接内存读写，只是偏移量来自运行时描述符而非编译期常量。

---

## 4. 状态存储引擎设计

### 4.1 StateEngine 总体结构

StateEngine 是核心存储组件，取代原有的 `ForL0StateStore`，采用完全不同的内存布局：

```
StateEngine<K, N, S>
│
├── KeyGroupRange [startKG, endKG]
│
├── VoidNamespace 模式:
│   └── SwissTable<K,S>* tables[numKeyGroups]
│       每个 table 独立管理一个 key group 的状态
│
└── General Namespace 模式:
    └── NamespaceRouter<N>* routers[numKeyGroups]
        每个 router 管理 N → SwissTable<K,S>* 的映射
        └── SwissTable<K,S>* (每个 namespace 独立)
```

### 4.2 SwissTable 设计（对齐 abseil-cpp）

SwissTable 的 C++ 实现应严格对齐 abseil-cpp 的 `raw_hash_set`，主要包含以下要素：

#### 4.2.1 内存布局

采用 abseil 的经典布局：ctrl 数组在前，紧随 slot 数组，二者在同一块连续内存中分配。这保证了探测过程中 ctrl 与对应 slot 的物理距离最小化，大幅降低 cache miss。

- **ctrl 数组**：每 slot 1 字节，`EMPTY=0x80`, `DELETED=0xFE`, `FULL=H2(hash)`
- **slot 数组**：紧跟 ctrl 之后，每个 slot 存储一个完整的 `slot_type` 对象（即 `std::pair<const K, V>` 或等价结构），对象直接内联存储于 slot 中
- **对齐**：整个块以 16 字节（SSE2）或 64 字节（cache line）对齐

对比重写前：原实现使用三个独立数组（`ctrl[]`, `entries[]`, `hashes[]`），重写后将 ctrl 和 slots 合并为单一连续内存块，消除了三次独立数组访问的 cache miss。

#### 4.2.2 SIMD 并行匹配

原实现使用 SWAR（8 路并行），重写后直接使用平台原生 SIMD：

- **x86-64**：SSE2 `_mm_cmpeq_epi8` + `_mm_movemask_epi8`，16 路并行匹配
- **AArch64 (鲲鹏)**：NEON `vceqq_u8`，16 路并行匹配
- **Fallback**：portable SWAR（与原实现等价）

16 路并行意味着 group size 从 8 提升到 16，每次探测覆盖的 slot 数量翻倍，减少探测次数。

#### 4.2.3 探测策略

采用 abseil 的三角数探测（triangular probing）：第 i 次探测偏移为 `i*(i+1)/2`，等价于依次探测 `0, 1, 3, 6, 10, ...` 组。这比简单的线性或二次探测有更好的分布特性。

#### 4.2.4 增长策略

- 负载因子上限：87.5%（与 abseil 一致，7/8）
- 增长倍数：2x（capacity 始终为 2 的幂）
- Rehash 触发：tombstone 过多时原容量 rehash，无 tombstone 时 2x grow
- 初始容量：16 slots（一个 SSE group），比原实现的 64 更保守，因为 C++ slot 占用更少

#### 4.2.5 Slot 存储模型（对象内联，与 abseil 一致）

与 abseil `flat_hash_map` 完全一致：每个 slot 直接存储 C++ 对象本身（通过 placement new 构造），而非指向外部存储的指针。

- **原始类型**（`int64_t`, `double` 等）：slot 大小在编译期确定，完全内联，零指针追踪
- **`std::string`**：string 对象（通常 32 字节）内联在 slot 中。短字符串（≤ 22 字节）通过 SSO 完全存储在 slot 内；长字符串由 `std::string` 自行管理堆内存
- **结构类型**：struct 对象整体内联在 slot 中，其内部的 `std::string` 成员同样遵循上述规则
- **容器类型**：`std::vector`/`absl::flat_hash_map` 对象内联在 slot 中，元素数据由容器自行管理堆内存

这正是 abseil 的做法——`flat_hash_map<std::string, std::string>` 的每个 slot 包含两个 `std::string` 对象，各自管理自己的堆内存。没有单独的 arena。

**Rehash 时的对象移动**：当 SwissTable 增长时，slot 中的对象需要迁移到新内存。对于支持 move 语义的类型（`std::string`, `std::vector` 等），这是 O(1) 的指针转移，不涉及数据拷贝。

### 4.3 内存管理

SwissTable 的内存管理与 abseil 保持一致：

- **表内存**：每个 SwissTable 拥有一块连续内存（ctrl + slots），通过 `aligned_alloc` 分配，64 字节对齐以确保 cache line 友好
- **对象生命周期**：slot 中的 C++ 对象通过 placement new 构造、显式析构函数销毁，严格遵循 RAII
- **对象内部堆内存**：`std::string`、`std::vector` 等类型自行管理的堆内存，随对象析构自动释放
- **分配器抽象**：所有表内存分配通过统一的 `Allocator` 接口完成，为后续集成 L0 Cache 分配器预留替换入口

对比重写前：原实现使用三个独立数组（`ctrl[]`, `entries[]`, `hashes[]`）且存储 Java 对象引用；重写后以单一连续内存块存储 ctrl + 内联对象，消除了对象头、引用链和多次独立数组访问的 cache miss。

---

## 5. JNI 边界设计

### 5.1 设计哲学：最少穿越次数，最大信息密度

每次 JNI 调用都有固定开销（约 50-100 ns），因此设计目标是：

- 每个用户状态操作只穿越 JNI 边界 **一次**
- 每次穿越携带完整的操作所需信息
- 避免在 JNI 边界上传递 Java 对象引用（尽可能传原始类型）

### 5.2 Handle 体系

C++ 侧的所有持久对象通过 `long nativeHandle` 暴露给 Java。Handle 的本质是 C++ 对象的指针地址（cast 为 `jlong`）。

| Handle 类型 | 持有者 | 生命周期 |
|------------|--------|---------|
| `backendHandle` | `ForL0KeyedStateBackend` | backend 整个生命周期 |
| `stateHandle` | 各 State 实现类 | state 注册后到 backend 关闭 |

### 5.3 热路径 JNI 接口设计

对于所有类型，JNI 边界上传递的是**字段的原始值**（原始类型直传、字节块类型传 byte[]），而非序列化后的字节流。对于结构类型，Java 侧的字段提取器在 JNI 调用前通过 Unsafe 逐字段提取原始值，打包传入 C++ 侧。C++ 侧根据 TypeLayout 信息从参数中读取字段值，构造 C++ 对象存入 SwissTable slot。这使得结构类型的 JNI 穿越次数保持为 1 次。

**ValueState**：

| 操作 | JNI 方向 | 传递内容 | 说明 |
|------|---------|---------|------|
| `value()` | Java→C++→Java | `stateHandle`, `keyValue`, `keyGroup` → 返回值 | 原始类型直传；字节块类型返回 `byte[]`；结构类型返回各字段值 |
| `update(v)` | Java→C++ | `stateHandle`, `keyValue`, `keyGroup`, `value` 或字段值数组 | 原始类型直传；结构类型传各字段值 |
| `clear()` | Java→C++ | `stateHandle`, `keyValue`, `keyGroup` | 仅标记删除 |

**ListState**：

| 操作 | JNI 方向 | 说明 |
|------|---------|------|
| `add(v)` | Java→C++ | 追加一个元素到 C++ 侧的 list 结构 |
| `get()` | Java→C++→Java | C++ 侧返回 list 元素的 compact 表示，Java 侧解码为 `Iterable` |
| `addAll(list)` | Java→C++ | 批量传递，减少穿越次数 |

**MapState**：

| 操作 | JNI 方向 | 说明 |
|------|---------|------|
| `get(uk)` | Java→C++→Java | 在 C++ 嵌套 HashMap 中查找 |
| `put(uk, uv)` | Java→C++ | 写入 C++ 嵌套 HashMap |
| `entries()` | Java→C++→Java | 返回所有 kv 对的 compact 表示 |

**关键优化：setCurrentKey() 的处理**

`setCurrentKey()` 是每条记录必调用的方法。原实现在 Java 侧设置 `currentKey` 字段，State 方法通过公有字段访问。重写后的设计：

**Key 随操作传递**：State 方法每次调用时，将 currentKey + currentKeyGroup 作为 JNI 参数传入 C++ 侧。这避免了 setCurrentKey 的单独 JNI 调用。理由：setCurrentKey 后通常紧随 1-2 次状态操作，将 key 信息合并到操作调用中，减少总穿越次数。对于原始类型 key（long/int），额外的参数开销可忽略。

### 5.4 Namespace 的处理

Namespace 的传递策略：

- **VoidNamespace**：C++ 侧在注册时已知是 VoidNamespace 模式，所有操作不传递 namespace 参数，节省 JNI 参数开销。JNI bridge 层拥有 void/non-void 两套函数。
- **General Namespace**：Namespace 作为附加参数传入 C++。原始类型 namespace（如 `long` 类型的 window namespace）直传；结构类型 namespace 通过字段提取器逐字段提取后传入。

### 5.5 原始类型的 JNI 零开销路径

对于原始类型组合（如 `Long key + VoidNamespace + Long value`），整条路径上不存在任何对象创建、序列化或内存拷贝：

1. Java `state.value()` → JNI call `nativeGetValueLong(stateHandle, keyLong, keyGroupInt)`
2. C++ `StateEngine<int64_t, void, int64_t>::get(key, keyGroup)` → SwissTable 查找 → 返回 `int64_t`
3. JNI return `jlong` → Java 自动装箱为 `Long`（如有需要）

全链路仅 JNI 一次穿越、零序列化、零内存拷贝。

### 5.6 结构类型的 JNI 路径

对于结构类型（如 POJO），路径同样不涉及序列化：

1. Java `state.update(profile)` → 字段提取器通过 Unsafe 读取 profile 的各字段原始值（`long userId`, `String name`, `double score`）
2. JNI call `nativeUpdateStruct(stateHandle, keyLong, keyGroupInt, userId, nameBytes, nameLen, score)`
3. C++ 从 JNI 参数构造 `UserProfileSlot` 对象（placement new），存入 SwissTable slot

返回方向同理：C++ 从 SwissTable slot 读取各字段值，通过 JNI 返回给 Java 侧，字段提取器用 Unsafe 从各字段值构造 Java 对象。全程是字段值的直接传递，不是序列化。

对于字段数较多的结构类型，字段值可打包到一个 `byte[]`（按 TypeLayout 定义的固定偏移布局），单次 JNI 调用传入，C++ 侧从已知偏移处读取。这依然是**已知布局的内存读写**，不是序列化。

### 5.7 RowData 的 JNI 路径（SQL/Table API 核心路径）

RowData 的 JNI 策略根据 §3.1.5 的分类确定：

**单字段原始类型 RowData（如 `RowData[BIGINT]`）**：

```
Java: long key = currentKeyRow.getLong(0)
      long val = (Long) state.value()
      → long actualKey = keyRow.getLong(0)                          // RowData getter
      → JNI: nativeGetValueLongLong(handle, actualKey, keyGroup)    // 与 Long key 完全相同
      → C++: SwissTable<int64_t, int64_t>::get(key)
```

复用 §5.5 的原始类型零开销路径。Java 薄壳在 `setCurrentKey` 时从 RowData 提取原始值并缓存。

**多字段全定长 RowData（如 `RowData[BIGINT, TIMESTAMP, TIMESTAMP]`）**：

```
Java: long f0 = keyRow.getLong(0)
      long f1 = keyRow.getLong(1)
      long f2 = keyRow.getLong(2)
      → JNI: nativeGetValueFixedRow(handle, keyGroup, keyFields)
      → C++: FixedLengthRow<3> key; memcpy(key.fields, ...)
              SwissTable<FixedLengthRow<3>, V>::get(key)
```

JNI 穿越一次，传递 `jlongArray`。C++ 使用 `GetLongArrayElements`（可能 pin 而非 copy），构造 `FixedLengthRow`。

**含变长字段 RowData**：

使用 §5.6 的通用结构类型路径（packed buffer）。

---

## 6. Checkpoint 设计

### 6.1 设计目标

Checkpoint 是状态后端的关键组成部分。Checkpoint 实现需要满足：

1. **格式兼容**：输出格式与 Flink 标准 Heap 格式兼容，确保可以从旧 checkpoint 恢复、与其他 StateBackend 互操作
2. **全 C++ 执行**：序列化逻辑在 C++ 侧完成，不回调 Java TypeSerializer
3. **高效**：利用 C++ 对象内联存储的优势，直接从 SwissTable slot 读取字段值并写出 Flink 二进制格式

### 6.2 兼容性约束

Flink 的 checkpoint 格式（`KeyGroupsStateHandle`）结构如下：

```
[KeyedBackendSerializationProxy]    ← 元数据（serializer 快照）
[Per-KeyGroup 数据块]               ← 按 keyGroup 组织的状态数据
  ├─ keyGroupId
  ├─ 压缩流 {
  │    stateId (short)
  │    entryCount (int)
  │    for each entry:
  │      namespace_bytes | key_bytes | value_bytes
  │  }
  └─ ...
[KeyGroupRangeOffsets]              ← 各 keyGroup 的偏移量索引
```

C++ 侧必须能够按照此格式输出字节流。

### 6.3 Checkpoint 数据写出策略

#### 6.3.1 元数据部分

`KeyedBackendSerializationProxy` 包含 TypeSerializer 的快照信息（用于 schema evolution）。此部分仍由 Java 侧负责写入，因为：

- 它涉及 TypeSerializer 的 `snapshotConfiguration()` 方法，属于 Java 类型系统的元信息
- 仅在 checkpoint 头部写入一次，不是性能瓶颈
- 保持与 Flink 生态的完全兼容

#### 6.3.2 状态数据部分（C++ 直写）

这是 Checkpoint 的主体部分，也是性能关键。由 C++ 引擎直接写入。

C++ 侧的数据以 C++ 对象的形式存储在 SwissTable slot 中，checkpoint 时需要遍历 slot、读取对象字段、按 Flink 二进制格式写出。这个过程的本质是**格式转换**（从 C++ 对象 → Flink 二进制格式），不是序列化（运行时探测对象结构）。

TypeLayout 在注册时已经记录了"C++ 对象字段"和"Flink 二进制输出格式"之间的映射关系。Checkpoint 写入只需按此映射逐字段转换：

- **定长字段**：从 slot 中 C++ 对象的对应成员读取原始值，按大端序写出（如 `int64_t` → `htobe64()` → 8 字节）
- **变长字段**：从 C++ 对象的 `std::string` 成员读取 `(data(), size())`，按 Flink 格式写出（如 String → VarInt 长度前缀 + UTF-8 字节）
- **结构字段**：递归应用上述规则
- **容器字段**：写出元素数，逐元素递归应用

#### 6.3.3 核心洞察：Flink 二进制格式完全可由 C++ 无歧义复现

Flink 内置 TypeSerializer 的二进制格式是稳定、确定性的：

| TypeSerializer | 二进制格式 | C++ 复现难度 |
|---------------|-----------|-------------|
| IntSerializer | 4 字节大端 | trivial |
| LongSerializer | 8 字节大端 | trivial |
| FloatSerializer / DoubleSerializer | IEEE 754 大端 | trivial |
| BooleanSerializer | 1 字节 | trivial |
| StringSerializer | VarInt 长度 + UTF-8 | 简单 |
| ListSerializer | 元素数 + 逐元素写出 | 递归应用 |
| MapSerializer | 元素数 + 逐 KV 写出 | 递归应用 |
| RowDataSerializer | 按字段写出 | 需复现 BinaryRowData 格式 |

由于 C++ 侧持有完整的 TypeLayout 信息，每种格式都可以被无歧义地复现，不需要回调 Java TypeSerializer。

### 6.4 Checkpoint 执行流程

```
Java SnapshotStrategy.syncPrepareResources()
  │
  ├─ 收集元数据（MetaInfoSnapshot 列表）         ← Java 侧完成
  │
  └─ JNI: nativePrepareSnapshot(backendHandle)   ← 通知 C++ 准备快照
         C++ 侧冻结当前状态视图（如 COW 或版本号）

Java SnapshotStrategy.asyncSnapshot()
  │
  ├─ Java 侧写入 KeyedBackendSerializationProxy  ← Java 完成元数据头
  │
  ├─ For each keyGroup:
  │    记录 offset
  │    写入 keyGroupId
  │    JNI: nativeWriteKeyGroupData(
  │           backendHandle, keyGroupId,
  │           outputStreamHandle)                 ← C++ 写入该 keyGroup 的全部状态
  │
  └─ 构造 KeyGroupRangeOffsets 并完成             ← Java 侧收尾
```

关键点：状态数据的二进制写出循环（for each entry: write ns/key/value）完全在 C++ 内部完成，Java 侧仅控制外层的 keyGroup 循环和 stream 管理。

### 6.5 Checkpoint 输出流对接

C++ 侧需要将序列化字节写入 Flink 的 `CheckpointStateOutputStream`（Java 对象）。

**Buffer 批量写入**：C++ 侧将一个 keyGroup 的数据序列化到内部 buffer（如 64KB），然后一次 JNI 调用将整个 buffer 写入 Java OutputStream。这将 JNI 穿越次数降至 `O(keyGroups × states)`，且每次穿越的 payload 较大，分摊了 JNI 开销。不采用 mmap 方案，因为 Flink 的 Checkpoint 存储抽象需要支持 HDFS、S3 等多种存储后端。

### 6.6 Restore（恢复）设计

恢复流程与 Checkpoint 对称：

1. Java 侧读取 `KeyedBackendSerializationProxy`（元数据），获取 serializer 快照
2. Java 侧根据元数据创建状态注册信息，JNI 调用 C++ 注册 TypeLayout
3. For each keyGroup：Java 侧定位到 offset，JNI 调用 `nativeReadKeyGroupData(backendHandle, keyGroupId, inputBuffer)`
4. C++ 侧按 TypeLayout 解析字节流，直接填充 SwissTable

---

## 7. State 类型具体设计

### 7.1 ValueState

最简单的状态类型。C++ 侧：每个 (key, namespace) 对应 SwissTable 中一个 slot，slot 直接存储 value 的 C++ 对象（与 abseil flat_hash_map 一致，通过 placement new 内联在 slot 中）。

### 7.2 ListState

C++ 侧在 SwissTable slot 中直接存储 `std::vector<T>` 对象（T 为元素的 C++ 类型）。vector 对象本身内联在 slot 中，其管理的元素数据存储在 vector 自行分配的堆内存中。

**`add()` 操作**：直接调用 `vector.push_back()`，利用 `std::vector` 的 2x 增长策略自动管理内存扩展。

**mergeNamespaces**：窗口触发时需要合并多个 namespace 的 list。C++ 通过 `std::move` 将源 vector 的元素 append 到目标 vector（`insert(end, make_move_iterator(...), ...)`），利用 move 语义避免不必要的拷贝。

### 7.3 MapState

C++ 侧：slot 中存储一个指向嵌套 `flat_hash_map<UK, UV>` 的指针。即 SwissTable 的 value 本身是一个嵌套的 hash map。

此设计保持了与 Flink API 相同的语义——外层 SwissTable 按 primary key 索引，内层 hash map 存储用户的 map state。

### 7.4 ReducingState / AggregatingState

C++ 侧与 ValueState 相同的存储结构（单个 accumulator），但 `add()` 操作的语义不同：

- **ReducingState**：`add(v)` → `stored = reduce(stored, v)`
- **AggregatingState**：`add(in)` → `acc = aggregate(acc, in)`

**关键问题：ReduceFunction / AggregateFunction 是 Java 对象**。

这些函数由用户定义，包含任意 Java 逻辑（如 `(a, b) -> a + b`），无法翻译为 C++。解决方案：

- **Tier-1 内置聚合**：对于常见聚合（SUM、MIN、MAX、COUNT），C++ 提供内置实现，通过识别 Flink 内置的聚合函数类型来启用
- **通用路径**：对于用户自定义聚合函数，`add()` 操作需要回调 Java 侧执行用户函数，然后将结果写回 C++。这在 JNI 边界上增加了一次来回调用

对于通用路径，调用链为：Java `add(v)` → JNI → C++ 读取旧 accumulator → JNI 回调 Java `reduce(old, v)` 得到 new accumulator → C++ 写入 new accumulator。虽有额外 JNI 开销，但聚合操作本身通常计算量较轻，JNI 开销相对可以接受。

---

## 8. Concurrency 与 Snapshot 一致性

### 8.1 线程模型

Flink 状态访问是**单线程**的——每个 task slot 仅由一个线程处理记录并访问状态。但 checkpoint 的 async 阶段在独立线程执行。因此需要处理以下并发场景：

- 主线程：正常的状态 get/put/remove
- Checkpoint 线程：遍历状态并序列化

### 8.2 Snapshot 一致性方案：Version-based Copy-on-Write

每次 `syncPrepareResources()` 时递增全局版本号。主线程在修改 slot 时检查版本号，若该 slot 尚未被 snapshot 线程读取（即该 slot 的版本 < snapshot 版本），则拷贝旧值到 COW buffer 后再修改。Snapshot 线程优先从 COW buffer 读取。

此方案的优势：
- 非 checkpoint 期间零开销（无版本检查）
- Checkpoint 期间仅修改的 slot 需要 COW，大部分 slot 直接读取
- 适合 Flink 的单一主线程 + 单一 snapshot 线程模型

---

## 9. 构建与部署

### 9.1 项目结构

```
src/main/
├── java/org/apache/flink/state/forl0/    ← Java 层（仅 JNI 转发薄壳，旧实现代码全部删除）
│   ├── ForL0StateBackend.java              (重写为 JNI 转发)
│   ├── ForL0KeyedStateBackend.java         (重写为 JNI 转发)
│   ├── ForL0ValueState.java                (重写为 JNI 转发)
│   ├── ForL0ListState.java                 (重写为 JNI 转发)
│   ├── ForL0MapState.java                  (重写为 JNI 转发)
│   ├── ForL0ReducingState.java             (重写为 JNI 转发)
│   ├── ForL0AggregatingState.java          (重写为 JNI 转发)
│   └── ForL0SnapshotStrategy.java          (重写为 JNI 转发)
│
├── native/
│   ├── CMakeLists.txt                      ← CMake 构建
│   ├── jni/
│   │   ├── forl0_jni.cpp                   ← JNI 函数注册
│   │   ├── jni_value_state.cpp             ← ValueState JNI 入口
│   │   ├── jni_list_state.cpp              ← ListState JNI 入口
│   │   ├── jni_map_state.cpp               ← MapState JNI 入口
│   │   └── jni_checkpoint.cpp              ← Checkpoint JNI 入口
│   │
│   ├── engine/
│   │   ├── state_engine.h                  ← StateEngine 核心
│   │   ├── swiss_table.h                   ← SwissTable 实现
│   │   ├── type_layout.h                   ← 类型描述符
│   │   └── allocator.h                     ← 内存分配器接口（用于 L0 Cache 替换）
│   │
│   ├── checkpoint/
│   │   ├── checkpoint_writer.h             ← Checkpoint 序列化
│   │   ├── checkpoint_reader.h             ← Checkpoint 反序列化
│   │   └── flink_binary_format.h           ← Flink 序列化格式复现
│   │
│   └── platform/
│       ├── simd_x86.h                      ← SSE2/AVX2 SIMD 实现
│       ├── simd_neon.h                     ← NEON SIMD 实现
│       └── simd_portable.h                 ← 通用 SWAR fallback
│
└── resources/native/
    ├── libforl0_engine.so                  ← Linux 产物
    └── libforl0_engine.dylib               ← macOS 产物
```

> **旧代码清理**：上述 Java 层仅保留 JNI 转发薄壳文件。原有的 Java 实现文件（`SwissTable.java`、`ForL0StateStore*.java`、`ForL0*RestoreOperation.java`、`NativeL0Memory.java`、`FieldAccessor.java` 等）在重写完成后全部删除，具体清单见 §2.3。

### 9.2 构建系统

使用 CMake 构建 C++ 部分，通过 Maven 的 `maven-antrun-plugin` 或 `cmake-maven-plugin` 集成到 Maven 构建流程中。

编译选项要求：
- C++17（用于 `if constexpr`、结构化绑定等）
- `-O3 -march=native`（生产构建）
- `-fsanitize=address,undefined`（开发构建）
- SSE2 默认启用（x86-64 baseline），NEON 在 AArch64 上自动启用

### 9.3 跨平台支持

| 平台 | SIMD | 构建产物 | 分发方式 |
|------|------|---------|---------|
| Linux x86-64 | SSE2 (baseline), AVX2 (runtime detect) | `libforl0_engine.so` | JAR 内嵌 |
| Linux AArch64 (鲲鹏) | NEON | `libforl0_engine.so` | JAR 内嵌 |

---

## 10. 交付范围与验收标准

### 10.1 交付范围

本项目一次性交付完整可用的 C++ State Backend，交付物涵盖以下全部内容：

**基础设施**
- C++ 项目骨架（CMake 构建，与 Maven 集成）
- JNI 注册与 native 库加载机制
- TypeLayout 类型系统：原始类型、字节块类型、结构类型（POJO/Tuple/RowData）、容器类型（List/Map）
- 字段提取器生成框架（Unsafe 字段提取的自动生成）

**状态存储引擎**
- SwissTable C++ 实现（对齐 abseil，SSE2/NEON 16路 SIMD 并行匹配，SWAR portable fallback）
- SIMD runtime detection（AVX2 on x86, SVE on AArch64）
- StateEngine 核心（KeyGroup 分区、VoidNamespace 模式、General Namespace 模式）
- 内存管理（C++ 对象内联存储于 slot，与 abseil 一致；Allocator 抽象预留 L0 Cache 集成入口）

**全部五种状态类型**
- ValueState（JNI 转发 + C++ engine）
- ListState（含 addAll 批量接口、mergeNamespaces）
- MapState（嵌套 flat_hash_map）
- ReducingState / AggregatingState（内置聚合 C++ 实现 + 用户自定义函数 Java 回调路径）

**Checkpoint / Restore**
- Flink 二进制格式 C++ 复现（flink_binary_format.h）
- Checkpoint 写入（C++ 侧格式转换 + buffer 批量写入 Java OutputStream）
- Checkpoint 恢复（C++ 侧从 Flink 二进制格式重建 SwissTable）
- Version-based COW 异步快照

**生产化**
- 内存使用监控与指标上报
- 跨平台产物（Linux x86-64, Linux AArch64, macOS x86-64, macOS AArch64）
- ASAN/UBSAN 测试覆盖

**旧代码清理**
- 删除不再需要的 Java 实现文件（SwissTable.java、ForL0StateStore*.java、snapshot 内部实现、NativeL0Memory.java 等，完整清单见 §2.3）
- 将保留的 Java 文件重写为纯 JNI 转发薄壳（ForL0ValueState.java、ForL0KeyedStateBackend.java 等）
- 删除旧 native 代码（forl0_native.c、Makefile）
- 验证无遗留死代码

### 10.2 验收标准

验收的唯一标准是**直接可用**——与当前 ForL0StateBackend 一样，用户仅需一行配置即可选择本状态后端：

```java
env.setStateBackend(new ForL0StateBackend());
```

或在 `flink-conf.yaml` 中：

```yaml
state.backend: forl0
```

具体验收项：

| 验收项 | 标准 |
|--------|------|
| **API 完全兼容** | 实现 Flink `StateBackend` + `ConfigurableStateBackend` 接口；用户代码无需任何修改 |
| **五种状态类型** | ValueState、ListState、MapState、ReducingState、AggregatingState 全部可用，行为与 Flink 语义一致 |
| **Checkpoint / Restore** | 支持 checkpoint 触发与恢复；输出格式与 Flink Heap 格式兼容 |
| **Namespace 支持** | VoidNamespace 和 General Namespace 均支持；mergeNamespaces 正常工作（窗口场景可用） |
| **类型覆盖** | 原始类型（int/long/float/double/boolean）、String、byte[]、POJO、Tuple、RowData、List<T>、Map<K,V> 均支持 |
| **RowData 零序列化** | SQL/Table API 的 RowData key/value 全程不经过 TypeSerializer；单字段 RowData 展开为原始类型使用 int64_t 快速路径；多字段全定长 RowData 使用 FixedLengthRow 快速路径 |
| **Nexmark SQL 基准** | 能在 Flink 上运行 Nexmark SQL 全部查询并正确产出结果；RowData 状态访问性能接近 DataStream 原始类型路径 |
| **MiniCluster 集成测试** | 能在 Flink MiniCluster 上运行 WordCount、窗口聚合等典型作业并正确产出结果 |
| **Savepoint 互操作** | 能从 Flink Heap StateBackend 的 savepoint 恢复，也能生成可被 Heap StateBackend 恢复的 savepoint |
| **跨平台** | Linux x86-64 与 Linux AArch64（鲲鹏）上均可正常运行 |

### 10.3 实施顺序

交付虽然一次完成，但实施上按依赖关系自底向上推进：

1. **底层先行**：C++ 项目骨架 → Allocator → SwissTable → SIMD 适配
2. **类型系统**：TypeLayout → 字段提取器 → TypeAnalyzer
3. **状态引擎**：StateEngine → JNI bridge → Java 薄壳 → 五种 State 实现
4. **Checkpoint**：Flink 二进制格式复现 → Writer/Reader → COW → SnapshotStrategy 对接
5. **集成验证**：单元测试 → MiniCluster 集成测试 → 性能基准测试

---

## 11. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| JNI 开销抵消 C++ 收益 | 对于极轻量状态操作（如单个 int 读取），JNI 开销可能占比较高 | 批量操作接口；原始类型使用 Critical JNI（无 safepoint check） |
| C++ 内存泄漏 | JVM 无法管理 off-heap 内存，泄漏难以诊断 | ASAN 测试；RAII 严格管理；定期 leak check |
| Flink 二进制格式变更 | 新版 Flink 修改 TypeSerializer 的输出格式 | 按 Flink 版本锁定格式；格式版本号校验 |
| 字段提取器正确性 | Unsafe 偏移量依赖 JVM 实现，不同 JVM 可能不同 | 注册时验证偏移量正确性；支持 VarHandle fallback（Java 9+） |
| 调试困难 | C++ native crash 难以与 Java 堆栈关联 | 完善的异常转换；signal handler 打印 C++ 堆栈；gdb attach 支持 |
| ReduceFunction/AggregateFunction 回调开销 | 自定义聚合需要 JNI 来回调用 | 识别内置聚合函数并使用 C++ in-place 实现；批量聚合接口 |

---

## 12. 预期收益

| 指标 | 重写前 | 重写后 | 提升来源 |
|------|------------|-----------|---------|
| ValueState get/put 延迟 | ~100 ns | ~30-50 ns | 消除对象头、GC、间接引用；SIMD 16路匹配 |
| 内存占用（per entry） | ~80-120 bytes（含对象头、padding） | ~16-32 bytes（flat layout） | 消除对象头、指针、padding |
| Checkpoint 吞吐 | 受 TypeSerializer 与 GC 限制 | 接近 memcpy 速度 | C++ 对象字段直读直写 Flink 二进制格式；无对象遍历 |
| GC 影响 | 状态量 > 1GB 时 GC pause 显著 | 零 GC 影响 | 所有状态数据在 off-heap |
| Tail latency (P99) | 受 GC 暂停影响较大 | 稳定低延迟 | 无 GC，确定性内存访问 |
