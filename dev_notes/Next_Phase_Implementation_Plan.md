# ForL0 State Backend 下一阶段实施计划

> 基于 C++ StateBackend 重写验收报告 + 四条优化建议制定。
> 日期：2025-03-11

## 目录

- [总览](#总览)
- [Phase 0: 正确性修复](#phase-0-正确性修复必须先行)
- [Phase 1: TimeWindow Namespace 特化](#phase-1-timewindow-namespace-特化)
- [Phase 2: MapState 原始类型 & RowData 零拷贝](#phase-2-mapstate-原始类型--rowdata-零拷贝)
- [Phase 3: BinaryRowData 全链路零拷贝](#phase-3-binaryrowdata-全链路零拷贝)
- [Phase 4: 热路径优化](#phase-4-热路径优化)
- [Phase 5: 工程完善](#phase-5-工程完善)
- [实施顺序与依赖关系](#实施顺序与依赖关系)

---

## 总览

### 四条优化建议

1. **BinaryRowData 零拷贝零序列化**：补全 ValueState / ListState / ReducingState 中 BinaryRowData value 的全链路零拷贝
2. **MapState RowData & 原始类型零拷贝**：扩展 MapState 的 UK/UV 类型特化，不仅限于 Long-Long
3. **TimeWindow Namespace 特化**：TimeWindow = `{long start, long end}`，无需序列化为 bytes
4. **HeapStateBackend 借鉴**：Strategy 模式消除 if 链、MapState 迭代器优化、ReducingState 合并操作

### 验收报告中的正确性问题

- **COW Snapshot 未激活**：`StateEngine::prepare_snapshot()` 未传播到各 StateTable
- **StateHandle 内存泄漏**：`destroyEngine` 不清理 StateHandle
- **StringPtr 零拷贝读生命周期风险**：SwissTable rehash 或 put 后指针可能失效

---

## Phase 0: 正确性修复（必须先行）

> 验收报告 P0 级问题。后续所有 checkpoint 相关优化都依赖此 Phase。

### 0.1 激活 COW Snapshot

**问题**：`StateEngine::prepare_snapshot()` 仅递增 `snapshot_version_`，未调用各 `StateTable::prepare_snapshot()`；`jni_checkpoint.cpp` 写出时使用 `for_each` 而非 `for_each_snapshot_in_key_group`。

**改动清单**：

| 文件 | 变更 |
|------|------|
| `src/main/native/engine/state_engine.h` — `StateTableHandle` 基类 | 新增 `virtual void prepare_snapshot() = 0` 和 `virtual void release_snapshot() = 0` |
| `src/main/native/engine/state_engine.h` — `TypedStateTableHandle<K,V>` | 实现上述两个虚方法，转发到 `StateTable<K,V>::prepare_snapshot()` / `release_snapshot()` |
| `src/main/native/engine/state_engine.h` — `StateEngine::prepare_snapshot()` | 遍历所有 `state_handles_`，调用 `handle->prepare_snapshot()` |
| `src/main/native/engine/state_engine.h` — `StateEngine::release_snapshot()` | 遍历所有 `state_handles_`，调用 `handle->release_snapshot()` |
| `src/main/native/jni/jni_checkpoint.cpp` — `write_state_key_group()` | VoidNamespace 模式下用 `for_each_snapshot_in_key_group` 替代 `for_each_in_key_group` |
| `src/main/native/jni/forl0_jni.cpp` | 新增 `Java_..._releaseSnapshot` JNI 入口 |
| `src/main/java/org/apache/flink/state/forl0/NativeEngine.java` | 新增 `native static void releaseSnapshot(long engineHandle)` |
| `src/main/java/org/apache/flink/state/forl0/ForL0SnapshotStrategy.java` | async 完成后调用 `NativeEngine.releaseSnapshot(engineHandle)` |

**COW Namespace 扩展**（Phase 1 的前置工作）：

当前 `COWState` 仅跟踪 VoidNamespace 表的 `put`/`remove`。Namespace 模式需独立的 COW 跟踪。推荐方案：

- 为每个 namespace 的 SwissTable 维护独立的 COW 状态
- 在 `StateTable` 中新增 `std::unordered_map<int, std::unordered_map<NsKey, COWState>>` — 按 keyGroup+namespace 索引
- namespace `put`/`remove` 时调用 `cow_before_write_ns` / `cow_before_erase_ns`
- 新增 `for_each_snapshot_in_key_group_ns()` 方法

### 0.2 StateHandle 内存泄漏修复

**问题**：`registerState` 中 `new StateHandle()` 通过 `to_handle()` 转为 `jlong` 返回 Java，但 `destroyEngine` 只 `delete engine`，不释放 StateHandle。

**改动清单**：

| 文件 | 变更 |
|------|------|
| `src/main/native/jni/jni_utils.h` | `StateHandle` 的所有权交给 `StateEngine` 管理 |
| `src/main/native/engine/state_engine.h` — `StateEngine` | 新增 `std::vector<std::unique_ptr<void, void(*)(void*)>> owned_handles_` 或类型擦除 owner |
| `src/main/native/jni/forl0_jni.cpp` — `registerState` | 将 `StateHandle*` 注册到 `StateEngine` 的 owned list |
| `src/main/native/jni/forl0_jni.cpp` — `destroyEngine` | `StateEngine` 析构时自动释放所有 owned StateHandle |

**推荐实现**：在 `StateEngine` 中维护 `std::vector<std::unique_ptr<StateHandle>> owned_state_handles_`（需将 `StateHandle` 头文件前置声明或移至 engine 头文件）。注册时 `engine->own_state_handle(std::unique_ptr<StateHandle>(handle))`，析构时自动释放。

### 0.3 StringPtr 零拷贝读生命周期安全

**问题**：`valueGetLongStringPtr` 返回 `std::string::data()` 指针，SwissTable rehash 或同 key put 覆盖时指针失效。

**改动**：
- 在 `ForL0ValueState.wrapNativePtr()` 和 `NativeEngine.valueGetLongStringPtr` 的 javadoc 中添加安全约束说明："返回的 BinaryRowData 仅在下次对同一 StateTable 的写入操作前有效"
- 在 COW snapshot 激活后，snapshot 迭代不触发 rehash，checkpoint read 安全
- **中长期方案**：改用 copy-on-read 策略 — 利用 SSO (Small String Optimization)，大多数 BinaryRowData < 64B 时 `std::string` 无堆分配，copy 开销极小

---

## Phase 1: TimeWindow Namespace 特化

> TimeWindow 是窗口算子中最高频的 namespace 类型。特化后所有窗口状态（ValueState / ReducingState / AggregatingState / ListState / MapState）都受益。

### 1.1 类型系统扩展

TimeWindow = `{long start, long end}` → C++ 侧用 `std::pair<int64_t, int64_t>` 作为 namespace key，免去序列化。

| 文件 | 变更 |
|------|------|
| `src/main/java/.../util/TypeAnalyzer.java` | 新增 `TYPE_TIME_WINDOW = 21`；`getTypeId(serializer)` 中检测 `TimeWindowSerializer`（通过类名反射，类似 RowDataSerializer 检测方式） |
| `src/main/native/engine/type_layout.h` | 新增 `TYPE_TIME_WINDOW = 21`；`TypeLayoutParser` 支持解析 |
| `src/main/native/jni/jni_utils.h` — `StateHandle` | `NsType` 枚举新增 `TIME_WINDOW` |

### 1.2 C++ StateTable namespace 存储

在 `state_engine.h` 的 `StateTable<K,V>` 中新增：

```cpp
using TimeWindow = std::pair<int64_t, int64_t>;  // {start, end}

struct TimeWindowHash {
    size_t operator()(const TimeWindow& tw) const {
        size_t h = std::hash<int64_t>()(tw.first);
        h ^= std::hash<int64_t>()(tw.second) * 0x9e3779b97f4a7c15ULL;
        return h;
    }
};

// 新增成员
std::unordered_map<int, std::unordered_map<TimeWindow, std::unique_ptr<Table>, TimeWindowHash>> tw_namespace_maps_;
```

对应在 `find_namespace_table` / `get_or_create_namespace_table` / `remove_namespace_table` / `for_each_in_key_group_ns` 中增加 `TimeWindow` 的 `if constexpr` 分支。

### 1.3 注册路径

| 文件 | 变更 |
|------|------|
| `src/main/native/jni/forl0_jni.cpp` — `registerState` | 当 `nsTypeId == TYPE_TIME_WINDOW` 时，注册 typed `StateTable<KeyType, ValueType>` 而非 `<string, string>`。根据 keyTypeId/valueTypeId 选择正确的 K/V 模板参数 |
| `src/main/java/.../ForL0KeyedStateBackend.java` — `ensureNativeState` | 传递 `nsTypeId = TYPE_TIME_WINDOW` 给 C++ |

**关键变化**：非 VoidNamespace 不再统一降级为 `<std::string, std::string>`。TimeWindow namespace 下可保留 typed key/value。

### 1.4 JNI 操作方法

**推荐方案**：新增统一的 `*WithTimeWindow` 后缀 JNI 方法集，接收 `long nsStart, long nsEnd` 参数：

```java
// NativeEngine.java 新增 — ValueState
static native boolean valueGetLongLongWithTW(long h, long key, int kg, long nsStart, long nsEnd, long[] out);
static native void valuePutLongLongWithTW(long h, long key, int kg, long nsStart, long nsEnd, long value);
static native boolean valueContainsWithTW(long h, long key, int kg, long nsStart, long nsEnd);
static native void valueClearWithTW(long h, long key, int kg, long nsStart, long nsEnd);
// 其他 value type 组合类推...

// NativeEngine.java 新增 — ListState
static native long[] listGetLongElementsWithTW(long h, long key, int kg, long nsStart, long nsEnd);
static native void listAddLongWithTW(long h, long key, int kg, long nsStart, long nsEnd, long element);
// ...

// NativeEngine.java 新增 — MapState
static native long mapGetLongLongWithTW(long h, long key, int kg, long nsStart, long nsEnd, long uk);
static native void mapPutLongLongWithTW(long h, long key, int kg, long nsStart, long nsEnd, long uk, long uv);
// ...

// NativeEngine.java 新增 — ReducingState / AggregatingState
static native long reduceGetLongWithTW(long h, long key, int kg, long nsStart, long nsEnd);
// ...
```

C++ JNI 实现模式：
```cpp
auto tw = TimeWindow{nsStart, nsEnd};
auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
auto* swiss = table->get_or_create_namespace_table(keyGroup, tw);
// ... 操作 swiss table
```

### 1.5 Java State 类 dispatch

在各 State 实现类（ForL0ValueState、ForL0ListState、ForL0MapState、ForL0ReducingState、ForL0AggregatingState）中：

```java
// 构造时检测
private final boolean isTimeWindowNs = TypeAnalyzer.isTimeWindowSerializer(namespaceSerializer);

// 热路径中
if (isTimeWindowNs && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT64) {
    TimeWindow tw = (TimeWindow) currentNamespace;
    NativeEngine.valueGetLongLongWithTW(stateHandle, (Long)key, keyGroup, tw.getStart(), tw.getEnd(), primitiveBuf);
    // ...
}
```

同时需要 `TypeAnalyzer` 新增：
```java
public static boolean isTimeWindowSerializer(TypeSerializer<?> serializer) {
    // 通过类名反射检测 TimeWindowSerializer
}
```

### 1.6 Checkpoint TimeWindow namespace

| 文件 | 变更 |
|------|------|
| `src/main/native/checkpoint/checkpoint_writer.h` | 遍历 `tw_namespace_maps_[idx]` 时写出 `[ns_start(8B BE)][ns_end(8B BE)]` 作为 namespace 标识 |
| `src/main/native/checkpoint/checkpoint_reader.h` | 读取时重建 `TimeWindow` namespace |
| `src/main/native/jni/jni_checkpoint.cpp` | `write_state_key_group()` / `read_state_key_group()` 中增加 TimeWindow namespace 遍历分支 |

---

## Phase 2: MapState 原始类型 & RowData 零拷贝

> 目前 MapState 仅有 Long-Long 一条特化路径。扩展后覆盖 SQL 场景中常见的 RowData UK/UV 组合。

### 2.1 InnerMap C++ 侧类型特化

**当前问题**：所有 MapState 的 InnerMap 统一为 `std::unordered_map<std::string, std::string>`，即使 UK/UV 是原始类型也需要编码为 8 字节 BE 字符串。

**引入类型参数化的 InnerMap**：

```cpp
// 新文件或在 type_layout.h 中
template <typename UK, typename UV>
using TypedInnerMap = std::unordered_map<UK, UV>;

// 常见实例化
using InnerMapLongLong   = TypedInnerMap<int64_t, int64_t>;     // 已有
using InnerMapLongString = TypedInnerMap<int64_t, std::string>;  // UK=Long, UV=BinaryRowData
using InnerMapStringLong = TypedInnerMap<std::string, int64_t>;  // UK=BinaryRowData, UV=Long
using InnerMapStringString = TypedInnerMap<std::string, std::string>;  // generic fallback
```

SwissTable slot value type 从固定的 `InnerMap` 变为类型参数：
```
SwissTable<int64_t, InnerMapLongLong>        // Key=Long, UK=Long, UV=Long
SwissTable<int64_t, InnerMapLongString>      // Key=Long, UK=Long, UV=BinaryRowData
SwissTable<int64_t, InnerMapStringString>    // Key=Long, UK=generic, UV=generic (fallback)
```

### 2.2 注册路径扩展

| 文件 | 变更 |
|------|------|
| `src/main/native/jni/forl0_jni.cpp` — `registerState` | MapState 分支：根据 `value_layout->children[0]`（UK type）和 `value_layout->children[1]`（UV type）选择 InnerMap 特化 |
| `src/main/java/.../util/TypeAnalyzer.java` | `generateStateDescriptor` 中确保 MapState 的 `value_layout` 包含 UK/UV 的 TypeId（当前已有，需确认 RowData UK/UV 场景） |
| `src/main/native/jni/jni_utils.h` — `StateHandle` | 新增 `uk_type` / `uv_type` 字段 |

### 2.3 新增 JNI 特化方法

**MapState UK/UV 组合矩阵**（仅实现高频组合）：

| UK Type | UV Type | JNI 方法后缀 | C++ InnerMap |
|---------|---------|------------|--------------|
| Long | Long | `LongLong`（已有） | `InnerMapLongLong` |
| **Long** | **Bytes（BinaryRowData）** | `LongBytes` | `InnerMapLongString` |
| **Bytes（BinaryRowData）** | **Long** | `BytesLong` | `InnerMapStringLong` |
| **Bytes（BinaryRowData）** | **Bytes（BinaryRowData）** | — 复用现有 byte[]  方法 | `InnerMapStringString` |
| Long | Int/Double | `LongInt`/`LongDouble` | `InnerMapLongLong`（bitcast 复用） |

新增 NativeEngine JNI 方法：
```java
// UK=Long, UV=bytes (BinaryRowData raw bytes)
static native byte[] mapGetLongBytes(long h, long key, int kg, long uk);
static native void mapPutLongBytes(long h, long key, int kg, long uk, byte[] uv);
static native byte[] mapEntriesLongBytes(long h, long key, int kg);
// Entries 格式: [count(4B)][uk0(8B)][uv0_len(4B)][uv0_bytes]...

// UK=bytes, UV=Long
static native long mapGetBytesLong(long h, long key, int kg, byte[] uk);
static native void mapPutBytesLong(long h, long key, int kg, byte[] uk, long uv);
```

### 2.4 Java MapState RowData 零拷贝分发

```java
// ForL0MapState 构造时确定分发策略
private enum MapFastPath {
    LONG_LONG,        // 已有
    LONG_BYTES,       // UK=Long(或 RowData[BIGINT]), UV=BinaryRowData
    BYTES_LONG,       // UK=BinaryRowData, UV=Long(或 RowData[BIGINT])
    BYTES_BYTES,      // UK=BinaryRowData, UV=BinaryRowData
    GENERIC           // fallback
}
```

**RowData UV 零拷贝读取**：MapState `get(UK)` 返回 UV 时，对于 BinaryRowData value：
- C++ 返回 `std::string` raw bytes
- Java 侧 `deserializeUserValue` 已有 `RowDataKeyAccessor.wrapBinaryRowDataCompat` 路径 ✅

**RowData UK 零拷贝处理**：
- 单字段 BIGINT UK → 提取为 `long`，走 `mapGetLongBytes` 等
- 多字段/变长 UK → `extractBinaryRowDataBytesCompat` 提取 raw bytes，走 byte[] 方法

### 2.5 MapState Entries 批量零拷贝

当 UK/UV 是 BinaryRowData 时，C++ 直接写出 raw bytes（不经 TypeSerializer），Java 侧直接 `wrapBinaryRowData`：

```java
// 新增反序列化路径
if (isRowDataUK && isRowDataUV) {
    int count = in.readInt();
    for (int i = 0; i < count; i++) {
        int ukLen = in.readInt();
        byte[] ukRaw = new byte[ukLen]; in.readFully(ukRaw);
        int uvLen = in.readInt();
        byte[] uvRaw = new byte[uvLen]; in.readFully(uvRaw);
        UK uk = (UK) RowDataKeyAccessor.wrapBinaryRowData(ukRaw, rowDataUKAccessor.getArity());
        UV uv = (UV) RowDataKeyAccessor.wrapBinaryRowData(uvRaw, rowDataUVAccessor.getArity());
        map.put(uk, uv);
    }
}
```

### 2.6 Checkpoint 适配

`checkpoint_writer.h` 为新的 `InnerMapLongString` / `InnerMapStringLong` 等类型增加 `write_flink_value` 特化模板。保持与现有 `InnerMap` 格式兼容（`[len][count][uk][uv]...` 结构），仅类型编码不同。

---

## Phase 3: BinaryRowData 全链路零拷贝

> 补全 ValueState 和其他 State 类中 BinaryRowData value 的零拷贝路径。

### 3.1 ValueState BinaryRowData value write 路径

**当前状态**：`serializeValue` 中 `RowDataKeyAccessor.extractBinaryRowDataBytes(value)` 返回 BinaryRowData 的 raw bytes → JNI 传到 C++。相比 TypeSerializer 路径（serialize → DataOutputSerializer → getCopyOfBuffer → JNI → C++），已减少了 1 次拷贝 + TypeSerializer 虚方法调度。

**进一步优化**：确认 `extractBinaryRowDataBytes` 在 BinaryRowData backing array offset==0 时直接返回 backing array 引用（已部分实现），避免多余拷贝。

### 3.2 ValueState BinaryRowData value zero-copy read

当前 `zeroCopyGetLong` / `zeroCopyGetGeneric` 通过 `valueGetLongStringPtr` / `valueGetGenericPtr` 返回 C++ 内存指针，Java 侧 `MemorySegmentBridge.wrapNativeAddress` 包装为 off-heap `MemorySegment`。**此路径已工作。**

**补充缺失的路径**：
- FixedRow key + BinaryRowData value：新增 `valueGetFixedRowGenericPtr(handle, fields, keyGroup, out)` JNI 方法
- 在 `ForL0ValueState.valueForFixedRowKey()` 的 String/bytes value 分支中，增加 `isRowDataValue` 判断调用零拷贝路径

### 3.3 ListState BinaryRowData 元素零拷贝

**当前问题**：ListState 元素总是通过 `elementSerializer.serialize` 序列化。

**改动**：

| 文件 | 变更 |
|------|------|
| `ForL0ListState.java` 构造 | 新增 `isRowDataElement` / `rowDataElementAccessor` 检测 |
| `NativeEngine.java` | 新增 `listAddBytes(handle, key, kg, bytes)` — 直接传递 BinaryRowData raw bytes |
| `NativeEngine.java` | 新增 `listGetBytesElements(handle, key, kg)` → 返回 `[count][len1][bytes1][len2][bytes2]...` |
| `ForL0ListState.java` — `add(V)` | 当 `isRowDataElement` 时，`extractBinaryRowDataBytes(element)` → `listAddBytes` |
| `ForL0ListState.java` — `get()` | 当 `isRowDataElement` 时，逐元素 `wrapBinaryRowData` |

### 3.4 ReducingState / AggregatingState BinaryRowData 路径

这些 State 常用于窗口聚合，value 频繁是 RowData（accumulator 类型）。

- `ForL0ReducingState` 中 `zeroCopyGetLong` / `zeroCopyGetGeneric` 已存在 ✅
- 确认对应的 write 路径也使用 `extractBinaryRowDataBytes` 而非 `valueSerializer.serialize`

---

## Phase 4: 热路径优化

> 基于验收报告性能建议 + HeapStateBackend 借鉴。

### 4.1 Strategy 模式消除 if 链

**问题**：`value()` / `update()` 中多级 `if` 判断（voidNamespace → isRowDataKey → keyTypeId → valueTypeId → isTimeWindowNs）在运行时不变但每次调用都执行。

**方案**：构造时计算 `Strategy` 枚举，热路径 `switch(strategy)`：

```java
enum ValueStateStrategy {
    LONG_LONG_VOID,
    LONG_INT_VOID,
    LONG_DOUBLE_VOID,
    LONG_STRING_VOID,
    LONG_ROWDATA_VOID,
    INT_LONG_VOID,
    INT_DOUBLE_VOID,
    INT_STRING_VOID,
    ROWDATA_SINGLE_LONG_LONG_VOID,
    ROWDATA_SINGLE_LONG_ROWDATA_VOID,
    ROWDATA_FIXED_ROW_LONG_VOID,
    ROWDATA_FIXED_ROW_ROWDATA_VOID,
    LONG_LONG_TIME_WINDOW,   // Phase 1 新增
    LONG_ROWDATA_TIME_WINDOW,
    // ...
    GENERIC
}
```

Java `switch` 在枚举上被 JIT 编译为 `tableswitch` 指令（O(1) 分发），比多级 if 链更高效。

**同样的模式应用到**：ForL0ListState、ForL0MapState、ForL0ReducingState、ForL0AggregatingState。

### 4.2 Checkpoint WriteBuffer → jbyteArray 消除中间拷贝

**当前**：`WriteBuffer` → `std::string(buf.data(), buf.size())` → `string_to_jbytearray`(NewByteArray + SetByteArrayRegion)。

**优化**：直接从 `WriteBuffer::data()` 写入 `NewByteArray`，跳过 `std::string` 中间构造：

```cpp
// jni_checkpoint.cpp — writeKeyGroupData 修改
jbyteArray result = env->NewByteArray(static_cast<jsize>(buf.size()));
env->SetByteArrayRegion(result, 0, static_cast<jsize>(buf.size()),
    reinterpret_cast<const jbyte*>(buf.data()));
return result;
```

### 4.3 MapState entries() 流式迭代器（借鉴 HeapMapState）

**HeapMapState 模式**：`entries()` 直接返回 `HashMap.entrySet()`，不拷贝；修改 entry 直接反映到状态。

**当前 ForL0**：`entries()` 全量读出 + 反序列化 + 构建 `HashMap` + 返回 `entrySet()`。

**优化方案**（适用于 generic 路径的大 Map）：
- C++ 新增 `mapIteratorCreate(handle, key, kg)` → 返回迭代器 handle
- C++ 新增 `mapIteratorNext(iterHandle, outBuf)` → 返回下一对 UK/UV bytes
- C++ 新增 `mapIteratorDestroy(iterHandle)` → 释放迭代器
- Java 返回 `Iterator<Entry<UK, UV>>`，按需反序列化
- 对于 Long-Long 路径和小 Map，保持现有全量返回模式（JNI 穿越次数少更优）

### 4.4 ReducingState 单次/两次 JNI read-modify-write

**当前**：`add(v)` = `get(old)` + Java `reduceFunction.reduce(old, v)` + `put(new)` = 3 次 JNI + 3 次 hash lookup。

**方案 A — 合并 get+put（2 次 JNI + 2 次 lookup → 1 次）**：
```java
// NativeEngine.java 新增
static native long reduceGetAndPutLong(long h, long key, int kg, long newValue);
// C++: find → 如果存在返回旧值; insert newValue → 返回 NOT_FOUND 标记
```

Java 侧：
```java
void add(V value) {
    long oldValue = NativeEngine.reduceGetAndPutLong(stateHandle, key, keyGroup, (Long) value);
    if (oldValue != NOT_FOUND) {
        long reduced = reduceFunction.reduce(oldValue, (Long) value);
        NativeEngine.valuePutLongLong(stateHandle, key, keyGroup, reduced);
    }
    // 不存在时 C++ 已 put 了 newValue，无需额外操作
}
```

**方案 B — C++ 内置聚合（1 次 JNI + 1 次 lookup）**：
```java
// 对于 SUM/MIN/MAX
static native void reduceBuiltinLong(long h, long key, int kg, long value, int aggType);
// C++: find → if exists: *existing = builtin_agg(*existing, value); else: put(value)
```

---

## Phase 5: 工程完善

### 5.1 C++ 单元测试

使用 Google Test 框架建立测试套件：

| 测试类 | 覆盖范围 |
|--------|---------|
| `SwissTableTest` | 插入/查找/删除/rehash/遍历，边界条件（满载、全删除后插入） |
| `StateTableCOWTest` | prepare_snapshot → put/remove → for_each_snapshot 一致性验证 |
| `CheckpointRoundTripTest` | 各类型 write → read 往返正确性 |
| `TypeLayoutParserTest` | 解析 Java 生成的 descriptor bytes |
| `TimeWindowNamespaceTest` | Phase 1 新增后的 namespace 操作 |
| `TypedInnerMapTest` | Phase 2 新增的 MapState 类型特化 |

### 5.2 集成测试

扩展 `src/test/java/` 下的 minicluster 测试：

| 测试场景 | 覆盖内容 |
|---------|---------|
| Window 聚合 + checkpoint/restore | 验证 TimeWindow namespace + COW |
| MapState<RowData, RowData> 端到端 | 验证 Phase 2 特化路径 |
| ListState<RowData> 端到端 | 验证 Phase 3 BinaryRowData 元素 |
| Savepoint 兼容性 | 新格式读旧 savepoint 数据 |
| 大规模状态 GC 压力测试 | 验证 off-heap 优势 |

---

## 实施顺序与依赖关系

```
Phase 0 (正确性)  ──────────────────────────────┐
  0.1 COW 激活                                   │
  0.2 StateHandle 泄漏修复                        │
  0.3 StringPtr 安全文档                          │
                                                  ▼
Phase 1 (TimeWindow)  ◄──── 依赖 Phase 0.1 (COW 先就绪)
  1.1-1.2 类型系统 + C++ 存储
  1.3-1.5 注册 + JNI + Java dispatch
  1.6 Checkpoint 适配
                                   ┌──────────────┘
Phase 2 (MapState) ◄──── 依赖 Phase 0    │  可与 Phase 1 并行
  2.1-2.2 InnerMap 类型 + 注册           │
  2.3-2.5 JNI + Java + Entries           │
  2.6 Checkpoint 适配                    │
                                         │
Phase 3 (BinaryRowData) ◄──── 依赖 Phase 0  可与 Phase 1/2 并行
  3.1-3.4 各 State 类补全 RowData 路径
                                         │
Phase 4 (热路径优化) ◄──── 依赖 Phase 1+2+3 (分发逻辑稳定后)
  4.1 Strategy 模式
  4.2 Checkpoint 消除拷贝
  4.3 MapState 迭代器
  4.4 ReducingState 合并操作
                                         │
Phase 5 (工程完善) ◄──── 贯穿始终
  5.1 C++ 单元测试（每 Phase 完成后补充）
  5.2 集成测试
```

**关键规则**：
- Phase 0 **必须先行** — 所有后续工作基于正确的 COW 和内存管理
- Phase 1 / Phase 2 / Phase 3 **可并行**开发（独立的类型路径和 JNI 入口）
- Phase 4 在功能路径稳定后**统一重构**分发逻辑
- Phase 5 **贯穿始终**，每个 Phase 完成后补充对应测试
