# SwissTable Key 特化版本设计方案

## 1. 问题背景

VTune 分析显示 `SwissTable.put()` 中存在严重的 Memory Bound（~54%），热点指令：

```asm
mov rcx, qword ptr [rcx+rbx*8+0x18]  // 14.5s, 49.4% MB, 108M LLC Miss
mov rbx, qword ptr [rcx+0x8]         // 20.4s, 54.3% MB, 119M LLC Miss
```

对应源码（当前实现）：
```java
if (key.equals(entries[idx])) {
    return (S) entries[idx + 1];
}
```

**根因**：两层 Pointer Chasing
1. `entries[idx]` → 加载 Object 引用（key）
2. `Object.field` → 解引用访问对象内部字段（如 Long.value）

## 2. 当前架构

当前采用 **Namespace → Key 两级 Map** 架构，SwissTable 只存储 Key 和 State：

```
ForL0StateStore<K, N, S>
├── VoidNamespace 模式: SwissTable<K,S>[] tables
└── General Namespace 模式: Map<N, SwissTable<K,S>>[] namespaceMaps
    └── SwissTable<K, S>
        ├── ctrl[]           // 控制字节
        ├── entries[]        // AoS: [k0,v0,k1,v1,...] 
        └── hashes[]         // 32-bit hash
```

**优势**：Namespace 已在上层处理，SwissTable 只需特化 Key 类型。

## 3. Go 特化方案参考

Go 1.24 Swiss Tables 提供特化版本：

| 文件 | Key 类型 | 核心优化 |
|------|----------|----------|
| `runtime_fast64.go` | uint64 | `key == *(*uint64)(slotKey)` 直接比较 |
| `runtime_faststr.go` | string | 长度 + 首尾 8 字节快速比较 |

**核心思想**：对于 primitive 类型，直接存储值而非对象引用。

## 4. 目标场景覆盖

| Benchmark | Key 类型 | 特化类 |
|-----------|----------|--------|
| Nexmark Q4/9/18/19/20 | Long | `SwissTableLong` |
| WordCount | String | `SwissTableString` |
| Nexmark Q1/2/3 (通用) | Integer | `SwissTableInt` |
| 复合 Key | Object | `SwissTable`（通用 fallback） |

**需要实现 3 个特化版本 + 1 个通用 fallback。**

## 5. 类型层次设计

### 5.1 设计选择：完全内联 vs 抽象类

| 方案 | 优点 | 缺点 |
|------|------|------|
| **完全内联（各自独立实现）** | 零虚方法开销，JIT 内联最优 | 代码重复 |
| 抽象类 + 模板方法 | 代码复用 | 虚方法调用开销，热路径 devirtualization 不确定 |

**选择完全内联**：考虑到 `get()`/`put()` 是热路径，虚方法调用可能阻止 JIT 内联优化。每个特化版本独立实现核心方法。

### 5.2 类型层次

```
SwissTable<K, S>                    // 通用实现（现有代码保持不变）
    │
    │   // 存储布局
    │   byte[] ctrl
    │   Object[] entries            // AoS: [k0,v0,k1,v1,...]
    │   int[] hashes
    │
SwissTableLong<S>                   // Long key 特化
    │
    │   // 存储布局（Group-Interleaved）
    │   long[] groupData            // [ctrl|k0|k1|...|k7] per group
    │   Object[] values             // slot i → values[i]
    │   int[] hashes
    │
SwissTableInt<S>                    // Integer key 特化
    │
    │   // 存储布局（Group-Interleaved）
    │   long[] groupData            // [ctrl|k0k1|k2k3|k4k5|k6k7] per group (int pairs packed)
    │   Object[] values
    │   int[] hashes
    │
SwissTableString<S>                 // String key 特化
    │
    │   // 存储布局
    │   byte[] ctrl
    │   String[] keys               // 仍为引用，但类型明确
    │   Object[] values
    │   int[] hashes
```

## 6. 存储结构设计

### 6.1 通用版本（现有 SwissTable）

```java
byte[] ctrl;           // 控制字节
Object[] entries;      // AoS: [k0,v0,k1,v1,...], length = capacity * 2
int[] hashes;          // 32-bit hash
```

### 6.2 SwissTableLong（Group-Interleaved 布局）

**核心优化**：将 ctrl 和 keys 放在同一个 `long[]` 数组中，实现单次 cache line 加载。

```java
// 每组 9 个 long: 1 ctrl word + 8 keys
private static final int LONGS_PER_GROUP = 9;

long[] groupData;      // [ctrl|k0|k1|...|k7] × groupCount
Object[] values;       // slot i → values[i]
int[] hashes;          // 32-bit hash
```

**内存布局**（每组 72 字节）：
```
Group 0: [ctrlWord0][key0][key1][key2][key3][key4][key5][key6][key7]
Group 1: [ctrlWord1][key0][key1][key2][key3][key4][key5][key6][key7]
...
```

**访问方式**：
```java
int groupBase = group * LONGS_PER_GROUP;  // 或 (group << 3) + group
long ctrlWord = groupData[groupBase];
long keyAtLane = groupData[groupBase + 1 + lane];
```

### 6.3 SwissTableInt（紧凑布局）

```java
// 每组 5 个 long: 1 ctrl word + 4 个 int pairs (2 ints packed per long)
private static final int LONGS_PER_GROUP = 5;

long[] groupData;      // [ctrl|k0k1|k2k3|k4k5|k6k7] × groupCount
Object[] values;
int[] hashes;
```

**访问方式**：
```java
int groupBase = group * LONGS_PER_GROUP;
long ctrlWord = groupData[groupBase];
int pairIdx = lane >>> 1;  // 0-3
long pair = groupData[groupBase + 1 + pairIdx];
int keyAtLane = (lane & 1) == 0 ? (int) pair : (int) (pair >>> 32);
```

### 6.4 SwissTableString

String 无法内联存储，但使用强类型数组可以：
1. 避免类型检查开销
2. 帮助 JIT 优化 `equals()` 调用

```java
byte[] ctrl;
String[] keys;         // 强类型数组
Object[] values;
int[] hashes;
```

## 7. 核心算法实现

### 7.1 SwissTableLong.get()

```java
@SuppressWarnings("unchecked")
public S get(int hash, long key) {  // 注意：参数是 primitive long
    int group = h1(hash) & groupMask;
    int stride = 1;

    while (true) {
        int groupBase = (group << 3) + group;  // group * 9，融合乘法
        long ctrlWord = groupData[groupBase];

        // SWAR 并行匹配 H2
        for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
            int lane = Long.numberOfTrailingZeros(match) >>> 3;
            // 直接 long 比较，零 Pointer Chasing
            if (groupData[groupBase + 1 + lane] == key) {
                return (S) values[(group << 3) + lane];
            }
        }

        if (matchEmpty(ctrlWord) != 0) return null;

        group = (group + stride) & groupMask;
        stride++;
    }
}
```

### 7.2 SwissTableLong.put()

```java
public int put(int hash, long key) {  // 返回 slot | NEW_FLAG 或 NEED_REHASH/NEED_GROW
    int group = h1(hash) & groupMask;
    int stride = 1;
    int firstDeletedSlot = -1;

    while (true) {
        int groupBase = (group << 3) + group;
        long ctrlWord = groupData[groupBase];

        // 1. 查找已存在的 key
        for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
            int lane = Long.numberOfTrailingZeros(match) >>> 3;
            if (groupData[groupBase + 1 + lane] == key) {
                return (group << 3) + lane;  // 已存在，返回 slot
            }
        }

        // 2. 记录第一个 DELETED 位置
        long emptyOrDeleted = matchEmptyOrDeleted(ctrlWord);
        if (emptyOrDeleted == 0) {
            group = (group + stride) & groupMask;
            stride++;
            continue;
        }

        int lane = Long.numberOfTrailingZeros(emptyOrDeleted) >>> 3;
        if (getCtrlByte(ctrlWord, lane) == CTRL_DELETED) {
            if (firstDeletedSlot == -1) firstDeletedSlot = (group << 3) + lane;
            group = (group + stride) & groupMask;
            stride++;
            continue;
        }

        // 3. 找到 EMPTY - 执行插入
        int insertSlot = (firstDeletedSlot != -1) ? firstDeletedSlot : (group << 3) + lane;
        boolean useDeleted = firstDeletedSlot != -1;

        if (!useDeleted && growthLeft == 0) {
            if (tomb > 0) return NEED_REHASH;
            return NEED_GROW;
        }

        // 写入 key 和 ctrl
        int insertBase = ((insertSlot >>> 3) << 3) + (insertSlot >>> 3);  // insertGroup * 9
        int insertLane = insertSlot & 7;
        groupData[insertBase + 1 + insertLane] = key;
        groupData[insertBase] = setCtrlByte(groupData[insertBase], insertLane, (byte) h2(hash));
        hashes[insertSlot] = hash;
        used++;
        if (useDeleted) tomb--; else growthLeft--;
        return insertSlot | NEW_FLAG;
    }
}
```

### 7.3 SwissTableString.get()

```java
@SuppressWarnings("unchecked")
public S get(int hash, String key) {  // 强类型参数
    int h2Hash = h2(hash);
    long h2Pattern = LSB * (h2Hash & 0xFFL);
    int group = h1(hash) & groupMask;
    int stride = 1;

    while (true) {
        long ctrlWord = loadCtrlWord(group);
        int base = group << 3;

        for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
            int slot = base + (Long.numberOfTrailingZeros(match) >>> 3);
            // String.equals() - JIT 可以内联
            if (key.equals(keys[slot])) {
                return (S) values[slot];
            }
        }

        if (matchEmpty(ctrlWord) != 0) return null;

        group = (group + stride) & groupMask;
        stride++;
    }
}
```

## 8. 简化设计：只特化 StateStore 和 SwissTable

### 8.1 设计原则

**简化优先**：
- ❌ 不修改 KeyContext
- ❌ 不缓存 primitive key
- ❌ 不特化 State 类
- ✅ 只特化 StateStore 和 SwissTable

**Trade-off**：
- 每次调用有一次拆箱开销（`((Long) key).longValue()`）
- 但拆箱在 CPU pipeline 中可以被很好地优化
- 核心收益（消除 Pointer Chasing）仍然保留

### 8.2 调用链

```
用户代码: state.value()
    │
    ▼
ForL0ValueState.value()
    │ stateStore.get(currentKey, namespace, keyGroup)
    ▼
ForL0StateStoreLong.get(K key, N ns, int kg)  // K = Long
    │ long k = ((Long) key).longValue();       // 一次拆箱
    │ int hash = hashLong(k);
    │ table.get(hash, k)
    ▼
SwissTableLong.get(int hash, long key)
    │ groupData[groupBase]                     // ctrl word
    │ groupData[groupBase + 1 + lane] == key   // 直接 long 比较
    ▼
返回 values[slot]
```

### 8.3 State 类（无需修改）

```java
public class ForL0ValueState<K, N, V> implements InternalValueState<K, N, V> {

    // 持有基类引用，无需 cast
    private final ForL0StateStore<K, N, V> stateStore;
    private final ForL0KeyContext<K> keyContext;

    @Override
    public V value() {
        // 直接调用，无需类型转换
        // JIT 运行时知道实际类型，会做 Monomorphic Inline Caching
        return stateStore.get(
            keyContext.getCurrentKey(),
            keyContext.getCurrentNamespace(),
            keyContext.getCurrentKeyGroup()
        );
    }

    @Override
    public void update(V value) {
        stateStore.put(
            keyContext.getCurrentKey(),
            keyContext.getCurrentNamespace(),
            value,
            keyContext.getCurrentKeyGroup()
        );
    }
}
```

**关键点**：
- 持有 `ForL0StateStore<K, N, V>` 基类引用（非 Object）
- **无需 cast**，编译期类型安全
- 实际类型可以是 `ForL0StateStoreLong/Int/String` 或通用 `ForL0StateStore`
- 通过继承多态调用，JIT 会内联具体实现

### 8.4 特化 StateStore

```java
/**
 * Specialized StateStore for Long keys.
 * Inherits from ForL0StateStore to maintain API compatibility.
 */
public class ForL0StateStoreLong<N, S> extends ForL0StateStore<Long, N, S> {

    /** VoidNamespace mode: direct SwissTableLong array. */
    private final SwissTableLong<S>[] tablesLong;

    /** General Namespace mode: HashMap per key group. */
    private final Map<N, SwissTableLong<S>>[] namespaceMapsLong;

    @Override
    public final S get(Long key, N namespace, int keyGroup) {  // final!
        long k = key.longValue();  // 一次拆箱
        int hash = hashLong(k);
        
        if (isVoidNamespace) {
            SwissTableLong<S> table = tablesLong[keyGroup - keyGroupOffset];
            return table == null ? null : table.get(hash, k);
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[keyGroup - keyGroupOffset];
            if (nsMap == null) return null;
            SwissTableLong<S> table = nsMap.get(namespace);
            return table == null ? null : table.get(hash, k);
        }
    }

    @Override
    public final void put(Long key, N namespace, S state, int keyGroup) {  // final!
        long k = key.longValue();
        int hash = hashLong(k);
        SwissTableLong<S> table = getOrCreateTableLong(keyGroup, namespace);
        
        while (true) {
            int result = table.put(hash, k);
            if (result == SwissTableLong.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTableLong.NEED_GROW) {
                table.grow();
                continue;
            }
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = state;
            return;
        }
    }

    // Hash function: 内联 XOR 折叠 + smear
    private static int hashLong(long key) {
        int h = (int) (key ^ (key >>> 32));
        return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
    }
}
```

### 8.5 继承 vs 接口

**选择继承**：`ForL0StateStoreLong extends ForL0StateStore<Long, N, S>`

优点：
- State 类无需修改，直接调用父类方法
- API 完全兼容
- 子类 override `get()`/`put()` 提供特化实现

```java
// 类型层次
ForL0StateStore<K, N, S>                    // 通用实现
    │
    ├── ForL0StateStoreLong<N, S>           // Long 特化
    ├── ForL0StateStoreInt<N, S>            // Int 特化
    └── ForL0StateStoreString<N, S>         // String 特化
```

### 8.6 ForL0KeyedStateBackend 创建 StateStore

```java
@SuppressWarnings("unchecked")
private <N, S> ForL0StateStore<K, N, S> createStateStore(
        RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
    
    // 类型检测发生在构造时
    if (keySerializer instanceof LongSerializer) {
        return (ForL0StateStore<K, N, S>) 
            new ForL0StateStoreLong<>(keyGroupRange, metaInfo, this);
    }
    if (keySerializer instanceof IntSerializer) {
        return (ForL0StateStore<K, N, S>) 
            new ForL0StateStoreInt<>(keyGroupRange, metaInfo, this);
    }
    if (keySerializer instanceof StringSerializer) {
        return (ForL0StateStore<K, N, S>) 
            new ForL0StateStoreString<>(keyGroupRange, metaInfo, this);
    }
    return new ForL0StateStore<>(keyGroupRange, keySerializer, metaInfo, this);
}
```

### 8.7 性能分析

| 操作 | 开销 | 说明 |
|------|------|------|
| `((Long) key).longValue()` | ~1 cycle | CPU pipeline 优化好 |
| `hashLong(k)` | ~5 cycles | 内联计算 |
| `table.get(hash, k)` | 大幅减少 | 零 Pointer Chasing |

**核心收益保留**：
- ✅ SwissTableLong 内部零 Pointer Chasing
- ✅ ctrl + keys 在同一 cache line
- ✅ 直接 long 比较

**额外开销**：
- ⚠️ 每次调用一次拆箱（~1 cycle，可接受）
- ⚠️ StateStore.get() 是虚方法调用（JIT 可内联）

### 8.8 完整调用链分析

```
state.value()
    │ 虚调用 stateStore.get()          // JIT 可内联（单态）
    ▼
ForL0StateStoreLong.get(Long key, N ns, int kg)
    │ key.longValue()                   // 拆箱 ~1 cycle
    │ hashLong(k)                       // 内联 ~5 cycles
    ▼
SwissTableLong.get(hash, k)
    │ groupData[base]                   // ctrl word (L1 hit)
    │ matchH2()                         // SWAR ~2 cycles
    │ groupData[base + 1 + lane] == k   // 直接比较 (同一 cache line)
    ▼
return values[slot]                     // 返回结果
```

**对比原来的调用链**：

| 步骤 | 原方案 | 新方案 |
|------|--------|--------|
| key 比较 | `key.equals(entries[idx])` → 2 次 Pointer Chasing | `groupData[...] == k` → 0 次 |
| 内存访问 | entries[idx] + Object.field | 同一 cache line |
| LLC miss | 高 | 极低 |

## 9. Hash 计算

### 9.1 原始类型 Hash 优化

Java 的 `hashCode()` 实现：
- `Integer.hashCode(int)` → 直接返回 `value` 本身
- `Long.hashCode(long)` → `(int)(value ^ (value >>> 32))`

因此：
- **Integer**：`hashCode()` 调用完全多余，直接用 `key` 本身
- **Long**：可以内联 XOR 折叠操作

### 9.2 Smear 函数

所有类型都需要经过 smear 函数（aligned with hash-smith）来改善 hash 分布：

```java
// Smear function from Guava
private static int smear(int h) {
    return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
}
```

### 9.3 特化版本 Hash 函数

```java
// SwissTableLong: 内联 Long 的 XOR 折叠 + smear
static int hashLong(long key) {
    int h = (int) (key ^ (key >>> 32));  // 直接内联，无方法调用
    return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
}

// SwissTableInt: 直接使用 key + smear
static int hashInt(int key) {
    // key 本身就是 hashCode，无需调用 Integer.hashCode()
    return (int) (0x1b873593 * Integer.rotateLeft(key * 0xcc9e2d51, 15));
}

// SwissTableString: String.hashCode() + smear
static int hashString(String key) {
    int h = key.hashCode();  // String 需要调用，有缓存
    return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
}
```

### 9.4 性能影响

| 类型 | 原方案 | 优化后 | 节省 |
|------|--------|--------|------|
| Integer | `Integer.hashCode(key)` + smear | `smear(key)` | 1 次方法调用 |
| Long | `Long.hashCode(key)` + smear | 内联 XOR + smear | 1 次方法调用 |
| String | `key.hashCode()` + smear | 同左（无法优化） | - |

## 10. 预期收益

| 特化类型 | Pointer Chasing | Memory Bound 预期 | 说明 |
|----------|-----------------|-------------------|------|
| Generic（现有） | 2 层 | ~54%（基线） | entries[idx] → Object.field |
| **SwissTableLong** | **0 层** | **<5%** | ctrl+keys 同 cache line |
| **SwissTableInt** | **0 层** | **<5%** | 更紧凑，2 ints/long |
| SwissTableString | 1 层 | ~20% | String.equals 内部仍需访问 char[] |

## 11. 文件结构

```
src/main/java/org/apache/flink/state/forl0/
├── SwissTable.java              // 通用实现（保持不变）
├── SwissTableLong.java          // Long key 特化（Group-Interleaved）
├── SwissTableInt.java           // Integer key 特化
├── SwissTableString.java        // String key 特化
│
├── ForL0StateStore.java         // 通用 StateStore（保持不变）
├── ForL0StateStoreLong.java     // Long key 特化，继承 ForL0StateStore
├── ForL0StateStoreInt.java      // Int key 特化，继承 ForL0StateStore
├── ForL0StateStoreString.java   // String key 特化，继承 ForL0StateStore
│
├── ForL0KeyContext.java         // 保持不变
├── ForL0KeyedStateBackend.java  // 类型检测，选择特化 StateStore
└── ForL0ValueState.java         // 保持不变（通过继承多态调用）
```

**代码量**：
| 组件 | 新增类 | 修改类 |
|------|--------|--------|
| SwissTable | 3 个 | 0 |
| StateStore | 3 个 | 1 个（添加类型检测） |
| State 类 | 0 | 0 |
| KeyContext | 0 | 0 |
| **总计** | **6 个新类** | **1 个修改** |

## 12. 实施计划

### 12.1 Step 1：实现 SwissTableLong（1天）

**目标**：覆盖 Nexmark 大部分 Query（Long key 是最常见场景）。

**实现**：
1. 创建 `SwissTableLong<S>` 类
2. 实现 Group-Interleaved 布局：`long[] groupData`
3. 实现 `get(int hash, long key)` - 零 Pointer Chasing
4. 实现 `put(int hash, long key)` - 返回 slot 编码
5. 实现 `remove(int hash, long key)`
6. 实现 `rehash()` 和 `grow()`
7. 实现迭代器

**验证**：
1. 新增 `SwissTableLongTest` 单元测试
2. 确保 API 与 `SwissTable<K,S>` 对齐

### 12.2 Step 2：实现 SwissTableInt 和 SwissTableString（1天）

**SwissTableInt**：
1. 创建 `SwissTableInt<S>` 类
2. 实现紧凑布局：每个 long 存储 2 个 int
3. API: `get(int hash, int key)`, `put(int hash, int key)`

**SwissTableString**：
1. 创建 `SwissTableString<S>` 类
2. 使用强类型 `String[] keys` 数组
3. API: `get(int hash, String key)`, `put(int hash, String key)`

### 12.3 Step 3：实现特化 StateStore（1天）

**目标**：实现 ForL0StateStoreLong/Int/String，继承 ForL0StateStore。

**实现**：
1. 创建 `ForL0StateStoreLong<N, S> extends ForL0StateStore<Long, N, S>`
   - 持有 `SwissTableLong<S>[]` 和 `Map<N, SwissTableLong<S>>[]`
   - Override `get(Long key, N ns, int kg)` 拆箱并调用特化 SwissTable
2. 同理创建 `ForL0StateStoreInt` 和 `ForL0StateStoreString`
3. 保持与 `ForL0StateStore` 相同的 VoidNamespace 优化

### 12.4 Step 4：ForL0KeyedStateBackend 集成（0.5天）

**实现**：
1. 修改 `createStateStore()` 方法
2. 根据 `TypeSerializer<K>` 选择特化 StateStore
3. 返回 `ForL0StateStore<K, N, S>` 类型（多态）

### 12.5 Step 5：单元测试与集成测试（0.5天）

**验证项**：
1. 各 SwissTable 特化版本单元测试
2. 各 StateStore 特化版本单元测试
3. 现有 `ForL0StateStoreTest` 通过（通用版本不受影响）
4. MiniCluster ITCase 通过

### 12.6 Step 6：性能验证（1天）

**验证项**：
1. JMH StateMapBenchmark 对比：
   - Generic vs Long 特化：预期 50%+ 提升
   - Generic vs Int 特化：预期 50%+ 提升
   - Generic vs String 特化：预期 20-30% 提升
2. VTune 分析确认 Memory Bound 下降
3. Nexmark Q4 端到端验证

### 12.7 时间线

| Step | 内容 | 预计时间 |
|------|------|----------|
| 1 | SwissTableLong 实现 | 1 天 |
| 2 | SwissTableInt + SwissTableString | 1 天 |
| 3 | ForL0StateStoreLong/Int/String | 1 天 |
| 4 | KeyedStateBackend 集成 | 0.5 天 |
| 5 | 单元测试与集成测试 | 0.5 天 |
| 6 | 性能验证 | 1 天 |
| **总计** | | **5 天** |

## 13. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| StateStore 虚调用未被 JIT 内联 | 性能损失 | 每个 State 实例只持有一个具体类型，满足单态条件；JIT 可做 Monomorphic Inline Caching |
| 拆箱开销（~1 cycle） | 较小 | 可接受的 trade-off，核心收益（消除 Pointer Chasing）仍然保留 |
| 特化版本 API 与通用版本不对齐 | 集成困难 | 通过继承保证 API 兼容，特化 StateStore 继承通用 StateStore |
| 继承层次影响 JIT 优化 | 性能不达预期 | 特化类 override 的方法是 final，JIT 可以内联 |

## 14. 后续优化方向

1. **复合 Key 特化**：如 `Tuple2<Long, Long>` 可特化为 `long[2]`
2. **SIMD 加速**：Java 19+ Vector API 实现真正的 SIMD 匹配
3. **L0 Cache 集成**：将 `groupData` 分配到 L0 内存空间
