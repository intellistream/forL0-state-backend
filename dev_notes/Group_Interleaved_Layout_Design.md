# SwissTable Group-Interleaved 布局优化设计

## 1. 背景

### 1.1 问题描述

通过性能测试发现，ForL0StateBackend 的最大热点是 SwissTable 中 key/namespace 比较操作导致的 **cache miss**。

尽管我们已经为常用类型（Long、TimeWindow、VoidNamespace、String）实现了特化版本的 SwissTable，将 key/namespace 存储为 primitive 类型避免了对象比较开销，但当前实现仍然存在严重的 **memory bound** 问题。

### 1.2 根因分析

当前实现使用**独立数组**存储各类数据：

```java
// 当前 SwissTableLongTimeWindow 的数据结构
byte[] ctrl;           // 控制字节数组
long[] keys;           // key 数组
long[] namespaces;     // namespace 数组 (interleaved: start, end, start, end, ...)
Object[] values;       // value 数组
long[] hashes;         // hash 数组
```

**问题**：查找时需要访问多个独立数组，每个数组在内存中位置不连续：

```
典型查找路径:
1. ctrl[i]                    → cache line A
2. keys[i]                    → cache line B (cache miss!)
3. namespaces[2*i], [2*i+1]   → cache line C (cache miss!)
```

每次查找需要 **3-5 次 cache line 访问**，导致严重的 memory bound。

## 2. Go 1.24 Swiss Tables 内存布局分析

### 2.1 Group 结构定义

Go 1.24 的 Swiss Tables 采用**以 Group 为单位的紧凑布局**：

```go
// group.go 中的布局定义
type group struct {
    ctrls ctrlGroup           // 8 bytes: 8 个 ctrl 字节打包
    slots [8]slot             // 8 个 slot 连续存储
}

type slot struct {
    key  typ.Key              // key 类型
    elem typ.Elem             // element 类型
}
```

### 2.2 内存布局示意图

对于 `map[uint64]V` 类型：

```
┌─────────────────────────────────────────────────────────────────────┐
│                             Group 0                                 │
├──────────────┬──────────────────────────────────────────────────────┤
│  ctrlGroup   │              slots[8]                                │
│   (8 bytes)  │  [slot0][slot1][slot2][slot3][slot4][slot5][slot6][slot7]  │
└──────────────┴──────────────────────────────────────────────────────┘

每个 slot 的内部布局 (key=uint64):
┌────────────┬────────────┐
│    key     │   elem     │
│  (8 bytes) │ (sizeof V) │
└────────────┴────────────┘
```

### 2.3 访问模式

```go
// runtime_fast64.go 中的关键代码
slotKey := g.key(typ, i)                           // ctrl 同一/相邻 cache line
if key == *(*uint64)(slotKey) {
    slotElem := unsafe.Pointer(uintptr(slotKey) + 8)  // key 相邻位置
    return slotElem
}
```

**关键优势**：
1. `ctrl` 和 `slots` 在内存中连续
2. 同一 slot 的 key 和 elem 相邻
3. SWAR 匹配 ctrl 后，访问 key 很可能命中同一 cache line

## 3. 改造方案

### 3.1 设计原则

1. **单一 primitive 数组**：将 ctrl、key、namespace 打包到一个 `long[]` 数组
2. **按 Group 对齐**：每个 Group 在数组中连续存储
3. **slot 内交错**：同一 slot 的 key 和 namespace 相邻存储
4. **values 独立**：`Object[] values` 因 GC 要求需要独立
5. **hashes 独立**：`long[] hashes` 只在 rehash/grow 时使用，可独立

### 3.2 SwissTableLongVoid 布局

**场景**：`key=Long`, `namespace=VoidNamespace` (无需存储)

```
┌─────────────────────────────────────────────────────────────────┐
│                      Group i (72 bytes)                         │
├───────────┬─────────────────────────────────────────────────────┤
│ ctrlWord  │ key0 │ key1 │ key2 │ key3 │ key4 │ key5 │ key6 │ key7 │
│ (8 bytes) │  8B  │  8B  │  8B  │  8B  │  8B  │  8B  │  8B  │  8B  │
└───────────┴─────────────────────────────────────────────────────┘
```

**数组布局**：

```java
// 每个 Group 占用 9 个 long (72 bytes)
static final int LONGS_PER_GROUP_LONG_VOID = 9;

long[] groupData;  // 单一连续数组

// Group g 的访问:
// baseOffset = g * 9
// groupData[baseOffset]     = ctrl word (8 个 ctrl bytes 打包为 1 个 long)
// groupData[baseOffset + 1] = key for slot 0
// groupData[baseOffset + 2] = key for slot 1
// ...
// groupData[baseOffset + 8] = key for slot 7
```

**访问方法**：

```java
// 加载 ctrl word
long loadCtrlWord(int groupIdx) {
    return groupData[groupIdx * LONGS_PER_GROUP_LONG_VOID];
}

// 获取 key
long getKeyAt(int groupIdx, int slotInGroup) {
    return groupData[groupIdx * LONGS_PER_GROUP_LONG_VOID + 1 + slotInGroup];
}

// 全局 slot 索引转换
int globalSlot(int groupIdx, int slotInGroup) {
    return (groupIdx << 3) + slotInGroup;
}
```

### 3.3 SwissTableLongTimeWindow 布局

**场景**：`key=Long`, `namespace=TimeWindow` (需要存储 start, end)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Group i (200 bytes)                                │
├───────────┬──────────────────────────────────────────────────────────────────┤
│ ctrlWord  │  slot0   │  slot1   │  slot2   │ ... │  slot7                    │
│ (8 bytes) │ (24 bytes)│(24 bytes)│(24 bytes)│     │(24 bytes)                 │
└───────────┴──────────────────────────────────────────────────────────────────┘

每个 slot 的布局 (24 bytes = 3 longs):
┌────────────┬────────────┬────────────┐
│    key     │   start    │    end     │
│  (8 bytes) │ (8 bytes)  │ (8 bytes)  │
└────────────┴────────────┴────────────┘
```

**数组布局**：

```java
// 每个 Group 占用 25 个 long (200 bytes)
// 1 (ctrl) + 8 slots × 3 longs/slot = 25
static final int LONGS_PER_GROUP_LONG_TW = 25;
static final int LONGS_PER_SLOT = 3;  // key + start + end

long[] groupData;  // 单一连续数组

// Group g, slot s 的访问:
// baseOffset = g * 25
// groupData[baseOffset]                    = ctrl word
// groupData[baseOffset + 1 + s*3]          = slot s 的 key
// groupData[baseOffset + 1 + s*3 + 1]      = slot s 的 start
// groupData[baseOffset + 1 + s*3 + 2]      = slot s 的 end
```

**访问方法**：

```java
// 加载 ctrl word
long loadCtrlWord(int groupIdx) {
    return groupData[groupIdx * LONGS_PER_GROUP_LONG_TW];
}

// 获取 slot 数据
long getKeyAt(int groupIdx, int slotInGroup) {
    int base = groupIdx * LONGS_PER_GROUP_LONG_TW + 1 + slotInGroup * LONGS_PER_SLOT;
    return groupData[base];
}

long getStartAt(int groupIdx, int slotInGroup) {
    int base = groupIdx * LONGS_PER_GROUP_LONG_TW + 1 + slotInGroup * LONGS_PER_SLOT;
    return groupData[base + 1];
}

long getEndAt(int groupIdx, int slotInGroup) {
    int base = groupIdx * LONGS_PER_GROUP_LONG_TW + 1 + slotInGroup * LONGS_PER_SLOT;
    return groupData[base + 2];
}

// 比较操作 - 所有数据在连续内存中
boolean keyEquals(int groupIdx, int slotInGroup, long key, long start, long end) {
    int base = groupIdx * LONGS_PER_GROUP_LONG_TW + 1 + slotInGroup * LONGS_PER_SLOT;
    return groupData[base] == key 
        && groupData[base + 1] == start 
        && groupData[base + 2] == end;
}
```

### 3.4 SwissTableStringTimeWindow 布局

**场景**：`key=String` (引用类型), `namespace=TimeWindow` (可内联为 start, end)

虽然 String key 是引用类型无法内联，但 TimeWindow 的两个 long 字段可以采用 Group-Interleaved 布局优化。

**混合布局设计**：

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Group i (136 bytes)                                │
├───────────┬──────────────────────────────────────────────────────────────────┤
│ ctrlWord  │  slot0   │  slot1   │  slot2   │ ... │  slot7                    │
│ (8 bytes) │ (16 bytes)│(16 bytes)│(16 bytes)│     │(16 bytes)                 │
└───────────┴──────────────────────────────────────────────────────────────────┘

每个 slot 的布局 (16 bytes = 2 longs):
┌────────────┬────────────┐
│   start    │    end     │
│  (8 bytes) │ (8 bytes)  │
└────────────┴────────────┘

String keys 独立存储 (GC 要求):
String[] keys;  // keys[globalSlot] = String key
```

**数组布局**：

```java
// 每个 Group 占用 17 个 long (136 bytes)
// 1 (ctrl) + 8 slots × 2 longs/slot = 17
static final int LONGS_PER_GROUP_STR_TW = 17;
static final int LONGS_PER_SLOT = 2;  // start + end

long[] groupData;    // ctrl + TimeWindow 数据 (连续存储)
String[] keys;       // String 引用 (独立存储，GC 可达)

// Group g, slot s 的访问:
// baseOffset = g * 17
// groupData[baseOffset]                = ctrl word
// groupData[baseOffset + 1 + s*2]      = slot s 的 start
// groupData[baseOffset + 1 + s*2 + 1]  = slot s 的 end
// keys[g * 8 + s]                      = slot s 的 String key
```

**访问方法**：

```java
// 加载 ctrl word
long loadCtrlWord(int groupIdx) {
    return groupData[groupIdx * LONGS_PER_GROUP_STR_TW];
}

// 获取 TimeWindow 数据 (连续内存访问)
long getStartAt(int groupIdx, int slotInGroup) {
    int base = groupIdx * LONGS_PER_GROUP_STR_TW + 1 + slotInGroup * LONGS_PER_SLOT;
    return groupData[base];
}

long getEndAt(int groupIdx, int slotInGroup) {
    int base = groupIdx * LONGS_PER_GROUP_STR_TW + 1 + slotInGroup * LONGS_PER_SLOT;
    return groupData[base + 1];
}

// 获取 String key (需要独立数组访问)
String getKeyAt(int groupIdx, int slotInGroup) {
    return keys[(groupIdx << 3) + slotInGroup];
}

// 比较操作 - TimeWindow 在连续内存中，减少 1 次 cache miss
boolean keyEquals(int groupIdx, int slotInGroup, String key, long start, long end) {
    int base = groupIdx * LONGS_PER_GROUP_STR_TW + 1 + slotInGroup * LONGS_PER_SLOT;
    // 先比较 TimeWindow (连续内存，cache 友好)
    if (groupData[base] != start || groupData[base + 1] != end) {
        return false;
    }
    // 再比较 String (需要解引用)
    int globalSlot = (groupIdx << 3) + slotInGroup;
    return key.equals(keys[globalSlot]);
}
```

**优化收益**：

| 操作 | 当前实现 | 优化后 |
|------|---------|--------|
| ctrl 匹配 | 1 次 cache 访问 | 1 次 cache 访问 |
| TimeWindow 比较 | 额外 1 次 cache miss | **同一/相邻 cache line** |
| String 比较 | 1 次 cache miss | 1 次 cache miss (无变化) |
| **总计** | **3 次** | **2 次** |

通过将 TimeWindow 与 ctrl 放在连续内存中，可以减少约 33% 的 cache miss。

### 3.5 String 类型暂不优化

对于 `SwissTableStringVoid`：

- String key 是引用类型，无法内联到 primitive 数组
- VoidNamespace 无需存储
- 当前实现已经是最优布局，暂不做改造

后续如需优化，可考虑：
- 缓存 String 的 hashCode 和 length 到 `long[] groupData`
- 实现类似 Go 的 `longStringQuickEqualityTest` 快速预检

## 4. 预期收益

### 4.1 Cache 访问对比

| 操作 | 当前实现 (独立数组) | Group-Interleaved 布局 |
|------|---------------------|------------------------|
| SWAR 匹配 ctrl | 1 次 cache 访问 | 1 次 cache 访问 |
| 读取 key 比较 | 额外 1 次 cache miss | **同一/相邻 cache line** |
| 读取 namespace 比较 | 额外 1-2 次 cache miss | **同一/相邻 cache line** |
| **总 cache lines / 查找** | **3-5 次** | **1-2 次** |

### 4.2 内存访问模式

**当前实现**：
```
ctrl[]  ─────────────────────────►  [c0][c1][c2]...[cn]
keys[]  ─────────────────────────►  [k0][k1][k2]...[kn]
ns[]    ─────────────────────────►  [s0,e0][s1,e1]...[sn,en]
                                    ↑↑↑ 随机跳跃访问
```

**优化后**：
```
groupData[] ─►[ctrl0|k0,s0,e0|k1,s1,e1|...|k7,s7,e7][ctrl1|k0,s0,e0|...]
              ├─────────── Group 0 ───────────────┤├── Group 1 ──...
              ↑ 顺序访问，cache 友好
```

### 4.3 估算提升

- **Cache miss 减少**：预计减少 60-70%
- **查找延迟降低**：预计提升 30-50%
- **内存带宽节省**：相同容量下减少无效预取

## 5. 实现计划

### 5.1 阶段一：SwissTableLongVoid 改造

1. 重构数据结构为 Group-Interleaved 布局
2. 重写 `get()`, `put()`, `remove()` 方法
3. 更新 `grow()`, `rehash()` 方法
4. 单元测试验证正确性
5. Benchmark 对比性能

### 5.2 阶段二：SwissTableLongTimeWindow 改造

1. 扩展 Group 布局包含 TimeWindow 数据
2. 实现相应的访问方法
3. 单元测试 + Benchmark

### 5.3 阶段三：SwissTableStringTimeWindow 改造

1. 实现混合布局：`long[] groupData` + `String[] keys`
2. TimeWindow 数据与 ctrl 连续存储
3. 调整比较顺序：先比较 TimeWindow，再比较 String
4. 单元测试 + Benchmark

### 5.4 SwissTableStringVoid 暂不改造

- 当前实现已是最优布局
- 后续根据性能测试结果决定是否增加 hashCode/length 缓存

## 6. 风险与注意事项

### 6.1 实现复杂度

- Group-Interleaved 布局增加了索引计算复杂度
- 需要仔细处理 slot 索引的转换（group + slotInGroup vs globalSlot）

### 6.2 内存对齐

- 确保 Group 边界对齐到 cache line (64 bytes)
- 考虑在 Group 之间添加 padding 避免 false sharing

### 6.3 兼容性

- 需要更新 `ForL0StateMap` 中对 SwissTable 的使用
- `split()` 操作需要适配新布局
- 迭代器实现需要调整

### 6.4 测试覆盖

- 边界条件：空表、单 Group、多 Group
- 扩容路径：rehash、grow、split

## 7. AbstractSwissTable 接口重构

### 7.1 当前问题

当前 `AbstractSwissTable` 存在以下设计问题：

1. **定义了过多抽象方法**：`keyEquals`, `storeKeyNs`, `getKey`, `getNamespace`, `initStorage`, `copySlot`, `clearSlot`, `copyStorageFrom` 等
2. **实现了不该实现的方法**：`get()`, `put()`, `remove()`, `rehash()`, `grow()` 等核心操作在基类实现，导致无法针对 Group-Interleaved 布局优化
3. **依赖 Unsafe 和 byte[] ctrl**：使用 `loadCtrlWord()` 通过 Unsafe 访问 `byte[] ctrl`，而新布局使用 `long[] groupData`

### 7.2 新接口设计

`AbstractSwissTable` 应该只定义**接口契约**，与原 `SwissTable` 的 API 对齐：

```java
/**
 * Abstract base class for SwissTable implementations.
 * 
 * <p>This class defines the interface contract for SwissTable.
 * Subclasses implement the actual storage layout and core operations.
 */
abstract class AbstractSwissTable<K, N, S> {

    // ========== Return value encoding for put() ==========
    static final int NEW_FLAG = 1 << 16;
    static final int SLOT_MASK = 0xFFFF;
    static final int NEED_SPLIT = -1;
    static final int NEED_REHASH = -2;
    static final int NEED_GROW = -3;

    // ========== Control byte values ==========
    static final byte CTRL_EMPTY = (byte) 0x80;
    static final byte CTRL_DELETED = (byte) 0xFE;

    // ========== SWAR constants (子类可复用) ==========
    protected static final long LSB = 0x0101010101010101L;
    protected static final long MSB = 0x8080808080808080L;

    // ========== 公共状态 (子类直接访问) ==========
    short used;          // FULL slot count
    short tomb;          // DELETED slot count
    short capacity;      // total slots
    short growthLeft;    // remaining budget
    byte localDepth;     // local depth for directory split
    int index;           // starting index in directory
    int groupMask;       // (capacity / 8) - 1
    
    // ========== 仅 values 和 hashes 在基类 (GC + rehash 需要) ==========
    Object[] values;     // length = capacity
    long[] hashes;       // length = capacity (for rehash/grow)

    // ========== 核心操作 (子类实现) ==========

    /**
     * Gets the state value for the given hash, key and namespace.
     */
    abstract S get(long hash, K key, N namespace);

    /**
     * Finds or inserts an entry for the given key and namespace.
     * Returns slot encoding or NEED_REHASH/NEED_GROW/NEED_SPLIT.
     */
    abstract int put(long hash, K key, N namespace, int maxTableCapacity);

    /**
     * Removes the entry for the given key and namespace.
     */
    abstract S remove(long hash, K key, N namespace);

    /**
     * Direct insert without duplicate check. For split/rehash/grow.
     */
    abstract void putDirect(long hash, K key, N namespace, S value);

    /**
     * Rehash with same capacity to clear tombstones.
     */
    abstract void rehash();

    /**
     * Grow table to double capacity.
     */
    abstract void grow();

    // ========== 迭代与查询 (子类实现) ==========

    /**
     * Counts entries matching the given namespace.
     */
    abstract int countNamespace(Object namespace);

    /**
     * Gets the key at the given global slot index.
     */
    abstract K getKey(int slot);

    /**
     * Gets the namespace at the given global slot index.
     */
    abstract N getNamespace(int slot);

    /**
     * Creates a new table of the same type for split operation.
     */
    abstract AbstractSwissTable<K, N, S> createNew(int slotCount, byte localDepth, int index);

    // ========== Hash 函数 (static, 可复用) ==========

    static int h1(long hash) {
        return (int)(hash >>> 7);
    }

    static int h2(long hash) {
        return (int)(hash & 0x7F);
    }

    // ========== SWAR 匹配算法 (static, 可复用) ==========

    static long matchH2(long ctrlWord, int h2) {
        long pattern = LSB * (h2 & 0xFFL);
        long x = ctrlWord ^ pattern;
        return (x - LSB) & ~x & MSB;
    }

    static long matchEmpty(long ctrlWord) {
        return (ctrlWord & ~(ctrlWord << 6)) & MSB;
    }

    static long matchDeleted(long ctrlWord) {
        return (ctrlWord & (ctrlWord << 6)) & MSB;
    }

    static long matchEmptyOrDeleted(long ctrlWord) {
        return ctrlWord & MSB;
    }

    static int laneFromTz(int trailingZeros) {
        return trailingZeros >>> 3;
    }

    static long clearLowestBit(long mask) {
        return mask & (mask - 1);
    }

    static int maxOcc(int capacity) {
        return capacity * 11 / 16;
    }

    static boolean isFull(byte ctrl) {
        return (ctrl & 0x80) == 0;
    }
}
```

### 7.3 移除的方法

以下方法从 `AbstractSwissTable` 中**移除**，改为在子类中内联实现：

| 移除的方法 | 原因 |
|-----------|------|
| `keyEquals(slot, key, ns)` | 内联到 `get()`, `put()`, `remove()` 中，避免虚方法调用 |
| `storeKeyNs(slot, key, ns)` | 内联到 `put()`, `putDirect()` 中 |
| `initStorage(capacity)` | 在子类构造函数中直接初始化 |
| `copySlot(from, target, to)` | 在子类 `rehash()`, `grow()` 中内联 |
| `clearSlot(slot)` | 在子类 `remove()` 中内联 |
| `copyStorageFrom(other)` | 在子类 `rehash()`, `grow()` 中内联 |
| `loadCtrlWord(groupIdx)` | 新布局直接从 `long[] groupData` 读取 |

### 7.4 移除 Unsafe 依赖

当前实现：
```java
private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
private static final long BYTE_ARRAY_BASE_OFFSET = UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

long loadCtrlWord(int groupIdx) {
    return UNSAFE.getLong(ctrl, BYTE_ARRAY_BASE_OFFSET + ((long) groupIdx << 3));
}
```

新布局使用 `long[] groupData`，ctrl word 直接存储在数组元素中：
```java
// 子类实现 (无需 Unsafe)
long loadCtrlWord(int groupIdx) {
    return groupData[groupIdx * LONGS_PER_GROUP];
}
```

### 7.5 子类实现示例

```java
class SwissTableLongVoid<S> extends AbstractSwissTable<Long, VoidNamespace, S> {
    
    static final int LONGS_PER_GROUP = 9;  // 1 ctrl + 8 keys
    
    long[] groupData;  // Group-Interleaved layout

    SwissTableLongVoid(int slotCount, byte localDepth, int index) {
        // 初始化公共状态
        this.capacity = (short) slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.localDepth = localDepth;
        this.index = index;
        this.used = 0;
        this.tomb = 0;
        this.growthLeft = (short) maxOcc(slotCount);
        
        // 初始化存储 (无需 initStorage 抽象方法)
        int groupCount = slotCount >>> 3;
        this.groupData = new long[groupCount * LONGS_PER_GROUP];
        // 初始化 ctrl words 为 EMPTY
        long emptyCtrl = 0x8080808080808080L;
        for (int g = 0; g < groupCount; g++) {
            groupData[g * LONGS_PER_GROUP] = emptyCtrl;
        }
        this.values = new Object[slotCount];
        this.hashes = new long[slotCount];
    }

    @Override
    S get(long hash, Long key, VoidNamespace namespace) {
        int h2Hash = h2(hash);
        int group = h1(hash) & groupMask;
        int stride = 1;
        long keyVal = key.longValue();

        while (true) {
            int base = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[base];

            // Match H2 (内联 SWAR)
            long match = matchH2(ctrlWord, h2Hash);
            while (match != 0) {
                int lane = laneFromTz(Long.numberOfTrailingZeros(match));
                // 内联 keyEquals: 直接比较 primitive
                if (groupData[base + 1 + lane] == keyVal) {
                    int globalSlot = (group << 3) + lane;
                    return (S) values[globalSlot];
                }
                match = clearLowestBit(match);
            }

            if (matchEmpty(ctrlWord) != 0) {
                return null;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    // put(), remove(), rehash(), grow() 同样完全内联实现...
}
```

## 8. 参考资料

- [Go 1.24 Swiss Tables 实现](../reference/go_maps/)
- [Abseil Swiss Tables 设计](https://abseil.io/about/design/swisstables)
- [CPU Cache 优化最佳实践](https://www.intel.com/content/www/us/en/developer/articles/technical/memory-performance-in-a-nutshell.html)
