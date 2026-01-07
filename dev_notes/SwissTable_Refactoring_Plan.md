# ForL0StateMap/SwissTable 重构方案

> 创建日期: 2026-01-05  
> 状态: **设计完成，待实施**  
> 目标: 对齐 Go 1.24 Swiss Tables 架构，ForL0StateMap 直接管理 directory + SwissTable 存储

---

## 一、当前问题分析

### 1.1 职责划分混乱

| 问题 | 当前实现 | Go 参考实现 |
|------|----------|-------------|
| Entry 存储位置 | 全局管理 `keys[]`, `states[]` | 每个 Table 独立管理 |
| Get/Put 实现 | 在 Map 层中 | 在 Table 中 |
| Map 职责 | 混合了路由+存储+操作 | 仅负责 directory 路由 |

### 1.2 冗余的间接层

当前设计：
```
slot → entryId → keys[entryId*2], states[entryId]
```

Go 设计（简化）：
```
slot → 直接存储 key/value
```

### 1.3 不必要的 FreeList

当前需要 `freeList` 是因为 `entryId` 是全局分配的，删除后需要回收。
如果 Entry 直接存在 slot 中，删除只需置 `ctrl[slot] = DELETED`，无需回收管理。

---

## 二、改造目标

### 2.1 新的职责划分

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ ForL0StateMap<K, N, S> (实现 StateMap 接口 + Directory 路由)                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ 职责:                                                                        │
│   - 实现 Flink StateMap 接口 (get/put/remove/transform/iterator/snapshot)   │
│   - Directory 管理 (路由到正确的 Table)                                      │
│   - 全局计数 (size)                                                          │
│   - Directory 扩展 (growDirectory)                                           │
│   - Split 协调 (接收 Table 的 NEED_SPLIT 信号)                               │
│   - Namespace 去重 (lastNamespace 优化)                                      │
│                                                                              │
│ 成员:                                                                        │
│   - size: int                   // 总条目数                                  │
│   - globalDepth: int            // directory 位数                            │
│   - globalShift: int            // 64 - globalDepth                          │
│   - directory: SwissTable[]     // 路由表                                    │
│   - tables: List<SwissTable>    // 唯一表列表 (用于遍历)                     │
│   - lastNamespace: N            // 上次访问的 namespace (去重优化)           │
│                                                                              │
│ StateMap 接口方法:                                                           │
│   - get(key, ns): S                                                         │
│   - put(key, ns, value): void                                               │
│   - putAndGetOld(key, ns, value): S                                         │
│   - remove(key, ns): void                                                   │
│   - removeAndGetOld(key, ns): S                                             │
│   - transform(key, ns, value, transformer): void                            │
│   - iterator(): Iterator                                                     │
│   - stateSnapshot(): StateMapSnapshot                                        │
│                                                                              │
│ 内部方法:                                                                    │
│   - computeHash(key, ns): long                                              │
│   - locateTable(hash): SwissTable                                           │
│   - handleSplit(t): void                                                    │
│   - growDirectory(): void                                                   │
│   - split(t): void                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ SwissTable<K, N, S> (存储层)                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ 职责:                                                                        │
│   - 真正的 Swiss Table 哈希表                                                │
│   - 执行 get/put/remove 操作                                                 │
│   - 管理自己的 Entry 存储 (ctrl + keysNs + values)                          │
│   - 决定何时 rehash/grow                                                     │
│   - 返回 NEED_SPLIT 信号让 ForL0StateMap 处理分裂                            │
│                                                                              │
│ 成员:                                                                        │
│   - used: short                 // FULL slot 数                              │
│   - tomb: short                 // DELETED slot 数 (O(1) 判断墓碑)           │
│   - capacity: short             // slot 总数                                 │
│   - growthLeft: short           // 剩余插入预算                              │
│   - localDepth: byte            // 本地深度                                  │
│   - index: int                  // 在 directory 中的起始索引 (Go 风格)       │
│   - groupMask: int              // (capacity / 8) - 1                        │
│   - ctrl: byte[]                // 控制字节, length = capacity              │
│   - keysNs: Object[]            // length = capacity * 2                    │
│   - values: Object[]            // length = capacity                        │
│                                                                              │
│ 方法:                                                                        │
│   - get(hash, key, ns): S                                                   │
│   - put(hash, key, ns): int (返回 slot 编码，调用者写 values[slot])         │
│   - remove(hash, key, ns): S                                                │
│   - putDirect(hash, key, ns, value): void (split 时使用，跳过查重)          │
│   - rehash(): void                                                          │
│   - grow(): void                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 新的内存布局

```
SwissTable 内部布局:
┌────────────────────────────────────────────────────────────────────────────┐
│ ctrl[0..capacity-1]                                                         │
│   EMPTY=0x80, DELETED=0xFE, FULL=h2(0x00-0x7F)                             │
├────────────────────────────────────────────────────────────────────────────┤
│ keysNs[0..capacity*2-1]                                                     │
│   slot i → keysNs[2*i]=key, keysNs[2*i+1]=namespace                        │
├────────────────────────────────────────────────────────────────────────────┤
│ values[0..capacity-1]                                                       │
│   slot i → values[i]                                                        │
└────────────────────────────────────────────────────────────────────────────┘

访问示例:
  slot 5 的 key:       keysNs[10]
  slot 5 的 namespace: keysNs[11]
  slot 5 的 value:     values[5]
```

### 2.3 Hash 位分配（对齐 Go 1.24）

```
64 位 hash 的位分配:
┌───────────────────────────────────────────────────────────────┐
│ 63                    7 6       0 │
│ ├─────────────────────┤ ├───────┤ │
│ │      H1 (57 bits)     │ │H2(7b)│ │
│ ├─────────────────────┤ └───────┘ │
│ │                       │               │
│ ├─────────┬───────────┤               │
│ │directory│  probing   │               │
│ │ routing │  (group)   │               │
│ └─────────┴───────────┘               │
└───────────────────────────────────────────────────────────────┘

Go 实现 (reference/go_maps/map.go:183-191):
  h1(hash) = hash >>> 7    // 高 57 位，用于探测起始 group
  h2(hash) = hash & 0x7F   // 低 7 位，存入 ctrl 字节

我们的实现:
  - Directory 路由: hash >>> globalShift   (取高 globalDepth 位)
  - Probe 起始:   h1(hash) & groupMask    (取 h1 的低位对齐 group 数)
  - Ctrl 字节:    h2(hash)                (取低 7 位)
```

```java
// 对齐 Go 1.24 的 h1/h2 定义
static int h1(long hash) {
    return (int)(hash >>> 7);  // 高 57 位 (截断为 int 用于索引)
}

static int h2(long hash) {
    return (int)(hash & 0x7F); // 低 7 位
}
```

---

## 三、详细设计

### 3.1 SwissTable 新结构

```java
class SwissTable<K, N, S> {
    // 返回值编码
    // 正常返回: slot | (isNew ? NEW_FLAG : 0)
    //   - slot: 低 16 位，插入/找到的槽位索引
    //   - isNew: 高位标志，表示是否新插入
    // 特殊返回: NEED_SPLIT
    static final int NEW_FLAG = 1 << 16;     // 新插入标志
    static final int SLOT_MASK = 0xFFFF;     // 槽位掩码
    static final int NEED_SPLIT = -1;        // 需要 split (由 Map 处理)
    
    // Control bytes
    static final byte CTRL_EMPTY = (byte) 0x80;
    static final byte CTRL_DELETED = (byte) 0xFE;
    
    // SWAR constants
    private static final long LSB = 0x0101010101010101L;
    private static final long MSB = 0x8080808080808080L;
    
    // Unsafe for SWAR
    private static final Unsafe UNSAFE;
    private static final long BYTE_ARRAY_BASE_OFFSET;
    
    // Counts
    short used;          // FULL slot count
    short tomb;          // DELETED slot count (用于 O(1) 判断是否有墓碑)
    short capacity;      // total slots (power of 2, multiple of 8)
    short growthLeft;    // remaining insert budget = maxOcc - used - tomb
    
    // Directory info (Go 风格)
    byte localDepth;     // 本地深度
    int index;           // 在 directory 中的起始索引
    
    // Group mask for probing
    int groupMask;       // (capacity / 8) - 1
    
    // Storage (直接按 slot 索引)
    byte[] ctrl;         // length = capacity
    Object[] keysNs;     // length = capacity * 2
    Object[] values;     // length = capacity
    
    // ======== 构造函数 ========
    
    SwissTable(int slotCount, byte localDepth, int index) {
        this.capacity = (short) slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.localDepth = localDepth;
        this.index = index;
        this.ctrl = new byte[slotCount];
        Arrays.fill(this.ctrl, CTRL_EMPTY);
        this.keysNs = new Object[slotCount * 2];
        this.values = new Object[slotCount];
        this.used = 0;
        this.tomb = 0;
        this.growthLeft = (short) maxOcc(slotCount);
    }
    
    static int maxOcc(int capacity) {
        return capacity * 7 / 8;  // 87.5% load factor
    }
    
    // ======== Hash 函数 (对齐 Go 1.24) ========
    
    /** H1: 高 57 位，用于探测起始 group */
    static int h1(long hash) {
        return (int)(hash >>> 7);
    }
    
    /** H2: 低 7 位，存入 ctrl 字节 */
    static int h2(long hash) {
        return (int)(hash & 0x7F);
    }
}
```

### 3.2 ForL0StateMap 结构

```java
class ForL0StateMap<K, N, S> implements StateMap<K, N, S> {
    private static final int INITIAL_TABLE_CAPACITY = 64;
    private static final int MAX_TABLE_CAPACITY = 1024;
    
    // 全局计数
    int size;            // 总条目数
    
    // Directory
    int globalDepth;     // directory 位数
    int globalShift;     // 64 - globalDepth
    SwissTable<K, N, S>[] directory;
    List<SwissTable<K, N, S>> tables;  // 唯一表列表 (用于 snapshot/iteration)
    
    // Namespace 去重优化
    N lastNamespace;
    
    // ======== 构造函数 ========
    
    ForL0StateMap() {
        this.globalDepth = 0;
        this.globalShift = 64;
        SwissTable<K, N, S> initTable = new SwissTable<>(INITIAL_TABLE_CAPACITY, (byte) 0, 0);
        this.directory = new SwissTable[] { initTable };
        this.tables = new ArrayList<>();
        this.tables.add(initTable);
        this.size = 0;
    }
    
    // ======== Hash 计算 ========
    
    private long computeHash(Object key, Object namespace) {
        int h32 = MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
        long x = (h32 & 0xFFFFFFFFL) * 0x9e3779b97f4a7c15L;
        x ^= x >>> 32;
        return x;
    }
    
    // ======== 路由 ========
    
    private SwissTable<K, N, S> locateTable(long hash) {
        int dirIdx = globalDepth == 0 ? 0 : (int)(hash >>> globalShift);
        return directory[dirIdx];
    }
}
```

### 3.3 SwissTable.put() - 返回 slot 编码

```java
/**
 * 查找或插入 key/namespace，返回 slot 编码。
 * 
 * 返回值:
 *   - slot | NEW_FLAG: 新插入，调用者需写 values[slot]
 *   - slot: 已存在，调用者可读/写 values[slot]
 *   - NEED_SPLIT: 需要 split，调用者处理后重试
 * 
 * 注意: 新插入时已填充 keysNs[slot*2], keysNs[slot*2+1], ctrl[slot]
 */
@SuppressWarnings("unchecked")
int put(long hash, K key, N namespace) {
    int h2Hash = h2(hash);  // 低 7 位
    
    while (true) {  // 主循环，rehash/grow 后 continue 重试
        int group = h1(hash) & groupMask;  // 高 57 位取低位
        int stride = 1;
        int firstDeletedSlot = -1;
        
        while (true) {  // 探测循环
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;
            
            // 1. 查找已存在的 key
            long match = matchH2(ctrlWord, h2Hash);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                int keyIdx = slot << 1;
                if (key.equals(keysNs[keyIdx]) && namespace.equals(keysNs[keyIdx + 1])) {
                    // 找到，返回 slot（无 NEW_FLAG）
                    return slot;
                }
                match = clearLowestBit(match);
            }
            
            // 2. 记录第一个 DELETED 位置
            if (firstDeletedSlot == -1) {
                long delMask = matchDeleted(ctrlWord);
                if (delMask != 0) {
                    firstDeletedSlot = base + laneFromTz(Long.numberOfTrailingZeros(delMask));
                }
            }
            
            // 3. 检查 EMPTY (探测终止)
            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int emptySlot = base + laneFromTz(Long.numberOfTrailingZeros(emptyMask));
                
                // 确定插入位置
                int insertSlot;
                boolean useDeleted;
                
                if (firstDeletedSlot != -1) {
                    insertSlot = firstDeletedSlot;
                    useDeleted = true;
                } else {
                    insertSlot = emptySlot;
                    useDeleted = false;
                    
                    // 使用 EMPTY 需要检查预算
                    if (growthLeft == 0) {
                        // O(1) 判断是否有墓碑
                        if (tomb > 0) {
                            rehash();
                            break;  // 跳出探测循环，continue 主循环重试
                        }
                        // 无墓碑，需要扩容
                        if (capacity < MAX_TABLE_CAPACITY) {
                            grow();
                            break;  // 跳出探测循环，continue 主循环重试
                        }
                        // 已达最大容量，需要 split
                        return NEED_SPLIT;
                    }
                }
                
                // 执行插入（只填充 key/ns/ctrl，value 由调用者写）
                int keyIdx = insertSlot << 1;
                keysNs[keyIdx] = key;
                keysNs[keyIdx + 1] = namespace;
                // values[insertSlot] 由调用者填充
                ctrl[insertSlot] = (byte) h2Hash;
                used++;
                
                if (useDeleted) {
                    tomb--;  // 复用墓碑，growthLeft 不变
                } else {
                    growthLeft--;
                }
                
                return insertSlot | NEW_FLAG;  // 返回 slot + 新插入标志
            }
            
            // 4. 继续探测
            group = (group + stride) & groupMask;
            stride++;
        }
        // rehash/grow 后会到这里，continue 主循环重试
    }
}
```

### 3.4 SwissTable.remove()

```java
@SuppressWarnings("unchecked")
S remove(long hash, K key, N namespace) {
    int h2Hash = h2(hash);
    int group = h1(hash) & groupMask;
    int stride = 1;
    
    while (true) {
        long ctrlWord = loadCtrlWord(group);
        int base = group << 3;
        
        // Match H2
        long match = matchH2(ctrlWord, h2Hash);
        while (match != 0) {
            int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
            int keyIdx = slot << 1;
            if (key.equals(keysNs[keyIdx]) && namespace.equals(keysNs[keyIdx + 1])) {
                // 找到 - 删除前判断策略
                boolean hasEmpty = matchEmpty(ctrlWord) != 0;
                
                // 获取旧值
                S oldValue = (S) values[slot];
                
                // 清除 slot 数据
                keysNs[keyIdx] = null;
                keysNs[keyIdx + 1] = null;
                values[slot] = null;
                
                // 更新 ctrl 和计数
                if (hasEmpty) {
                    ctrl[slot] = CTRL_EMPTY;
                    used--;
                    growthLeft++;
                } else {
                    ctrl[slot] = CTRL_DELETED;
                    used--;
                    tomb++;
                }
                
                return oldValue;
            }
            match = clearLowestBit(match);
        }
        
        // Check for empty (early stop)
        if (matchEmpty(ctrlWord) != 0) {
            return null;
        }
        
        // Continue probing
        group = (group + stride) & groupMask;
        stride++;
    }
}
```

### 3.5 SwissTable.putDirect() - Split 专用

```java
/**
 * 直接插入，跳过查重。仅用于 split/rehash/grow 时迁移数据。
 * 假设: key 不存在，且 growthLeft > 0
 */
void putDirect(long hash, K key, N namespace, S value) {
    int h2Hash = h2(hash);
    int group = h1(hash) & groupMask;
    int stride = 1;
    
    while (true) {
        int base = group << 3;
        for (int j = 0; j < 8; j++) {
            int slot = base + j;
            if (ctrl[slot] == CTRL_EMPTY) {
                int keyIdx = slot << 1;
                keysNs[keyIdx] = key;
                keysNs[keyIdx + 1] = namespace;
                values[slot] = value;
                ctrl[slot] = (byte) h2Hash;
                used++;
                growthLeft--;
                return;
            }
        }
        group = (group + stride) & groupMask;
        stride++;
    }
}
```

### 3.6 ForL0StateMap.split() - 正确的分流逻辑

```java
/**
 * Split table 为两个新表
 * 
 * 关键点:
 * 1. 新表容量 = 旧表容量 (同容量分裂)
 * 2. 分流 bit 必须和 directory 路由一致
 * 3. 使用 oldTable.index 定位 directory 区间
 */
private void split(SwissTable<K, N, S> oldTable) {
    byte newLocalDepth = (byte)(oldTable.localDepth + 1);
    
    // 计算新表在 directory 中的索引
    // oldTable 覆盖的 directory 区间: [index, index + span)
    int span = 1 << (globalDepth - oldTable.localDepth);
    int leftIndex = oldTable.index;
    int rightIndex = oldTable.index + (span >>> 1);
    
    // 【关键修正】新表容量 = 旧表容量，不是 INITIAL_TABLE_CAPACITY
    SwissTable<K, N, S> left = new SwissTable<>(oldTable.capacity, newLocalDepth, leftIndex);
    SwissTable<K, N, S> right = new SwissTable<>(oldTable.capacity, newLocalDepth, rightIndex);
    
    // 【关键修正】分流 bit 必须和 directory 路由规则一致
    // directory 使用 hash 的高 globalDepth 位
    // split 时新增 1 位区分 left/right
    // 分流位 = globalShift + (globalDepth - newLocalDepth)
    //        = globalShift + (globalDepth - oldLocalDepth - 1)
    //        = globalShift - 1 + (globalDepth - oldLocalDepth)
    // 由于 globalDepth == oldLocalDepth (触发 split 的条件), 所以:
    //        = globalShift - 1
    // 但更通用的写法是:
    int splitShift = 64 - newLocalDepth;  // 等价于 globalShift + (globalDepth - newLocalDepth)
    
    // 遍历旧表，分发到新表
    for (int slot = 0; slot < oldTable.capacity; slot++) {
        if (isFull(oldTable.ctrl[slot])) {
            int keyIdx = slot << 1;
            @SuppressWarnings("unchecked")
            K key = (K) oldTable.keysNs[keyIdx];
            @SuppressWarnings("unchecked")
            N ns = (N) oldTable.keysNs[keyIdx + 1];
            @SuppressWarnings("unchecked")
            S value = (S) oldTable.values[slot];
            
            // 重新计算 hash
            long hash = computeHash(key, ns);
            
            // 取分流 bit
            int side = (int)((hash >>> splitShift) & 1L);
            
            SwissTable<K, N, S> target = (side == 0) ? left : right;
            target.putDirect(hash, key, ns, value);
        }
    }
    
    // 更新 directory
    // oldTable 覆盖 [index, index + span)
    // left 覆盖 [index, index + span/2)
    // right 覆盖 [index + span/2, index + span)
    int mid = oldTable.index + (span >>> 1);
    for (int i = oldTable.index; i < mid; i++) {
        directory[i] = left;
    }
    for (int i = mid; i < oldTable.index + span; i++) {
        directory[i] = right;
    }
    
    // 更新 tables 列表
    tables.remove(oldTable);
    tables.add(left);
    tables.add(right);
}

/**
 * 处理 Table 返回的 NEED_SPLIT
 * ForL0StateMap 作为 directory 管理者处理分裂
 */
private void handleSplit(SwissTable<K, N, S> t) {
    // 如果 localDepth == globalDepth，需要先扩展 directory
    if (t.localDepth == globalDepth) {
        growDirectory();
    }
    split(t);
}

/**
 * Directory 翻倍
 * 
 * 【关键】Go 的去重技巧：
 * 同一个 table 可能被多个 directory 槽位指向（别名），
 * 通过检查 t.index == i 确保每个 table 只更新一次：
 * - 遍历顺序是 i = 0, 1, 2, ...
 * - table.index 存储的是该 table 在 directory 中的第一个位置
 * - 当遍历到别名位置时，t.index != i（已被更新为 2*原始index），所以跳过
 */
private void growDirectory() {
    @SuppressWarnings("unchecked")
    SwissTable<K, N, S>[] newDir = new SwissTable[directory.length * 2];
    for (int i = 0; i < directory.length; i++) {
        SwissTable<K, N, S> t = directory[i];
        newDir[2 * i] = t;
        newDir[2 * i + 1] = t;
        // 【Go 风格去重】只在第一次遇到时更新 index
        if (t.index == i) {
            t.index = 2 * i;
        }
    }
    directory = newDir;
    globalDepth++;
    globalShift--;
}
```

### 3.7 ForL0StateMap StateMap 接口实现

```java
// ==================== StateMap 接口实现 ====================
// 签名对齐 reference/heap/StateMap.java

@Override
public int size() {
    return size;
}

@Override
public S get(K key, N namespace) {
    long hash = computeHash(key, namespace);
    SwissTable<K, N, S> t = locateTable(hash);
    return t.get(hash, key, namespace);
}

@Override
public boolean containsKey(K key, N namespace) {
    return get(key, namespace) != null;
}

@Override
public void put(K key, N namespace, S state) {
    // Namespace 去重优化
    if (namespace.equals(lastNamespace)) {
        namespace = lastNamespace;
    } else {
        lastNamespace = namespace;
    }
    
    long hash = computeHash(key, namespace);
    
    while (true) {
        SwissTable<K, N, S> t = locateTable(hash);
        int result = t.put(hash, key, namespace);
        
        if (result == SwissTable.NEED_SPLIT) {
            handleSplit(t);
            continue;  // 重试
        }
        
        int slot = result & SwissTable.SLOT_MASK;
        boolean isNew = (result & SwissTable.NEW_FLAG) != 0;
        
        t.values[slot] = state;
        if (isNew) {
            size++;
        }
        return;
    }
}

@Override
@SuppressWarnings("unchecked")
public S putAndGetOld(K key, N namespace, S state) {
    // Namespace 去重优化
    if (namespace.equals(lastNamespace)) {
        namespace = lastNamespace;
    } else {
        lastNamespace = namespace;
    }
    
    long hash = computeHash(key, namespace);
    
    while (true) {
        SwissTable<K, N, S> t = locateTable(hash);
        int result = t.put(hash, key, namespace);
        
        if (result == SwissTable.NEED_SPLIT) {
            handleSplit(t);
            continue;
        }
        
        int slot = result & SwissTable.SLOT_MASK;
        boolean isNew = (result & SwissTable.NEW_FLAG) != 0;
        
        S oldState = isNew ? null : (S) t.values[slot];
        t.values[slot] = state;
        if (isNew) {
            size++;
        }
        return oldState;
    }
}

@Override
public void remove(K key, N namespace) {
    long hash = computeHash(key, namespace);
    SwissTable<K, N, S> t = locateTable(hash);
    S old = t.remove(hash, key, namespace);
    if (old != null) {
        size--;
    }
}

@Override
public S removeAndGetOld(K key, N namespace) {
    long hash = computeHash(key, namespace);
    SwissTable<K, N, S> t = locateTable(hash);
    S old = t.remove(hash, key, namespace);
    if (old != null) {
        size--;
    }
    return old;
}

/**
 * transform - 单次探测完成读改写
 * 
 * 注意：transformation.apply() 可能抛出 Exception
 */
@Override
@SuppressWarnings("unchecked")
public <T> void transform(K key, N namespace, T value,
                          StateTransformationFunction<S, T> transformation) throws Exception {
    // Namespace 去重优化
    if (namespace.equals(lastNamespace)) {
        namespace = lastNamespace;
    } else {
        lastNamespace = namespace;
    }
    
    long hash = computeHash(key, namespace);
    
    while (true) {
        SwissTable<K, N, S> t = locateTable(hash);
        int result = t.put(hash, key, namespace);
        
        if (result == SwissTable.NEED_SPLIT) {
            handleSplit(t);
            continue;  // 重试
        }
        
        int slot = result & SwissTable.SLOT_MASK;
        boolean isNew = (result & SwissTable.NEW_FLAG) != 0;
        
        S oldState = isNew ? null : (S) t.values[slot];
        t.values[slot] = transformation.apply(oldState, value);
        
        if (isNew) {
            size++;
        }
        return;
    }
}

@Override
public Stream<K> getKeys(N namespace) {
    // 遍历所有 tables，过滤 namespace
    return tables.stream()
        .flatMap(t -> t.streamKeys(namespace));
}

@Override
public int sizeOfNamespace(Object namespace) {
    int count = 0;
    for (SwissTable<K, N, S> t : tables) {
        count += t.countNamespace(namespace);
    }
    return count;
}

@Nonnull
@Override
public StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot() {
    return new ForL0StateMapSnapshot<>(this);
}

@Override
public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
        int recommendedMaxNumberOfReturnedRecords) {
    // 实现增量访问器
    final int batchSize = Math.max(1, recommendedMaxNumberOfReturnedRecords);
    final Iterator<StateEntry<K, N, S>> iter = iterator();
    
    return new InternalKvState.StateIncrementalVisitor<K, N, S>() {
        @Override
        public java.util.List<StateEntry<K, N, S>> nextEntries() {
            java.util.ArrayList<StateEntry<K, N, S>> batch = new java.util.ArrayList<>(batchSize);
            int i = 0;
            while (i < batchSize && iter.hasNext()) {
                batch.add(iter.next());
                i++;
            }
            return batch;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public void remove(StateEntry<K, N, S> stateEntry) {
            ForL0StateMap.this.remove(stateEntry.getKey(), stateEntry.getNamespace());
        }

        @Override
        public void update(StateEntry<K, N, S> stateEntry, S newValue) {
            ForL0StateMap.this.put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
        }
    };
}
```

---

## 四、计数不变量

```java
// SwissTable 内部不变量
growthLeft = maxOcc(capacity) - used - tomb
maxOcc = capacity * 7 / 8

// 操作对计数的影响
┌─────────────────────┬───────────────────┬─────────────────┐
│ 操作                │ 计数变化          │ growthLeft 变化 │
├─────────────────────┼───────────────────┼─────────────────┤
│ 插入到 EMPTY        │ used++            │ growthLeft--    │
│ 复用 DELETED        │ used++, tomb--    │ 不变            │
│ 删除 → EMPTY        │ used--            │ growthLeft++    │
│ 删除 → DELETED      │ used--, tomb++    │ 不变            │
│ rehash/grow 后      │ tomb=0            │ = maxOcc - used │
└─────────────────────┴───────────────────┴─────────────────┘
```

---

## 五、改造步骤

### Phase 1: SwissTable 重构

1. **修改 SwissTable 数据结构**
   - 添加 `keysNs[]`, `values[]`, `tomb`, `index`
   - 修改构造函数为 `(slotCount, localDepth, index)`

2. **实现 SwissTable 核心方法**
   - `get(hash, key, ns)` → 返回 `S`
   - `put(hash, key, ns)` → 返回 slot 编码，循环结构
   - `remove(hash, key, ns)` → 返回旧值，维护 `tomb`
   - `putDirect(hash, key, ns, value)` → split 专用
   - `rehash()` → 原地重建，重置 `tomb=0`
   - `grow()` → 容量翻倍

### Phase 2: ForL0StateMap 重构

1. **合并 SwissMap 逻辑到 ForL0StateMap**
   - ForL0StateMap 直接管理 directory + SwissTable 存储
   - 移除原有 `keys[]`, `states[]`, `entryHash[]`, `freeList[]`
   - 新增 directory 路由逻辑

2. **实现核心方法**
   - `locateTable(hash)` 通过 hash 定位 SwissTable
   - `handleSplit(t)` 处理 NEED_SPLIT
   - `split(t)` 正确的分流逻辑，使用 `t.index`
   - `growDirectory()` 使用 Go 风格去重更新 `t.index`

3. **实现 StateMap 接口**
   - `get`, `put`, `putAndGetOld` 路由到 table
   - `remove`, `removeAndGetOld` 路由到 table
   - `transform` 单次探测完成读改写
   - `lastNamespace` 去重优化
   - `iterator`, `stateSnapshot` 遍历所有 tables

### Phase 3: 测试 & 验证

1. **单元测试**
   - SwissTable SWAR 匹配、增删改查
   - ForL0StateMap StateMap 接口合规性
   - split/growDirectory 边界条件

2. 运行全量测试

---

## 六、对比总结

| 方面 | 当前实现 | 改造后 |
|------|----------|--------|
| **架构层次** | ForL0StateMap → SwissMap → 存储 | ForL0StateMap (directory + 路由) → SwissTable (存储) |
| **类数量** | 3 层 | 2 层（去掉 SwissMap 中间层） |
| **职责划分** | 混乱，多层重复 | ForL0StateMap 路由, SwissTable 存储 |
| **Entry 存储** | 全局 `keys[]`, `states[]` | 每 Table 独立 `keysNs[]`, `values[]` |
| **slot→entry 映射** | slot→entryId→数据 | slot→数据 (直接索引) |
| **put 返回值** | entryId | slot 编码 (slot \| NEW_FLAG) |
| **删除管理** | freeList 回收 entryId | 标记 DELETED + tomb 计数 |
| **墓碑判断** | 无 (隐式) | O(1) 通过 `tomb > 0` |
| **split 新表容量** | ❌ INITIAL (错误) | ✅ oldTable.capacity |
| **分流 bit** | ❌ 绝对位置 (错误) | ✅ 对齐 directory 路由 |
| **table 定位** | ❌ 无 index | ✅ table.index (Go 风格去重) |
| **rehash/grow 重试** | ❌ 递归 | ✅ 循环 |
| **内存访问** | 多次间接 | 减少一层间接 |
| **代码复杂度** | 高（3层抽象） | 显著降低（2层清晰分工） |
