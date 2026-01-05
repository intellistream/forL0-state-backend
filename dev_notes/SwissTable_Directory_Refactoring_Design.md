# Swiss Table + Directory 重构方案

> 创建日期: 2026-01-04  
> 更新日期: 2026-01-04  
> 状态: **设计中**  
> 目标: 彻底替换 MainTable + L0Table，采用 Go 1.24 Swiss Tables 架构

---

## 1. 动机

当前 MainTable + L0Table 双层架构存在以下问题：

- **L0Table 命中率有限**: 热点缓存层收益不稳定
- **扩展机制复杂**: 树形扩展链导致探测路径长
- **缓存利用率低**: 开放寻址探测跨缓存行

Go 1.24 的 Swiss Tables 实现已证明其优越性能，核心优势：

- **SIMD 友好的控制字**: 8 字节控制字可并行匹配 8 个槽位
- **增量扩容**: Directory + 多 Table 架构避免全表 rehash
- **高负载因子**: 支持 7/8 (87.5%) 负载率

---

## 2. 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                         SwissMap                                 │
├─────────────────────────────────────────────────────────────────┤
│  used, seed, globalDepth, globalShift                           │
│  directory: SwissTable[]                                         │
│  tables: List<SwissTable>  (唯一表集合，用于 snapshot)           │
│  keys[], states[], entryHash[], freeList[]  (Entry 存储)        │
└─────────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
      ┌──────────┐      ┌──────────┐      ┌──────────┐
      │SwissTable│      │SwissTable│      │SwissTable│
      │ ctrl[]   │      │ ctrl[]   │      │ ctrl[]   │
      │ slots[]  │      │ slots[]  │      │ slots[]  │
      └──────────┘      └──────────┘      └──────────┘

SwissTable 内部布局 (扁平数组):
┌────────────────────────────────────────────────────────────────┐
│ ctrl[0..7] ctrl[8..15] ...  (byte[]，每 8 字节 = 1 group)      │
│ slots[0..7] slots[8..15] ... (int[]，每 group 8 个指针)        │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. 容量单位定义

为避免混淆，统一以下术语：

| 术语 | 定义 |
|------|------|
| `groupCount` | group 数量，必须是 2^N |
| `slotCount` | 槽位总数 = groupCount × 8 |
| `maxSlotCount` | 单表最大槽位数 = 1024 (即 maxGroupCount = 128) |
| `capacity` | 同 slotCount |

---

## 4. 核心数据结构

### 4.1 控制字节 (Control Byte)

```
每个 control byte (8 bits):
  empty:   0b1000_0000 (0x80)
  deleted: 0b1111_1110 (0xFE)  
  full:    0b0hhh_hhhh (H2, hash 低 7 位, 范围 0x00-0x7F)
```

### 4.2 Ctrl 存储方案

**选择 `byte[]` + Unsafe getLong 扫描**（而非 `long[]`）：

- **扫描**: `Unsafe.getLong(ctrl, BYTE_ARRAY_BASE + groupIdx * 8)` 一次加载 8 字节
- **更新**: 直接 `ctrl[slotIdx] = newByte`，无需 read-modify-write

```java
// 扫描 group
long ctrlWord = UNSAFE.getLong(ctrl, BYTE_ARRAY_BASE_OFFSET + (groupIdx << 3));
long match = matchH2(ctrlWord, h2);

// 更新单个 slot
ctrl[groupIdx * 8 + slotInGroup] = (byte) h2;
```

### 4.3 并行匹配算法 (Go bit trick + SWAR)

```java
private static final long LSB = 0x0101010101010101L;
private static final long MSB = 0x8080808080808080L;

// ctrl 编码：empty=0x80, deleted=0xFE, full=0x00..0x7F(H2)

// H2 精确匹配：SWAR 等值比较
static long matchH2(long ctrlWord, int h2 /*0..127*/) {
    long pattern = LSB * (h2 & 0xFFL);
    long x = ctrlWord ^ pattern;
    return (x - LSB) & ~x & MSB;
}

// empty 或 deleted：MSB=1 (bit trick)
static long matchEmptyOrDeleted(long ctrlWord) {
    return ctrlWord & MSB;
}

// full：MSB=0
static long matchFull(long ctrlWord) {
    return ~ctrlWord & MSB;
}

// empty：bit7=1 且 bit1=0 (bit trick)
static long matchEmpty(long ctrlWord) {
    return (ctrlWord & ~(ctrlWord << 6)) & MSB;
}

// deleted：bit7=1 且 bit1=1 (bit trick)
static long matchDeleted(long ctrlWord) {
    return (ctrlWord & (ctrlWord << 6)) & MSB;
}
```

**bitset 迭代方式**：

```java
while (mask != 0) {
    int tz = Long.numberOfTrailingZeros(mask);
    int lane = tz >>> 3;          // 0..7
    int slot = groupIdx * 8 + lane;
    // ... 处理 slot ...
    mask &= (mask - 1);           // 清最低 set bit
}
```

### 4.4 SwissTable

```java
class SwissTable {
    short used;          // FULL 槽位数
    short tomb;          // DELETED 槽位数
    short capacity;      // slotCount = groupCount * 8
    short growthLeft;    // 剩余可插入到 EMPTY 的预算
    int groupMask;       // groupCount - 1，用于 group 索引取模
    byte localDepth;     // 本表深度
    // 注意：不存储 directory index，因为一个 table 可被多个 directory 项指向
    
    byte[] ctrl;         // 控制字节数组，length = slotCount
    int[] slots;         // 槽位指针数组，length = slotCount，值为 entryId+1 (0 表示无效)
}
```

**构造时初始化**：

```java
SwissTable(int slotCount) {
    this.capacity = (short) slotCount;
    this.groupMask = (slotCount >>> 3) - 1;
    this.ctrl = new byte[slotCount];
    Arrays.fill(this.ctrl, CTRL_EMPTY);  // 必须初始化为 0x80
    this.slots = new int[slotCount];
    this.growthLeft = (short) (slotCount * 7 / 8);  // maxOcc
}
```

**ctrl 写入常量**：

```java
private static final byte CTRL_EMPTY   = (byte) 0x80;
private static final byte CTRL_DELETED = (byte) 0xFE;
// FULL: (byte) h2，范围 0x00..0x7F
```

### 4.5 SwissMap (顶层)

```java
class SwissMap<K, N, S> {
    long used;           // 总条目数
    long seed;           // hash seed
    int globalDepth;     // directory 位数
    int globalShift;     // 64 - globalDepth
    
    SwissTable[] directory;          // 2^globalDepth 个指针
    List<SwissTable> tables;         // 唯一表集合 (用于 snapshot 遍历)
    
    // Entry 存储
    Object[] keys;       // [K0, N0, K1, N1, ...]，entryId -> keys[entryId*2], keys[entryId*2+1]
    Object[] states;     // [S0, S1, ...]，entryId -> states[entryId]
    long[] entryHash;    // 完整 64-bit hash，仅用于 rehash/grow/split
    int[] freeList;      // 空闲 entryId 栈
    int freeCount;
    int nextEntryId;
}
```

---

## 5. 不变量与计数更新规则

### 5.1 计数定义

| 计数 | 含义 | 存储位置 |
|------|------|----------|
| `used` | FULL 槽位数 (有效条目) | SwissTable.used |
| `tomb` | DELETED 槽位数 (墓碑) | SwissTable.tomb |
| `capacity` | 槽位总数 | SwissTable.capacity |
| `growthLeft` | 剩余可插入到 EMPTY 的预算 | SwissTable.growthLeft |
| `maxOcc` | 最大占用数 = capacity × 7 / 8 | 计算值 |
| `occ` | 占用槽位数 = used + tomb | 计算值 |

**核心不变量**：

```java
t.growthLeft = maxOcc - t.used - t.tomb
```

> **实现策略**：采用增量维护（见 8.2/8.3）。

### 5.2 更新规则

| 操作 | ctrl 变化 | 计数变化 | growthLeft 变化 |
|------|-----------|----------|-----------------|
| 插入到 EMPTY | EMPTY → H2 | used++ | growthLeft-- |
| 复用 DELETED | DELETED → H2 | used++, tomb-- | 不变 |
| 删除 → EMPTY | H2 → EMPTY | used-- | growthLeft++ |
| 删除 → DELETED | H2 → DELETED | used--, tomb++ | 不变 |

### 5.3 触发条件

当需要使用 EMPTY 但 `growthLeft == 0` 时，按以下优先级：

| 优先级 | 条件 | 动作 |
|--------|------|------|
| 1 | `tomb > 0` | rehash same-capacity (清除墓碑) |
| 2 | `capacity < maxSlotCount` | grow (容量翻倍) |
| 3 | 其他 | split 成两个表 |

**rehash/grow/split 后重置**：

```java
tomb = 0;
growthLeft = maxOcc - used;  // 即 maxOcc - occ，因为 tomb=0
```

---

## 6. Slot 与 Ctrl 一致性约束

**核心约束**: `slots[i]` 与 `ctrl[i]` 必须一致

| ctrl[i] 状态 | slots[i] 要求 |
|--------------|---------------|
| EMPTY (0x80) | 必须为 0 |
| DELETED (0xFE) | 必须为 0 |
| FULL (H2) | 必须为 entryId + 1 (> 0) |

**删除操作**: 必须同时清除 `slots[i] = 0` 和设置 `ctrl[i] = EMPTY/DELETED`

**插入操作**: 必须同时设置 `slots[i] = entryId + 1` 和 `ctrl[i] = h2`

---

## 7. Hash 分割

```
64-bit hash:
┌────────────────────────────────────────────────────────────────┐
│  高位 (globalDepth bits): Directory 选择                        │
│  H1 (hash >>> 7): Group 探测                                    │
│  H2 (hash & 0x7F): 存入控制字节，用于并行匹配                     │
└────────────────────────────────────────────────────────────────┘
```

**具体计算式**：

```java
int h2 = (int)(hash & 0x7F);              // 低 7 bits
long h1 = hash >>> 7;                      // 高 57 bits
int group = (int)h1 & groupMask;           // group 索引
int dirIdx = (int)(hash >>> globalShift);  // directory 索引 (高 globalDepth bits)
```

---

## 8. 核心操作

### 8.1 Get

```
1. hash = hash(key, namespace)
2. dirIdx = (int)(hash >>> globalShift)
3. t = directory[dirIdx]                   
4. h2 = (int)(hash & 0x7F)
5. group = (int)(hash >>> 7) & t.groupMask, stride = 1
6. loop:
   - ctrlWord = UNSAFE.getLong(t.ctrl, BYTE_ARRAY_BASE_OFFSET + ((long)group << 3))
   - base = group << 3  // slot 起点
   
   - match = matchH2(ctrlWord, h2)
   - while match != 0:
       lane = Long.numberOfTrailingZeros(match) >>> 3
       slot = base + lane
       entryId = t.slots[slot] - 1
       if keys[entryId*2].equals(key) && keys[entryId*2+1].equals(namespace):
           return states[entryId]
       match &= (match - 1)
   
   - if matchEmpty(ctrlWord) != 0: return null  // 遇到 EMPTY 即停止
   - group = (group + stride) & t.groupMask; stride++
```

### 8.2 Put

```
1. 初始化:
   - hash = hash(key, namespace)
   - dirIdx = (int)(hash >>> globalShift)
   - t = directory[dirIdx]                   // 显式绑定 table
   - h2 = (int)(hash & 0x7F)
   - firstDeletedSlot = -1
   - group = (int)(hash >>> 7) & t.groupMask, stride = 1

2. Probe 循环:
   a. ctrlWord = UNSAFE.getLong(t.ctrl, BYTE_ARRAY_BASE_OFFSET + ((long)group << 3))
      base = group << 3
   
   b. 查找已存在的 key:
      - match = matchH2(ctrlWord, h2)
      - while match != 0:
          lane = Long.numberOfTrailingZeros(match) >>> 3
          slot = base + lane
          entryId = t.slots[slot] - 1
          if keys[entryId*2].equals(key) && keys[entryId*2+1].equals(namespace):
              states[entryId] = state  // 更新
              return
          match &= (match - 1)
   
   c. 记录第一个 DELETED 位置:
      - if firstDeletedSlot == -1:
          delMask = matchDeleted(ctrlWord)
          if delMask != 0:
              lane = Long.numberOfTrailingZeros(delMask) >>> 3
              firstDeletedSlot = base + lane
   
   d. 检查是否遇到 EMPTY (早停):
      - emptyMask = matchEmpty(ctrlWord)
      - if emptyMask != 0:
          firstEmptyLane = Long.numberOfTrailingZeros(emptyMask) >>> 3
          firstEmptySlot = base + firstEmptyLane
          break
   
   e. 继续探测:
      - group = (group + stride) & t.groupMask; stride++

3. 确定插入位置与是否需要扩容:
   - if firstDeletedSlot != -1:
       insertSlot = firstDeletedSlot
       useDeleted = true
   - else:
       insertSlot = firstEmptySlot
       useDeleted = false
       // 使用 EMPTY 需检查预算
       if t.growthLeft == 0:
           rehashOrGrowOrSplit(t, dirIdx, hash)  // 见 5.3 触发条件
           重新执行 put (从头开始，因为 t 可能已变)
           return

4. 执行插入:
   - entryId = allocateEntry(key, namespace, state, hash)
   - t.slots[insertSlot] = entryId + 1
   - t.ctrl[insertSlot] = (byte) h2
   - 更新计数 (增量维护):
       t.used++
       map.used++  // 全局总数
       if useDeleted:
           t.tomb--         // t.growthLeft 不变
       else:
           t.growthLeft--   // 消耗预算
```

### 8.3 Delete

```
1. 查找 slot (同 Get 流程，获得 t, group, slot, entryId)

2. 找到后，在修改前读取 group 的 ctrlWord:
   - ctrlWord = UNSAFE.getLong(t.ctrl, BYTE_ARRAY_BASE_OFFSET + ((long)group << 3))
   - hasEmpty = matchEmpty(ctrlWord) != 0  // 基于删除前判断！

3. 清除 entry:
   - keys[entryId*2] = null
   - keys[entryId*2+1] = null
   - states[entryId] = null
   - freeList[freeCount++] = entryId

4. 清除 slot:
   - t.slots[slot] = 0

5. 更新 ctrl 和计数 (基于删除前的 hasEmpty，增量维护):
   - if hasEmpty:
       t.ctrl[slot] = CTRL_EMPTY   // group 中有 empty，可直接复用
       t.used--
       t.growthLeft++              // 释放预算
   - else:
       t.ctrl[slot] = CTRL_DELETED // group 全满，需保持探测链
       t.used--
       t.tomb++                    // t.growthLeft 不变
   - map.used--  // 全局总数
```

> **重要**：必须在修改 ctrl 前读取 ctrlWord 判断 hasEmpty，否则会出现"删完自己变空导致误判"。

---

## 9. 扩容策略

### 9.1 Rehash Same-Capacity (清除墓碑)

当 `growthLeft == 0` 且 `tomb > 0`:

1. 分配新的 ctrl[] 和 slots[]:
   ```java
   byte[] newCtrl = new byte[capacity];
   Arrays.fill(newCtrl, CTRL_EMPTY);  // 必须 fill 0x80
   int[] newSlots = new int[capacity];
   ```
2. 遍历旧 slots 的 FULL 项，用 `entryHash[entryId]` 重新插入
3. 重置计数:
   ```java
   tomb = 0;
   growthLeft = maxOcc - used;
   ```

**关键**: 使用 `entryHash`，不重新调用 `hashCode()`

### 9.2 Grow (容量翻倍)

当 `growthLeft == 0` 且 `tomb == 0` 且 `capacity < maxSlotCount`:

1. 新 groupCount = 旧 groupCount × 2
2. 分配新 ctrl[] 和 slots[]:
   ```java
   int newCapacity = capacity * 2;
   byte[] newCtrl = new byte[newCapacity];
   Arrays.fill(newCtrl, CTRL_EMPTY);  // 必须 fill 0x80
   int[] newSlots = new int[newCapacity];
   ```
3. 用 `entryHash` 重新插入所有 FULL 项
4. 重置计数:
   ```java
   tomb = 0;
   growthLeft = newMaxOcc - used;
   ```

### 9.3 Split (表分裂)

当 `capacity >= maxSlotCount` 且需要扩容:

1. 创建左表和右表:
   - `newLocalDepth = oldTable.localDepth + 1`
   - left.localDepth = right.localDepth = newLocalDepth

2. 遍历旧表 FULL 项，按 hash 前缀分流:
   ```java
   int bitPos = 64 - newLocalDepth;  // 分流 bit 位置
   for (int i = 0; i < oldTable.capacity; i++) {
       if ((oldTable.ctrl[i] & 0x80) == 0) {  // FULL
           int entryId = oldTable.slots[i] - 1;
           long hash = entryHash[entryId];
           int side = (int)((hash >>> bitPos) & 1L);  // 0->left, 1->right
           if (side == 0) {
               insertInto(left, hash, entryId);
           } else {
               insertInto(right, hash, entryId);
           }
       }
   }
   ```

3. 调用 installSplit 更新 directory

### 9.4 Directory 更新公式

Split 时传入任意一个指向该 table 的 `dirIdx`、`oldTable` 和 **split 前的** `oldLocalDepth`：

```java
void installSplit(int dirIdx, SwissTable oldTable, int oldLocalDepth,
                  SwissTable left, SwissTable right) {
    // 使用 split 前的 localDepth 计算 span
    int spanOld = 1 << (globalDepth - oldLocalDepth);
    int start = (dirIdx / spanOld) * spanOld;  // 起始 index
    int mid = start + (spanOld >>> 1);         // 中点
    
    // 更新 directory
    for (int i = start; i < mid; i++) directory[i] = left;
    for (int i = mid; i < start + spanOld; i++) directory[i] = right;
    
    // 更新 tables 列表
    tables.remove(oldTable);
    tables.add(left);
    tables.add(right);
}
```

**调用方式**：

```java
int oldLocalDepth = oldTable.localDepth;  // 保存 split 前的值
// ... 创建 left, right (localDepth = oldLocalDepth + 1) ...
installSplit(dirIdx, oldTable, oldLocalDepth, left, right);
```

### 9.5 Directory 扩展

当 split 时 `oldTable.localDepth == globalDepth`:

```java
void growDirectory() {
    SwissTable[] newDir = new SwissTable[directory.length * 2];
    for (int i = 0; i < directory.length; i++) {
        newDir[2 * i] = directory[i];
        newDir[2 * i + 1] = directory[i];
    }
    directory = newDir;
    globalDepth++;
    globalShift--;
}
```

**growDirectory + split 的调用顺序**：

```java
void rehashOrGrowOrSplit(SwissTable t, int dirIdx, long hash) {
    if (t.tomb > 0) {
        rehashSameCapacity(t);
    } else if (t.capacity < maxSlotCount) {
        grow(t);
    } else {
        // split
        int oldLocalDepth = t.localDepth;
        if (oldLocalDepth == globalDepth) {
            growDirectory();
            // grow 后 globalShift 已变，重算 dirIdx
            dirIdx = (int)(hash >>> globalShift);
        }
        SwissTable left = new SwissTable(...);
        SwissTable right = new SwissTable(...);
        // ... 分流 entries ...
        installSplit(dirIdx, t, oldLocalDepth, left, right);
    }
}
```

> **关键**：`growDirectory()` 后 `globalShift--` 已变，重算 `dirIdx` 永远正确。

---

## 10. 探测序列

采用**二次探测** (Quadratic Probing)，直接内联在循环中：

```java
int offset = (int)(h1(hash) & groupMask);
int stride = 1;

while (true) {
    long ctrlWord = UNSAFE.getLong(ctrl, BYTE_ARRAY_BASE_OFFSET + (offset << 3));
    // ... 匹配逻辑 ...
    
    offset = (offset + stride) & groupMask;
    stride++;
}
```

由于 groupCount 是 2 的幂，二次探测保证访问所有 group。

---

## 11. Snapshot 遍历策略

### 11.1 遍历方式

使用 `tables` 列表（唯一表集合）避免重复遍历：

```java
Iterator<Entry<K, N, S>> snapshotIterator() {
    for (SwissTable table : tables) {
        for (int i = 0; i < table.capacity; i++) {
            if ((table.ctrl[i] & 0x80) == 0) {  // FULL
                int entryId = table.slots[i] - 1;
                yield (keys[entryId*2], keys[entryId*2+1], states[entryId]);
            }
        }
    }
}
```

### 11.2 并发语义

**策略: 阻塞写线程**

- Flink 状态访问是单线程的，snapshot 时暂停写入
- 不实现 copy-on-write（复杂度高，收益有限）
- 如后续需要异步快照，可考虑版本化或 COW

---

## 12. 与 Flink 集成

### 12.1 ForL0StateMap 改造

```java
public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> {
    private final SwissMap<K, N, S> map;
    
    public S get(K key, N namespace) { return map.get(key, namespace); }
    public void put(K key, N namespace, S state) { map.put(key, namespace, state); }
    public void remove(K key, N namespace) { map.remove(key, namespace); }
    public Iterator<StateEntry<K,N,S>> iterator() { return map.snapshotIterator(); }
}
```

### 12.2 移除的组件

- `MainTable` - 完全移除
- `L0Table` - 完全移除
- `L0MemoryAllocator` - 不再需要

### 12.3 保留的接口

- `StateMap<K, N, S>` 抽象类
- `ForL0StateMapSnapshot` (适配新遍历方式)
- `ForL0KeyedStateBackend`

---

## 13. 性能预期

| 指标 | 当前架构 | Swiss Table |
|------|----------|-------------|
| 单次查找内存访问 | 2-3 次 (L0 + Main) | 1-2 次 (ctrl + slot) |
| 负载因子 | ~60% | 87.5% |
| 扩容开销 | 全表 rehash | 单表 split |
| equals 调用 | 每次候选 | 先比 entryHash 再 equals |

---

## 14. 实施步骤

### 验收标准

全部测试通过：`mvn test` 无失败

### Phase 1: 实现核心数据结构

**目标**: 实现独立可测试的 SwissTable 和 SwissMap

**Step 1.1**: 创建 `SwissTable.java`
```
src/main/java/org/apache/flink/runtime/state/heap/SwissTable.java
```
- 字段: used, tomb, capacity, growthLeft, groupMask, localDepth, ctrl[], slots[]
- 构造函数: Arrays.fill(ctrl, CTRL_EMPTY)
- 静态方法: matchH2(), matchEmpty(), matchDeleted(), matchEmptyOrDeleted()

**Step 1.2**: 创建 `SwissMap.java`
```
src/main/java/org/apache/flink/runtime/state/heap/SwissMap.java
```
- 字段: used, seed, globalDepth, globalShift, directory[], tables, keys[], states[], entryHash[], freeList[]
- 核心方法: get(), put(), remove()
- 扩容方法: rehashSameCapacity(), grow(), split(), installSplit(), growDirectory()

**Step 1.3**: 创建单元测试 `SwissMapTest.java`
```
src/test/java/org/apache/flink/runtime/state/heap/SwissMapTest.java
```
- 基础 CRUD 测试
- 扩容触发测试 (rehash/grow/split)
- 边界条件测试

**验证点**: `mvn test -Dtest=SwissMapTest` 通过

---

### Phase 2: 改造 ForL0StateMap

**目标**: 用 SwissMap 替换 MainTable，保持 API 不变

**Step 2.1**: 修改 `ForL0StateMap.java`
- 删除 `MainTable<K, N, S> mainTable` 字段
- 新增 `SwissMap<K, N, S> map` 字段
- 重写 get/put/remove/size/iterator 方法委托到 SwissMap
- 简化构造函数（移除 L0 相关参数，保留签名兼容）

**Step 2.2**: 修改 `ForL0StateMapSnapshot.java`
- 适配 SwissMap 的遍历方式 (tables 列表)

**Step 2.3**: 运行现有测试
```bash
mvn test -Dtest=ForL0StateMapTest
mvn test -Dtest=ForL0StateMapStressTest
```

**验证点**: ForL0StateMap 相关测试全部通过

---

### Phase 3: 清理与集成

**目标**: 移除旧代码，确保全量测试通过

**Step 3.1**: 删除废弃文件
- `MainTable.java`
- `L0Table.java`
- `L0TableMetricsCollector.java`

**Step 3.2**: 更新依赖组件
- `ForL0KeyedStateBackend.java`: 移除 L0 相关配置
- `ForL0StateBackendConfig.java`: 简化配置项
- `ForL0StateBackendOptions.java`: 移除 L0 相关选项

**Step 3.3**: 删除/更新废弃测试
- 删除 `MainTableTest.java`
- 删除 `MainTableStressTest.java`
- 更新 `ForL0StateBackendOptionsTest.java`

**Step 3.4**: 全量测试
```bash
mvn clean test
```

**验证点**: `mvn test` 全部通过，无编译错误

---

### Phase 4: Benchmark 清理

**目标**: 移除 L0 统计相关代码，简化 benchmark 配置

**Step 4.1**: 删除 L0 指标工具
- 删除 `benchmark/scripts/utils/l0_metrics.py`
- 删除 `benchmark/results/l0metrics/` 目录

**Step 4.2**: 更新 benchmark 脚本
- `benchmark/scripts/run_wordcount.py`: 移除 L0 指标采集逻辑
- `benchmark/scripts/clean_results.py`: 移除 l0metrics 目录清理
- `benchmark/scripts/generate_report.py`: 移除 L0 统计卡片生成

**Step 4.3**: 更新配置文件
- `benchmark/config/benchmark.yaml`: 
  - 移除 `l0_cache_enabled`, `l0_cache_size`, `l0_cache_replacement_policy`, `l0_memory_max_size` 配置
  - 移除 `collect_l0_stats` 配置
  - 更新 forl0 backend description

**Step 4.4**: 更新文档
- `benchmark/README.md`: 移除 L0Table 指标采集章节，更新配置说明

**验证点**: benchmark 脚本可正常运行（无 import 错误）

---

### 文件变更清单

#### 主代码 (src/)

| 操作 | 文件 |
|------|------|
| 新增 | `SwissTable.java` |
| 新增 | `SwissMap.java` |
| 新增 | `SwissMapTest.java` |
| 修改 | `ForL0StateMap.java` |
| 修改 | `ForL0StateMapSnapshot.java` |
| 修改 | `ForL0KeyedStateBackend.java` |
| 修改 | `ForL0StateBackendConfig.java` |
| 修改 | `ForL0StateBackendOptions.java` |
| 修改 | `ForL0StateBackendOptionsTest.java` |
| 删除 | `MainTable.java` |
| 删除 | `L0Table.java` |
| 删除 | `L0TableMetricsCollector.java` |
| 删除 | `MainTableTest.java` |
| 删除 | `MainTableStressTest.java` |

> **保留**: `space/` 目录下的 `L0MemoryAllocator.java`, `NativeL0MemoryAllocator.java`, `NativeL0Memory.java` 及其测试保留，供后续使用。

#### Benchmark (benchmark/)

| 操作 | 文件 |
|------|------|
| 修改 | `config/benchmark.yaml` |
| 修改 | `scripts/run_wordcount.py` |
| 修改 | `scripts/clean_results.py` |
| 修改 | `scripts/generate_report.py` |
| 修改 | `README.md` |
| 删除 | `scripts/utils/l0_metrics.py` |
| 删除 | `results/l0metrics/` |

---

## 15. 参考

- Go 源码: `reference/go_maps/` (map.go, table.go, group.go)
- Abseil Swiss Tables: https://abseil.io/about/design/swisstables
