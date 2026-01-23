# L0 Hot Key Cache 设计方案

> 文档版本: 2.0  
> 创建日期: 2026-01-22  
> 更新日期: 2026-01-22  
> 状态: **设计中**  
> 目标: 利用 L0 Cache 加速热键状态访问（同时支持 VoidNamespace 和 General Namespace）

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [设计目标与约束](#2-设计目标与约束)
3. [整体架构](#3-整体架构)
4. [L0HotKeyCache 详细设计](#4-l0hotkeycache-详细设计)
5. [集成方案](#5-集成方案)
6. [配置与调优](#6-配置与调优)
7. [性能分析](#7-性能分析)
8. [实施计划](#8-实施计划)

---

## 1. 背景与动机

### 1.1 L0 Cache 特性

L0 Cache 是鲲鹏服务器提供的用户可控 LLC（Last Level Cache）：

| 特性 | 说明 |
|------|------|
| 访问延迟 | 接近 L3 Cache，远低于 DRAM |
| 容量 | 有限，通常 64MB - 256MB |
| 访问方式 | 通过 `/dev/hisi_l0` 分配，之后作为堆外内存使用 |
| JNI 开销 | 仅分配时需要 JNI，后续通过 Unsafe 直接访问 |

### 1.2 Namespace 使用分析

基于 Nexmark 查询分析，**两种模式各占 50%**：

| 模式 | 查询示例 | 特点 |
|------|----------|------|
| **VoidNamespace** | q4, q9, q18, q19, q20 | 非窗口状态，只需 key 匹配 |
| **General Namespace** | q5, q7, q8, q11, q12 | 窗口状态，需要 key + namespace 都匹配 |

**关键洞察**：L0Cache 必须同时支持两种模式，否则只能加速一半场景。

### 1.3 当前 SwissTable 架构

```
ForL0StateStore<K, N, S>
├── VoidNamespace 模式: SwissTable<K,S>[] tables
│   └── 直接访问，key 唯一确定 slot
│
└── General Namespace 模式: Map<N, SwissTable<K,S>>[] namespaceMaps
    └── 每个 namespace 独立的 SwissTable
        └── 同一 key 在不同 namespace 有不同 slot

SwissTable<K, S>
├── ctrl[]      byte[]     控制字节，用于 SWAR 并行匹配
├── entries[]   Object[]   AoS 布局 [k0,v0,k1,v1,...]
└── hashes[]    int[]      32 位 hash 存储
```

**问题**：
- L0 容量有限，无法容纳所有 SwissTable 的 ctrl[]
- General Namespace 模式下，同一 key 在不同 window 有不同的 SwissTable 和 slot
- 运行时有多个 StateStore，每个 StateStore 有多个 KeyGroup

### 1.4 核心洞察

流处理中数据分布通常符合 **Zipf 分布**：少数热键占据大部分访问。

```
访问频率
  │
  │  ████
  │  ████ ██
  │  ████ ██ █
  │  ████ ██ █ █ █ │ . . . . . . . .
  └────────────────────────────────── keys
     热键(~1%)  冷键（大多数）
```

对于**窗口状态**，还有**时间局部性**：当前活跃窗口是热点。

```
时间 ──────────────────────────────────►
      [window-1] [window-2] [window-3] ...
         冷        冷         热(当前)
```

**策略**：不缓存整个 SwissTable，只缓存**热键+热窗口的直接定位信息**

---

## 2. 设计目标与约束

### 2.1 设计目标

1. **加速热键访问**：热键命中时跳过 SwissTable 的 SWAR probe
2. **同时支持两种模式**：VoidNamespace 和 General Namespace
3. **固定内存占用**：L0 Cache 容量固定，不随状态增长
4. **自适应淘汰**：冷键/冷窗口自然被热键/热窗口覆盖
5. **零配置可用**：合理默认值，无需用户调优

### 2.2 设计约束

| 约束 | 说明 |
|------|------|
| **单线程** | 每个 Subtask 独立的 L0Cache，无并发访问 |
| **每 Backend 一个** | 不做 TaskManager 级别共享，保持简单 |
| **L0 容量有限** | 典型 16MB = 1M entries（每 entry 16 bytes）|
| **不引入序列化** | 只存索引信息，不存储实际数据 |

### 2.3 非目标

- 不替代 SwissTable，只作为加速层
- 不保证一致性（L0 缓存可以随时失效，SwissTable 是 source of truth）
- 不处理 Checkpoint（L0 缓存是瞬态的）

---

## 3. 整体架构

### 3.1 核心问题：如何识别唯一状态？

```
状态唯一标识 = (storeId, keyGroup, namespace, key)
                  │         │          │        │
                  │         │          │        └── hash 可能冲突
                  │         │          └── General Namespace 模式必须匹配
                  │         └── 已知（从 key 计算）
                  └── 已知
```

**挑战**：
- VoidNamespace：只需验证 key 相等
- General Namespace：需要验证 namespace + key 都相等
- L0 只能存固定大小的 metadata，无法直接比较 Object

### 3.2 设计决策：Composite Hash

**核心思想**：把 namespace 信息编码进 hash

```java
// VoidNamespace 模式（isVoidNamespace = true）
int compositeHash = keyHash;  // 直接使用 key hash

// General Namespace 模式（isVoidNamespace = false）
int compositeHash = keyHash ^ (namespaceHash * 0x9e3779b9);  // 组合 hash
```

**优势**：
- 统一的 slot 格式，无需区分模式
- namespace 冲突的概率极低（不同 window 的 hash 不同）
- 热窗口的 entries 自然覆盖冷窗口

**验证流程**：
1. L0 查找：用 compositeHash 定位 slot
2. 元数据匹配：验证 storedHash == compositeHash && storeId 匹配 && kgIdx 匹配
3. SwissTable 定位：用 tableSlot 直接访问 entries[]
4. **Key 验证**：比较 entries[slot*2] 是否 equals(key)
5. **Namespace 验证**：General Namespace 模式额外比较 namespace

### 3.3 架构图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      ForL0KeyedStateBackend (per Subtask)                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │                     L0HotKeyCache (L0 内存)                              │   │
│   │   容量: 1M entries = 16MB (每 entry 16 bytes)                            │   │
│   │                                                                          │   │
│   │   Entry 格式 (16 bytes):                                                 │   │
│   │   ┌────────────────────────────────────────────────────────────────────┐│   │
│   │   │ compositeHash:32 │ storeId:8 │ kgIdx:8 │ tableSlot:16 │ nsRef:32   ││   │
│   │   └────────────────────────────────────────────────────────────────────┘│   │
│   │                                                                          │   │
│   │   - compositeHash: keyHash 或 keyHash^namespaceHash                      │   │
│   │   - nsRef: namespace 对象的 identity hash (用于快速排除)                 │   │
│   │                                                                          │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│          ▲                    ▲                    ▲                            │
│          │                    │                    │                            │
│   ┌──────┴─────────┐  ┌───────┴────────┐  ┌───────┴────────┐                   │
│   │ ForL0StateStore│  │ ForL0StateStore│  │ ForL0StateStore│                   │
│   │  "valueState"  │  │  "windowState" │  │  "mapState"    │                   │
│   │   storeId=0    │  │   storeId=1    │  │   storeId=2    │                   │
│   │  VoidNamespace │  │ GeneralNamespace│ │  VoidNamespace │                   │
│   └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                   │
│           │                   │                   │                             │
│           ▼                   ▼                   ▼                             │
│   ┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐               │
│   │SwissTable[] │     │Map<N,SwissTable>[]  │     │SwissTable[] │               │
│   │  直接索引   │     │ HashMap 路由        │     │  直接索引   │               │
│   └─────────────┘     └─────────────────────┘     └─────────────┘               │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.4 General Namespace 模式的 SwissTable 定位

**问题**：General Namespace 模式下，每个 namespace 有独立的 SwissTable。L0Cache 如何定位正确的 SwissTable？

**方案**：存储 `(storeId, kgIdx, tableSlot, nsRef)`

```
L0 Entry (16 bytes):
┌─────────────────────────────────────────────────────────────────────────────┐
│  compositeHash:32  │  storeId:8  │  kgIdx:8  │  tableSlot:16  │  nsRef:32   │
└─────────────────────────────────────────────────────────────────────────────┘
         │                │            │              │               │
         │                │            │              │               └── System.identityHashCode(namespace)
         │                │            │              └── slot in the specific SwissTable
         │                │            └── keyGroup index within range
         │                └── StateStore ID
         └── keyHash ^ namespaceHash (or just keyHash for VoidNamespace)
```

**查找流程（General Namespace）**：

```java
// 1. 计算 composite hash
int keyHash = computeKeyHash(key);
int nsHash = namespace.hashCode();
int compositeHash = keyHash ^ (nsHash * 0x9e3779b9);

// 2. L0 查找
int l0Index = compositeHash & (l0Capacity - 1);
long entry = l0Cache.getEntry(l0Index);

// 3. 快速验证
int storedHash = (int)(entry >>> 32);
if (storedHash != compositeHash) {
    return MISS; // hash 不匹配
}

int storedNsRef = (int)(entry2 >>> 32); // 从第二个 long 读取
if (storedNsRef != System.identityHashCode(namespace)) {
    return MISS; // namespace identity 不匹配（快速排除）
}

// 4. 提取定位信息
int storeId = (int)((entry >>> 24) & 0xFF);
int kgIdx = (int)((entry >>> 16) & 0xFF);
int tableSlot = (int)(entry & 0xFFFF);

// 5. 定位 SwissTable 并验证
Map<N, SwissTable> nsMap = namespaceMaps[kgIdx];
SwissTable table = nsMap.get(namespace);  // HashMap lookup 仍然需要
if (table == null) {
    return MISS;
}

// 6. 直接访问 slot
Object storedKey = table.entries[tableSlot << 1];
if (!key.equals(storedKey)) {
    return MISS; // key 不匹配（hash 冲突）
}

return table.entries[(tableSlot << 1) + 1]; // HIT!
```

**注意**：General Namespace 模式仍需要 `HashMap.get(namespace)` 来获取正确的 SwissTable。L0Cache 的价值在于：
1. 快速排除不匹配（通过 compositeHash + nsRef）
2. 跳过 SWAR probe（直接用 tableSlot）

### 3.5 VoidNamespace 模式的优化

VoidNamespace 模式更简单，不需要 nsRef：

```java
// 1. 计算 hash（不需要 namespace）
int compositeHash = computeKeyHash(key);

// 2. L0 查找
int l0Index = compositeHash & (l0Capacity - 1);
long entry = l0Cache.getEntry(l0Index);

// 3. 快速验证
int storedHash = (int)(entry >>> 32);
if (storedHash != compositeHash) {
    return MISS;
}

// 4. 提取定位信息
int storeId = (int)((entry >>> 24) & 0xFF);
int kgIdx = (int)((entry >>> 16) & 0xFF);
int tableSlot = (int)(entry & 0xFFFF);

// 5. 直接访问（无 HashMap lookup）
SwissTable table = tables[kgIdx];
Object storedKey = table.entries[tableSlot << 1];
if (!key.equals(storedKey)) {
    return MISS;
}

return table.entries[(tableSlot << 1) + 1]; // HIT!
```

### 3.6 数据流对比

```
                VoidNamespace                    General Namespace
                     │                                  │
                     ▼                                  ▼
         ┌───────────────────┐              ┌───────────────────┐
         │ hash = keyHash    │              │ hash = keyHash ^  │
         │                   │              │   nsHash * MAGIC  │
         └─────────┬─────────┘              └─────────┬─────────┘
                   │                                  │
                   └──────────────┬───────────────────┘
                                  ▼
                   ┌─────────────────────────┐
                   │ L0 lookup: hash & mask  │
                   └────────────┬────────────┘
                                │
                   ┌────────────┴────────────┐
                   │                         │
              Entry Match              Entry Mismatch
                   │                         │
                   ▼                         ▼
         ┌─────────────────┐        ┌─────────────────┐
         │ Extract slot    │        │ SwissTable.get()│
         │ Verify key      │        │ SWAR probe      │
         │ (+ verify ns    │        │ Update L0Cache  │
         │  if general ns) │        └────────┬────────┘
         └────────┬────────┘                 │
                  │                          │
                  └──────────┬───────────────┘
                             ▼
                        Return value
```

---

## 4. L0HotKeyCache 详细设计

### 4.1 Entry 格式（16 bytes）

为了同时支持 VoidNamespace 和 General Namespace，采用 16 bytes entry：

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                         L0 Cache Entry (16 bytes)                             │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                                │
│   Word 0 (bytes 0-7):                                                          │
│   63                              32 31     24 23     16 15                 0  │
│   ┌────────────────────────────────┬─────────┬─────────┬────────────────────┐  │
│   │     compositeHash (32 bits)    │ storeId │  kgIdx  │   tableSlot (16)   │  │
│   │  keyHash ^ nsHash*MAGIC        │ (8 bits)│ (8 bits)│   SwissTable slot  │  │
│   └────────────────────────────────┴─────────┴─────────┴────────────────────┘  │
│                                                                                │
│   Word 1 (bytes 8-15):                                                         │
│   63                              32 31                                     0  │
│   ┌────────────────────────────────┬────────────────────────────────────────┐  │
│   │     nsIdentityHash (32 bits)   │            reserved (32 bits)          │  │
│   │  System.identityHashCode(ns)   │            (未来扩展用)                 │  │
│   └────────────────────────────────┴────────────────────────────────────────┘  │
│                                                                                │
│   - compositeHash: keyHash（VoidNS）或 keyHash ^ namespaceHash*MAGIC（General）│
│   - storeId:       StateStore 编号 (0-255)                                     │
│   - kgIdx:         KeyGroup 在 range 内的索引 (0-255)                          │
│   - tableSlot:     SwissTable 中的 slot 索引 (0-65535)                         │
│   - nsIdentityHash: namespace 对象的 identity hash（快速排除用）               │
│                     VoidNamespace 时为 0                                       │
│                                                                                │
│   空 entry: word0 == 0（compositeHash 几乎不可能全为 0）                        │
│                                                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Composite Hash 计算

```java
// 常量：黄金比例相关的魔数，用于混合 hash
private static final int NS_HASH_MAGIC = 0x9e3779b9;

/**
 * 计算 compositeHash。
 * VoidNamespace 模式：直接返回 keyHash
 * General Namespace 模式：keyHash 与 namespaceHash 组合
 */
static int computeCompositeHash(int keyHash, Object namespace, boolean isVoidNamespace) {
    if (isVoidNamespace) {
        return keyHash;
    }
    // 组合 key hash 和 namespace hash
    // 使用乘法 + XOR 获得更好的分布
    int nsHash = namespace.hashCode();
    return keyHash ^ (nsHash * NS_HASH_MAGIC);
}
```

### 4.3 容量限制

| 字段 | 位数 | 最大值 | 说明 |
|------|------|--------|------|
| compositeHash | 32 | 2^32 | 组合 hash，用于快速验证 |
| storeId | 8 | 255 | 每个 Backend 最多 256 个 StateStore |
| kgIdx | 8 | 255 | KeyGroup range 内最多 256 个 KeyGroup |
| tableSlot | 16 | 65535 | 每个 SwissTable 最多 64K entries |
| nsIdentityHash | 32 | 2^32 | namespace 对象的 identity hash |

**注意**：tableSlot 16 位限制单个 SwissTable 最大 64K entries。如果 SwissTable 超过此大小，该表的热键将无法缓存到 L0。这是可接受的，因为：
1. 大表意味着 key 分布分散，热键效应减弱
2. 大表的 SWAR probe 效率依然很高

### 4.4 索引方式

采用**直接映射**（Direct-Mapped），每个 entry 16 bytes：

```java
int l0Index = compositeHash & (capacity - 1);
long entryOffset = (long)l0Index << 4;  // * 16 bytes per entry
```

- 简单高效，无需复杂的冲突处理
- 冲突时后来者覆盖，自然实现 LRU-like 效果
- 热键/热窗口访问频繁，会持续"占据"对应 slot
- 冷窗口（已过期的 window）自然被新窗口覆盖

### 4.5 类定义

```java
package org.apache.flink.state.forl0;

import org.apache.flink.state.forl0.space.NativeL0Memory;
import org.apache.flink.state.forl0.utils.UnsafeAccess;
import sun.misc.Unsafe;

/**
 * L0 Hot Key Cache - 基于 L0 内存的热键直接映射缓存。
 * 
 * <p>用于加速热键访问，命中时跳过 SwissTable 的 SWAR probe 过程，
 * 直接定位到 entries[] 中的 slot。
 * 
 * <p>同时支持 VoidNamespace 和 General Namespace 模式：
 * <ul>
 *   <li>VoidNamespace: compositeHash = keyHash</li>
 *   <li>General Namespace: compositeHash = keyHash ^ nsHash*MAGIC</li>
 * </ul>
 * 
 * <p>特性：
 * <ul>
 *   <li>固定容量，直接映射（无冲突链）</li>
 *   <li>冲突时后来者覆盖，自然淘汰冷键/冷窗口</li>
 *   <li>单线程访问，无需同步</li>
 *   <li>L0 缓存是瞬态的，不参与 Checkpoint</li>
 * </ul>
 * 
 * <p>Entry 格式 (16 bytes):
 * <pre>
 * Word 0: [compositeHash:32 | storeId:8 | kgIdx:8 | tableSlot:16]
 * Word 1: [nsIdentityHash:32 | reserved:32]
 * </pre>
 */
public class L0HotKeyCache {
    
    private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
    
    // Entry 布局常量
    private static final int ENTRY_SIZE = 16;  // bytes
    private static final int ENTRY_SHIFT = 4;  // log2(16)
    
    // Word 0 布局
    private static final int COMPOSITE_HASH_SHIFT = 32;
    private static final int STORE_ID_SHIFT = 24;
    private static final int KG_IDX_SHIFT = 16;
    private static final long TABLE_SLOT_MASK = 0xFFFFL;
    private static final long KG_IDX_MASK = 0xFFL;
    private static final long STORE_ID_MASK = 0xFFL;
    
    // Word 1 布局
    private static final int NS_IDENTITY_HASH_SHIFT = 32;
    
    // Composite hash 魔数
    private static final int NS_HASH_MAGIC = 0x9e3779b9;
    
    // L0 内存地址
    private final long l0Address;
    
    // 容量（必须是 2 的幂）
    private final int capacity;
    private final int mask;
    
    // 是否启用（L0 内存分配失败时禁用）
    private final boolean enabled;
    
    // 统计信息
    private long lookups;
    private long hits;
    
    /**
     * 创建 L0 Hot Key Cache。
     * 
     * @param capacityEntries 容量（entry 数量，必须是 2 的幂）
     * @return L0HotKeyCache 实例，如果 L0 不可用则返回禁用的实例
     */
    public static L0HotKeyCache create(int capacityEntries) {
        if (!NativeL0Memory.isAvailable()) {
            return new L0HotKeyCache(0, 0, false);
        }
        
        // 确保是 2 的幂
        int capacity = Integer.highestOneBit(capacityEntries);
        if (capacity < capacityEntries) {
            capacity <<= 1;
        }
        
        // 分配 L0 内存（16 bytes per entry）
        long sizeBytes = (long) capacity << ENTRY_SHIFT;
        long address = NativeL0Memory.malloc(sizeBytes);
        
        if (address == 0) {
            return new L0HotKeyCache(0, 0, false);
        }
        
        // 清零
        UNSAFE.setMemory(address, sizeBytes, (byte) 0);
        
        return new L0HotKeyCache(address, capacity, true);
    }
    
    private L0HotKeyCache(long l0Address, int capacity, boolean enabled) {
        this.l0Address = l0Address;
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.enabled = enabled;
    }
    
    // ========== 核心操作 ==========
    
    /**
     * 查找缓存 entry。
     * 
     * @param compositeHash 组合 hash（keyHash 或 keyHash^nsHash*MAGIC）
     * @param expectedStoreId 期望的 storeId
     * @param expectedKgIdx 期望的 keyGroup 索引
     * @param namespace namespace 对象（用于 identity hash 比较，VoidNS 时为 null）
     * @return LookupResult，包含是否命中和 tableSlot
     */
    public LookupResult lookup(int compositeHash, int expectedStoreId, int expectedKgIdx, Object namespace) {
        if (!enabled) {
            return LookupResult.MISS;
        }
        
        lookups++;
        
        // 计算 entry 偏移
        int index = compositeHash & mask;
        long offset = l0Address + ((long) index << ENTRY_SHIFT);
        
        // 读取 word 0
        long word0 = UNSAFE.getLong(offset);
        if (word0 == 0) {
            return LookupResult.MISS;  // 空 entry
        }
        
        // 验证 compositeHash
        int storedHash = (int)(word0 >>> COMPOSITE_HASH_SHIFT);
        if (storedHash != compositeHash) {
            return LookupResult.MISS;
        }
        
        // 验证 storeId 和 kgIdx
        int storedStoreId = (int)((word0 >>> STORE_ID_SHIFT) & STORE_ID_MASK);
        int storedKgIdx = (int)((word0 >>> KG_IDX_SHIFT) & KG_IDX_MASK);
        if (storedStoreId != expectedStoreId || storedKgIdx != expectedKgIdx) {
            return LookupResult.MISS;
        }
        
        // 读取 word 1 并验证 namespace identity hash（General NS 模式）
        if (namespace != null) {
            long word1 = UNSAFE.getLong(offset + 8);
            int storedNsIdentityHash = (int)(word1 >>> NS_IDENTITY_HASH_SHIFT);
            int expectedNsIdentityHash = System.identityHashCode(namespace);
            if (storedNsIdentityHash != expectedNsIdentityHash) {
                return LookupResult.MISS;
            }
        }
        
        hits++;
        int tableSlot = (int)(word0 & TABLE_SLOT_MASK);
        return new LookupResult(true, tableSlot);
    }
    
    /**
     * 更新缓存 entry。
     * 
     * @param compositeHash 组合 hash
     * @param storeId StateStore ID
     * @param kgIdx KeyGroup 索引
     * @param tableSlot SwissTable 中的 slot
     * @param namespace namespace 对象（VoidNS 时为 null）
     */
    public void put(int compositeHash, int storeId, int kgIdx, int tableSlot, Object namespace) {
        if (!enabled) {
            return;
        }
        
        // 计算 entry 偏移
        int index = compositeHash & mask;
        long offset = l0Address + ((long) index << ENTRY_SHIFT);
        
        // 构建 word 0
        long word0 = ((long) compositeHash << COMPOSITE_HASH_SHIFT)
                   | ((long) storeId << STORE_ID_SHIFT)
                   | ((long) kgIdx << KG_IDX_SHIFT)
                   | (tableSlot & TABLE_SLOT_MASK);
        
        // 构建 word 1
        long word1 = 0;
        if (namespace != null) {
            int nsIdentityHash = System.identityHashCode(namespace);
            word1 = (long) nsIdentityHash << NS_IDENTITY_HASH_SHIFT;
        }
        
        // 写入
        UNSAFE.putLong(offset, word0);
        UNSAFE.putLong(offset + 8, word1);
    }
    
    /**
     * 使指定 entry 失效（用于 remove 操作）。
     */
    public void invalidate(int compositeHash) {
        if (!enabled) {
            return;
        }
        
        int index = compositeHash & mask;
        long offset = l0Address + ((long) index << ENTRY_SHIFT);
        
        // 只需清零 word 0，使 entry 变为 "空"
        UNSAFE.putLong(offset, 0);
    }
    
    /**
     * 计算 compositeHash。
     */
    public static int computeCompositeHash(int keyHash, Object namespace, boolean isVoidNamespace) {
        if (isVoidNamespace || namespace == null) {
            return keyHash;
        }
        return keyHash ^ (namespace.hashCode() * NS_HASH_MAGIC);
    }
    
    // ========== 生命周期 ==========
    
    public void close() {
        if (enabled && l0Address != 0) {
            NativeL0Memory.free(l0Address);
        }
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public long getLookups() {
        return lookups;
    }
    
    public long getHits() {
        return hits;
    }
    
    public double getHitRate() {
        return lookups == 0 ? 0.0 : (double) hits / lookups;
    }
    
    // ========== 内部类 ==========
    
    public static class LookupResult {
        public static final LookupResult MISS = new LookupResult(false, -1);
        
        public final boolean hit;
        public final int tableSlot;
        
        public LookupResult(boolean hit, int tableSlot) {
            this.hit = hit;
            this.tableSlot = tableSlot;
        }
    }
}
```
        }
        capacity = Math.max(capacity, 1024);  // 最小 1K entries
        
        // 分配 L0 内存（16 bytes per entry）
        long sizeBytes = (long) capacity << ENTRY_SHIFT;
        long address = NativeL0Memory.malloc(sizeBytes);
        
        if (address == 0) {
            return new L0HotKeyCache(0, 0, false);
        }
        
        // 初始化为 0
        UNSAFE.setMemory(address, sizeBytes, (byte) 0);
        
        return new L0HotKeyCache(address, capacity, true);
    }
    
    // ... 其余方法见 4.5 节 ...
}
```

---

## 5. 集成方案

### 5.1 ForL0KeyedStateBackend 集成

```java
public class ForL0KeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
    
    // L0 热键缓存（所有 StateStore 共享）
    private final L0HotKeyCache l0HotKeyCache;
    
    // StateStore 注册时分配 storeId
    private int nextStoreId = 0;
    
    public ForL0KeyedStateBackend(...) {
        // ... 现有初始化 ...
        
        // 创建 L0 缓存（默认 1M entries = 16MB）
        int l0CacheCapacity = ForL0Options.getL0CacheCapacity(configuration);
        this.l0HotKeyCache = L0HotKeyCache.create(l0CacheCapacity);
        
        if (l0HotKeyCache.isEnabled()) {
            LOG.info("[ForL0] L0 Hot Key Cache enabled with {} entries ({}MB)", 
                     l0HotKeyCache.getCapacity(),
                     (l0HotKeyCache.getCapacity() * 16L) / (1024 * 1024));
        } else {
            LOG.info("[ForL0] L0 Hot Key Cache disabled (L0 memory not available)");
        }
    }
    
    @Override
    public void dispose() {
        super.dispose();
        if (l0HotKeyCache != null) {
            LOG.info("[ForL0] L0 Cache stats: lookups={}, hits={}, hitRate={:.2f}%", 
                     l0HotKeyCache.getLookups(),
                     l0HotKeyCache.getHits(),
                     l0HotKeyCache.getHitRate() * 100);
            l0HotKeyCache.close();
        }
    }
    
    // 为新注册的 StateStore 分配 storeId
    int allocateStoreId() {
        return nextStoreId++;
    }
    
    L0HotKeyCache getL0HotKeyCache() {
        return l0HotKeyCache;
    }
}
```

### 5.2 ForL0StateStore 集成

```java
public class ForL0StateStore<K, N, S> implements StateSnapshotRestore {
    
    // L0 缓存引用和 storeId
    private final L0HotKeyCache l0HotKeyCache;
    private final int storeId;
    
    public ForL0StateStore(
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            L0HotKeyCache l0HotKeyCache,
            int storeId) {
        // ... 现有初始化 ...
        this.l0HotKeyCache = l0HotKeyCache;
        this.storeId = storeId;
    }
    
    /**
     * 带 L0 缓存加速的 get 操作（支持两种 Namespace 模式）。
     */
    public S get(K key, N namespace, int keyGroup) {
        int kgIdx = keyGroup - keyGroupOffset;
        int keyHash = computeKeyHash(key);
        
        // 计算 compositeHash
        int compositeHash = L0HotKeyCache.computeCompositeHash(keyHash, namespace, isVoidNamespace);
        
        // 1. 尝试 L0 缓存
        if (l0HotKeyCache != null) {
            L0HotKeyCache.LookupResult result = l0HotKeyCache.lookup(
                compositeHash, storeId, kgIdx, 
                isVoidNamespace ? null : namespace);
            
            if (result.hit) {
                SwissTable<K, S> table;
                if (isVoidNamespace) {
                    table = tables[kgIdx];
                } else {
                    // General Namespace: 仍需 HashMap lookup
                    Map<N, SwissTable<K, S>> nsMap = namespaceMaps[kgIdx];
                    table = (nsMap == null) ? null : nsMap.get(namespace);
                }
                
                if (table != null && result.tableSlot < table.size()) {
                    // 验证 key（防止 hash 冲突）
                    @SuppressWarnings("unchecked")
                    K cachedKey = (K) table.entries[result.tableSlot << 1];
                    if (key.equals(cachedKey)) {
                        @SuppressWarnings("unchecked")
                        S value = (S) table.entries[(result.tableSlot << 1) + 1];
                        return value;  // L0 Cache HIT!
                    }
                    // Hash 冲突或 slot 已变化，走正常路径
                }
            }
        }
        
        // 2. 正常 SwissTable 查找
        SwissTable<K, S> table;
        if (isVoidNamespace) {
            table = tables[kgIdx];
        } else {
            Map<N, SwissTable<K, S>> nsMap = namespaceMaps[kgIdx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return null;
        }
        
        // 使用返回 slot 的 get 方法
        int slot = table.getSlot(keyHash, key);
        if (slot < 0) {
            return null;
        }
        
        // 3. 更新 L0 缓存
        if (l0HotKeyCache != null) {
            l0HotKeyCache.put(compositeHash, storeId, kgIdx, slot, 
                             isVoidNamespace ? null : namespace);
        }
        
        @SuppressWarnings("unchecked")
        S value = (S) table.entries[(slot << 1) + 1];
        return value;
    }
    
    /**
     * 带 L0 缓存失效的 remove 操作。
     */
    public S remove(K key, N namespace, int keyGroup) {
        int kgIdx = keyGroup - keyGroupOffset;
        int keyHash = computeKeyHash(key);
        int compositeHash = L0HotKeyCache.computeCompositeHash(keyHash, namespace, isVoidNamespace);
        
        // 失效 L0 缓存
        if (l0HotKeyCache != null) {
            l0HotKeyCache.invalidate(compositeHash);
        }
        
        // ... 现有 remove 逻辑 ...
    }
}
```

### 5.3 SwissTable 新增 getSlot 方法

```java
public class SwissTable<K, S> {
    
    /**
     * 查找 key 并返回 slot 索引。
     * 
     * @param hash 32 位 hash（keyHash，不是 compositeHash）
     * @param key 要查找的 key
     * @return slot 索引，如果未找到返回 -1
     */
    public int getSlot(int hash, K key) {
        int h2Hash = h2(hash);
        long h2Pattern = LSB * (h2Hash & 0xFFL);
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            long match = matchH2(ctrlWord, h2Pattern);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                int idx = slot << 1;
                if (key.equals(entries[idx])) {
                    return slot;  // 返回 slot 而不是 value
                }
                match = clearLowestBit(match);
            }

            if (matchEmpty(ctrlWord) != 0) {
                return -1;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }
}
```

---

## 6. 配置与调优

### 6.1 配置选项

在 `ForL0Options.java` 中添加：

```java
public class ForL0Options {
    
    /**
     * L0 Hot Key Cache 容量（entry 数量）。
     * 每个 entry 16 字节，默认 1M entries = 16MB。
     */
    public static final ConfigOption<Integer> L0_CACHE_CAPACITY =
            ConfigOptions.key("state.backend.forl0.l0-cache.capacity")
                    .intType()
                    .defaultValue(1024 * 1024)  // 1M entries = 16MB
                    .withDescription("L0 Hot Key Cache capacity in number of entries. " +
                            "Each entry is 16 bytes. Default is 1M entries (16MB). " +
                            "Set to 0 to disable L0 caching.");
    
    /**
     * 是否启用 L0 Hot Key Cache。
     */
    public static final ConfigOption<Boolean> L0_CACHE_ENABLED =
            ConfigOptions.key("state.backend.forl0.l0-cache.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable L0 Hot Key Cache for accelerating " +
                            "hot key access. Only effective when L0 memory is available.");
}
```

### 6.2 容量建议

| 场景 | 推荐容量 | L0 内存 | 说明 |
|------|----------|---------|------|
| 小规模 | 128K entries | 2MB | 开发测试 |
| 中规模 | 1M entries | 16MB | **默认值** |
| 大规模 | 4M entries | 64MB | 大量热键场景 |
| 极端场景 | 16M entries | 256MB | 需要充足 L0 容量 |

---

## 7. 性能分析

### 7.1 访问延迟对比

| 路径 | 操作 | 预估延迟 |
|------|------|----------|
| L0 Cache Hit | 1 次 L0 读 + 1 次 key.equals() + 1 次堆内读 | ~10-20 ns |
| L0 Cache Miss → SwissTable Hit | 1 次 L0 读 + N 次 SWAR + M 次 equals() | ~50-100 ns |
| SwissTable Miss | 同上 | ~50-100 ns |

### 7.2 预期命中率

对于典型 Zipf 分布工作负载：

| 热键比例 | 访问占比 | L0 容量 | 预期命中率 |
|----------|----------|---------|-----------|
| Top 1% | ~20% | 足够 | ~20% |
| Top 5% | ~40% | 足够 | ~40% |
| Top 10% | ~55% | 足够 | ~55% |

**注意**：直接映射会有冲突，实际命中率可能低于理论值。

### 7.3 额外开销

| 操作 | 开销 |
|------|------|
| L0 Lookup | 1 次 L0 读 (~5 ns) + 比较 (~2 ns) |
| L0 Insert | 1 次 L0 写 (~5 ns) |
| L0 Invalidate | 1 次 L0 读 + 可能 1 次 L0 写 |

**结论**：即使 L0 未命中，额外开销仅 ~7 ns，可忽略。

---

## 8. 实施计划

### Phase 1: 基础实现（同时支持 VoidNamespace 和 General Namespace）

| 步骤 | 任务 | 文件 |
|------|------|------|
| 1.1 | 实现 `L0HotKeyCache` 类（16 bytes entry） | `space/L0HotKeyCache.java` |
| 1.2 | SwissTable 添加 `getSlot()` 方法 | `SwissTable.java` |
| 1.3 | ForL0Options 添加配置项 | `ForL0Options.java` |

### Phase 2: 集成

| 步骤 | 任务 | 文件 |
|------|------|------|
| 2.1 | ForL0KeyedStateBackend 创建和管理 L0Cache | `ForL0KeyedStateBackend.java` |
| 2.2 | ForL0KeyedStateBackendBuilder 传递 L0Cache | `ForL0KeyedStateBackendBuilder.java` |
| 2.3 | ForL0StateStore 注册 storeId 并使用 L0Cache | `ForL0StateStore.java` |

### Phase 3: 测试与优化

| 步骤 | 任务 |
|------|------|
| 3.1 | 单元测试：L0HotKeyCache 功能测试 |
| 3.2 | 集成测试：验证 VoidNS 和 General NS 两种模式 |
| 3.3 | Benchmark：测量热键加速效果（WordCount + Nexmark q5/q12） |
| 3.4 | 调优：根据 benchmark 调整默认配置 |

### Phase 4: 扩展优化（可选）

| 步骤 | 任务 | 说明 |
|------|------|------|
| 4.1 | 采样更新优化 | 减少 L0 写入频率，降低写放大 |
| 4.2 | 多路关联缓存 | 2-way/4-way 降低冲突率 |
| 4.3 | 分区 L0Cache | 每个 StateStore 独立 L0 分区，减少跨 Store 冲突 |

---

## 附录

### A. Composite Hash 的设计考量

**问题**：如何用固定大小的 L0 entry 同时支持 VoidNamespace 和 General Namespace？

**方案**：Composite Hash = `keyHash ^ (namespaceHash * MAGIC)`

**优点**：
1. VoidNamespace 时 namespace=null，compositeHash = keyHash，零额外开销
2. General Namespace 时，不同 window 的 compositeHash 几乎不可能相同
3. 统一的 entry 格式，代码路径简化

**MAGIC 常量选择**：使用 `0x9e3779b9`（黄金比例相关），确保良好的混合效果。

### B. nsIdentityHash vs nsHash

**为什么用 `System.identityHashCode(namespace)` 而不是 `namespace.hashCode()`？**

1. **更快**：identityHashCode 直接读取对象头，无需计算
2. **更精确**：Flink 的 window namespace 对象通常是复用的（如 `TimeWindow` 实例池）
3. **排除效果好**：不同 window 实例的 identityHashCode 一定不同

**场景分析**：
```java
// Tumbling window: 每个 window 有独立的 TimeWindow 实例
TimeWindow w1 = new TimeWindow(0, 1000);
TimeWindow w2 = new TimeWindow(1000, 2000);

System.identityHashCode(w1) != System.identityHashCode(w2)  // 不同对象
w1.hashCode() != w2.hashCode()  // 不同时间范围

// 但同一 window 被多次访问时：
TimeWindow w1_again = getWindow(0, 1000);  // 可能返回同一实例
System.identityHashCode(w1) == System.identityHashCode(w1_again)  // 命中！
```

### C. General Namespace 模式的 HashMap Lookup

**问题**：General Namespace 模式下仍需要 `HashMap.get(namespace)`，L0Cache 有什么价值？

**回答**：
1. **快速排除**：compositeHash + nsIdentityHash 不匹配时，直接 MISS，无需 HashMap lookup
2. **跳过 SWAR probe**：命中时直接用 tableSlot 访问 entries[]，跳过 SwissTable probe
3. **热窗口缓存**：当前活跃窗口（1-2个）会持续命中，HashMap lookup 仍需要但 SwissTable probe 被跳过

**性能分析**：
```
完整路径（无 L0Cache）:
  HashMap.get(namespace) + SwissTable.get(key)  ≈ 80-120 ns

L0 命中路径:
  L0.lookup() + HashMap.get(namespace) + entries[] 直接访问  ≈ 40-60 ns

L0 未命中路径:
  L0.lookup() + HashMap.get(namespace) + SwissTable.get(key)  ≈ 90-130 ns
```

**结论**：L0 未命中的额外开销 ~10ns，L0 命中可节省 ~40-60ns。只要命中率 > 20%，就有整体收益。

### D. 与之前 MainTable + L0Table 设计的区别

| 维度 | 之前设计 | 当前设计 |
|------|----------|----------|
| L0 用途 | 存储完整索引 + 序列化数据 | 只存热键直接定位信息 |
| 数据存储 | 堆外序列化 | 堆内对象引用 |
| Entry 大小 | 可变 | 固定 16 字节 |
| 复杂度 | 高（多层结构） | 低（单层缓存） |
| L0 容量要求 | 随状态增长 | 固定大小 |
| Namespace 支持 | 需要额外设计 | 原生支持（Composite Hash） |

### E. 窗口状态的时间局部性

窗口状态有强烈的时间局部性：

```
时间线:
t0      t1      t2      t3      t4
├───────┼───────┼───────┼───────┤
│  w1   │  w2   │  w3   │  w4   │
│ cold  │ cold  │ hot   │ future│
└───────┴───────┴───────┴───────┘
                  ↑
              当前窗口
```

**L0Cache 的自适应行为**：
1. w3 是当前窗口，其热键持续更新 L0Cache
2. w1, w2 已过期，其 L0 entries 会被 w3/w4 的热键自然覆盖
3. 无需主动清理过期窗口的缓存

### F. 失效策略详解

**当前策略：自然失效 + 主动删除失效**

1. **自然失效**：直接映射冲突时，后来者覆盖
   - 冷键/冷窗口自然被热键/热窗口替代
   - 无 LRU/LFU 开销

2. **主动删除失效**：`remove()` 时调用 `l0Cache.invalidate()`
   - 防止删除后的 stale entry 被误命中
   - 只需清零 word0，O(1) 操作

3. **SwissTable rehash/grow 后的 slot 变化**：
   - L0Cache 存储的 tableSlot 可能过期
   - 验证 `key.equals(entries[slot*2])` 会发现不匹配
   - 按 MISS 处理，更新 L0Cache 为新 slot

**为什么不需要批量失效？**
- Flink 状态访问是单线程的
- 每个 key 的 remove 都会主动失效
- 过期 entries 会被自然覆盖
