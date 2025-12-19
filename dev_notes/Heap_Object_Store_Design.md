# 堆内对象存储改造设计方案

> 文档版本: 1.1  
> 创建日期: 2025-12-19  
> 更新日期: 2025-12-19  
> 状态: **设计中**  
> **注意: 本次为彻底改造，不考虑向后兼容性，将在新分支进行**

## 版本历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| 1.0 | 2025-12-19 | 初始设计：堆外索引 + 堆内对象存储架构 |
| **1.1** | **2025-12-19** | **明确为彻底改造，删除/扩容策略设计，移除兼容性考虑** |

## 目录

1. [背景与动机](#1-背景与动机)
2. [可行性分析](#2-可行性分析)
3. [新架构设计](#3-新架构设计)
4. [详细设计](#4-详细设计)
5. [改造收益分析](#5-改造收益分析)
6. [Checkpoint 兼容性设计](#6-checkpoint-兼容性设计)
7. [实施计划](#7-实施计划)
8. [风险评估与缓解](#8-风险评估与缓解)

---

## 1. 背景与动机

### 1.1 问题背景

当前 ForL0 State Backend 采用 **堆外索引 + 堆外对象存储** 架构：

```
┌─────────────────────────────────────────────────────────────────┐
│                    当前架构 (序列化存储)                          │
├─────────────────────────────────────────────────────────────────┤
│   L0Table (堆外)  →  ┐                                          │
│                       ├→  EntryStore (堆外)                     │
│   MainTable (堆外) →  ┘   ├─ KeyNsPool: 序列化的 key/namespace    │
│                           └─ ValuePool: 序列化的 value           │
└─────────────────────────────────────────────────────────────────┘
```

**核心问题**：序列化开销过大，严重影响性能

| 问题 | 详细描述 | 性能影响 |
|------|----------|----------|
| **序列化开销** | 每次 get/put 都需要序列化/反序列化 key、namespace、value | 高 CPU 消耗，成为瓶颈 |
| **内存复制** | 序列化产生大量临时 byte[]，增加 GC 压力 | 额外的内存分配和复制 |
| **哈希计算复杂** | 需要基于序列化字节计算哈希 | 增加计算开销 |
| **比较开销** | 查找时需要比较序列化字节 | 无法直接使用 Object.equals() |

### 1.2 改造目标

改为 **堆外索引 + 堆内对象存储** 架构，**完全消除序列化开销**：

```
┌─────────────────────────────────────────────────────────────────┐
│                    新架构 (对象存储)                              │
├─────────────────────────────────────────────────────────────────┤
│   L0Table (堆外)  →  ┐                                          │
│                       ├→  HeapEntryStore (堆内)                  │
│   MainTable (堆外) →  ┘   └─ Object[] 数组存储原始对象              │
└─────────────────────────────────────────────────────────────────┘
```

**关键变化**：
- 索引中的 Pointer 从 **堆外内存地址** 改为 **数组下标**
- 对象直接存储在堆内数组中，**零序列化**

---

## 2. 可行性分析

### 2.1 技术可行性

#### ✅ 堆内存储的优势

| 方面 | 分析 |
|------|------|
| **哈希计算** | 直接使用 `Object.hashCode()`，对于大多数类型这是高效的原生实现 |
| **对象比较** | 直接使用 `Object.equals()`，避免字节比较开销 |
| **无序列化** | 完全消除序列化/反序列化开销 |
| **类型安全** | 编译时类型检查，减少运行时错误 |
| **调试友好** | 可以直接查看对象内容 |

#### ✅ 与 CopyOnWriteStateMap 对比

参考 Flink 原生的 `CopyOnWriteStateMap` 设计：

```java
// CopyOnWriteStateMap.StateMapEntry
protected static class StateMapEntry<K, N, S> implements StateEntry<K, N, S> {
    @Nonnull final K key;           // 直接存储对象引用
    @Nonnull final N namespace;     // 直接存储对象引用
    @Nullable S state;              // 直接存储对象引用
    final int hash;                 // 缓存的哈希值
    @Nullable StateMapEntry<K, N, S> next;  // 链表指针
    int entryVersion;               // COW 版本
    int stateVersion;               // 状态版本
}
```

**关键参考点**：
1. **对象直接存储**：key、namespace、state 都是直接存储对象引用
2. **哈希缓存**：使用 `compositeHash(key.hashCode() ^ namespace.hashCode())` 计算并缓存
3. **等值比较**：使用 `key.equals(eKey) && namespace.equals(eNamespace)`

#### ✅ 堆外索引改造

由于是彻底改造，索引层可以根据需要调整布局：

| 索引组件 | 当前设计 | 新设计 | 说明 |
|----------|----------|--------|------|
| L0Table Slot | `Tag(2B) + Extension(5B) + Pointer(8B)` | `Tag(2B) + Extension(5B) + ArrayIndex(4B) + Reserved(4B)` | Pointer 改为 4 字节下标即可 |
| MainTable Slot | `Tag(2B) + Pointer(8B) = 10B` | `Tag(2B) + ArrayIndex(4B) + Reserved(4B) = 10B` | 保持 10 字节对齐 |

**Pointer 字段改造**：
- 原来：8 字节堆外内存地址
- 现在：4 字节数组下标（最大支持 40 亿条目）
- 剩余 4 字节：可用于其他元数据（如版本号、标志位等），或保持预留

### 2.2 潜在挑战

| 挑战 | 分析 | 解决方案 |
|------|------|----------|
| **GC 压力** | 堆内存储会增加 GC 扫描范围 | 使用数组存储，GC 友好；大量小对象变为少量大数组 |
| **内存使用** | 对象头开销 (8-16 bytes per object) | 相比序列化开销，这是可接受的 |
| **数组扩容** | 动态增长时需要复制 | 分块数组设计，避免全量复制 |
| **Checkpoint** | 需要在 checkpoint 时序列化 | 仅在 checkpoint 时序列化，热路径无开销 |

### 2.3 结论：**方案可行** ✅

堆内对象存储可以：
1. 完全消除热路径上的序列化开销
2. 利用 JVM 原生的 hashCode/equals 优化
3. 保持堆外索引的缓存友好特性
4. 与 Flink 原生 HeapStateBackend 设计思路一致

---

## 3. 新架构设计

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     ForL0StateMap (新架构)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐     ┌──────────────────┐                  │
│  │     L0Table      │     │    MainTable     │                  │
│  │   (堆外缓存层)    │     │   (堆外索引层)    │                  │
│  │                  │     │                  │                  │
│  │ Bucket[N]        │     │ Bucket[M]        │                  │
│  │ ┌──────────────┐ │     │ ┌──────────────┐ │                  │
│  │ │Tag|Ext|Index │ │     │ │  Tag | Index │ │                  │
│  │ └──────────────┘ │     │ └──────────────┘ │                  │
│  └────────┬─────────┘     └────────┬─────────┘                  │
│           │                        │                             │
│           │    Index = 数组下标      │                             │
│           │                        │                             │
│           ▼                        ▼                             │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    HeapEntryStore (堆内)                     ││
│  │                                                              ││
│  │  Entry[] entries;  // 或 StateMapEntry<K,N,S>[]              ││
│  │                                                              ││
│  │  ┌─────────────────────────────────────────────────────────┐││
│  │  │ Entry[0] │ Entry[1] │ Entry[2] │ ... │ Entry[n-1]       │││
│  │  │ ┌──────┐ │ ┌──────┐ │ ┌──────┐ │     │ ┌──────┐         │││
│  │  │ │ key  │ │ │ key  │ │ │ key  │ │     │ │ key  │         │││
│  │  │ │ ns   │ │ │ ns   │ │ │ ns   │ │     │ │ ns   │         │││
│  │  │ │ state│ │ │ state│ │ │ state│ │     │ │ state│         │││
│  │  │ │ hash │ │ │ hash │ │ │ hash │ │     │ │ hash │         │││
│  │  │ └──────┘ │ └──────┘ │ └──────┘ │     │ └──────┘         │││
│  │  └─────────────────────────────────────────────────────────┘││
│  │                                                              ││
│  │  int[] freeList;  // 空闲槽位管理                             ││
│  │                                                              ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 核心数据结构

#### 3.2.1 HeapStateEntry (堆内条目)

```java
/**
 * 堆内状态条目，直接存储对象引用。
 * 类似于 CopyOnWriteStateMap.StateMapEntry，但不需要链表指针。
 */
public final class HeapStateEntry<K, N, S> implements StateEntry<K, N, S> {
    
    /** Key 对象引用，不可变 */
    @Nonnull
    final K key;
    
    /** Namespace 对象引用，不可变 */
    @Nonnull
    final N namespace;
    
    /** State 对象引用，可变 */
    @Nullable
    S state;
    
    /** 缓存的复合哈希值 (key.hashCode() ^ namespace.hashCode()) */
    final int hash;
    
    // 注意：不需要保存 tag 字段
    // Tag 仅用于堆外索引层的快速过滤，Entry 匹配时直接使用 equals()
    // 如需要 tag，可即时计算：(short)(hash >>> 16)
    
    public HeapStateEntry(@Nonnull K key, @Nonnull N namespace, @Nullable S state) {
        this.key = key;
        this.namespace = namespace;
        this.state = state;
        // 使用与 CopyOnWriteStateMap 相同的哈希计算方式
        this.hash = MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
    }
    
    /** 比较 key 和 namespace 是否匹配 */
    public boolean matches(K key, N namespace) {
        return this.key.equals(key) && this.namespace.equals(namespace);
    }
    
    // StateEntry 接口实现
    @Override public K getKey() { return key; }
    @Override public N getNamespace() { return namespace; }
    @Override public S getState() { return state; }
    
    public void setState(S state) { this.state = state; }
}
```

#### 3.2.2 HeapEntryStore (堆内条目存储)

```java
/**
 * 堆内条目存储，使用分块数组避免全量复制。
 * 
 * 设计要点：
 * 1. 使用 Object[][] 分块存储，每块固定大小
 * 2. 空闲槽位使用 int[] freeList 管理（栈结构）
 * 3. 返回的 "地址" 实际是数组下标
 */
public class HeapEntryStore<K, N, S> implements AutoCloseable {
    
    // ========== 常量 ==========
    
    /** 每个块的大小 (2^16 = 65536 个槽位) */
    private static final int CHUNK_SIZE_BITS = 16;
    private static final int CHUNK_SIZE = 1 << CHUNK_SIZE_BITS;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;
    
    /** 初始块数量 */
    private static final int INITIAL_CHUNKS = 1;
    
    // ========== 存储结构 ==========
    
    /** 分块数组：chunks[chunkIndex][slotIndex] */
    @SuppressWarnings("unchecked")
    private HeapStateEntry<K, N, S>[][] chunks;
    
    /** 当前块数量 */
    private int chunkCount;
    
    /** 下一个分配位置 */
    private int nextAllocIndex;
    
    // ========== 空闲槽位管理 ==========
    
    /** 空闲槽位栈 */
    private int[] freeList;
    private int freeCount;
    
    // ========== 统计 ==========
    
    private int activeEntries;
    
    // ========== 构造函数 ==========
    
    @SuppressWarnings("unchecked")
    public HeapEntryStore() {
        this.chunks = (HeapStateEntry<K, N, S>[][]) new HeapStateEntry[INITIAL_CHUNKS][];
        this.chunks[0] = (HeapStateEntry<K, N, S>[]) new HeapStateEntry[CHUNK_SIZE];
        this.chunkCount = 1;
        this.nextAllocIndex = 0;
        
        // 空闲列表初始为空
        this.freeList = new int[1024];
        this.freeCount = 0;
        
        this.activeEntries = 0;
    }
    
    // ========== 核心操作 ==========
    
    /**
     * 分配新条目，返回数组下标（作为 "地址"）。
     * 
     * @return 分配的下标，永远 > 0（0 保留为 NULL）
     */
    public long allocate(@Nonnull K key, @Nonnull N namespace, @Nullable S state) {
        int index;
        
        // 优先使用空闲槽位
        if (freeCount > 0) {
            index = freeList[--freeCount];
        } else {
            // 检查是否需要扩展
            if (nextAllocIndex >= chunkCount * CHUNK_SIZE) {
                expandChunks();
            }
            index = nextAllocIndex++;
        }
        
        // 存储条目
        HeapStateEntry<K, N, S> entry = new HeapStateEntry<>(key, namespace, state);
        int chunkIndex = index >> CHUNK_SIZE_BITS;
        int slotIndex = index & CHUNK_MASK;
        chunks[chunkIndex][slotIndex] = entry;
        
        activeEntries++;
        
        // 返回 index + 1，确保 0 作为 NULL
        return index + 1;
    }
    
    /**
     * 获取条目（零序列化）。
     * 
     * @param address 分配时返回的地址（实际是 index + 1）
     * @return 条目对象，或 null
     */
    public HeapStateEntry<K, N, S> get(long address) {
        if (address <= 0) return null;
        int index = (int) address - 1;
        int chunkIndex = index >> CHUNK_SIZE_BITS;
        int slotIndex = index & CHUNK_MASK;
        
        if (chunkIndex >= chunkCount) return null;
        return chunks[chunkIndex][slotIndex];
    }
    
    /**
     * 获取哈希值。
     */
    public int getHash(long address) {
        HeapStateEntry<K, N, S> entry = get(address);
        return entry != null ? entry.hash : 0;
    }
    
    /**
     * 获取 Tag（从 hash 计算，不单独存储）。
     */
    public short getTag(long address) {
        HeapStateEntry<K, N, S> entry = get(address);
        return entry != null ? (short)(entry.hash >>> 16) : 0;
    }
    
    /**
     * 检查 key 和 namespace 是否匹配。
     */
    public boolean matches(long address, K key, N namespace) {
        HeapStateEntry<K, N, S> entry = get(address);
        return entry != null && entry.matches(key, namespace);
    }
    
    /**
     * 更新 state（原地更新，地址不变）。
     */
    public void updateState(long address, S state) {
        HeapStateEntry<K, N, S> entry = get(address);
        if (entry != null) {
            entry.setState(state);
        }
    }
    
    /**
     * 删除条目，将槽位加入空闲列表。
     */
    public void remove(long address) {
        if (address <= 0) return;
        int index = (int) address - 1;
        int chunkIndex = index >> CHUNK_SIZE_BITS;
        int slotIndex = index & CHUNK_MASK;
        
        if (chunkIndex < chunkCount && chunks[chunkIndex][slotIndex] != null) {
            chunks[chunkIndex][slotIndex] = null;
            activeEntries--;
            
            // 加入空闲列表
            if (freeCount >= freeList.length) {
                freeList = Arrays.copyOf(freeList, freeList.length * 2);
            }
            freeList[freeCount++] = index;
        }
    }
    
    // ========== 私有方法 ==========
    
    @SuppressWarnings("unchecked")
    private void expandChunks() {
        int newChunkCount = chunkCount + 1;
        if (newChunkCount > chunks.length) {
            // 扩展 chunks 数组
            chunks = Arrays.copyOf(chunks, chunks.length * 2);
        }
        chunks[chunkCount] = (HeapStateEntry<K, N, S>[]) new HeapStateEntry[CHUNK_SIZE];
        chunkCount = newChunkCount;
    }
    
    // ========== 统计 ==========
    
    public int getActiveEntries() { return activeEntries; }
    
    public int getCapacity() { return chunkCount * CHUNK_SIZE; }
    
    @Override
    public void close() {
        // 帮助 GC
        for (int i = 0; i < chunkCount; i++) {
            Arrays.fill(chunks[i], null);
        }
        chunks = null;
    }
}
```

### 3.3 索引层改造

#### 3.3.1 L0Table 改造

```java
// L0Table 的变化很小，主要是：
// 1. Pointer 字段解释为数组下标
// 2. 匹配时调用 HeapEntryStore.matches() 而不是字节比较

public class L0Table {
    
    // ... 原有布局不变 ...
    
    /**
     * 查找条目（新版本：使用对象比较）
     */
    public long get(int keyHash, short tag, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        
        int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            byte valid = segment.get(bucketOffset + slot);
            if (valid == 0) continue;
            
            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;
            
            long entryIndex = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            
            // 直接对象比较，无需序列化
            if (store.matches(entryIndex, key, namespace)) {
                hitCount++;
                updateAccessInfo(segment, bucketOffset, slot, slotOffset);
                return entryIndex;
            }
        }
        
        missCount++;
        return 0;
    }
}
```

#### 3.3.2 MainTable 改造

```java
public class MainTable {
    
    // ... 原有布局不变 ...
    
    /**
     * 查找条目（新版本：使用对象比较）
     */
    public long get(int keyHash, short tag, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        return searchBucketTree(bucketIndex, bucketIndex, tag, key, namespace, store, segment, bucketOffset);
    }
    
    private long searchBucketSlots(MemorySegment segment, int bucketOffset, 
                                    short tag, K key, N namespace, 
                                    HeapEntryStore<K, N, S> store) {
        int slotOffset = bucketOffset;
        for (int i = 0; i < SLOTS_PER_BUCKET; i++, slotOffset += SLOT_SIZE) {
            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag == 0) continue;
            if (slotTag != tag) continue;
            
            long entryIndex = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (entryIndex == 0) continue;
            
            // 直接对象比较
            if (store.matches(entryIndex, key, namespace)) {
                lastFoundSegment = segment;
                lastFoundSlotOffset = slotOffset;
                return entryIndex;
            }
        }
        return 0;
    }
}
```

---

## 4. 详细设计

### 4.1 哈希计算策略

采用与 `CopyOnWriteStateMap` 一致的哈希策略：

```java
/**
 * 复合哈希计算：组合 key 和 namespace 的 hashCode
 */
private static int compositeHash(Object key, Object namespace) {
    return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
}

/**
 * Tag 提取：取哈希值的高 16 位
 */
private static short extractTag(int hash) {
    return (short) (hash >>> 16);
}
```

**优势**：
1. `Object.hashCode()` 对于大多数类型有高效的原生实现
2. `MathUtils.bitMix()` 提供良好的位分布，减少冲突
3. 与 Flink 原生实现一致，行为可预测

### 4.2 ForL0StateMap 改造

```java
public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {
    
    // ========== 存储组件 ==========
    
    private final MemoryManagerAllocator allocator;  // For MainTable
    private final L0MemoryAllocator l0Allocator;     // For L0Table
    
    // 新：堆内对象存储
    private final HeapEntryStore<K, N, S> entryStore;
    
    // 堆外索引（保持不变）
    private final MainTable<K, N, S> mainTable;
    private final L0Table<K, N, S> l0Table;
    
    // 不再需要序列化器（热路径）
    // 仅在 checkpoint 时使用
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;
    
    // ========== 核心操作 ==========
    
    @Override
    public S get(K key, N namespace) {
        if (key == null || namespace == null) return null;
        totalAccesses++;
        
        // 直接计算哈希，无需序列化
        int hash = compositeHash(key, namespace);
        short tag = extractTag(hash);
        
        long addr;
        
        // 先查 L0 缓存
        if (l0CacheEnabled && l0Table != null) {
            addr = l0Table.get(hash, tag, key, namespace, entryStore);
            if (addr > 0) {
                l0Hits++;
                HeapStateEntry<K, N, S> entry = entryStore.get(addr);
                return entry != null ? entry.getState() : null;
            }
        }
        
        // 再查主表
        addr = mainTable.get(hash, tag, key, namespace, entryStore);
        if (addr > 0) {
            mainTableHits++;
            updateL0Table(hash, tag, addr);
            HeapStateEntry<K, N, S> entry = entryStore.get(addr);
            return entry != null ? entry.getState() : null;
        }
        
        return null;
    }
    
    @Override
    public void put(K key, N namespace, S state) {
        if (key == null || namespace == null) return;
        
        int hash = compositeHash(key, namespace);
        short tag = extractTag(hash);
        
        // 检查是否需要扩容
        if (mainTable.needsResize() && !resizeInProgress) {
            performResize();
        }
        
        // 尝试查找现有条目
        long existing = mainTable.get(hash, tag, key, namespace, entryStore);
        
        if (existing == 0) {
            // 新条目：分配存储空间
            long addr = entryStore.allocate(key, namespace, state);
            mainTable.put(hash, tag, addr, key, namespace, entryStore);
            size++;
            updateL0Table(hash, tag, addr);
        } else {
            // 更新现有条目（原地更新，地址不变）
            entryStore.updateState(existing, state);
        }
    }
    
    @Override
    public void remove(K key, N namespace) {
        if (key == null || namespace == null) return;
        
        int hash = compositeHash(key, namespace);
        short tag = extractTag(hash);
        
        long removed = mainTable.remove(hash, tag, key, namespace, entryStore);
        if (removed > 0) {
            size--;
            if (l0CacheEnabled && l0Table != null) {
                l0Table.remove(hash, tag, key, namespace, entryStore);
            }
            entryStore.remove(removed);
        }
    }
    
    // ========== 辅助方法 ==========
    
    private static int compositeHash(Object key, Object namespace) {
        return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
    }
    
    private static short extractTag(int hash) {
        return (short) (hash >>> 16);
    }
}
```

### 4.3 接口变化对比

| 方法 | 旧实现 | 新实现 |
|------|--------|--------|
| `get(K, N)` | 序列化 key/ns → 计算哈希 → 查表 → 反序列化 value | 计算 hashCode → 查表 → 返回对象引用 |
| `put(K, N, S)` | 序列化 key/ns/state → 分配堆外 → 写入字节 | 计算 hashCode → 分配数组槽位 → 存储引用 |
| `containsKey(K, N)` | 序列化 → 查表 → 比较字节 | 计算 hashCode → 查表 → equals 比较 |
| `remove(K, N)` | 序列化 → 查表 → 删除 | 计算 hashCode → 查表 → 删除 |

### 4.4 删除与空闲槽位管理

#### 4.4.1 设计决策

采用 **空洞化 + 空闲列表** 策略管理删除后的空槽位：

| 方案 | 优点 | 缺点 | 选择 |
|------|------|------|------|
| **空闲链表 (Free List)** | 分配 O(1)，简单直接 | 链表本身占用内存 | ✅ 选用 |
| **位图 (Bitmap)** | 内存紧凑，支持批量操作 | 分配需要扫描找空位 | 备选 |
| **移动填充** | 无碎片 | 需更新索引指针，复杂度高 | ❌ 不选 |

**空闲列表设计**：

```java
/**
 * HeapEntryStore 的空闲槽位管理
 * 使用栈结构实现 LIFO，最近释放的槽位优先复用（缓存友好）
 */
public class HeapEntryStore<K, N, S> {
    
    /** 空闲槽位栈 */
    private int[] freeList;
    private int freeCount;
    
    /**
     * 分配槽位：优先使用空闲槽位，否则从末尾分配
     */
    public long allocate(K key, N namespace, S state) {
        int index;
        if (freeCount > 0) {
            // O(1) 从栈顶弹出
            index = freeList[--freeCount];
        } else {
            // 从末尾分配，可能触发扩容
            index = nextAllocIndex++;
            if (index >= chunkCount * CHUNK_SIZE) {
                expandChunks();
            }
        }
        // ... 存储 entry ...
        return index + 1;  // 0 保留为 NULL
    }
    
    /**
     * 删除槽位：加入空闲列表
     */
    public void remove(long address) {
        if (address <= 0) return;
        int index = (int) address - 1;
        
        // 清空槽位
        chunks[index >> CHUNK_SIZE_BITS][index & CHUNK_MASK] = null;
        activeEntries--;
        
        // O(1) 压入栈顶
        if (freeCount >= freeList.length) {
            freeList = Arrays.copyOf(freeList, freeList.length * 2);
        }
        freeList[freeCount++] = index;
    }
}
```

#### 4.4.2 删除流程

单条删除的完整流程：

```
1. ForL0StateMap.remove(key, namespace)
   │
   ├─→ 2. 计算 hash = compositeHash(key, namespace)
   │       tag = extractTag(hash)
   │
   ├─→ 3. MainTable.remove(hash, tag, key, namespace, entryStore)
   │       - 查找匹配的 Slot
   │       - 清零 Slot 的 Tag（空洞化）
   │       - 返回 entryIndex
   │
   ├─→ 4. L0Table.remove(hash, tag, key, namespace, entryStore)
   │       - 查找并清除缓存条目
   │
   └─→ 5. HeapEntryStore.remove(entryIndex)
           - 清空数组槽位（设为 null）
           - 将 index 加入空闲列表
```

**索引层空洞化**：

```java
// MainTable 删除：只清零 Tag，槽位变为"空洞"
public long remove(int keyHash, short tag, K key, N namespace, HeapEntryStore<K,N,S> store) {
    // ... 查找匹配的 slot ...
    if (store.matches(entryIndex, key, namespace)) {
        // 空洞化：清零 Tag，保留槽位
        segment.putShort(slotOffset + SLOT_TAG_OFFSET, (short) 0);
        segment.putLong(slotOffset + SLOT_POINTER_OFFSET, 0L);
        totalEntries--;
        return entryIndex;
    }
    return 0;
}
```

### 4.5 批量删除（按 Namespace）

#### 4.5.1 设计决策

与 `CopyOnWriteStateMap` 保持一致，采用 **全表扫描** 方式：

```java
// CopyOnWriteStateMap 的实现
@Override
public int sizeOfNamespace(Object namespace) {
    int count = 0;
    for (StateEntry<K, N, S> entry : this) {  // 全表遍历
        if (null != entry && namespace.equals(entry.getNamespace())) {
            ++count;
        }
    }
    return count;
}
```

**理由**：
1. 时间窗口等场景下，按 Namespace 删除需要遍历所有条目
2. CopyOnWriteStateMap 也是全表扫描，性能基线一致
3. 未来可优化：按 Namespace 建立二级索引（如需要）

#### 4.5.2 实现方式

```java
/**
 * 批量删除指定 Namespace 的所有条目
 * 
 * 复杂度：O(N)，N 为总条目数
 * 与 CopyOnWriteStateMap.sizeOfNamespace() 遍历方式一致
 */
public void removeNamespace(N namespace) {
    if (namespace == null) return;
    
    // 收集待删除的条目（避免遍历时修改）
    List<RemovalInfo> toRemove = new ArrayList<>();
    
    // 全表扫描 HeapEntryStore
    for (int i = 0; i < nextAllocIndex; i++) {
        HeapStateEntry<K, N, S> entry = getByIndex(i);
        if (entry != null && namespace.equals(entry.getNamespace())) {
            toRemove.add(new RemovalInfo(
                entry.hash, 
                extractTag(entry.hash), 
                entry.getKey(), 
                namespace,
                i + 1  // address
            ));
        }
    }
    
    // 批量执行删除
    for (RemovalInfo info : toRemove) {
        mainTable.remove(info.hash, info.tag, info.key, info.namespace, entryStore);
        if (l0CacheEnabled && l0Table != null) {
            l0Table.remove(info.hash, info.tag, info.key, info.namespace, entryStore);
        }
        entryStore.remove(info.address);
        size--;
    }
}
```

**优化空间（未来考虑）**：

| 优化方案 | 适用场景 | 复杂度 |
|----------|----------|--------|
| Namespace 二级索引 | 频繁按 NS 删除 | 额外内存开销 |
| 分区存储 | NS 数量有限且固定 | 架构改动大 |
| 惰性删除 + 标记 | 批量删除后立即 Checkpoint | 需要 Compaction |

### 4.6 扩容策略

#### 4.6.1 设计决策

采用 **MainTable 全量扩容 + Entry 下标不变** 策略：

```
┌─────────────────────────────────────────────────────────────────┐
│                         扩容设计原则                              │
├─────────────────────────────────────────────────────────────────┤
│ 1. HeapEntryStore 的数组下标（作为 Pointer）在扩容时不变          │
│ 2. MainTable 全量扩容：重新计算每个 Entry 的桶位置                │
│ 3. Entry 中已缓存 hash 值，扩容时直接使用，无需重新计算哈希        │
│ 4. L0Table 大小固定（缓存层），不参与扩容                         │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.6.2 扩容流程

```
触发条件：MainTable.loadFactor > threshold (默认 1.5)
   │
   ▼
1. 分配新的 MainTable 内存（容量 × 2）
   │
   ▼
2. 遍历 HeapEntryStore 中所有活跃条目
   │   for (int i = 0; i < nextAllocIndex; i++) {
   │       entry = chunks[i >> CHUNK_BITS][i & CHUNK_MASK];
   │       if (entry != null) {
   │           // 使用缓存的 hash，重新计算桶位置
   │           newBucketIndex = entry.hash & (newBucketCount - 1);
   │           // 插入新表（下标 i+1 不变）
   │           newMainTable.put(entry.hash, tag, i + 1, ...);
   │       }
   │   }
   │
   ▼
3. 释放旧 MainTable 内存
   │
   ▼
4. 清空 L0Table（缓存失效）
```

#### 4.6.3 实现代码

```java
/**
 * MainTable 扩容实现
 * 
 * 关键点：Entry 的数组下标不变，只是索引位置需要重新计算
 */
public void resize(HeapEntryStore<K, N, S> entryStore) {
    int newBucketCount = bucketCount * 2;
    int newBucketMask = newBucketCount - 1;
    
    // 1. 分配新内存
    MemorySegment[] newSegments = allocateSegments(newBucketCount);
    clearAllSlots(newSegments);
    
    // 2. 遍历所有条目，重新插入
    for (int addr = 1; addr <= entryStore.getMaxAddress(); addr++) {
        HeapStateEntry<K, N, S> entry = entryStore.get(addr);
        if (entry == null) continue;  // 跳过空洞
        
        int hash = entry.hash;
        short tag = (short) (hash >>> 16);
        int newBucketIndex = hash & newBucketMask;
        
        // 插入新表（addr 不变！）
        insertToNewTable(newSegments, newBucketIndex, tag, addr);
    }
    
    // 3. 释放旧内存，切换到新表
    releaseOldSegments();
    this.memorySegments = newSegments;
    this.bucketCount = newBucketCount;
    
    // 4. 重置扩展桶相关状态
    this.extensionBucketCounts = new int[newBucketCount];
    this.extensionBucketBaseIndices = new int[newBucketCount];
    this.needsResize = false;
}
```

#### 4.6.4 扩容复杂度分析

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| 内存分配 | O(newBucketCount) | 分配并清零新内存 |
| 条目迁移 | O(activeEntries) | 遍历所有活跃条目 |
| 哈希计算 | O(1) per entry | 使用缓存的 hash 值 |
| 总体 | O(N) | N = 活跃条目数 |

**与 CopyOnWriteStateMap 对比**：

| 方面 | CopyOnWriteStateMap | ForL0StateMap |
|------|---------------------|---------------|
| 扩容方式 | 增量 (每次操作迁移 4 条) | 全量 |
| 迁移开销 | 分摊到多次操作 | 一次性完成 |
| 暂停时间 | 很短 | 取决于条目数 |
| 实现复杂度 | 高（双表 + rehashIndex） | 低 |

**权衡**：当前采用全量扩容，实现简单。如果未来发现扩容暂停时间过长，可考虑增量扩容。

---

## 5. 改造收益分析

### 5.1 性能收益

| 操作 | 旧实现开销 | 新实现开销 | 预期提升 |
|------|------------|------------|----------|
| **哈希计算** | 序列化 + 字节哈希 | `Object.hashCode()` | **10-100x** |
| **等值比较** | 字节数组比较 | `Object.equals()` | **5-20x** |
| **get 操作** | 序列化 + 查表 + 反序列化 | hashCode + 查表 | **5-10x** |
| **put 操作** | 序列化 × 3 + 内存分配 | hashCode + 数组写入 | **5-10x** |
| **update 操作** | 可能需要重分配 | 原地更新引用 | **10-50x** |

### 5.2 内存收益

| 方面 | 旧实现 | 新实现 |
|------|--------|--------|
| **序列化缓冲区** | 需要临时 byte[] | 不需要 |
| **GC 压力** | 高（大量临时对象） | 低（只有数组扩容时） |
| **内存布局** | 分散的堆外内存 | 连续的数组结构 |

### 5.3 代码简化

| 方面 | 旧实现 | 新实现 |
|------|--------|--------|
| **EntryStore** | KeyNsPool + ValuePool + 复杂内存管理 | 简单的数组 + 空闲列表 |
| **序列化** | SerializerPack + 各种优化路径 | 仅 Checkpoint 时使用 |
| **类型处理** | FixedLengthTypeSupport + TupleTypeInfo | 无需特殊处理 |
| **代码行数** | ~3000 行 | 预计 ~500 行 |

---

## 6. Checkpoint 兼容性设计

### 6.1 Checkpoint 时序列化

由于对象存储在堆内，Checkpoint 时需要序列化。这个开销只在 Checkpoint 时发生，不影响热路径。

```java
/**
 * Checkpoint 快照实现
 */
public class HeapStateMapSnapshot<K, N, S> extends StateMapSnapshot<K, N, S> {
    
    private final HeapEntryStore<K, N, S> entryStore;
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;
    
    @Override
    public void writeState(DataOutputView dov) throws IOException {
        // 遍历所有活跃条目
        for (int i = 0; i < entryStore.getCapacity(); i++) {
            HeapStateEntry<K, N, S> entry = entryStore.get(i + 1);
            if (entry != null) {
                keySerializer.serialize(entry.getKey(), dov);
                namespaceSerializer.serialize(entry.getNamespace(), dov);
                stateSerializer.serialize(entry.getState(), dov);
            }
        }
    }
}
```

### 6.2 恢复时反序列化

```java
/**
 * 从 Checkpoint 恢复
 */
public void restoreState(DataInputView div) throws IOException {
    while (div.available() > 0) {
        K key = keySerializer.deserialize(div);
        N namespace = namespaceSerializer.deserialize(div);
        S state = stateSerializer.deserialize(div);
        
        // 重建索引和存储
        put(key, namespace, state);
    }
}
```

---

## 7. 实施计划 (Roadmap)

### 7.0 准备工作

```
┌─────────────────────────────────────────────────────────────────┐
│  准备工作 (Day 0)                                                │
├─────────────────────────────────────────────────────────────────┤
│  □ 创建新分支: feature/heap-object-store                         │
│  □ 备份当前测试用例和基准测试结果                                  │
│  □ 确认设计文档评审通过                                           │
└─────────────────────────────────────────────────────────────────┘
```

### 7.1 Phase 1: 堆内存储层实现 (Day 1-3)

**目标**: 实现 HeapStateEntry 和 HeapEntryStore，提供堆内对象存储能力

```
Week 1
─────────────────────────────────────────────────────────────────
Day 1        Day 2        Day 3        Day 4        Day 5
├────────────┼────────────┼────────────┼────────────┼────────────┤
│ Phase 1.1  │ Phase 1.2  │ Phase 1.3  │   Phase 2.1 & 2.2      │
│ Entry      │ Store      │ Unit Test  │   L0Table & MainTable  │
└────────────┴────────────┴────────────┴────────────────────────┘
```

| ID | 任务 | 输入 | 输出 | 验收标准 |
|----|------|------|------|----------|
| 1.1 | 实现 `HeapStateEntry<K,N,S>` | 设计文档 3.2.1 | HeapStateEntry.java | 编译通过，字段定义正确 |
| 1.2 | 实现 `HeapEntryStore<K,N,S>` | 设计文档 3.2.2, 4.4 | HeapEntryStore.java | 分块数组 + 空闲列表实现 |
| 1.3 | 单元测试 | HeapEntryStore | HeapEntryStoreTest.java | 覆盖 allocate/get/update/remove |

**Phase 1 交付物**:
- `src/main/java/.../heap/HeapStateEntry.java`
- `src/main/java/.../heap/HeapEntryStore.java`
- `src/test/java/.../heap/HeapEntryStoreTest.java`

**检查点**: 所有单元测试通过 ✓

---

### 7.2 Phase 2: 索引层改造 (Day 4-6)

**目标**: 改造 L0Table 和 MainTable，支持对象比较和数组下标索引

```
Week 1 (续)                    Week 2
─────────────────────────────────────────────────────────────────
Day 4        Day 5        │ Day 6        Day 7        Day 8
├────────────┼────────────│─┼────────────┼────────────┼──────────┤
│ L0Table    │ MainTable  │ │ 集成测试    │ Phase 3.1             │
│ 改造       │ 改造       │ │            │ ForL0StateMap         │
└────────────┴────────────┴─┴────────────┴────────────────────────┘
```

| ID | 任务 | 关键改动 | 验收标准 |
|----|------|----------|----------|
| 2.1 | 改造 `L0Table` | - 添加泛型 `<K,N,S>`<br>- get/put/remove 改用对象比较<br>- Pointer 解释为数组下标 | L0Table 单独可测试 |
| 2.2 | 改造 `MainTable` | - 添加泛型 `<K,N,S>`<br>- searchBucketSlots 改用 `entry.matches()`<br>- resize 使用 `entry.hash` | MainTable 单独可测试 |
| 2.3 | 索引层集成测试 | L0Table + MainTable + HeapEntryStore | 集成测试通过 |

**关键代码变更**:

```java
// 改造前 (字节比较)
public long get(int keyHash, short tag, byte[] kb, int klen, 
                byte[] nb, int nlen, EntryStore store);

// 改造后 (对象比较)  
public long get(int keyHash, short tag, K key, N namespace, 
                HeapEntryStore<K,N,S> store);
```

**Phase 2 交付物**:
- 改造后的 `L0Table.java`
- 改造后的 `MainTable.java`
- `src/test/java/.../heap/IndexIntegrationTest.java`

**检查点**: 索引层集成测试通过 ✓

---

### 7.3 Phase 3: ForL0StateMap 重构 (Day 7-9)

**目标**: 重构 ForL0StateMap，移除热路径序列化，实现 Checkpoint 支持

```
Week 2 (续)
─────────────────────────────────────────────────────────────────
Day 7        Day 8        Day 9        Day 10
├────────────┼────────────┼────────────┼────────────┤
│ StateMap   │ Checkpoint │ 全面测试    │ Phase 4    │
│ 核心方法   │ Snapshot   │            │ 清理       │
└────────────┴────────────┴────────────┴────────────┘
```

| ID | 任务 | 关键改动 | 验收标准 |
|----|------|----------|----------|
| 3.1 | 重构 `ForL0StateMap` 核心方法 | - get/put/remove 使用 Object.hashCode()<br>- 移除 serializeKeyNamespace()<br>- 移除 SerializerPack 热路径调用 | 基本操作测试通过 |
| 3.2 | 实现 `HeapStateMapSnapshot` | - Checkpoint 时序列化<br>- 遍历 HeapEntryStore | Checkpoint 测试通过 |
| 3.3 | 实现 Restore 逻辑 | - 反序列化重建 put() | Restore 测试通过 |
| 3.4 | 全面集成测试 | - StateMap 接口所有方法<br>- 迭代器、transform 等 | 所有测试通过 |

**移除的热路径代码**:

```java
// 删除这些热路径调用
- serializeKeyNamespace(key, namespace)
- serializerPack.writeState(state)
- deserializeValueFromArena(entryAddress)

// 保留这些 (仅 Checkpoint 使用)
+ keySerializer.serialize(key, dov)      // Checkpoint 时
+ stateSerializer.deserialize(div)       // Restore 时
```

**Phase 3 交付物**:
- 重构后的 `ForL0StateMap.java`
- 新增 `HeapStateMapSnapshot.java`
- `src/test/java/.../heap/ForL0StateMapTest.java` (更新)
- `src/test/java/.../heap/CheckpointRestoreTest.java`

**检查点**: 所有 ForL0StateMap 测试通过，Checkpoint/Restore 正常 ✓

---

### 7.4 Phase 4: 清理与性能验证 (Day 10-12)

**目标**: 删除旧代码，性能基准测试，完成改造

```
Week 2 (续)          Week 3
─────────────────────────────────────────────────────────────────
Day 10       Day 11       Day 12       Day 13
├────────────┼────────────┼────────────┼────────────┤
│ 代码清理    │ 性能测试    │ 文档&收尾   │ Code Review│
│            │ 基准对比    │            │ & Merge    │
└────────────┴────────────┴────────────┴────────────┘
```

| ID | 任务 | 详细内容 | 验收标准 |
|----|------|----------|----------|
| 4.1 | 删除旧存储代码 | - `entrystore/EntryStore.java`<br>- `entrystore/KeyNsPool.java`<br>- `entrystore/ValuePool.java`<br>- `entrystore/ValueSizeClass.java`<br>- `entrystore/EntryStoreConstants.java` | 编译通过，无引用 |
| 4.2 | 删除序列化优化代码 | - `io/SerializerPack.java`<br>- `io/FixedLengthTypeSupport.java` | 编译通过 |
| 4.3 | 性能基准测试 | - 运行 WordCount benchmark<br>- 对比新旧实现 TPS<br>- 验证 5-10x 提升目标 | 性能达标 |
| 4.4 | 文档更新 | - 更新 README.md<br>- 更新设计说明书<br>- 标注删除的文件 | 文档完整 |

**待删除文件清单**:

```
src/main/java/org/apache/flink/runtime/state/heap/
├── entrystore/
│   ├── EntryStore.java          ← 删除
│   ├── EntryStoreConstants.java ← 删除
│   ├── EntryStoreStats.java     ← 删除 (如果存在)
│   ├── KeyNsPool.java           ← 删除
│   ├── ValuePool.java           ← 删除
│   └── ValueSizeClass.java      ← 删除
└── io/
    ├── SerializerPack.java          ← 删除
    └── FixedLengthTypeSupport.java  ← 删除
```

**Phase 4 交付物**:
- 清理后的代码库
- 性能测试报告 (`benchmark/results/heap-store-comparison.md`)
- 更新后的 README.md

**检查点**: 性能提升 ≥5x，所有测试通过，文档更新完成 ✓

---

### 7.5 里程碑总览

```
┌─────────────────────────────────────────────────────────────────┐
│                        Roadmap 总览                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Week 1                                                          │
│  ┌─────┬─────┬─────┬─────┬─────┐                                │
│  │ D1  │ D2  │ D3  │ D4  │ D5  │                                │
│  ├─────┴─────┴─────┼─────┴─────┤                                │
│  │   Phase 1       │  Phase 2  │                                │
│  │   堆内存储层     │  索引层    │                                │
│  └─────────────────┴───────────┘                                │
│         ▼ Milestone 1: HeapEntryStore 可用                       │
│                                                                  │
│  Week 2                                                          │
│  ┌─────┬─────┬─────┬─────┬─────┐                                │
│  │ D6  │ D7  │ D8  │ D9  │ D10 │                                │
│  ├─────┼─────┴─────┴─────┼─────┤                                │
│  │ P2  │    Phase 3      │ P4  │                                │
│  │续   │ ForL0StateMap   │ 清理│                                │
│  └─────┴─────────────────┴─────┘                                │
│         ▼ Milestone 2: 热路径零序列化                            │
│                                                                  │
│  Week 3                                                          │
│  ┌─────┬─────┬─────┐                                            │
│  │ D11 │ D12 │ D13 │                                            │
│  ├─────┴─────┴─────┤                                            │
│  │    Phase 4      │                                            │
│  │  性能验证&收尾   │                                            │
│  └─────────────────┘                                            │
│         ▼ Milestone 3: 性能达标，合并主分支                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 7.6 时间估算

| 阶段 | 时间 | 累计 | 关键产出 |
|------|------|------|----------|
| Phase 1 | 3 天 | Day 3 | HeapEntryStore 可用 |
| Phase 2 | 3 天 | Day 6 | 索引层改造完成 |
| Phase 3 | 3 天 | Day 9 | ForL0StateMap 重构完成 |
| Phase 4 | 3 天 | Day 12 | 清理完成，性能验证 |
| Buffer | 1 天 | Day 13 | Code Review & Merge |
| **总计** | **13 天** | - | 约 2.5 周 |

### 7.7 风险与依赖

| 风险/依赖 | 影响阶段 | 缓解措施 |
|-----------|----------|----------|
| 索引层改造复杂度超预期 | Phase 2 | 保留 1 天 buffer |
| Checkpoint 兼容性问题 | Phase 3 | 提前设计测试用例 |
| 性能未达预期 | Phase 4 | 预留优化时间 |
| 测试覆盖不足 | 全阶段 | 每个 Phase 包含测试任务 |

---

## 8. 风险评估与缓解

### 8.1 风险评估

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| **GC 停顿增加** | 中 | 中 | 使用 G1/ZGC；监控 GC 指标；必要时调优 |
| **内存使用增加** | 低 | 低 | 对象头开销相比序列化开销可忽略 |
| **回归 bug** | 中 | 高 | 全面测试覆盖；与原实现对比测试 |
| **Checkpoint 变慢** | 低 | 中 | 增量 Checkpoint；异步序列化 |
| **API 兼容性** | 低 | 低 | 保持 StateMap 接口不变 |

### 8.2 改造策略

**彻底改造，不保留旧实现**：

```
┌─────────────────────────────────────────────────────────────────┐
│                         改造策略                                 │
├─────────────────────────────────────────────────────────────────┤
│ 1. 在新分支进行改造，主分支保持稳定                               │
│ 2. 彻底删除旧的序列化存储代码：                                   │
│    - EntryStore, KeyNsPool, ValuePool                           │
│    - SerializerPack, FixedLengthTypeSupport, TupleTypeInfo      │
│ 3. 改造完成后合并回主分支                                        │
│ 4. 不提供运行时切换机制（无需向后兼容）                           │
└─────────────────────────────────────────────────────────────────┘
```

**待删除的类/文件清单**：

| 类/文件 | 位置 | 说明 |
|---------|------|------|
| `EntryStore` | `entrystore/` | 旧的堆外存储 |
| `KeyNsPool` | `entrystore/` | 键命名空间池 |
| `ValuePool` | `entrystore/` | 值池 |
| `ValueSizeClass` | `entrystore/` | 值大小分类 |
| `EntryStoreConstants` | `entrystore/` | 旧常量定义 |
| `SerializerPack` | `io/` | 序列化打包器 |
| `FixedLengthTypeSupport` | `io/` | 定长类型优化 |

---

## 附录 A: 与 CopyOnWriteStateMap 的对比

| 特性 | CopyOnWriteStateMap | ForL0StateMap (新) |
|------|---------------------|-------------------|
| **存储** | 堆内 StateMapEntry[] | 堆内 HeapEntryStore + 堆外索引 |
| **索引** | 链表解决冲突 | L0Table + MainTable (堆外) |
| **哈希** | `bitMix(key ^ ns)` | 相同 |
| **COW 支持** | 有 (entryVersion/stateVersion) | 无 (简化) |
| **Rehash** | 增量 (每次迁移 4 条) | 全量 (一次性迁移) |
| **L0 缓存** | 无 | 有 |

---

## 附录 B: 类图

```
┌─────────────────────────────────────────────────────────────────┐
│                         ForL0StateMap<K,N,S>                     │
├─────────────────────────────────────────────────────────────────┤
│ - mainTable: MainTable<K,N,S>                                    │
│ - l0Table: L0Table<K,N,S>                                        │
│ - entryStore: HeapEntryStore<K,N,S>                              │
│ - keySerializer: TypeSerializer<K>                               │
│ - namespaceSerializer: TypeSerializer<N>                         │
│ - stateSerializer: TypeSerializer<S>                             │
├─────────────────────────────────────────────────────────────────┤
│ + get(K, N): S                                                   │
│ + put(K, N, S): void                                             │
│ + remove(K, N): void                                             │
│ + stateSnapshot(): StateMapSnapshot                              │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ uses
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    HeapEntryStore<K,N,S>                         │
├─────────────────────────────────────────────────────────────────┤
│ - chunks: HeapStateEntry<K,N,S>[][]                              │
│ - freeList: int[]                                                │
│ - activeEntries: int                                             │
├─────────────────────────────────────────────────────────────────┤
│ + allocate(K, N, S): long                                        │
│ + get(long): HeapStateEntry<K,N,S>                               │
│ + matches(long, K, N): boolean                                   │
│ + updateState(long, S): void                                     │
│ + remove(long): void                                             │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ stores
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   HeapStateEntry<K,N,S>                          │
├─────────────────────────────────────────────────────────────────┤
│ + key: K                                                         │
│ + namespace: N                                                   │
│ + state: S                                                       │
│ + hash: int     // tag 可从 hash 计算: (short)(hash >>> 16)       │
├─────────────────────────────────────────────────────────────────┤
│ + matches(K, N): boolean                                         │
│ + setState(S): void                                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 总结

本设计方案将 ForL0 State Backend 从 "堆外索引 + 堆外序列化存储" 改造为 "堆外索引 + 堆内对象存储"，主要收益：

1. **完全消除热路径序列化**：get/put/remove 操作零序列化开销
2. **简化代码**：移除 EntryStore、KeyNsPool、ValuePool 等复杂组件
3. **保留 L0 优势**：继续使用堆外 L0Table/MainTable 实现缓存友好的索引
4. **与 Flink 一致**：采用与 CopyOnWriteStateMap 相似的对象存储方式

预期性能提升 **5-10x**，代码量减少 **80%**。
