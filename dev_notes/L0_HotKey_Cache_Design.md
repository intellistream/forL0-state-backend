# L0 Hot Key Cache 设计方案

> 文档版本: 1.0  
> 创建日期: 2026-01-22  
> 状态: **设计中**  
> 目标: 利用 L0 Cache 加速热键状态访问

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

### 1.2 当前 SwissTable 架构

```
SwissTable<K, S>
├── ctrl[]      byte[]     控制字节，用于 SWAR 并行匹配
├── entries[]   Object[]   AoS 布局 [k0,v0,k1,v1,...]
└── hashes[]    int[]      32 位 hash 存储
```

**特点**：
- 全部在堆内，直接存储 Java 对象引用
- 零序列化开销
- SWAR 并行匹配已经很快

**问题**：
- L0 容量有限，无法容纳所有 SwissTable 的 ctrl[]
- 运行时有多个 StateStore，每个 StateStore 有多个 KeyGroup，每个 KeyGroup 一个 SwissTable

### 1.3 核心洞察

流处理中数据分布通常符合 **Zipf 分布**：少数热键占据大部分访问。

```
访问频率
  │
  │  ████
  │  ████ ██
  │  ████ ██ █
  │  ████ ██ █ █ █ █ . . . . . . . .
  └────────────────────────────────── keys
     热键(~1%)  冷键（大多数）
```

**策略**：不缓存整个 SwissTable，只缓存**热键的直接定位信息**

---

## 2. 设计目标与约束

### 2.1 设计目标

1. **加速热键访问**：热键命中时跳过 SwissTable 的 SWAR probe
2. **固定内存占用**：L0 Cache 容量固定，不随状态增长
3. **自适应淘汰**：冷键自然被热键覆盖
4. **零配置可用**：合理默认值，无需用户调优

### 2.2 设计约束

| 约束 | 说明 |
|------|------|
| **单线程** | 每个 Subtask 独立的 L0Cache，无并发访问 |
| **每 Backend 一个** | 不做 TaskManager 级别共享，保持简单 |
| **L0 容量有限** | 典型 16MB = 2M entries |
| **不引入序列化** | 只存索引信息，不存储实际数据 |

### 2.3 非目标

- 不替代 SwissTable，只作为加速层
- 不保证一致性（L0 缓存可以随时失效，SwissTable 是 source of truth）
- 不处理 Checkpoint（L0 缓存是瞬态的）

---

## 3. 整体架构

### 3.1 架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ForL0KeyedStateBackend (per Subtask)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                   L0HotKeyCache (L0 内存)                            │   │
│   │   容量: 2M entries = 16MB                                            │   │
│   │   Slot: [hash:32 | storeId:8 | keyGroupIdx:8 | tableSlot:16]        │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│          ▲                    ▲                    ▲                        │
│          │                    │                    │                        │
│   ┌──────┴─────────┐  ┌───────┴────────┐  ┌───────┴────────┐               │
│   │ ForL0StateStore│  │ ForL0StateStore│  │ ForL0StateStore│               │
│   │  "valueState"  │  │  "listState"   │  │  "mapState"    │               │
│   │   storeId=0    │  │   storeId=1    │  │   storeId=2    │               │
│   └───────┬────────┘  └───────┬────────┘  └───────┬────────┘               │
│           │                   │                   │                         │
│   ┌───────┴───────────────────┴───────────────────┴───────┐                │
│   │  KeyGroup 0   │  KeyGroup 1   │  ...  │  KeyGroup N   │                │
│   │ SwissTable[0] │ SwissTable[1] │  ...  │ SwissTable[N] │                │
│   └───────────────────────────────────────────────────────┘                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 数据流

```
get(key, namespace)
        │
        ▼
┌───────────────────┐
│ 计算 hash         │
│ hash = smear(key) │
└─────────┬─────────┘
          │
          ▼
┌───────────────────────────────────┐
│ L0HotKeyCache.lookup(hash, ...)  │
│ index = hash & mask               │
│ slot = L0[index]                  │
└─────────┬─────────────────────────┘
          │
    ┌─────┴─────┐
    │           │
   Hit         Miss
    │           │
    ▼           ▼
┌─────────┐  ┌──────────────────────┐
│验证 key │  │ SwissTable.get()     │
│entries[]│  │ SWAR probe           │
└────┬────┘  └──────────┬───────────┘
     │                  │
     │            ┌─────┴─────┐
     │           Found      NotFound
     │            │           │
     │            ▼           │
     │     ┌────────────┐     │
     │     │ 更新 L0Cache│     │
     │     └─────┬──────┘     │
     │           │            │
     └─────┬─────┴────────────┘
           │
           ▼
      返回 value
```

---

## 4. L0HotKeyCache 详细设计

### 4.1 Slot 格式

```
┌─────────────────────────────────────────────────────────────────────┐
│                      L0 Cache Slot (8 bytes)                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   63                              32 31     24 23     16 15        0 │
│   ┌────────────────────────────────┬─────────┬─────────┬────────────┐│
│   │         hash (32 bits)         │ storeId │  kgIdx  │ tableSlot  ││
│   │      完整 32 位 hash 值         │ (8 bits)│ (8 bits)│ (16 bits)  ││
│   └────────────────────────────────┴─────────┴─────────┴────────────┘│
│                                                                      │
│   - hash:      用于快速验证是否匹配                                   │
│   - storeId:   StateStore 编号 (0-255)                               │
│   - kgIdx:     KeyGroup 在 range 内的索引 (0-255)                    │
│   - tableSlot: SwissTable 中的 slot 索引 (0-65535)                   │
│                                                                      │
│   空槽: slot == 0 (因为有效 tableSlot >= 0，但 hash 不太可能全为 0)   │
│   实际判空: 检查整个 long == 0                                        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 容量限制

| 字段 | 位数 | 最大值 | 说明 |
|------|------|--------|------|
| storeId | 8 | 255 | 每个 Backend 最多 256 个 StateStore |
| kgIdx | 8 | 255 | KeyGroup range 内最多 256 个 KeyGroup |
| tableSlot | 16 | 65535 | 每个 SwissTable 最多 64K entries |

**注意**：tableSlot 16 位限制单个 SwissTable 最大 64K entries。如果 SwissTable 超过此大小，该表的热键将无法缓存到 L0。这是可接受的，因为：
1. 大表意味着 key 分布分散，热键效应减弱
2. 大表的 SWAR probe 效率依然很高

### 4.3 索引方式

采用**直接映射**（Direct-Mapped）：

```java
int l0Index = hash & (capacity - 1);
```

- 简单高效，无需复杂的冲突处理
- 冲突时后来者覆盖，自然实现 LRU-like 效果
- 热键访问频繁，会持续"占据"对应 slot

### 4.4 类定义

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
 * <p>特性：
 * <ul>
 *   <li>固定容量，直接映射（无冲突链）</li>
 *   <li>冲突时后来者覆盖，自然淘汰冷键</li>
 *   <li>单线程访问，无需同步</li>
 *   <li>L0 缓存是瞬态的，不参与 Checkpoint</li>
 * </ul>
 * 
 * <p>Slot 格式 (8 bytes):
 * <pre>
 * [hash:32 | storeId:8 | kgIdx:8 | tableSlot:16]
 * </pre>
 */
public class L0HotKeyCache {
    
    private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
    
    // Slot 布局常量
    private static final int HASH_SHIFT = 32;
    private static final int STORE_ID_SHIFT = 24;
    private static final int KG_IDX_SHIFT = 16;
    private static final long TABLE_SLOT_MASK = 0xFFFFL;
    private static final long KG_IDX_MASK = 0xFFL;
    private static final long STORE_ID_MASK = 0xFFL;
    
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
        capacity = Math.max(capacity, 1024);  // 最小 1K entries
        
        long sizeBytes = (long) capacity * 8;
        long address = NativeL0Memory.malloc(sizeBytes);
        
        if (address == 0) {
            return new L0HotKeyCache(0, 0, false);
        }
        
        // 初始化为 0
        NativeL0Memory.memset(address, (byte) 0, sizeBytes);
        
        return new L0HotKeyCache(address, capacity, true);
    }
    
    private L0HotKeyCache(long l0Address, int capacity, boolean enabled) {
        this.l0Address = l0Address;
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.enabled = enabled;
    }
    
    /**
     * 查询热键缓存。
     * 
     * @param hash 32 位 hash 值
     * @param storeId StateStore 编号
     * @param kgIdx KeyGroup 在 range 内的索引
     * @return SwissTable 中的 slot 索引，如果未命中返回 -1
     */
    public int lookup(int hash, int storeId, int kgIdx) {
        if (!enabled) {
            return -1;
        }
        
        lookups++;
        
        int idx = hash & mask;
        long slot = UNSAFE.getLong(l0Address + ((long) idx << 3));
        
        if (slot == 0) {
            return -1;  // 空槽
        }
        
        // 验证 hash + storeId + kgIdx
        int cachedHash = (int) (slot >>> HASH_SHIFT);
        if (cachedHash != hash) {
            return -1;
        }
        
        int cachedStoreId = (int) ((slot >>> STORE_ID_SHIFT) & STORE_ID_MASK);
        int cachedKgIdx = (int) ((slot >>> KG_IDX_SHIFT) & KG_IDX_MASK);
        
        if (cachedStoreId != storeId || cachedKgIdx != kgIdx) {
            return -1;
        }
        
        hits++;
        return (int) (slot & TABLE_SLOT_MASK);
    }
    
    /**
     * 插入或更新热键。
     * 
     * @param hash 32 位 hash 值
     * @param storeId StateStore 编号
     * @param kgIdx KeyGroup 在 range 内的索引
     * @param tableSlot SwissTable 中的 slot 索引
     */
    public void insert(int hash, int storeId, int kgIdx, int tableSlot) {
        if (!enabled || tableSlot > 0xFFFF) {
            return;  // 禁用或 tableSlot 超出 16 位范围
        }
        
        int idx = hash & mask;
        long slot = ((long) hash << HASH_SHIFT)
                  | ((long) (storeId & 0xFF) << STORE_ID_SHIFT)
                  | ((long) (kgIdx & 0xFF) << KG_IDX_SHIFT)
                  | (tableSlot & TABLE_SLOT_MASK);
        
        UNSAFE.putLong(l0Address + ((long) idx << 3), slot);
    }
    
    /**
     * 失效指定 key 的缓存（删除操作时调用）。
     * 
     * @param hash 32 位 hash 值
     * @param storeId StateStore 编号
     * @param kgIdx KeyGroup 在 range 内的索引
     */
    public void invalidate(int hash, int storeId, int kgIdx) {
        if (!enabled) {
            return;
        }
        
        int idx = hash & mask;
        long slot = UNSAFE.getLong(l0Address + ((long) idx << 3));
        
        if (slot == 0) {
            return;
        }
        
        int cachedHash = (int) (slot >>> HASH_SHIFT);
        int cachedStoreId = (int) ((slot >>> STORE_ID_SHIFT) & STORE_ID_MASK);
        int cachedKgIdx = (int) ((slot >>> KG_IDX_SHIFT) & KG_IDX_MASK);
        
        if (cachedHash == hash && cachedStoreId == storeId && cachedKgIdx == kgIdx) {
            UNSAFE.putLong(l0Address + ((long) idx << 3), 0);
        }
    }
    
    /**
     * 清空所有缓存（可选，用于测试或重置）。
     */
    public void clear() {
        if (enabled) {
            NativeL0Memory.memset(l0Address, (byte) 0, (long) capacity * 8);
        }
        lookups = 0;
        hits = 0;
    }
    
    /**
     * 释放 L0 内存。
     */
    public void dispose() {
        if (enabled && l0Address != 0) {
            NativeL0Memory.free(l0Address);
        }
    }
    
    // ========== 统计信息 ==========
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public int getCapacity() {
        return capacity;
    }
    
    public long getLookups() {
        return lookups;
    }
    
    public long getHits() {
        return hits;
    }
    
    public double getHitRate() {
        return lookups > 0 ? (double) hits / lookups : 0.0;
    }
    
    @Override
    public String toString() {
        if (!enabled) {
            return "L0HotKeyCache[disabled]";
        }
        return String.format("L0HotKeyCache[capacity=%d, lookups=%d, hits=%d, hitRate=%.2f%%]",
                capacity, lookups, hits, getHitRate() * 100);
    }
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
        
        // 创建 L0 缓存（默认 2M entries = 16MB）
        int l0CacheCapacity = ForL0Options.getL0CacheCapacity(configuration);
        this.l0HotKeyCache = L0HotKeyCache.create(l0CacheCapacity);
        
        if (l0HotKeyCache.isEnabled()) {
            LOG.info("[ForL0] L0 Hot Key Cache enabled with {} entries", 
                     l0HotKeyCache.getCapacity());
        } else {
            LOG.info("[ForL0] L0 Hot Key Cache disabled (L0 memory not available)");
        }
    }
    
    @Override
    public void dispose() {
        super.dispose();
        if (l0HotKeyCache != null) {
            LOG.info("[ForL0] L0 Cache stats: {}", l0HotKeyCache);
            l0HotKeyCache.dispose();
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
     * 带 L0 缓存加速的 get 操作。
     */
    public S get(K key, N namespace, int keyGroup) {
        int kgIdx = keyGroup - keyGroupOffset;
        int hash = computeKeyHash(key);
        
        // 1. 尝试 L0 缓存
        if (l0HotKeyCache != null && isVoidNamespace) {
            int cachedSlot = l0HotKeyCache.lookup(hash, storeId, kgIdx);
            if (cachedSlot >= 0) {
                SwissTable<K, S> table = tables[kgIdx];
                if (table != null) {
                    // 验证 key（防止 hash 冲突）
                    @SuppressWarnings("unchecked")
                    K cachedKey = (K) table.entries[cachedSlot << 1];
                    if (key.equals(cachedKey)) {
                        @SuppressWarnings("unchecked")
                        S value = (S) table.entries[(cachedSlot << 1) + 1];
                        return value;
                    }
                    // Hash 冲突，L0 缓存失效，继续走正常路径
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
        int slot = table.getSlot(hash, key);
        if (slot < 0) {
            return null;
        }
        
        // 3. 更新 L0 缓存（仅 VoidNamespace 模式）
        if (l0HotKeyCache != null && isVoidNamespace) {
            l0HotKeyCache.insert(hash, storeId, kgIdx, slot);
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
        int hash = computeKeyHash(key);
        
        // 失效 L0 缓存
        if (l0HotKeyCache != null && isVoidNamespace) {
            l0HotKeyCache.invalidate(hash, storeId, kgIdx);
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
     * @param hash 32 位 hash
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
     * 每个 entry 8 字节，默认 2M entries = 16MB。
     */
    public static final ConfigOption<Integer> L0_CACHE_CAPACITY =
            ConfigOptions.key("state.backend.forl0.l0-cache.capacity")
                    .intType()
                    .defaultValue(2 * 1024 * 1024)  // 2M entries
                    .withDescription("L0 Hot Key Cache capacity in number of entries. " +
                            "Each entry is 8 bytes. Default is 2M entries (16MB). " +
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
| 小规模 | 256K entries | 2MB | 开发测试 |
| 中规模 | 2M entries | 16MB | **默认值** |
| 大规模 | 8M entries | 64MB | 大量热键场景 |
| 极端场景 | 32M entries | 256MB | 需要充足 L0 容量 |

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

### Phase 1: 基础实现

| 步骤 | 任务 | 文件 |
|------|------|------|
| 1.1 | 实现 `L0HotKeyCache` 类 | `space/L0HotKeyCache.java` |
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
| 3.2 | 集成测试：验证 L0 模式和模拟模式 |
| 3.3 | Benchmark：测量热键加速效果 |
| 3.4 | 调优：根据 benchmark 调整默认配置 |

### Phase 4: 扩展（可选）

| 步骤 | 任务 | 说明 |
|------|------|------|
| 4.1 | 支持 General Namespace 模式 | 需要额外存储 namespace 信息 |
| 4.2 | 采样更新优化 | 减少 L0 写入频率 |
| 4.3 | 多路关联缓存 | 降低冲突率 |

---

## 附录

### A. 为什么只支持 VoidNamespace 模式

1. **Slot 格式限制**：当前 8 字节 slot 没有空间存储 namespace 信息
2. **VoidNamespace 是主流**：ValueState、ReducingState、AggregatingState 都使用 VoidNamespace
3. **简化实现**：先验证核心收益，再考虑扩展

### B. 与之前 MainTable + L0Table 设计的区别

| 维度 | 之前设计 | 当前设计 |
|------|----------|----------|
| L0 用途 | 存储完整索引 + 序列化数据 | 只存热键直接定位信息 |
| 数据存储 | 堆外序列化 | 堆内对象引用 |
| 复杂度 | 高（多层结构） | 低（单层缓存） |
| L0 容量要求 | 随状态增长 | 固定大小 |

### C. 失效策略说明

当前采用**自然失效**策略：
- 冲突时后来者覆盖
- 删除时主动失效
- 无 LRU/LFU 等复杂淘汰机制

这适用于大多数流处理场景，因为热键会持续访问，自然"保持"在缓存中。
