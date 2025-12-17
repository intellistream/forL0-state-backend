# EntryArena 重构设计方案

> 文档版本: 1.3  
> 创建日期: 2025-12-16  
> 更新日期: 2025-12-17  
> 状态: 设计草案  
> **注意: 本次为彻底改造，不考虑向后兼容性**

## 版本历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| 1.0 | 2025-12-16 | 初始设计：键值分离架构 |
| 1.1 | 2025-12-16 | 改进 updateValue 接口语义 |
| 1.2 | 2025-12-16 | 添加实施计划 (Chapter 6) |
| **1.3** | **2025-12-17** | **内存优化：细粒度 Size Class (29个) + 小值内联 (≤8B)** |

## 目录

1. [背景与目标](#1-背景与目标)
2. [现有实现分析](#2-现有实现分析)
3. [改造方案概述](#3-改造方案概述)
4. [详细设计](#4-详细设计)
5. [接口设计与上层适配](#5-接口设计与上层适配)
6. [实施计划](#6-实施计划)
7. [风险评估](#7-风险评估)

---

## 1. 背景与目标

### 1.1 问题背景

当前 `EntryArena` 采用键值一体存储的设计，存在以下问题：

| 问题 | 影响 |
|------|------|
| **Value 更新触发重分配** | 当新 value 大于旧 value 时，需要重新分配整个 entry（包含 key + namespace + value），导致索引指针变更 |
| **Free List 链表追逐** | 使用单链表管理空闲块，分配时需要扫描链表（bounded scan），缓存不友好 |
| **大小混杂的碎片** | 不同大小的 entry 混合在同一个 slab 中，导致外部碎片和内部碎片 |
| **缺乏预算控制** | 没有精确的内存预算机制，OOM 时缺乏有效的降级策略 |
| **Size Class 粒度粗** | 8 个 size class 倍增步长，最坏内部碎片约 50% |
| **小值分配开销大** | 即使 1B 的值也需要 ValuePool 槽位 + 指针间接跳转 |

### 1.2 改造目标

```
┌─────────────────────────────────────────────────────────────────┐
│                        改造目标                                   │
├─────────────────────────────────────────────────────────────────┤
│ 1. 键值分离：索引指针指向键区，键区引用值区，value 更新不影响索引   │
│ 2. 极简分配：键区追加写，值区 size-class + bitmap，减少分配开销     │
│ 3. 缓存友好：消除链表追逐，使用 bitmap 管理空闲块                   │
│ 4. 可控碎片：键区段级回收，值区 size-class 隔离                    │
│ 5. OOM 防护：分级预算 + 优先级回收                                 │
│ 6. 细粒度 Size Class：29 个 size class，最大碎片从 50% 降至 12%    │
│ 7. 小值内联：≤8B 的值直接存储在 KeyNsPool，避免 ValuePool 开销     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 现有实现分析

### 2.1 当前 Entry 布局

```
┌─────────────────────────────────────────────────────────────────┐
│                    Current Entry Layout                          │
├────────┬────────┬────────┬────────┬──────┬───────────┬──────────┤
│ hash   │ keyLen │ nsLen  │ valLen │ key  │ namespace │  value   │
│ (4B)   │ (4B)   │ (4B)   │ (4B)   │(var) │   (var)   │  (var)   │
├────────┴────────┴────────┴────────┴──────┴───────────┴──────────┤
│                     ENTRY_HEADER_SIZE = 16B                      │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 当前分配策略

```java
// 现有分配流程
allocateEntry(size):
    1. allocateFromFreeListBounded(size, MAX_SCAN=64)  // 扫描 free list
    2. allocateFromCurrentSegment(size)                 // bump alloc
    3. linearAllocate(size)                             // 扩展分配
        - 二次 free list 扫描
        - 使用预分配 segment
        - 分配新 segment
```

### 2.3 当前问题示例

**Value 更新导致指针变更**：

```
Before Update:
  MainTable slot → entry@0x1000 [key|ns|value_old]
  
After Update (new_value > old_value):
  1. Allocate new entry@0x2000 [key|ns|value_new]
  2. Update MainTable slot → entry@0x2000
  3. Update L0Table slot → entry@0x2000
  4. Free old entry@0x1000
```

---

## 3. 改造方案概述

### 3.1 新架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                      EntryStore (新架构)                         │
├─────────────────────────────────┬───────────────────────────────┤
│          KeyNsPool              │          ValuePool             │
│         (键命名空间池)            │           (值池)               │
├─────────────────────────────────┼───────────────────────────────┤
│  ┌─────────────────────────┐    │  ┌─────────────────────────┐  │
│  │ Segment 0 (追加写)       │    │  │  Small Value Runs       │  │
│  │ [entry][entry][entry]...│    │  │  (size-class + bitmap)  │  │
│  ├─────────────────────────┤    │  ├─────────────────────────┤  │
│  │ Segment 1 (追加写)       │    │  │  Medium Value Runs      │  │
│  │ [entry][entry]...       │    │  │  (size-class + bitmap)  │  │
│  ├─────────────────────────┤    │  ├─────────────────────────┤  │
│  │ Segment N               │    │  │  Large Object Pool      │  │
│  │ ...                     │    │  │  (页对齐分配)            │  │
│  └─────────────────────────┘    │  └─────────────────────────┘  │
│                                 │                               │
│  ✓ 追加写分配 (O(1))            │  ✓ Size-class 分配 (O(1))     │
│  ✓ 段级存活计数                 │  ✓ Bitmap 管理 (缓存友好)      │
│  ✓ 空段整体释放                 │  ✓ 原地更新优先               │
└─────────────────────────────────┴───────────────────────────────┘
                    │
                    ▼
        ┌─────────────────────────┐
        │     MemoryBudget        │
        │   (统一预算与回收)        │
        └─────────────────────────┘
```

### 3.2 新 Entry 布局

```
┌─────────────────────────────────────────────────────────────────┐
│              KeyNsPool Entry Layout (支持小值内联)                │
├────────┬────────────┬────────┬────────┬─────────────────────────┤
│ hash   │ mode+keyLen│ nsLen  │padding │ valueHandle/inlineValue │
│ (4B)   │   (4B)     │ (2B)   │ (2B)   │         (8B)            │
├────────┼────────────┴────────┴────────┴─────────────────────────┤
│        │           key (variable)                                │
├────────┼─────────────────────────────────────────────────────────┤
│        │         namespace (variable)                            │
├────────┴─────────────────────────────────────────────────────────┤
│              KEY_ENTRY_HEADER_SIZE = 20B                         │
└─────────────────────────────────────────────────────────────────┘

mode+keyLen 字段 (4B):
┌─────────┬─────────────┬─────────────────────────┐
│ mode    │ inlineLen   │       keyLen            │
│ (1 bit) │ (4 bits)    │      (27 bits)          │
└─────────┴─────────────┴─────────────────────────┘
- mode=0: pointer 模式，最后 8B 存储 ValuePool 地址
- mode=1: inline 模式，最后 8B 直接存储值内容 (≤8B)

valueHandle/inlineValue 字段 (8B):
┌─────────────────────────────────────────────────────────────────┐
│  pointer 模式: ValuePool 地址                                    │
│  inline 模式:  直接存储值内容 (≤8B，小端序，高位补零)              │
└─────────────────────────────────────────────────────────────────┘
```

┌─────────────────────────────────────────────────────────────────┐
│                    ValuePool Entry Layout                        │
├────────┬────────────────────────────────────────────────────────┤
│ valLen │                      value                              │
│ (4B)   │                      (var)                              │
├────────┴────────────────────────────────────────────────────────┤
│              VALUE_ENTRY_HEADER_SIZE = 4B                        │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 关键设计决策

| 决策项 | 选择 | 理由 |
|--------|------|------|
| valueHandle 大小 | 8 字节 | 统一指针格式或内联小值 |
| KeyNsPool 分配 | 追加写 | key/namespace 通常不变，适合 append-only |
| ValuePool 分配 | size-class + bitmap | value 频繁更新，需要高效重用 |
| 大对象阈值 | 4KB | 平衡小对象池效率与大对象独立管理 |
| Segment 大小 | 64KB (页对齐) | 与 MemoryManager 页大小对齐 |
| 类命名 | EntryStore 替代 EntryArena | 彻底重命名，避免混淆 |
| **小值内联** | ≤8B 内联到 KeyNsPool | 避免 ValuePool 开销，减少指针间接跳转 |
| **细粒度 Size Class** | 29 个 size class | 降低内部碎片从 ~50% 到 ~12% |

---

## 4. 详细设计

### 4.1 KeyNsPool 设计

#### 4.1.1 数据结构与内存布局

**Entry Layout (支持小值内联)**：

```
┌────────┬────────────┬────────┬────────┬─────────────────────┬──────┬───────────┐
│ hash   │ mode+keyLen│ nsLen  │padding │ valueHandle/inline  │ key  │ namespace │
│ (4B)   │   (4B)     │ (2B)   │ (2B)   │       (8B)          │(var) │   (var)   │
└────────┴────────────┴────────┴────────┴─────────────────────┴──────┴───────────┘
          KEY_ENTRY_HEADER_SIZE = 20B (保持不变)
```

**mode+keyLen 字段编码 (4B)**：
```
┌─────────┬─────────────┬─────────────────────────┐
│ mode    │ inlineLen   │       keyLen            │
│ (1 bit) │ (4 bits)    │      (27 bits)          │
└─────────┴─────────────┴─────────────────────────┘
 bit 31    bits 30-27     bits 26-0

- mode = 0: pointer 模式，valueHandle 存储 ValuePool 地址
- mode = 1: inline 模式，valueHandle 直接存储值内容
- inlineLen: 内联值长度 (0-8)，仅 mode=1 时有效
- keyLen: key 长度 (最大 128MB，远超需要)
```

**nsLen 字段 (2B)**：
- 支持最大 64KB namespace，足够使用

**设计优势**：
- Header 大小保持 20B 不变
- ≤8B 的值直接内联，避免 ValuePool 开销
- 与原有地址编码兼容

```java
/**
 * KeyNsPool: 键/命名空间存储池
 * 采用追加写策略，支持小值内联，仅做段级回收
 */
public class KeyNsPool implements AutoCloseable {
    
    // ===== 常量定义 =====
    private static final int KEY_ENTRY_HEADER_SIZE = 20;
    private static final int HASH_OFFSET = 0;
    private static final int MODE_KEYLEN_OFFSET = 4;   // mode(1b) + inlineLen(4b) + keyLen(27b)
    private static final int NS_LEN_OFFSET = 8;
    private static final int PADDING_OFFSET = 10;      // 2B padding for alignment
    private static final int VALUE_HANDLE_OFFSET = 12;
    private static final int KEY_DATA_OFFSET = 20;
    
    // Mode 标志位
    private static final int MODE_POINTER = 0;         // valueHandle 存储 ValuePool 地址
    private static final int MODE_INLINE = 1;          // valueHandle 直接存储值
    private static final int MODE_SHIFT = 31;
    private static final int INLINE_LEN_SHIFT = 27;
    private static final int INLINE_LEN_MASK = 0x0F;   // 4 bits
    private static final int KEY_LEN_MASK = 0x07FFFFFF; // 27 bits
    
    // 内联阈值
    public static final int INLINE_THRESHOLD = 8;      // ≤8B 内联
    
    // ===== Segment 管理 =====
    private final MemoryManagerAllocator allocator;
    private final int segmentSize;
    
    private MemorySegment[] segments;
    private List<List<MemorySegment>> allocHandles;
    private int segmentCount;
    
    // ===== 当前写入状态 =====
    private int currentSegmentIndex;
    private int currentOffset;
    
    // ===== 存活计数 (段级回收) =====
    private int[] segmentLiveCount;
    private Deque<Integer> freeSegmentIndices;
    
    // ===== 统计 =====
    private long totalAllocated;
    private int activeEntries;
}
```

#### 4.1.2 核心操作

```java
/**
 * 分配新的 Key Entry (追加写，支持小值内联)
 * 
 * @param valueLen 值长度 (≤8B 自动内联)
 * @param valueHandle 值句柄 (内联时为实际值，pointer 模式为 ValuePool 地址)
 * @return entry 地址: (segment_index + 1) << 32 | (offset + 1)
 */
public long allocate(int hash, byte[] key, int keyLen, byte[] ns, int nsLen, 
                     int valueLen, long valueHandle) {
    int entrySize = align8(KEY_ENTRY_HEADER_SIZE + keyLen + nsLen);
    
    // 1. 检查当前 segment 是否有足够空间
    if (currentOffset + entrySize > segmentSize) {
        if (!switchToNextSegment()) {
            return 0;  // OOM
        }
    }
    
    // 2. 追加写入 (极简路径)
    int segIdx = currentSegmentIndex;
    int offset = currentOffset;
    MemorySegment seg = segments[segIdx];
    
    // 3. 编码 mode+keyLen
    boolean isInline = (valueLen <= INLINE_THRESHOLD);
    int modeKeyLen = (isInline ? (MODE_INLINE << MODE_SHIFT) : 0)
                   | ((valueLen & INLINE_LEN_MASK) << INLINE_LEN_SHIFT)
                   | (keyLen & KEY_LEN_MASK);
    
    // 写入 header
    seg.putInt(offset + HASH_OFFSET, hash);
    seg.putInt(offset + MODE_KEYLEN_OFFSET, modeKeyLen);
    seg.putShort(offset + NS_LEN_OFFSET, (short) nsLen);
    seg.putLong(offset + VALUE_HANDLE_OFFSET, valueHandle);  // 内联值或指针
    
    // 写入 key + namespace
    seg.put(offset + KEY_DATA_OFFSET, key, 0, keyLen);
    seg.put(offset + KEY_DATA_OFFSET + keyLen, ns, 0, nsLen);
    
    // 4. 更新状态
    currentOffset += entrySize;
    segmentLiveCount[segIdx]++;
    activeEntries++;
    totalAllocated += entrySize;
    
    return encodeAddress(segIdx, offset);
}

/**
 * 检查 entry 是否为内联模式
 */
public boolean isInlineValue(long address) {
    int segIdx = decodeSegmentIndex(address);
    int offset = decodeOffset(address);
    int modeKeyLen = segments[segIdx].getInt(offset + MODE_KEYLEN_OFFSET);
    return (modeKeyLen >>> MODE_SHIFT) == MODE_INLINE;
}

/**
 * 获取内联值长度
 */
public int getInlineValueLen(long address) {
    int segIdx = decodeSegmentIndex(address);
    int offset = decodeOffset(address);
    int modeKeyLen = segments[segIdx].getInt(offset + MODE_KEYLEN_OFFSET);
    return (modeKeyLen >>> INLINE_LEN_SHIFT) & INLINE_LEN_MASK;
}

/**
 * 获取内联值 (直接从 valueHandle 字段读取)
 */
public long getInlineValue(long address) {
    int segIdx = decodeSegmentIndex(address);
    int offset = decodeOffset(address);
    return segments[segIdx].getLong(offset + VALUE_HANDLE_OFFSET);
}

/**
 * 更新内联值 (原地更新)
 */
public void updateInlineValue(long address, long newValue, int newLen) {
    int segIdx = decodeSegmentIndex(address);
    int offset = decodeOffset(address);
    MemorySegment seg = segments[segIdx];
    
    // 更新 inlineLen
    int modeKeyLen = seg.getInt(offset + MODE_KEYLEN_OFFSET);
    modeKeyLen = (modeKeyLen & ~(INLINE_LEN_MASK << INLINE_LEN_SHIFT))
               | ((newLen & INLINE_LEN_MASK) << INLINE_LEN_SHIFT);
    seg.putInt(offset + MODE_KEYLEN_OFFSET, modeKeyLen);
    
    // 更新值
    seg.putLong(offset + VALUE_HANDLE_OFFSET, newValue);
}

/**
 * 释放 Key Entry (仅做存活计数)
 */
public void free(long address) {
    int segIdx = decodeSegmentIndex(address);
    segmentLiveCount[segIdx]--;
    activeEntries--;
    
    // 检查是否可以释放整段
    if (segmentLiveCount[segIdx] == 0) {
        releaseSegment(segIdx);
    }
}

/**
 * 更新 valueHandle (原地更新，不改变地址)
 * 仅用于 pointer 模式
 */
public void updateValueHandle(long address, long newValueHandle) {
    int segIdx = decodeSegmentIndex(address);
    int offset = decodeOffset(address);
    segments[segIdx].putLong(offset + VALUE_HANDLE_OFFSET, newValueHandle);
}
```

#### 4.1.3 段级回收

```java
/**
 * 释放空段
 */
private void releaseSegment(int segIdx) {
    // 不释放当前写入段
    if (segIdx == currentSegmentIndex) {
        currentOffset = 0;  // 重置偏移，重用空间
        return;
    }
    
    // 释放内存
    List<MemorySegment> handle = allocHandles.get(segIdx);
    if (handle != null) {
        allocator.release(handle);
        allocHandles.set(segIdx, null);
        segments[segIdx] = null;
        freeSegmentIndices.addLast(segIdx);
    }
}
```

### 4.2 ValuePool 设计

#### 4.2.1 Size Class 定义 (细粒度优化)

```java
/**
 * Value Size Classes - 细粒度设计
 * 
 * 设计原则:
 * - 小值区间 (≤128B): 16B 步长，最大碎片 ~12%
 * - 中值区间 (128B-512B): 32-64B 步长，最大碎片 ~15%
 * - 大值区间 (512B-4KB): 128-512B 步长，最大碎片 ~20%
 * - 超大值 (>4KB): 独立分配
 * 
 * 相比原设计 (8 个 size class，~50% 碎片)，大幅提升内存利用率
 */
public enum ValueSizeClass {
    // ===== 小值区间: 16B 步长 (8 个) =====
    VS_16   (0,    16,    16),    // ≤16B   (含 4B header，实际值 ≤12B)
    VS_32   (16,   32,    32),    // 17-32B
    VS_48   (32,   48,    48),    // 33-48B
    VS_64   (48,   64,    64),    // 49-64B
    VS_80   (64,   80,    80),    // 65-80B
    VS_96   (80,   96,    96),    // 81-96B
    VS_112  (96,   112,   112),   // 97-112B
    VS_128  (112,  128,   128),   // 113-128B
    
    // ===== 中小值区间: 32B 步长 (4 个) =====
    VS_160  (128,  160,   160),   // 129-160B
    VS_192  (160,  192,   192),   // 161-192B
    VS_224  (192,  224,   224),   // 193-224B
    VS_256  (224,  256,   256),   // 225-256B
    
    // ===== 中值区间: 64B 步长 (4 个) =====
    VS_320  (256,  320,   320),   // 257-320B
    VS_384  (320,  384,   384),   // 321-384B
    VS_448  (384,  448,   448),   // 385-448B
    VS_512  (448,  512,   512),   // 449-512B
    
    // ===== 中大值区间: 128B 步长 (4 个) =====
    VS_640  (512,  640,   640),   // 513-640B
    VS_768  (640,  768,   768),   // 641-768B
    VS_896  (768,  896,   896),   // 769-896B
    VS_1K   (896,  1024,  1024),  // 897-1024B
    
    // ===== 大值区间: 256B 步长 (4 个) =====
    VS_1280 (1024, 1280,  1280),  // 1025-1280B
    VS_1536 (1280, 1536,  1536),  // 1281-1536B
    VS_1792 (1536, 1792,  1792),  // 1537-1792B
    VS_2K   (1792, 2048,  2048),  // 1793-2048B
    
    // ===== 超大值区间: 512B 步长 (4 个) =====
    VS_2560 (2048, 2560,  2560),  // 2049-2560B
    VS_3K   (2560, 3072,  3072),  // 2561-3072B
    VS_3584 (3072, 3584,  3584),  // 3073-3584B
    VS_4K   (3584, 4096,  4096),  // 3585-4096B
    
    // ===== 超大对象: 独立分配 =====
    LARGE   (4096, Integer.MAX_VALUE, -1);  // >4KB
    
    final int minSize;   // 最小值 (exclusive)
    final int maxSize;   // 最大值 (inclusive)
    final int slotSize;  // 槽位大小 (-1 表示按需分配)
    
    // 固定 size class 数量 (不含 LARGE)
    public static final int FIXED_SIZE_CLASS_COUNT = 28;
    
    /**
     * 快速查找 size class
     * 使用查找表实现 O(1) 查找
     */
    private static final ValueSizeClass[] LOOKUP_TABLE = buildLookupTable();
    
    static ValueSizeClass getSizeClass(int totalSize) {
        if (totalSize <= 0) return VS_16;
        if (totalSize > 4096) return LARGE;
        
        // 使用查找表: index = (totalSize - 1) / 16
        // 对于 ≤4096 的值，查找表最大 256 项
        int index = (totalSize - 1) >> 4;  // / 16
        if (index < LOOKUP_TABLE.length) {
            return LOOKUP_TABLE[index];
        }
        return LARGE;
    }
    
    private static ValueSizeClass[] buildLookupTable() {
        // 每 16B 一个槽位，最大 256 项 (覆盖 0-4096B)
        ValueSizeClass[] table = new ValueSizeClass[256];
        for (ValueSizeClass sc : values()) {
            if (sc == LARGE) continue;
            int startIdx = sc.minSize >> 4;
            int endIdx = Math.min((sc.maxSize - 1) >> 4, 255);
            for (int i = startIdx; i <= endIdx; i++) {
                table[i] = sc;
            }
        }
        return table;
    }
}
```

**Size Class 分布分析**：

| 区间 | Size Classes | 步长 | 最大内部碎片 | 典型场景 |
|------|--------------|------|--------------|----------|
| 0-128B | 8 个 | 16B | ~12% | 计数器、短字符串 |
| 128-256B | 4 个 | 32B | ~14% | 小对象、元数据 |
| 256-512B | 4 个 | 64B | ~17% | 中等对象 |
| 512-1KB | 4 个 | 128B | ~17% | 较大对象 |
| 1-2KB | 4 个 | 256B | ~17% | 大对象 |
| 2-4KB | 4 个 | 512B | ~17% | 超大对象 |
| >4KB | LARGE | 按需 | ~0% | 独立分配 |

#### 4.2.2 Run 结构 (页内 Bitmap 管理)

```java
/**
 * Run: 固定大小槽位的内存页
 * 使用 bitmap 管理槽位分配状态
 */
public class Run {
    // Run 元数据
    final ValueSizeClass sizeClass;
    final int slotSize;
    final int slotCount;
    
    // 内存
    final MemorySegment segment;
    final int baseOffset;
    final int runSize;
    
    // Bitmap (1 bit per slot, 1=used, 0=free)
    final long[] bitmap;
    
    // 统计
    int usedSlots;
    
    // Run 状态
    enum State { EMPTY, PARTIAL, FULL }
    State state;
    
    /**
     * 分配一个槽位
     * @return slot offset within run, or -1 if full
     */
    int allocateSlot() {
        if (usedSlots >= slotCount) return -1;
        
        // 使用 Long.numberOfTrailingZeros 快速找到第一个空闲位
        for (int i = 0; i < bitmap.length; i++) {
            if (bitmap[i] != -1L) {  // 有空闲位
                int bit = Long.numberOfTrailingZeros(~bitmap[i]);
                if (bit < 64) {
                    int slotIdx = i * 64 + bit;
                    if (slotIdx < slotCount) {
                        bitmap[i] |= (1L << bit);
                        usedSlots++;
                        updateState();
                        return slotIdx * slotSize;
                    }
                }
            }
        }
        return -1;
    }
    
    /**
     * 释放一个槽位
     */
    void freeSlot(int slotOffset) {
        int slotIdx = slotOffset / slotSize;
        int wordIdx = slotIdx / 64;
        int bitIdx = slotIdx % 64;
        bitmap[wordIdx] &= ~(1L << bitIdx);
        usedSlots--;
        updateState();
    }
    
    private void updateState() {
        if (usedSlots == 0) state = State.EMPTY;
        else if (usedSlots == slotCount) state = State.FULL;
        else state = State.PARTIAL;
    }
}
```

#### 4.2.3 ValuePool 主结构

```java
/**
 * ValuePool: 值存储池
 * 采用 size-class 分配 + Run/Bitmap 管理
 */
public class ValuePool implements AutoCloseable {
    
    private static final int VALUE_HEADER_SIZE = 4;  // valueLen(4)
    private static final int RUN_SIZE = 64 * 1024;   // 64KB per run
    
    // ===== Run 管理 (按 size class) =====
    private final Deque<Run>[] partialRuns;  // 部分使用的 runs
    private final Deque<Run>[] emptyRuns;    // 空闲 runs (可回收)
    // Full runs 不需要跟踪，等待槽位释放后自动变为 partial
    
    // ===== 大对象池 =====
    private final LargeObjectPool largePool;
    
    // ===== 分配器 =====
    private final MemoryManagerAllocator allocator;
    
    /**
     * 分配 value 空间
     * @return value 地址
     */
    public long allocate(int valueLen) {
        int totalSize = VALUE_HEADER_SIZE + valueLen;
        ValueSizeClass sc = ValueSizeClass.getSizeClass(totalSize);
        
        if (sc == ValueSizeClass.LARGE) {
            return largePool.allocate(totalSize);
        }
        
        return allocateFromRun(sc, totalSize);
    }
    
    private long allocateFromRun(ValueSizeClass sc, int totalSize) {
        int scIdx = sc.ordinal();
        
        // 1. 尝试从 partial run 分配
        Run run = partialRuns[scIdx].peekFirst();
        if (run != null) {
            int slotOffset = run.allocateSlot();
            if (slotOffset >= 0) {
                if (run.state == Run.State.FULL) {
                    partialRuns[scIdx].pollFirst();  // 移出 partial 列表
                }
                return encodeValueAddress(run, slotOffset);
            }
        }
        
        // 2. 尝试复用 empty run
        run = emptyRuns[scIdx].pollFirst();
        if (run == null) {
            // 3. 分配新 run
            run = allocateNewRun(sc);
            if (run == null) return 0;  // OOM
        }
        
        int slotOffset = run.allocateSlot();
        partialRuns[scIdx].addFirst(run);
        return encodeValueAddress(run, slotOffset);
    }
    
    /**
     * 释放 value 空间
     */
    public void free(long valueHandle) {
        if (isLargeObject(valueHandle)) {
            largePool.free(valueHandle);
            return;
        }
        
        Run run = decodeRun(valueHandle);
        int slotOffset = decodeSlotOffset(valueHandle);
        Run.State oldState = run.state;
        
        run.freeSlot(slotOffset);
        
        // 状态转换处理
        if (oldState == Run.State.FULL && run.state == Run.State.PARTIAL) {
            partialRuns[run.sizeClass.ordinal()].addFirst(run);
        } else if (run.state == Run.State.EMPTY) {
            partialRuns[run.sizeClass.ordinal()].remove(run);
            emptyRuns[run.sizeClass.ordinal()].addLast(run);
        }
    }
    
    /**
     * 尝试原地更新 (新值 ≤ 已分配槽位大小)
     */
    public boolean updateInPlace(long valueHandle, byte[] newValue, int newLen) {
        if (isLargeObject(valueHandle)) {
            return largePool.updateInPlace(valueHandle, newValue, newLen);
        }
        
        Run run = decodeRun(valueHandle);
        int slotOffset = decodeSlotOffset(valueHandle);
        int slotSize = run.slotSize;
        
        int totalSize = VALUE_HEADER_SIZE + newLen;
        if (totalSize <= slotSize) {
            // 可以原地更新
            int absOffset = run.baseOffset + slotOffset;
            run.segment.putInt(absOffset, newLen);
            run.segment.put(absOffset + VALUE_HEADER_SIZE, newValue, 0, newLen);
            return true;
        }
        return false;
    }
}
```

### 4.3 大对象池设计

```java
/**
 * LargeObjectPool: 大对象(>4KB)独立分配
 * 按页对齐分配，避免污染小对象池
 */
public class LargeObjectPool implements AutoCloseable {
    
    private static final int PAGE_SIZE = 4096;
    
    // 大对象跟踪: address -> (segment, size)
    private final Map<Long, LargeAllocation> allocations;
    
    private final MemoryManagerAllocator allocator;
    
    static class LargeAllocation {
        final List<MemorySegment> handle;
        final MemorySegment segment;
        final int offset;
        final int allocatedSize;
        int usedSize;
    }
    
    public long allocate(int size) {
        int alignedSize = alignToPage(size);
        List<MemorySegment> handle = allocator.allocate(alignedSize);
        if (handle.isEmpty()) return 0;
        
        MemorySegment seg = handle.get(0);
        long addr = encodeLargeAddress(seg, 0);
        allocations.put(addr, new LargeAllocation(handle, seg, 0, alignedSize, size));
        return addr;
    }
    
    public void free(long address) {
        LargeAllocation alloc = allocations.remove(address);
        if (alloc != null) {
            allocator.release(alloc.handle);
        }
    }
    
    public boolean updateInPlace(long address, byte[] newValue, int newLen) {
        LargeAllocation alloc = allocations.get(address);
        if (alloc != null && VALUE_HEADER_SIZE + newLen <= alloc.allocatedSize) {
            alloc.segment.putInt(alloc.offset, newLen);
            alloc.segment.put(alloc.offset + VALUE_HEADER_SIZE, newValue, 0, newLen);
            alloc.usedSize = VALUE_HEADER_SIZE + newLen;
            return true;
        }
        return false;
    }
    
    private int alignToPage(int size) {
        return (size + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
    }
}
```

### 4.4 统一入口 EntryStore

```java
/**
 * EntryStore: 键值分离的条目存储
 * 统一管理 KeyNsPool 和 ValuePool，对外提供简洁接口
 */
public class EntryStore implements AutoCloseable {
    
    private final KeyNsPool keyNsPool;
    private final ValuePool valuePool;
    private final MemoryManagerAllocator allocator;
    
    // ========== 构造器 ==========
    
    public EntryStore(MemoryManagerAllocator allocator) {
        this(allocator, 0);
    }
    
    public EntryStore(MemoryManagerAllocator allocator, long initialSizeBytes) {
        this.allocator = allocator;
        this.keyNsPool = new KeyNsPool(allocator);
        this.valuePool = new ValuePool(allocator);
        
        if (initialSizeBytes > 0) {
            preAllocate(initialSizeBytes);
        }
    }
    
    // ========== 写入操作 ==========
    
    /**
     * 分配并存储新条目
     * @param valueBuffer 可为 null，此时预留 valueLen 空间用于零拷贝写入
     * @return 条目地址，0 表示分配失败
     */
    public long allocateEntry(int hash, 
                              byte[] keyBuffer, int keyLen, 
                              byte[] nsBuffer, int nsLen, 
                              byte[] valueBuffer, int valueLen) {
        // 1. 分配 value 空间
        long valueHandle = 0;
        if (valueLen > 0) {
            valueHandle = valuePool.allocate(valueLen);
            if (valueHandle == 0) return 0;  // OOM
            
            // 写入 value (如果提供了数据)
            if (valueBuffer != null) {
                valuePool.write(valueHandle, valueBuffer, valueLen);
            }
        }
        
        // 2. 分配 key entry
        long keyAddr = keyNsPool.allocate(hash, keyBuffer, keyLen, nsBuffer, nsLen, valueHandle);
        if (keyAddr == 0) {
            if (valueHandle != 0) valuePool.free(valueHandle);
            return 0;
        }
        
        return keyAddr;
    }
    
    /**
     * 更新条目的 value (地址保证不变)
     * @return true 更新成功，false 更新失败 (OOM)
     */
    public boolean updateValue(long address, byte[] valueBuffer, int valueLen) {
        if (address == 0 || valueBuffer == null) {
            return false;
        }
        
        long oldValueHandle = keyNsPool.getValueHandle(address);
        
        // 1. 尝试原地更新 (新 value ≤ 已分配槽位)
        if (oldValueHandle != 0 && valuePool.updateInPlace(oldValueHandle, valueBuffer, valueLen)) {
            return true;
        }
        
        // 2. 分配新 value 空间
        long newValueHandle = valuePool.allocate(valueLen);
        if (newValueHandle == 0) {
            return false;  // OOM
        }
        valuePool.write(newValueHandle, valueBuffer, valueLen);
        
        // 3. 更新 key entry 中的 valueHandle (原子操作)
        keyNsPool.updateValueHandle(address, newValueHandle);
        
        // 4. 释放旧 value 空间
        if (oldValueHandle != 0) {
            valuePool.free(oldValueHandle);
        }
        
        return true;  // 地址不变，更新成功
    }
    
    /**
     * 删除条目，释放所有关联内存
     */
    public void removeEntry(long address) {
        if (address == 0) return;
        
        long valueHandle = keyNsPool.getValueHandle(address);
        if (valueHandle != 0) {
            valuePool.free(valueHandle);
        }
        keyNsPool.free(address);
    }
    
    // ========== 读取操作 ==========
    
    public int getHash(long address) {
        return keyNsPool.getHash(address);
    }
    
    public byte[] getKeyBytes(long address) {
        return keyNsPool.getKeyBytes(address);
    }
    
    public byte[] getNamespaceBytes(long address) {
        return keyNsPool.getNamespaceBytes(address);
    }
    
    public byte[] getValueBytes(long address) {
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.read(valueHandle);
    }
    
    public MemorySegmentSlice getValueSlice(long address) {
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.getSlice(valueHandle);
    }
    
    // ========== 零拷贝 Slice 接口 ==========
    
    public MemorySegmentSlice getKeySlice(long address) {
        return keyNsPool.getKeySlice(address);
    }
    
    public MemorySegmentSlice getNamespaceSlice(long address) {
        return keyNsPool.getNamespaceSlice(address);
    }
    
    // ========== 比较操作 ==========
    
    public boolean matchesKey(long address, byte[] keyBuffer, int keyLen, 
                              byte[] nsBuffer, int nsLen) {
        return keyNsPool.matchesKey(address, keyBuffer, keyLen, nsBuffer, nsLen);
    }
    
    // ========== 统计与生命周期 ==========
    
    public EntryStoreStats getStats() {
        return new EntryStoreStats(
            keyNsPool.getAllocatedBytes(),
            keyNsPool.getSegmentCount(),
            keyNsPool.getActiveEntries(),
            valuePool.getAllocatedBytes(),
            valuePool.getRunCount(),
            valuePool.getEmptyRunCount()
        );
    }
    
    @Override
    public void close() {
        keyNsPool.close();
        valuePool.close();
    }
    
    // ========== 内部方法 ==========
    
    private void preAllocate(long totalBytes) {
        // 按比例预分配给 KeyNsPool 和 ValuePool
        long keyNsBytes = totalBytes / 3;
        long valueBytes = totalBytes - keyNsBytes;
        keyNsPool.preAllocate(keyNsBytes);
        valuePool.preAllocate(valueBytes);
    }
}

/**
 * EntryStore 统计信息
 */
public class EntryStoreStats {
    // KeyNsPool 统计
    public final long keyNsPoolAllocated;
    public final int keyNsPoolSegments;
    public final int keyNsPoolActiveEntries;
    
    // ValuePool 统计
    public final long valuePoolAllocated;
    public final int valuePoolRuns;
    public final int valuePoolEmptyRuns;
    
    public EntryStoreStats(long keyNsPoolAllocated, int keyNsPoolSegments, 
                           int keyNsPoolActiveEntries, long valuePoolAllocated,
                           int valuePoolRuns, int valuePoolEmptyRuns) {
        this.keyNsPoolAllocated = keyNsPoolAllocated;
        this.keyNsPoolSegments = keyNsPoolSegments;
        this.keyNsPoolActiveEntries = keyNsPoolActiveEntries;
        this.valuePoolAllocated = valuePoolAllocated;
        this.valuePoolRuns = valuePoolRuns;
        this.valuePoolEmptyRuns = valuePoolEmptyRuns;
    }
    
    public long totalAllocated() {
        return keyNsPoolAllocated + valuePoolAllocated;
    }
    
    @Override
    public String toString() {
        return String.format(
            "EntryStoreStats{keyNsPool=[%d bytes, %d segments, %d entries], " +
            "valuePool=[%d bytes, %d runs, %d empty]}",
            keyNsPoolAllocated, keyNsPoolSegments, keyNsPoolActiveEntries,
            valuePoolAllocated, valuePoolRuns, valuePoolEmptyRuns
        );
    }
}
```

### 4.5 内存优化设计

本节详细描述两个关键内存优化策略：细粒度 Size Class 和小值内联。

#### 4.5.1 优化一：细粒度 Size Class

**问题**：原设计使用 8 个 size class (32B→64B→...→4KB)，倍增步长导致最坏内部碎片约 50%。

**解决方案**：使用 28 个固定 size class + 1 个 LARGE，渐进步长：

```
Size Classes (28 + LARGE):
┌────────────────────────────────────────────────────────────────────┐
│ 区间        │ Classes                           │ 步长  │ 碎片   │
├────────────────────────────────────────────────────────────────────┤
│ 0-128B      │ 16,32,48,64,80,96,112,128        │ 16B   │ ~12%   │
│ 128-256B    │ 160,192,224,256                   │ 32B   │ ~14%   │
│ 256-512B    │ 320,384,448,512                   │ 64B   │ ~17%   │
│ 512-1KB     │ 640,768,896,1024                  │ 128B  │ ~17%   │
│ 1-2KB       │ 1280,1536,1792,2048               │ 256B  │ ~17%   │
│ 2-4KB       │ 2560,3072,3584,4096               │ 512B  │ ~17%   │
│ >4KB        │ LARGE (独立分配)                   │ 页对齐 │ ~0%    │
└────────────────────────────────────────────────────────────────────┘
```

**查找优化**：使用查找表实现 O(1) size class 定位：

```java
// 每 16B 一个查找表项，最大 256 项 (覆盖 0-4096B)
private static final ValueSizeClass[] LOOKUP_TABLE = new ValueSizeClass[256];

static ValueSizeClass getSizeClass(int totalSize) {
    if (totalSize <= 0) return VS_16;
    if (totalSize > 4096) return LARGE;
    return LOOKUP_TABLE[(totalSize - 1) >> 4];  // O(1)
}
```

**碎片分析**：

| 值大小 | 旧方案 (8 class) | 新方案 (28 class) | 节省 |
|--------|------------------|-------------------|------|
| 17B | 64B (碎片 73%) | 32B (碎片 47%) | 50% |
| 33B | 64B (碎片 48%) | 48B (碎片 31%) | 25% |
| 65B | 128B (碎片 49%) | 80B (碎片 19%) | 37% |
| 129B | 256B (碎片 50%) | 160B (碎片 19%) | 37% |
| 300B | 512B (碎片 41%) | 320B (碎片 6%) | 37% |

**管理开销分析**：

| 开销项 | 旧方案 | 新方案 | 增量 |
|--------|--------|--------|------|
| partialRuns 数组 | 8 Deque | 28 Deque | +20 指针 |
| emptyRuns 数组 | 8 Deque | 28 Deque | +20 指针 |
| 查找表 | 条件分支 | 256 项数组 | +2KB 内存 |
| bitmap 管理 | 不变 | 不变 | 0 |

**结论**：管理开销可忽略，内存节省显著（平均 30-40%）。

---

#### 4.5.2 优化二：小值内联 (≤8B)

**问题**：即使 1B 的值也需要：
- ValuePool 槽位分配 (最小 16B)
- 4B value header
- 指针间接跳转

**解决方案**：≤8B 的值直接内联到 KeyNsPool entry 的 valueHandle 字段。

**Entry 布局变更**：

```
原布局 (20B header):
┌────────┬────────┬────────┬─────────────┬──────┬───────────┐
│ hash   │ keyLen │ nsLen  │ valueHandle │ key  │ namespace │
│ (4B)   │ (4B)   │ (4B)   │    (8B)     │(var) │   (var)   │
└────────┴────────┴────────┴─────────────┴──────┴───────────┘

新布局 (20B header，字段重编码):
┌────────┬────────────┬────────┬────────┬─────────────────────┬──────┬───────────┐
│ hash   │ mode+keyLen│ nsLen  │padding │ valueHandle/inline  │ key  │ namespace │
│ (4B)   │   (4B)     │ (2B)   │ (2B)   │       (8B)          │(var) │   (var)   │
└────────┴────────────┴────────┴────────┴─────────────────────┴──────┴───────────┘
```

**mode+keyLen 字段编码 (32 bits)**：

```
┌─────────┬─────────────┬─────────────────────────┐
│ mode    │ inlineLen   │       keyLen            │
│ (1 bit) │ (4 bits)    │      (27 bits)          │
└─────────┴─────────────┴─────────────────────────┘
 bit 31    bits 30-27     bits 26-0

- mode = 0: pointer 模式
  - valueHandle 存储 ValuePool 地址
  - inlineLen 未使用
  
- mode = 1: inline 模式
  - valueHandle 直接存储值内容 (小端序)
  - inlineLen 存储值长度 (0-8)
```

**内联值存储**：

```
valueHandle 字段 (8B) 在 inline 模式下:
┌────────────────────────────────────────────────────────────────┐
│        value bytes (小端序，高位补零)                            │
│        最多 8 字节                                               │
└────────────────────────────────────────────────────────────────┘

示例:
- 值 [0x12]           → valueHandle = 0x0000000000000012
- 值 [0x12, 0x34]     → valueHandle = 0x0000000000003412
- 值 [0x12,...,0x9A]  → valueHandle = 0x9A78563412000000 (8B)
```

**内联操作**：

```java
// 写入内联值
public void writeInlineValue(long address, byte[] value, int len) {
    assert len <= 8;
    long inlineValue = 0;
    for (int i = 0; i < len; i++) {
        inlineValue |= ((long) (value[i] & 0xFF)) << (i * 8);
    }
    // 设置 mode=1 + inlineLen
    int modeKeyLen = getModeKeyLen(address);
    modeKeyLen |= (1 << 31) | (len << 27);
    setModeKeyLen(address, modeKeyLen);
    setValueHandle(address, inlineValue);
}

// 读取内联值
public byte[] readInlineValue(long address) {
    int modeKeyLen = getModeKeyLen(address);
    int len = (modeKeyLen >>> 27) & 0x0F;
    long inlineValue = getValueHandle(address);
    byte[] result = new byte[len];
    for (int i = 0; i < len; i++) {
        result[i] = (byte) (inlineValue >>> (i * 8));
    }
    return result;
}
```

**收益分析**：

| 值大小 | 旧方案 | 新方案 (inline) | 节省 |
|--------|--------|-----------------|------|
| 1B | 20B + 16B = 36B | 20B | 44% |
| 4B | 20B + 16B = 36B | 20B | 44% |
| 8B | 20B + 16B = 36B | 20B | 44% |
| 9B | 20B + 16B = 36B | 20B + 16B = 36B | 0% |

**典型受益场景**：

| 场景 | 值类型 | 大小 | 内联收益 |
|------|--------|------|----------|
| Long 计数器 | LongSerializer | 8B | ✅ 44% |
| Double 聚合 | DoubleSerializer | 8B | ✅ 44% |
| Int 计数器 | IntSerializer | 4B | ✅ 44% |
| Boolean 标志 | BooleanSerializer | 1B | ✅ 44% |
| 短字符串 | StringSerializer | 1-8B | ✅ 44% |
| 长字符串 | StringSerializer | >8B | ❌ 不适用 |

**与 FastPath 的关系**：

FastPath 优化（FixedLengthTypeSupport）通过零拷贝直接读写 MemorySegment，避免序列化开销。小值内联是互补优化：

| 优化 | 目标 | 机制 |
|------|------|------|
| FastPath | 避免序列化 | 直接 segment 读写 |
| 小值内联 | 避免 ValuePool 开销 | 值存储在 KeyNsPool |

两者可以组合使用：
```java
// FastPath + 内联: 最优路径
if (fixedLengthType && valueLen <= 8) {
    // 1. 内联分配 (无 ValuePool 开销)
    long addr = keyNsPool.allocateInline(hash, key, ns, valueLen);
    // 2. 零拷贝写入 (无序列化开销)
    MemorySegment seg = keyNsPool.getSegment(addr);
    int offset = keyNsPool.getInlineValueOffset(addr);
    typeInfo.write(seg, offset, value);
}
```

---

#### 4.5.3 EntryStore 适配

EntryStore 需要适配两个优化：

```java
public class EntryStore implements AutoCloseable {
    
    // 内联阈值
    private static final int INLINE_THRESHOLD = 8;
    
    /**
     * 分配并存储新条目
     * 自动判断是否使用内联模式
     */
    public long allocateEntry(int hash, 
                              byte[] keyBuffer, int keyLen,
                              byte[] nsBuffer, int nsLen,
                              byte[] valueBuffer, int valueLen) {
        
        // 小值内联
        if (valueLen <= INLINE_THRESHOLD) {
            long inlineValue = encodeInlineValue(valueBuffer, valueLen);
            return keyNsPool.allocateInline(hash, keyBuffer, keyLen, 
                                            nsBuffer, nsLen, inlineValue, valueLen);
        }
        
        // 正常路径: 分配 ValuePool 槽位
        long valueHandle = valuePool.allocate(valueLen);
        if (valueHandle == 0) return 0;
        
        valuePool.write(valueHandle, valueBuffer, valueLen);
        return keyNsPool.allocatePointer(hash, keyBuffer, keyLen, 
                                         nsBuffer, nsLen, valueHandle);
    }
    
    /**
     * 更新 value (地址保证不变)
     * 处理 inline ↔ pointer 模式切换
     */
    public boolean updateValue(long address, byte[] valueBuffer, int valueLen) {
        boolean wasInline = keyNsPool.isInlineValue(address);
        boolean shouldInline = (valueLen <= INLINE_THRESHOLD);
        
        if (shouldInline) {
            // 新值应该内联
            if (!wasInline) {
                // pointer → inline: 释放旧 ValuePool 槽位
                long oldHandle = keyNsPool.getValueHandle(address);
                valuePool.free(oldHandle);
            }
            // 写入内联值
            long inlineValue = encodeInlineValue(valueBuffer, valueLen);
            keyNsPool.updateInlineValue(address, inlineValue, valueLen);
            return true;
        }
        
        // 新值不应内联 (>8B)
        if (wasInline) {
            // inline → pointer: 分配新 ValuePool 槽位
            long newHandle = valuePool.allocate(valueLen);
            if (newHandle == 0) return false;
            valuePool.write(newHandle, valueBuffer, valueLen);
            keyNsPool.convertToPointer(address, newHandle);
            return true;
        }
        
        // pointer → pointer: 正常更新
        long oldHandle = keyNsPool.getValueHandle(address);
        if (valuePool.updateInPlace(oldHandle, valueBuffer, valueLen)) {
            return true;
        }
        
        long newHandle = valuePool.allocate(valueLen);
        if (newHandle == 0) return false;
        valuePool.write(newHandle, valueBuffer, valueLen);
        keyNsPool.updateValueHandle(address, newHandle);
        valuePool.free(oldHandle);
        return true;
    }
    
    /**
     * 获取 value 字节
     */
    public byte[] getValueBytes(long address) {
        if (keyNsPool.isInlineValue(address)) {
            return keyNsPool.readInlineValue(address);
        }
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.read(valueHandle);
    }
    
    /**
     * 获取 value slice (零拷贝)
     * 内联模式返回 KeyNsPool 中的 slice
     */
    public MemorySegmentSlice getValueSlice(long address) {
        if (keyNsPool.isInlineValue(address)) {
            return keyNsPool.getInlineValueSlice(address);
        }
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.getSlice(valueHandle);
    }
    
    private long encodeInlineValue(byte[] value, int len) {
        long result = 0;
        for (int i = 0; i < len; i++) {
            result |= ((long) (value[i] & 0xFF)) << (i * 8);
        }
        return result;
    }
}
```

---

### 4.6 内存管理策略

#### 设计原则

```
┌─────────────────────────────────────────────────────────────────┐
│                     内存管理策略                                   │
├─────────────────────────────────────────────────────────────────┤
│  ✗ 不采用: 预先分配限额给各个池                                    │
│  ✓ 采用: 统一内存池，按需分配，OOM 时触发回收                        │
└─────────────────────────────────────────────────────────────────┘
```

**理由**:
- KeyNsPool 和 ValuePool 的内存需求取决于工作负载，无法预先确定合理比例
- 预设限额可能导致一个池有空闲而另一个池 OOM
- 统一内存池更灵活，内存利用率更高

#### 分配流程

```java
// 统一内存池: 由 MemoryManagerAllocator 管理
// KeyNsPool 和 ValuePool 按需申请

// KeyNsPool 分配新 Segment
private boolean allocateNewSegment() {
    try {
        List<MemorySegment> segments = allocator.allocate(segmentSize);
        // 成功分配
        return true;
    } catch (MemoryAllocationException e) {
        // 分配失败，触发回收后重试
        if (tryReclaim()) {
            return allocateNewSegment();  // 重试一次
        }
        return false;  // 真正 OOM
    }
}

// ValuePool 分配新 Run
private Run allocateNewRun(ValueSizeClass sc) {
    try {
        List<MemorySegment> segments = allocator.allocate(RUN_SIZE);
        return new Run(sc, segments.get(0));
    } catch (MemoryAllocationException e) {
        if (tryReclaim()) {
            return allocateNewRun(sc);
        }
        return null;
    }
}
```

#### OOM 回收策略

```java
/**
 * 分配失败时触发回收
 * 按优先级回收空闲资源，释放后重试分配
 */
private boolean tryReclaim() {
    long reclaimed = 0;
    
    // 优先级 1: ValuePool 空 runs (最可能有空闲)
    reclaimed += valuePool.releaseEmptyRuns();
    if (reclaimed > 0) return true;
    
    // 优先级 2: KeyNsPool 空 segments
    reclaimed += keyNsPool.releaseEmptySegments();
    if (reclaimed > 0) return true;
    
    // 优先级 3: LargeObjectPool 中已释放但未归还的内存
    reclaimed += largePool.releaseFreedMemory();
    
    return reclaimed > 0;
}
```

---

## 5. 接口设计与上层适配

### 5.1 设计原则

```
┌─────────────────────────────────────────────────────────────────┐
│                    接口设计原则                                   │
├─────────────────────────────────────────────────────────────────┤
│  1. 简洁优先: 只暴露必要的公共方法，内部细节不外露                   │
│  2. 语义清晰: 方法名准确反映操作意图                               │
│  3. 零拷贝友好: 支持直接内存操作，避免不必要的字节数组复制            │
│  4. 地址稳定: updateValue 保证地址不变，简化上层指针管理             │
│  5. 不追求兼容: 设计更好的接口，上层代码相应适配                     │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 EntryStore 公共接口

```java
/**
 * EntryStore: 键值分离的条目存储
 * 
 * <p>核心设计: Key/Namespace 存储在 KeyNsPool，Value 存储在 ValuePool。
 * 返回的地址指向 KeyNsPool entry，entry 内部持有 valueHandle 指向 ValuePool。
 * 
 * <p>关键特性:
 * <ul>
 *   <li>updateValue 保证地址不变，上层无需处理指针切换</li>
 *   <li>支持零拷贝写入：allocateEntry + getValueSlice 组合</li>
 *   <li>支持零拷贝读取：getXxxSlice 返回内存区域直接访问</li>
 * </ul>
 */
public class EntryStore implements AutoCloseable {
    
    // ==================== 构造器 ====================
    
    /**
     * 创建 EntryStore
     * @param allocator 内存分配器
     */
    public EntryStore(MemoryManagerAllocator allocator);
    
    /**
     * 创建 EntryStore，支持内存预分配
     * @param allocator 内存分配器
     * @param initialSizeBytes 预分配内存大小 (0 表示不预分配)
     */
    public EntryStore(MemoryManagerAllocator allocator, long initialSizeBytes);
    
    // ==================== 写入操作 ====================
    
    /**
     * 分配并存储新条目
     * 
     * @param hash 完整 hash 值 (由调用方计算)
     * @param keyBuffer key 字节数组
     * @param keyLen key 有效长度
     * @param nsBuffer namespace 字节数组
     * @param nsLen namespace 有效长度
     * @param valueBuffer value 字节数组，可为 null (预留空间用于零拷贝写入)
     * @param valueLen value 长度 (valueBuffer 为 null 时表示预留空间大小)
     * @return 条目地址，0 表示分配失败
     */
    public long allocateEntry(int hash, 
                              byte[] keyBuffer, int keyLen, 
                              byte[] nsBuffer, int nsLen, 
                              byte[] valueBuffer, int valueLen);
    
    /**
     * 更新条目的 value (地址保证不变)
     * 
     * <p>内部自动处理:
     * <ul>
     *   <li>优先尝试原地更新 (新 value ≤ 已分配槽位)</li>
     *   <li>否则分配新 value 空间，更新 valueHandle，释放旧空间</li>
     * </ul>
     * 
     * @param address 条目地址
     * @param valueBuffer 新 value 字节数组
     * @param valueLen 新 value 长度
     * @return true 更新成功，false 更新失败 (OOM)
     */
    public boolean updateValue(long address, byte[] valueBuffer, int valueLen);
    
    /**
     * 删除条目，释放 key/namespace 和 value 占用的空间
     * 
     * @param address 条目地址
     */
    public void removeEntry(long address);
    
    // ==================== 读取操作 ====================
    
    /**
     * 获取条目存储的 hash 值
     */
    public int getHash(long address);
    
    /**
     * 读取 key 字节数组 (会创建新数组)
     */
    public byte[] getKeyBytes(long address);
    
    /**
     * 读取 namespace 字节数组 (会创建新数组)
     */
    public byte[] getNamespaceBytes(long address);
    
    /**
     * 读取 value 字节数组 (会创建新数组)
     */
    public byte[] getValueBytes(long address);
    
    // ==================== 零拷贝访问 ====================
    
    /**
     * 获取 key 的内存切片 (零拷贝读取)
     * 
     * @return MemorySegmentSlice 包含 segment, offset, length
     */
    public MemorySegmentSlice getKeySlice(long address);
    
    /**
     * 获取 namespace 的内存切片 (零拷贝读取)
     */
    public MemorySegmentSlice getNamespaceSlice(long address);
    
    /**
     * 获取 value 的内存切片 (零拷贝读写)
     * 
     * <p>典型用法 (零拷贝写入):
     * <pre>
     * long addr = store.allocateEntry(hash, key, klen, ns, nlen, null, valueSize);
     * MemorySegmentSlice slice = store.getValueSlice(addr);
     * // 直接写入 slice.segment 的 slice.offset 位置
     * </pre>
     */
    public MemorySegmentSlice getValueSlice(long address);
    
    // ==================== 比较操作 ====================
    
    /**
     * 检查条目的 key 和 namespace 是否匹配
     * 
     * @return true 如果完全匹配
     */
    public boolean matchesKey(long address, 
                              byte[] keyBuffer, int keyLen, 
                              byte[] nsBuffer, int nsLen);
    
    // ==================== 统计与生命周期 ====================
    
    /**
     * 获取内存使用统计
     */
    public EntryStoreStats getStats();
    
    /**
     * 关闭并释放所有内存
     */
    @Override
    public void close();
}
```

### 5.3 与 EntryArena 接口对比

| EntryArena 方法 | EntryStore 方法 | 变化说明 |
|----------------|-----------------|----------|
| `putEntry(hash, k, kl, n, nl, v, vl)` | `allocateEntry(...)` | 重命名，语义更清晰 |
| `updateEntry(addr, v, vl)` → 返回新地址 | `updateValue(addr, v, vl)` → 返回 boolean | **语义变化**: 地址不变，返回成功/失败 |
| `updateValueInPlace(addr, v, vl)` | (内部实现) | 不再暴露，由 updateValue 内部处理 |
| `removeEntry(addr)` | `removeEntry(addr)` | 相同 |
| `getHash/getKeyBytes/...` | 相同 | 相同 |
| `getValueSlice/getKeySlice/...` | 相同 | 相同 |
| `matchesKey(addr, k, kl, n, nl)` | 相同 | 相同 |
| `getEntrySize(addr)` | (移除) | 内部使用，不暴露 |
| `putEntry(k, n, v)` (测试用) | (移除) | 测试代码自行计算 hash |
| `matchesKey(addr, k, n)` (便捷版) | (移除) | 使用完整版 |

### 5.4 上层适配指南

#### 5.4.1 ForL0StateMap 适配

```java
// ===== 旧代码 (EntryArena) =====
private void updateExistingEntryBytes(long result, byte[] vb, int vlen, KeyNamespaceHash knh) {
    if (entryArena.updateValueInPlace(result, vb, vlen)) {
        return;
    }
    long newAddr = entryArena.putEntry(...);
    if (newAddr != 0) {
        mainTable.setSlotPointer(newAddr);   // 切换指针
        updateL0Table(knh, newAddr);          // 切换 L0
        entryArena.removeEntry(result);       // 释放旧
    } else {
        long addr = entryArena.updateEntry(result, vb, vlen);
        if (addr != 0 && addr != result) {
            mainTable.setSlotPointer(addr);
            updateL0Table(knh, addr);
        }
    }
}

// ===== 新代码 (EntryStore) =====
private void updateExistingEntryBytes(long address, byte[] vb, int vlen) {
    // 一行搞定！地址不变，无需指针切换
    entryStore.updateValue(address, vb, vlen);
}
```

#### 5.4.2 MainTable / L0Table 适配

```java
// 参数类型变更: EntryArena → EntryStore
// 方法调用保持不变 (matchesKey, getHash 签名相同)

// MainTable.java
public long get(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, 
                EntryStore store) {  // 参数类型改变
    // ...
    if (store.matchesKey(ptr, kb, klen, nb, nlen)) {  // 调用方式不变
        return ptr;
    }
}
```

#### 5.4.3 零拷贝写入适配

```java
// ===== 固定长度类型 (Long, Int, Double 等) =====
// allocateEntry 预留空间，getValueSlice 直接写入

long addr = entryStore.allocateEntry(hash, kb, klen, nb, nlen, null, fixedLengthValueSize);
MemorySegmentSlice slice = entryStore.getValueSlice(addr);
stateTypeInfo.write(slice.segment, slice.offset, state);  // 零拷贝写入
```

### 5.5 组件适配清单

```
┌─────────────────────────────────────────────────────────────────┐
│                    组件适配清单                                   │
├─────────────────────────────────────────────────────────────────┤
│  ForL0StateMap                                                   │
│       │                                                          │
│       ├─ 字段: entryArena → entryStore                           │
│       ├─ 构造: new EntryArena(...) → new EntryStore(...)         │
│       ├─ put: putEntry → allocateEntry                           │
│       └─ update: 大幅简化 (移除指针切换逻辑)                        │
│                                                                  │
│  MainTable                                                       │
│       └─ 参数类型: EntryArena → EntryStore (约 10 处签名)         │
│                                                                  │
│  L0Table                                                         │
│       └─ 参数类型: EntryArena → EntryStore (约 5 处签名)          │
│                                                                  │
│  测试类                                                           │
│       ├─ EntryArenaTest → EntryStoreTest                         │
│       ├─ EntryArenaStressTest → EntryStoreStressTest             │
│       └─ 测试用例逻辑基本保持，适配新接口                           │
└─────────────────────────────────────────────────────────────────┘
```

### 5.6 Snapshot 适配

```java
// 快照遍历方式不变
mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
    byte[] key = entryStore.getKeyBytes(entryAddress);
    byte[] ns = entryStore.getNamespaceBytes(entryAddress);
    byte[] value = entryStore.getValueBytes(entryAddress);
    // 序列化到输出流...
});
```

**注意**: Snapshot 数据格式与 Flink HeapStateBackend 保持一致，仅内部存储结构变化。

---

## 6. 实施计划

### 6.1 总体策略

```
┌─────────────────────────────────────────────────────────────────┐
│                        实施策略                                   │
├─────────────────────────────────────────────────────────────────┤
│  1. 增量开发: 新建 EntryStore，与 EntryArena 并存开发测试          │
│  2. 测试驱动: 每个组件先写测试，再实现功能                          │
│  3. 渐进替换: 确保 EntryStore 通过所有测试后，再替换上层引用        │
│  4. 一次切换: 最终一次性替换，避免中间状态                          │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 分阶段实施

---

#### Phase 1: 核心组件实现 (5-7 天)

**目标**: 实现 KeyNsPool、ValuePool、EntryStore 核心类，通过单元测试

##### Step 1.1: 创建目录结构与基础类 (0.5 天)

```bash
# 创建目录
mkdir -p src/main/java/org/apache/flink/runtime/state/heap/entrystore
mkdir -p src/test/java/org/apache/flink/runtime/state/heap/entrystore
```

**创建文件清单**:

| 文件 | 说明 | 优先级 |
|------|------|:---:|
| `EntryStoreConstants.java` | 常量定义 (段大小、对齐等) | P0 |
| `ValueSizeClass.java` | Size class 枚举 | P0 |
| `EntryStoreStats.java` | 统计信息类 | P1 |

**EntryStoreConstants.java 内容**:
```java
public final class EntryStoreConstants {
    private EntryStoreConstants() {}
    
    // KeyNsPool 常量
    public static final int KEY_NS_SEGMENT_SIZE = 64 * 1024;  // 64KB
    public static final int KEY_ENTRY_HEADER_SIZE = 20;       // hash(4)+keyLen(4)+nsLen(4)+valueHandle(8)
    
    // ValuePool 常量
    public static final int VALUE_RUN_SIZE = 64 * 1024;       // 64KB
    public static final int VALUE_HEADER_SIZE = 4;            // valueLen(4)
    public static final int LARGE_OBJECT_THRESHOLD = 4 * 1024; // 4KB
    
    // 内存对齐
    public static final int ALIGNMENT = 8;
    
    public static int align8(int size) {
        return (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    }
}
```

##### Step 1.2: 实现 KeyNsPool (1.5 天)

**功能清单**:

| 方法 | 说明 | 复杂度 |
|------|------|:---:|
| `allocate(hash, key, klen, ns, nlen, valueHandle)` | 追加写分配 | 中 |
| `free(address)` | 标记释放，更新存活计数 | 低 |
| `updateValueHandle(address, newHandle)` | 原地更新 valueHandle | 低 |
| `getHash/getKeyBytes/getNamespaceBytes` | 读取操作 | 低 |
| `getKeySlice/getNamespaceSlice` | 零拷贝读取 | 低 |
| `matchesKey(address, key, klen, ns, nlen)` | key 比较 | 中 |
| `releaseEmptySegments()` | 释放空段 | 中 |

**实现要点**:
```java
public class KeyNsPool implements AutoCloseable {
    // 地址编码: (segmentIndex + 1) << 32 | (offset + 1)
    // +1 确保有效地址 > 0
    
    private MemorySegment[] segments;
    private int[] segmentLiveCount;  // 每段存活条目数
    private int currentSegmentIndex;
    private int currentOffset;
    
    // 追加写: O(1) 分配
    public long allocate(...) {
        int entrySize = align8(KEY_ENTRY_HEADER_SIZE + keyLen + nsLen);
        if (currentOffset + entrySize > SEGMENT_SIZE) {
            switchToNextSegment();
        }
        // 写入并返回地址
    }
}
```

**测试用例 (KeyNsPoolTest.java)**:
- `testBasicAllocateAndRead` - 基本分配读取
- `testMultipleAllocations` - 连续分配
- `testSegmentSwitch` - 跨段分配
- `testFreeAndLiveCount` - 释放与存活计数
- `testUpdateValueHandle` - valueHandle 更新
- `testMatchesKey` - key 匹配
- `testEmptySegmentRelease` - 空段释放

##### Step 1.3: 实现 ValuePool (2 天)

**功能清单**:

| 方法 | 说明 | 复杂度 |
|------|------|:---:|
| `allocate(valueLen)` | 按 size class 分配 | 中 |
| `free(valueHandle)` | 释放槽位 | 中 |
| `write(valueHandle, buffer, len)` | 写入 value | 低 |
| `read(valueHandle)` | 读取 value | 低 |
| `getSlice(valueHandle)` | 零拷贝读取 | 低 |
| `updateInPlace(valueHandle, buffer, len)` | 原地更新 | 中 |
| `releaseEmptyRuns()` | 释放空 Run | 中 |

**核心数据结构**:
```java
public class ValuePool implements AutoCloseable {
    // Run 管理 (按 size class)
    private final Deque<Run>[] partialRuns;  // 部分使用
    private final Deque<Run>[] emptyRuns;    // 可回收
    
    // 大对象单独管理
    private final Map<Long, LargeAllocation> largeObjects;
}

class Run {
    final ValueSizeClass sizeClass;
    final MemorySegment segment;
    final int baseOffset;
    final long[] bitmap;  // 1=used, 0=free
    int usedSlots;
    
    int allocateSlot() {
        // 使用 Long.numberOfTrailingZeros 找空闲位
    }
    
    void freeSlot(int slotOffset) {
        // 清除 bitmap 位
    }
}
```

**测试用例 (ValuePoolTest.java)**:
- `testAllocateSmallValue` - 小 value 分配
- `testAllocateLargeValue` - 大对象分配
- `testSizeClassSelection` - size class 选择
- `testBitmapAllocation` - bitmap 分配逻辑
- `testFreeAndReuse` - 释放与重用
- `testUpdateInPlace` - 原地更新
- `testUpdateRequiresRealloc` - 需要重分配的更新
- `testEmptyRunRelease` - 空 Run 释放

##### Step 1.4: 实现 EntryStore (1 天)

**功能清单**:

| 方法 | 说明 | 复杂度 |
|------|------|:---:|
| `allocateEntry(...)` | 分配完整条目 | 中 |
| `updateValue(address, buffer, len)` | 更新 value (地址不变) | 中 |
| `removeEntry(address)` | 删除条目 | 低 |
| `getHash/getKeyBytes/...` | 读取操作 | 低 |
| `getKeySlice/getValueSlice/...` | 零拷贝访问 | 低 |
| `matchesKey(...)` | key 匹配 | 低 |
| `getStats()` | 统计信息 | 低 |
| `close()` | 资源释放 | 低 |

**实现要点**:
```java
public class EntryStore implements AutoCloseable {
    private final KeyNsPool keyNsPool;
    private final ValuePool valuePool;
    
    public boolean updateValue(long address, byte[] valueBuffer, int valueLen) {
        long oldHandle = keyNsPool.getValueHandle(address);
        
        // 1. 尝试原地更新
        if (oldHandle != 0 && valuePool.updateInPlace(oldHandle, valueBuffer, valueLen)) {
            return true;
        }
        
        // 2. 分配新空间
        long newHandle = valuePool.allocate(valueLen);
        if (newHandle == 0) return false;
        valuePool.write(newHandle, valueBuffer, valueLen);
        
        // 3. 更新 handle，释放旧空间
        keyNsPool.updateValueHandle(address, newHandle);
        if (oldHandle != 0) valuePool.free(oldHandle);
        
        return true;
    }
}
```

**测试用例 (EntryStoreTest.java)**:
- `testAllocateAndRead` - 基本分配读取
- `testUpdateValueInPlace` - 原地更新
- `testUpdateValueRealloc` - 重分配更新
- `testUpdateValueAddressStable` - **关键**: 验证地址不变
- `testRemoveEntry` - 删除条目
- `testZeroCopyWrite` - 零拷贝写入
- `testZeroCopyRead` - 零拷贝读取
- `testMatchesKey` - key 匹配
- `testStats` - 统计信息

##### Step 1.5: 集成测试与压力测试 (1 天)

**EntryStoreStressTest.java**:
- `testHighVolumeAllocations` - 大量分配
- `testMixedOperations` - 混合操作 (分配/更新/删除)
- `testMemoryEfficiency` - 内存利用率
- `testFragmentationUnderChurn` - 频繁更新下的碎片
- `testConcurrentStyleAccess` - 模拟并发风格访问 (单线程)

---

#### Phase 2: 上层适配与集成 (3-4 天)

**目标**: 将 EntryStore 集成到 ForL0StateMap，替换 EntryArena

##### Step 2.1: MainTable 适配 (0.5 天)

**修改内容**:
```java
// MainTable.java - 仅修改参数类型

// 修改前
public long get(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, 
                EntryArena arena)

// 修改后
public long get(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, 
                EntryStore store)
```

**涉及方法** (~10 处签名):
- `get()`
- `put()`
- `remove()`
- `searchBucketSlots()`
- `putInSlots()`
- `removeFromBucketTree()`
- `resize()`
- `migrateAllEntriesToNewTable()`
- `migrateBucketTree()`
- `migrateBucketSlots()`

##### Step 2.2: L0Table 适配 (0.5 天)

**修改内容**: 同 MainTable，约 5 处签名修改

- `get()`
- `put()`
- `remove()`

##### Step 2.3: ForL0StateMap 适配 (2 天)

**修改清单**:

| 位置 | 修改内容 | 复杂度 |
|------|----------|:---:|
| 字段声明 | `EntryArena entryArena` → `EntryStore entryStore` | 低 |
| 构造函数 | `new EntryArena(...)` → `new EntryStore(...)` | 低 |
| `put()` 方法 | `putEntry` → `allocateEntry` | 低 |
| `updateExistingEntry()` | **大幅简化** | 高 |
| `updateExistingEntryBytes()` | **大幅简化** | 高 |
| 所有 MainTable/L0Table 调用 | 传入 `entryStore` | 低 |
| `close()` | `entryArena.close()` → `entryStore.close()` | 低 |

**核心简化 - updateExistingEntryBytes**:
```java
// 修改前: ~20 行复杂逻辑
private void updateExistingEntryBytes(long result, byte[] vb, int vlen, KeyNamespaceHash knh) {
    if (entryArena.updateValueInPlace(result, vb, vlen)) {
        return;
    }
    long newAddr = entryArena.putEntry(...);
    if (newAddr != 0) {
        mainTable.setSlotPointer(newAddr);
        updateL0Table(knh, newAddr);
        entryArena.removeEntry(result);
    } else {
        long addr = entryArena.updateEntry(result, vb, vlen);
        if (addr != 0 && addr != result) {
            mainTable.setSlotPointer(addr);
            updateL0Table(knh, addr);
        }
    }
}

// 修改后: 1 行
private void updateExistingEntryBytes(long address, byte[] vb, int vlen) {
    entryStore.updateValue(address, vb, vlen);
}
```

##### Step 2.4: 现有测试适配 (1 天)

**需要适配的测试类**:

| 测试类 | 修改量 | 说明 |
|--------|:---:|------|
| `MainTableTest` | 中 | 修改 EntryArena → EntryStore |
| `MainTableStressTest` | 中 | 同上 |
| `L0TableTest` | 中 | 同上 |
| `ForL0StateMapTest` | 低 | 构造方式可能调整 |
| `ForL0StateMapStressTest` | 低 | 同上 |

---

#### Phase 2.5: 内存优化实现 (2-3 天)

**目标**: 实现细粒度 Size Class 和小值内联优化，提升内存利用率

**前置条件**: Phase 2 完成，所有测试通过

##### Step 2.5.1: ValueSizeClass 细粒度优化 (0.5 天)

**修改文件**: `ValueSizeClass.java`

**实现要点**:
```java
public enum ValueSizeClass {
    // 28 个细粒度 size class
    SIZE_16(16), SIZE_32(32), SIZE_48(48), SIZE_64(64),
    SIZE_80(80), SIZE_96(96), SIZE_112(112), SIZE_128(128),
    SIZE_160(160), SIZE_192(192), SIZE_224(224), SIZE_256(256),
    SIZE_320(320), SIZE_384(384), SIZE_448(448), SIZE_512(512),
    SIZE_640(640), SIZE_768(768), SIZE_896(896), SIZE_1024(1024),
    SIZE_1280(1280), SIZE_1536(1536), SIZE_1792(1792), SIZE_2048(2048),
    SIZE_2560(2560), SIZE_3072(3072), SIZE_3584(3584), SIZE_4096(4096),
    LARGE(Integer.MAX_VALUE);
    
    // O(1) 查找表
    private static final ValueSizeClass[] LOOKUP_TABLE = new ValueSizeClass[256];
    
    public static ValueSizeClass fromSize(int size) {
        if (size <= 0) return SIZE_16;
        if (size > 4096) return LARGE;
        return LOOKUP_TABLE[(size + 15) >> 4];  // 除以 16 向上取整
    }
}
```

**测试用例**:
- `testLookupTableCorrectness` - 验证所有 1-4096 映射正确
- `testLookupPerformance` - 验证 O(1) 性能

##### Step 2.5.2: KeyNsPool 内联支持 (1 天)

**修改文件**: `KeyNsPool.java`, `EntryStoreConstants.java`

**修改内容**:

| 变更 | 说明 |
|------|------|
| header 布局 | keyLen(4B) → mode+keyLen(4B), nsLen(4B) → nsLen(2B)+padding(2B) |
| `allocate()` | 支持 inline 模式分配 |
| `isInlineMode()` | 新增：判断 entry 是否内联模式 |
| `getInlineValue()` | 新增：读取内联值 |
| `setInlineValue()` | 新增：设置内联值 |
| `getInlineValueLength()` | 新增：获取内联值长度 |

**常量更新**:
```java
// EntryStoreConstants.java
public static final int INLINE_THRESHOLD = 8;           // ≤8B 可内联
public static final int MODE_MASK = 0x80000000;         // bit 31
public static final int INLINE_LEN_MASK = 0x78000000;   // bits 30-27
public static final int KEY_LEN_MASK = 0x07FFFFFF;      // bits 26-0
```

**测试用例**:
- `testInlineValueAllocation` - 内联值分配
- `testInlineValueReadWrite` - 内联值读写
- `testInlineModeDetection` - 模式检测
- `testMixedInlineAndPointer` - 混合模式

##### Step 2.5.3: EntryStore 适配 (0.5 天)

**修改文件**: `EntryStore.java`

**修改内容**:
```java
public long allocateEntry(..., byte[] valueBuffer, int valueLen) {
    if (valueLen <= INLINE_THRESHOLD) {
        // 内联模式: 值直接存在 valueHandle 字段
        return keyNsPool.allocateInline(hash, keyBuffer, keyLen, 
                                        nsBuffer, nsLen, valueBuffer, valueLen);
    } else {
        // 指针模式: 分配 ValuePool 空间
        long valueHandle = valuePool.allocate(valueLen);
        valuePool.write(valueHandle, valueBuffer, valueLen);
        return keyNsPool.allocate(hash, keyBuffer, keyLen, 
                                  nsBuffer, nsLen, valueHandle);
    }
}

public boolean updateValue(long address, byte[] valueBuffer, int valueLen) {
    boolean wasInline = keyNsPool.isInlineMode(address);
    boolean willInline = valueLen <= INLINE_THRESHOLD;
    
    if (willInline) {
        // 新值可内联
        if (!wasInline) {
            // 释放旧的 ValuePool 空间
            valuePool.free(keyNsPool.getValueHandle(address));
        }
        keyNsPool.setInlineValue(address, valueBuffer, valueLen);
    } else {
        // 新值需要 ValuePool
        // ... 原有逻辑
    }
    return true;
}
```

**测试用例**:
- `testInlineToInlineUpdate` - 内联→内联更新
- `testInlineToPointerUpdate` - 内联→指针更新
- `testPointerToInlineUpdate` - 指针→内联更新
- `testPointerToPointerUpdate` - 指针→指针更新

##### Step 2.5.4: 内存优化测试 (0.5 天)

**新增测试文件**: `MemoryOptimizationTest.java`

**测试用例**:
- `testSizeClassFragmentation` - 验证碎片率 ≤17%
- `testSmallValueInlineEfficiency` - 验证小值无 ValuePool 分配
- `testMixedValueSizeDistribution` - 真实分布下的内存效率
- `testInlineThresholdBoundary` - 边界值 (7B, 8B, 9B) 测试

---

#### Phase 3: 验证与清理 (2-3 天)

##### Step 3.1: 全量测试 (1 天)

```bash
# 运行所有单元测试
mvn test

# 运行压力测试
mvn test -Dtest=*StressTest

# 运行集成测试
mvn test -Dtest=*ITCase
```

**验收标准**:
- [ ] 所有现有测试通过
- [ ] 新增 EntryStore 测试全部通过
- [ ] 无内存泄漏 (使用 getStats() 验证)

##### Step 3.2: Benchmark 对比 (1 天)

```bash
cd benchmark/scripts
python run_wordcount.py --backend forl0 --iterations 3
```

**对比指标**:

| 指标 | 预期变化 | 说明 |
|------|----------|------|
| 吞吐量 | ≥ 原有 | 不应退化 |
| 延迟 P99 | ≤ 原有 | 不应退化 |
| 内存利用率 | ≥ 原有 | size-class 应减少碎片 |
| value 更新延迟 | 下降 | 无需指针切换 |

##### Step 3.3: 代码清理 (0.5 天)

**删除文件**:
```bash
rm src/main/java/org/apache/flink/runtime/state/heap/EntryArena.java
rm src/test/java/org/apache/flink/runtime/state/heap/EntryArenaTest.java
rm src/test/java/org/apache/flink/runtime/state/heap/EntryArenaStressTest.java
```

**更新引用**:
- 确认无残留的 `EntryArena` 引用
- 更新 Javadoc 注释

##### Step 3.4: 文档更新 (0.5 天)

- 更新 `ForL0-State-Backend设计说明书.md`
- 更新 `README.md`
- 更新本设计文档状态为"已完成"

---

### 6.3 文件结构

```
src/main/java/org/apache/flink/runtime/state/heap/
├── entrystore/                      # 新增目录
│   ├── EntryStore.java              # 统一入口
│   ├── EntryStoreConstants.java     # 常量定义
│   ├── EntryStoreStats.java         # 统计信息
│   ├── KeyNsPool.java               # 键/命名空间池
│   ├── ValuePool.java               # 值池 (含 Run 内部类)
│   ├── ValueSizeClass.java          # Size class 枚举
│   └── LargeObjectPool.java         # 大对象池
│
├── EntryArena.java                  # [Phase 3 删除]
├── ForL0StateMap.java               # [Phase 2 适配]
├── MainTable.java                   # [Phase 2 适配]
├── L0Table.java                     # [Phase 2 适配]
└── ...

src/test/java/org/apache/flink/runtime/state/heap/
├── entrystore/                      # 新增目录
│   ├── KeyNsPoolTest.java           # KeyNsPool 单元测试
│   ├── ValuePoolTest.java           # ValuePool 单元测试
│   ├── EntryStoreTest.java          # EntryStore 单元测试
│   └── EntryStoreStressTest.java    # EntryStore 压力测试
│
├── EntryArenaTest.java              # [Phase 3 删除]
├── EntryArenaStressTest.java        # [Phase 3 删除]
├── MainTableTest.java               # [Phase 2 适配]
├── L0TableTest.java                 # [Phase 2 适配]
└── ...
```

### 6.4 时间线与里程碑

```
Week 1                                        Week 2
├────────────────────────────────────────────┼────────────────────────────────────────────┤
│ Phase 1: 核心组件实现                       │ Phase 2 │ Phase 2.5: 优化 │   Phase 3     │
├────────────────────────────────────────────┼─────────┼─────────────────┼───────────────┤
│ Day 1   │ Day 2-3  │ Day 4-5 │ Day 6       │ Day 7-8 │ Day 9-10        │ Day 11-13     │
│ 基础类  │ KeyNsPool│ValuePool│EntryStore   │ 上层    │ SizeClass优化   │ 测试/Bench/   │
│ 常量    │ + 测试   │ + 测试  │+ 集成测试   │ 适配    │ 小值内联        │ 清理/文档     │
└─────────┴──────────┴─────────┴─────────────┴─────────┴─────────────────┴───────────────┘

里程碑:
  M1 (Day 6):   EntryStore 独立测试通过
  M2 (Day 8):   Phase 2 完成，所有上层适配测试通过
  M2.5 (Day 10): 内存优化实现完成，碎片率 ≤17%
  M3 (Day 13):  清理完成，合并到 main
```

### 6.5 风险缓解检查点

| 检查点 | 时机 | 检查内容 | 回退条件 |
|--------|------|----------|----------|
| CP1 | Phase 1 结束 | EntryStore 单元测试 100% 通过 | 测试覆盖率 < 80% |
| CP2 | Phase 2 中期 | ForL0StateMap 基本测试通过 | 核心功能失败 |
| CP3 | Phase 2 结束 | 所有现有测试通过 | 任何回归 |
| CP2.5 | Phase 2.5 结束 | 内存优化测试通过，碎片率达标 | 碎片率 > 20% |
| CP4 | Phase 3 中期 | Benchmark 性能不低于原有 | 性能下降 > 10% |

**回退策略**: 如果任何检查点失败，保留 EntryArena，EntryStore 作为可选实现继续优化。

### 6.6 代码审查要点

**Phase 1 审查重点**:
- [ ] 地址编码格式正确 (segment+1, offset+1)
- [ ] Bitmap 操作无越界
- [ ] 内存对齐正确 (8 字节)
- [ ] 资源释放完整 (close 方法)

**Phase 2 审查重点**:
- [ ] 所有 EntryArena 引用已替换
- [ ] updateValue 返回值正确处理
- [ ] 零拷贝路径保持

**Phase 3 审查重点**:
- [ ] 无残留的 EntryArena 依赖
- [ ] 测试覆盖率不低于原有
- [ ] 文档与代码一致

---

## 7. 风险评估

### 7.1 技术风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 间接寻址开销 | 中 | 低 | valueHandle 一次内存访问，value 较大时开销可忽略；可选内联小 value |
| 地址编码冲突 | 低 | 高 | 使用高位区分 KeyNsPool/ValuePool/LargeObject |
| 并发安全 | 低 | 低 | Flink 状态访问单线程，无需额外同步 |
| Bitmap 实现 bug | 中 | 中 | 充分的单元测试覆盖边界情况 |

### 7.2 实施风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 改造范围扩大 | 中 | 中 | 严格按阶段推进，每阶段完成后再进入下一阶段 |
| 性能回归 | 低 | 中 | 每阶段进行 benchmark 对比，发现问题及时调整 |
| 测试覆盖不足 | 中 | 中 | 新建完整测试套件，覆盖边界条件 |

---

## 附录

### A. 地址编码格式

```
KeyNsPool 地址 (64-bit):
┌────────┬────────────────────────┬────────────────────────────────┐
│ 0x00   │    segment_index + 1   │          offset + 1            │
│ (8-bit)│       (24-bit)         │           (32-bit)             │
└────────┴────────────────────────┴────────────────────────────────┘

ValuePool 地址 (64-bit):
┌────────┬────────────────────────┬────────────────────────────────┐
│ 0x01   │      run_index + 1     │        slot_offset + 1         │
│ (8-bit)│       (24-bit)         │           (32-bit)             │
└────────┴────────────────────────┴────────────────────────────────┘

LargeObject 地址 (64-bit):
┌────────┬────────────────────────┬────────────────────────────────┐
│ 0x02   │    allocation_id       │          offset + 1            │
│ (8-bit)│       (24-bit)         │           (32-bit)             │
└────────┴────────────────────────┴────────────────────────────────┘
```

### B. 常量定义 (代码内配置)

```java
/**
 * EntryStore 常量定义
 * 所有配置以代码常量形式定义，后续如需暴露给 Flink 配置系统可添加
 */
public final class EntryStoreConstants {
    
    private EntryStoreConstants() {}
    
    // ===== KeyNsPool 常量 =====
    
    /** Segment 大小 (与 MemoryManager 页大小对齐) */
    public static final int KEY_NS_SEGMENT_SIZE = 64 * 1024;  // 64KB
    
    /** Key Entry 头部大小: hash(4) + keyLen(4) + nsLen(4) + valueHandle(8) */
    public static final int KEY_ENTRY_HEADER_SIZE = 20;
    
    // ===== ValuePool 常量 =====
    
    /** Run 大小 */
    public static final int VALUE_RUN_SIZE = 64 * 1024;  // 64KB
    
    /** Value Entry 头部大小: valueLen(4) */
    public static final int VALUE_HEADER_SIZE = 4;
    
    /** 大对象阈值: 超过此值走 LargeObjectPool */
    public static final int LARGE_OBJECT_THRESHOLD = 4 * 1024;  // 4KB
    
    /** 大对象页对齐大小 */
    public static final int LARGE_OBJECT_PAGE_SIZE = 4 * 1024;  // 4KB
    
    // ===== Size Class 槽位大小 =====
    
    public static final int[] SIZE_CLASS_SLOTS = {
        32,    // VS_32:  ≤32B
        64,    // VS_64:  33-64B
        128,   // VS_128: 65-128B
        256,   // VS_256: 129-256B
        512,   // VS_512: 257-512B
        1024,  // VS_1K:  513-1024B
        2048,  // VS_2K:  1025-2048B
        4096,  // VS_4K:  2049-4096B
    };
    
    // ===== 内存对齐 =====
    
    /** 通用内存对齐 */
    public static final int ALIGNMENT = 8;
}
```

---

> **下一步**: 确认设计方案后，开始 Phase 1 基础架构实现
