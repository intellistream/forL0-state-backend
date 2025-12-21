# MainTable 堆内数组 & L0Table 布局统一改造设计方案

> 文档版本: 1.2  
> 创建日期: 2025-12-21  
> 更新日期: 2025-12-21  
> 状态: **实施中**  
> 改造类型: **彻底改造**，不考虑向后兼容

## 版本历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| 1.0 | 2025-12-21 | 初始设计 |
| 1.1 | 2025-12-21 | 修正：L0Table 保留 L0 内存，只调整布局 |
| 1.2 | 2025-12-21 | 补充：L0Table 多替换策略实现要求 |

## 目录

1. [背景与动机](#1-背景与动机)
2. [设计概述](#2-设计概述)
3. [MainTable 详细设计](#3-maintable-详细设计)
4. [L0Table 详细设计](#4-l0table-详细设计)
5. [L0Table 多替换策略实现规范](#5-l0table-多替换策略实现规范)
6. [统一 Slot 格式](#6-统一-slot-格式)
7. [内存管理](#7-内存管理)
8. [性能优化要点](#8-性能优化要点)
9. [改造影响分析](#9-改造影响分析)
10. [实施计划](#10-实施计划)

---

## 1. 背景与动机

### 1.1 当前架构问题

当前架构使用 **堆外 MemorySegment** 存储索引：

```
┌─────────────────────────────────────────────────────────────────┐
│                    当前架构                                      │
├─────────────────────────────────────────────────────────────────┤
│   MainTable                                                      │
│   ├─ MemorySegment[] (堆外)                                      │
│   ├─ 10 字节 slot: Tag(2B) + Pointer(8B)                        │
│   └─ Bucket 64B: 6 slots + 4 extension pointers                 │
│                                                                  │
│   L0Table                                                        │
│   ├─ MemorySegment[] (堆外 L0 内存)                               │
│   ├─ 15 字节 slot: Tag(2B) + Extension(5B) + Pointer(8B)         │
│   └─ Bucket 64B: ValidBitmap(4B) + 4 slots(60B)                 │
└─────────────────────────────────────────────────────────────────┘
```

**存在的问题**：

| 问题 | 详细描述 |
|------|----------|
| **堆外访问开销** | MemorySegment 的 get/put 需要通过 Unsafe 或 ByteBuffer，存在边界检查开销 |
| **内存管理复杂** | 需要通过 Flink MemoryManager 分配和释放，生命周期管理复杂 |
| **布局不自然** | 10B/15B 的 slot 大小无法对齐到 long，需要多次内存读取 |
| **Pointer 过大** | 8 字节 Pointer 实际只需要存储数组下标 (4B 足够支持 40 亿条目) |

### 1.2 改造目标

**MainTable**: 改为 **堆内 chunked long[][] 数组**  
**L0Table**: 保留 **L0 内存 + MemorySegment**，调整 bucket 布局

```
┌─────────────────────────────────────────────────────────────────┐
│                    新架构                                        │
├─────────────────────────────────────────────────────────────────┤
│   MainTable (堆内改造)                                           │
│   ├─ long[][] chunks (堆内数组)                                  │
│   ├─ 8 字节 slot: Hash(高32位) + Ptr(低32位)                     │
│   └─ Bucket 64B: 7 slots + 1 expansion long                     │
│                                                                  │
│   L0Table (布局调整，保留 L0 内存)                                │
│   ├─ MemorySegment[] (L0 内存，通过 JNI 分配)                    │
│   ├─ 8 字节 slot: Hash(高32位) + Ptr(低32位)                     │
│   └─ Bucket 64B: 7 slots + 1 extension long                     │
└─────────────────────────────────────────────────────────────────┘
```

**改造收益**：

| 组件 | 收益 |
|------|------|
| **MainTable** | 堆内数组访问更快，无 MemorySegment 开销；GC 自动管理；slot 数量从 6 增加到 7 |
| **L0Table** | 统一 long 访问模式；slot 数量从 4 增加到 7；无需 Valid bitmap |

---

## 2. 设计概述

### 2.1 改造范围

| 组件 | 存储介质 | 改造内容 |
|------|----------|----------|
| **MainTable** | 堆内 `long[][]` | 从 MemorySegment 改为堆内数组；重新设计 bucket 布局 |
| **L0Table** | L0 内存 (MemorySegment) | 保留 L0 内存不变；仅调整 bucket 布局以统一 slot 格式 |

### 2.2 Bucket 布局对比

#### MainTable

| 维度 | 当前设计 | 新设计 |
|------|----------|--------|
| **存储介质** | MemorySegment (堆外) | **long[][] (堆内)** |
| Bucket 大小 | 64 字节 | 64 字节 (8 longs) |
| Slot 数量 | 6 | 7 |
| Slot 大小 | 10 字节 | 8 字节 (1 long) |
| Slot 格式 | Tag(2B) + Pointer(8B) | Hash(高32位) + Ptr(低32位) |
| 扩展指针 | 4 字节 (4 × 1B offset) | 低 32 位 of 第 8 个 long |
| 扩展指针数量 | 4 | 4 (每个 8 位) |

#### L0Table

| 维度 | 当前设计 | 新设计 |
|------|----------|--------|
| **存储介质** | MemorySegment (L0 内存) | **MemorySegment (L0 内存，不变)** |
| Bucket 大小 | 64 字节 | 64 字节 (8 longs) |
| Slot 数量 | 4 | 7 |
| Slot 大小 | 15 字节 | 8 字节 (1 long) |
| Slot 格式 | Tag(2B) + Ext(5B) + Ptr(8B) | Hash(高32位) + Ptr(低32位) |
| Valid 标记 | 独立 4B bitmap | 通过 Ptr == 0 判断 |
| Extension | 5B per slot | 最后 8B (用于替换算法) |

### 2.2 核心数据结构

```
┌─────────────────────────────────────────────────────────────────┐
│                      Slot Layout (8 bytes = 1 long)              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   63                              32 31                        0 │
│   ┌────────────────────────────────┬────────────────────────────┐│
│   │         Hash (32 bits)         │       Pointer (32 bits)    ││
│   │     (完整 hashCode 高32位)       │    (HeapEntryStore 下标)   ││
│   └────────────────────────────────┴────────────────────────────┘│
│                                                                  │
│   - Hash: 直接使用完整 hashCode，不再需要 16 位 Tag              │
│   - Pointer: 数组下标 + 1 (0 表示空槽)                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Empty Slot 判定

**新设计**: MainTable 和 L0Table 统一通过 `slot == 0` 判断槽位是否为空

- **判空**: `slot == 0` (整个 long 为 0)
- **删除**: `putLong(offset, 0)` (直接将整个 slot 置零)
- **约束**: HeapEntryStore 返回的地址从 1 开始 (已满足)
- **优势**: 无需额外的 Valid 字段，单次比较即可完成判空

---

## 3. MainTable 详细设计

### 3.1 Bucket 内存布局

```
┌─────────────────────────────────────────────────────────────────┐
│                  MainTable Bucket (64 bytes = 8 longs)           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   long[0]: Slot 0  ─  [Hash(32b) | Ptr(32b)]                    │
│   long[1]: Slot 1  ─  [Hash(32b) | Ptr(32b)]                    │
│   long[2]: Slot 2  ─  [Hash(32b) | Ptr(32b)]                    │
│   long[3]: Slot 3  ─  [Hash(32b) | Ptr(32b)]                    │
│   long[4]: Slot 4  ─  [Hash(32b) | Ptr(32b)]                    │
│   long[5]: Slot 5  ─  [Hash(32b) | Ptr(32b)]                    │
│   long[6]: Slot 6  ─  [Hash(32b) | Ptr(32b)]                    │
│   long[7]: Extension ─ [Reserved(32b) | ExpPtrs(32b)]           │
│                                                                  │
│   ExpPtrs 布局 (低 32 位):                                       │
│   ┌────────┬────────┬────────┬────────┐                         │
│   │ Ext3   │ Ext2   │ Ext1   │ Ext0   │                         │
│   │ (8b)   │ (8b)   │ (8b)   │ (8b)   │                         │
│   │ bit24-31│bit16-23│ bit8-15│ bit0-7 │                         │
│   └────────┴────────┴────────┴────────┘                         │
│                                                                  │
│   - Ext0-3: 扩展桶偏移量 (1-255, 0 表示无扩展)                    │
│   - 根据 hash & 0x3 选择扩展桶                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 常量定义

```java
// Bucket 配置
private static final int BUCKET_SIZE_LONGS = 8;          // 64B / 8B = 8 longs
private static final int SLOTS_PER_BUCKET = 7;           // 7 个数据 slot
private static final int EXTENSION_SLOT_INDEX = 7;       // 第 8 个 long 存扩展指针

// Slot 位操作
private static final int HASH_SHIFT = 32;                // Hash 在高 32 位
private static final long PTR_MASK = 0xFFFFFFFFL;        // 低 32 位掩码
private static final long HASH_MASK = 0xFFFFFFFF00000000L; // 高 32 位掩码

// 扩展指针位操作
private static final int EXT_PTR_BITS = 8;               // 每个扩展指针 8 位
private static final int EXT_PTR_MASK = 0xFF;            // 单个扩展指针掩码

// Chunked array 配置
private static final int CHUNK_SIZE_BITS = 16;           // 每 chunk 65536 buckets
private static final int CHUNK_SIZE = 1 << CHUNK_SIZE_BITS;
private static final int CHUNK_MASK = CHUNK_SIZE - 1;
```

### 3.3 核心操作

#### 3.3.1 Slot 编解码 (内联实现)

```java
// 编码: hash + ptr -> slot value
long slot = ((long) hash << 32) | (ptr & 0xFFFFFFFFL);

// 解码 hash
int hash = (int) (slot >>> 32);

// 解码 ptr
int ptr = (int) slot;

// 判断空槽
boolean empty = (slot == 0);
```

#### 3.3.2 扩展指针操作 (内联实现)

```java
// 扩展指针布局: 低 32 位存 4 个 8-bit 指针 (Ext0 在 bit0-7, Ext3 在 bit24-31)

// 选择扩展桶索引 (根据 hash 低 2 位)
int extIndex = hash & 0x3;

// 读取扩展指针
int extOffset = (int) ((extLong >>> (extIndex << 3)) & 0xFF);

// 设置扩展指针
int shift = extIndex << 3;
extLong = (extLong & ~(0xFFL << shift)) | ((long) (extOffset & 0xFF) << shift);
```

#### 3.3.3 Bucket 访问

```java
// 获取 bucket 所在的 chunk 和 offset
int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
int bucketOffset = (bucketIndex & CHUNK_MASK) * BUCKET_SIZE_LONGS;

// 读取 slot
long slot = chunks[chunkIndex][bucketOffset + slotIndex];

// 读取扩展指针 long
long extLong = chunks[chunkIndex][bucketOffset + EXTENSION_SLOT_INDEX];
```

### 3.4 查找算法

```
GET(hash, key, namespace):
    bucketIndex = hash & (bucketCount - 1)
    mainBucketIndex = bucketIndex
    
    LOOP:
        chunk = chunks[bucketIndex >>> CHUNK_SIZE_BITS]
        offset = (bucketIndex & CHUNK_MASK) * 8
        
        FOR i = 0 TO 6:
            slot = chunk[offset + i]
            IF slot == 0:              // 整个 long 为 0 表示空槽
                CONTINUE
            IF (slot >>> 32) != hash:  // 高 32 位是 hash
                CONTINUE
            ptr = (int) slot           // 低 32 位是 ptr
            IF store.matches(ptr, key, namespace):
                RETURN ptr
        
        // 查找扩展桶
        extLong = chunk[offset + 7]
        extIndex = hash & 0x3
        extOffset = (extLong >>> (extIndex * 8)) & 0xFF
        IF extOffset == 0:
            RETURN 0  // 未找到
        
        bucketIndex = getExtensionBucketIndex(mainBucketIndex, extOffset)
        GOTO LOOP
```

### 3.5 插入算法

```
PUT(hash, ptr, key, namespace):
    bucketIndex = hash & (bucketCount - 1)
    mainBucketIndex = bucketIndex
    
    LOOP:
        chunk = chunks[bucketIndex >>> CHUNK_SIZE_BITS]
        offset = (bucketIndex & CHUNK_MASK) * 8
        emptySlot = -1
        
        FOR i = 0 TO 6:
            slot = chunk[offset + i]
            
            IF slot == 0:              // 整个 long 为 0 表示空槽
                IF emptySlot == -1: emptySlot = i
                CONTINUE
            
            IF (slot >>> 32) == hash AND store.matches((int) slot, key, namespace):
                // 更新已存在条目
                chunk[offset + i] = encodeSlot(hash, ptr)
                RETURN (int) slot  // 返回旧 ptr
        
        IF emptySlot != -1:
            // 插入到空槽
            chunk[offset + emptySlot] = encodeSlot(hash, ptr)
            RETURN 0  // 新条目
        
        // 当前桶满，查找/分配扩展桶
        extLong = chunk[offset + 7]
        extIndex = hash & 0x3
        extOffset = (extLong >>> (extIndex * 8)) & 0xFF
        
        IF extOffset == 0:
            extOffset = allocateExtensionBucket(mainBucketIndex)
            IF extOffset == 0:
                RETURN -1  // 需要全局扩容
            chunk[offset + 7] = setExtensionPtr(extLong, extIndex, extOffset)
        
        bucketIndex = getExtensionBucketIndex(mainBucketIndex, extOffset)
        GOTO LOOP
```

### 3.6 删除算法

```
REMOVE(hash, key, namespace):
    bucketIndex = hash & (bucketCount - 1)
    mainBucketIndex = bucketIndex
    
    LOOP:
        chunk = chunks[bucketIndex >>> CHUNK_SIZE_BITS]
        offset = (bucketIndex & CHUNK_MASK) * 8
        
        FOR i = 0 TO 6:
            slot = chunk[offset + i]
            IF slot == 0:
                CONTINUE
            IF (slot >>> 32) != hash:
                CONTINUE
            ptr = (int) slot
            IF store.matches(ptr, key, namespace):
                // 直接将整个 slot 置零
                chunk[offset + i] = 0
                RETURN ptr
        
        // 查找扩展桶
        extLong = chunk[offset + 7]
        extIndex = hash & 0x3
        extOffset = (extLong >>> (extIndex * 8)) & 0xFF
        IF extOffset == 0:
            RETURN 0  // 未找到
        
        bucketIndex = getExtensionBucketIndex(mainBucketIndex, extOffset)
        GOTO LOOP
```

---

## 4. L0Table 详细设计

> **注意**: L0Table 使用的是通过 JNI 分配的 L0 内存（鲲鹏 CPU L0 Cache 或模拟模式下的 malloc），
> 通过 MemorySegment 包装访问。本次改造**仅调整 bucket 内存布局**，不改变存储介质。

### 4.1 Bucket 内存布局

```
┌─────────────────────────────────────────────────────────────────┐
│          L0Table Bucket (64 bytes，通过 MemorySegment 访问)       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Offset 0:  Slot 0  ─  [Hash(32b) | Ptr(32b)]   (getLong)      │
│   Offset 8:  Slot 1  ─  [Hash(32b) | Ptr(32b)]                  │
│   Offset 16: Slot 2  ─  [Hash(32b) | Ptr(32b)]                  │
│   Offset 24: Slot 3  ─  [Hash(32b) | Ptr(32b)]                  │
│   Offset 32: Slot 4  ─  [Hash(32b) | Ptr(32b)]                  │
│   Offset 40: Slot 5  ─  [Hash(32b) | Ptr(32b)]                  │
│   Offset 48: Slot 6  ─  [Hash(32b) | Ptr(32b)]                  │
│   Offset 56: Extension ─  替换算法元数据 (8 bytes)               │
│                                                                  │
│   Extension 布局 (根据替换策略):                                  │
│                                                                  │
│   CLOCK 策略:                                                    │
│   ┌─────────────────────────────────────────────────────────────┐│
│   │ bit 0-6: 7 个 accessed 标志位 (每 slot 1 bit)               ││
│   │ bit 7-63: 保留                                              ││
│   └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│   LRU 策略:                                                      │
│   ┌─────────────────────────────────────────────────────────────┐│
│   │ 7 × 9-bit timestamp: slot0-slot6 的相对访问时间              ││
│   │ (共 63 bits, 最高位保留)                                     ││
│   └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│   LFU 策略:                                                      │
│   ┌─────────────────────────────────────────────────────────────┐│
│   │ 7 × 8-bit frequency: slot0-slot6 的访问频率计数              ││
│   │ (共 56 bits, 最高 8 位保留)                                  ││
│   └─────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 常量定义

```java
// Bucket 配置
private static final int BUCKET_SIZE = 64;           // 64 字节
private static final int BUCKET_SIZE_BITS = 6;       // 64 = 2^6
private static final int SLOTS_PER_BUCKET = 7;       // 7 个数据 slot
private static final int SLOT_SIZE = 8;              // 每 slot 8 字节
private static final int EXTENSION_OFFSET = 56;      // 第 8 个 long 的偏移量

// Slot 位操作 (与 MainTable 统一)
private static final int HASH_SHIFT = 32;
private static final long PTR_MASK = 0xFFFFFFFFL;

// CLOCK 策略位操作
private static final int CLOCK_ACCESSED_BIT = 1;     // 每 slot 1 bit

// LRU 策略位操作
private static final int LRU_TIMESTAMP_BITS = 9;     // 每 slot 9 bits (512 levels)
private static final int LRU_TIMESTAMP_MASK = 0x1FF;

// LFU 策略位操作
private static final int LFU_FREQ_BITS = 8;          // 每 slot 8 bits (max 255)
private static final int LFU_FREQ_MASK = 0xFF;
```

### 4.3 替换策略实现

#### 4.3.1 CLOCK 算法 (推荐默认)

```java
// 读取 accessed 位
static boolean isAccessed(long extLong, int slotIndex) {
    return (extLong & (1L << slotIndex)) != 0;
}

// 设置 accessed 位
static long setAccessed(long extLong, int slotIndex) {
    return extLong | (1L << slotIndex);
}

// 清除 accessed 位
static long clearAccessed(long extLong, int slotIndex) {
    return extLong & ~(1L << slotIndex);
}

// 选择 victim (使用 MemorySegment 访问)
int selectClockVictim(MemorySegment segment, int bucketOffset) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    
    // 第一轮: 找未访问的有效槽
    for (int i = 0; i < 7; i++) {
        long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
        if (slot == 0) return i;                        // 空槽 (整个 long 为 0)
        if ((extLong & (1L << i)) == 0) return i;       // 未访问
    }
    
    // 第二轮: 清除所有 accessed 位，返回第一个
    segment.putLong(bucketOffset + EXTENSION_OFFSET, 0);
    return 0;
}
```

#### 4.3.2 LRU 算法

```java
// 获取 slot 的 timestamp
static int getLruTimestamp(long extLong, int slotIndex) {
    int shift = slotIndex * LRU_TIMESTAMP_BITS;
    return (int) ((extLong >>> shift) & LRU_TIMESTAMP_MASK);
}

// 设置 slot 的 timestamp
static long setLruTimestamp(long extLong, int slotIndex, int timestamp) {
    int shift = slotIndex * LRU_TIMESTAMP_BITS;
    long mask = ~((long) LRU_TIMESTAMP_MASK << shift);
    return (extLong & mask) | ((long) (timestamp & LRU_TIMESTAMP_MASK) << shift);
}

// 获取当前相对时间戳 (循环使用 0-511)
private int currentTimestamp = 0;
int getRelativeTimestamp() {
    return (currentTimestamp++) & LRU_TIMESTAMP_MASK;
}

// 选择 victim: 找最旧的 timestamp (使用 MemorySegment)
int selectLruVictim(MemorySegment segment, int bucketOffset) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    int oldestSlot = 0;
    int oldestAge = 0;
    int now = currentTimestamp & LRU_TIMESTAMP_MASK;
    
    for (int i = 0; i < 7; i++) {
        long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
        if (slot == 0) return i;  // 空槽 (整个 long 为 0)
        
        int ts = getLruTimestamp(extLong, i);
        int age = (now - ts) & LRU_TIMESTAMP_MASK;  // 处理循环
        if (age > oldestAge) {
            oldestAge = age;
            oldestSlot = i;
        }
    }
    return oldestSlot;
}
```

#### 4.3.3 LFU 算法

```java
// 获取 slot 的 frequency
static int getLfuFrequency(long extLong, int slotIndex) {
    int shift = slotIndex * LFU_FREQ_BITS;
    return (int) ((extLong >>> shift) & LFU_FREQ_MASK);
}

// 增加频率 (饱和计数)
static long incrementFrequency(long extLong, int slotIndex) {
    int shift = slotIndex * LFU_FREQ_BITS;
    int freq = (int) ((extLong >>> shift) & LFU_FREQ_MASK);
    if (freq < LFU_FREQ_MASK) {
        long mask = ~((long) LFU_FREQ_MASK << shift);
        return (extLong & mask) | ((long) (freq + 1) << shift);
    }
    return extLong;  // 已饱和
}

// 选择 victim: 找最低频率 (使用 MemorySegment)
int selectLfuVictim(MemorySegment segment, int bucketOffset) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    int minSlot = 0;
    int minFreq = Integer.MAX_VALUE;
    
    for (int i = 0; i < 7; i++) {
        long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
        if (slot == 0) return i;  // 空槽 (整个 long 为 0)
        
        int freq = getLfuFrequency(extLong, i);
        if (freq < minFreq) {
            minFreq = freq;
            minSlot = i;
        }
    }
    return minSlot;
}
```

### 4.4 L0Table 操作流程

> L0Table 通过 MemorySegment 访问 L0 内存，使用 `getLong`/`putLong` 进行读写。

#### 4.4.1 查找

```
L0_GET(hash, key, namespace):
    bucketIndex = hash & (bucketCount - 1)
    segment = getSegmentForBucket(bucketIndex)
    bucketOffset = getBucketOffsetInSegment(bucketIndex)
    
    FOR i = 0 TO 6:
        slotOffset = bucketOffset + i * 8
        slot = segment.getLong(slotOffset)
        IF slot == 0:              // 整个 long 为 0 表示空槽
            CONTINUE
        IF (slot >>> 32) != hash:  // 高 32 位是 hash
            CONTINUE
        ptr = (int) slot           // 低 32 位是 ptr
        IF store.matches(ptr, key, namespace):
            // 命中: 更新替换算法元数据
            updateAccessMetadata(segment, bucketOffset, i)
            RETURN ptr
    
    RETURN 0  // Miss
```

#### 4.4.2 插入

```
L0_PUT(hash, ptr, key, namespace):
    bucketIndex = hash & (bucketCount - 1)
    segment = getSegmentForBucket(bucketIndex)
    bucketOffset = getBucketOffsetInSegment(bucketIndex)
    emptySlot = -1
    
    FOR i = 0 TO 6:
        slotOffset = bucketOffset + i * 8
        slot = segment.getLong(slotOffset)
        
        IF slot == 0:              // 整个 long 为 0 表示空槽
            IF emptySlot == -1: emptySlot = i
            CONTINUE
        
        IF (slot >>> 32) == hash AND store.matches(slotPtr, key, namespace):
            // 更新已存在
            segment.putLong(slotOffset, encodeSlot(hash, ptr))
            updateAccessMetadata(segment, bucketOffset, i)
            RETURN slotPtr
    
    IF emptySlot != -1:
        // 使用空槽
        segment.putLong(bucketOffset + emptySlot * 8, encodeSlot(hash, ptr))
        initAccessMetadata(segment, bucketOffset, emptySlot)
        RETURN 0
    
    // 需要驱逐
    victimSlot = selectVictim(segment, bucketOffset)
    victimOffset = bucketOffset + victimSlot * 8
    oldSlot = segment.getLong(victimOffset)
    segment.putLong(victimOffset, encodeSlot(hash, ptr))
    initAccessMetadata(segment, bucketOffset, victimSlot)
    
    RETURN (int) oldSlot  // 返回被驱逐的 ptr
```

#### 4.4.3 删除

```
L0_REMOVE(hash, key, namespace):
    bucketIndex = hash & (bucketCount - 1)
    segment = getSegmentForBucket(bucketIndex)
    bucketOffset = getBucketOffsetInSegment(bucketIndex)
    
    FOR i = 0 TO 6:
        slotOffset = bucketOffset + i * 8
        slot = segment.getLong(slotOffset)
        IF slot == 0:
            CONTINUE
        IF (slot >>> 32) != hash:
            CONTINUE
        ptr = (int) slot
        IF store.matches(ptr, key, namespace):
            // 直接将整个 slot 置零
            segment.putLong(slotOffset, 0)
            RETURN ptr
    
    RETURN 0  // 未找到
```

---

## 5. L0Table 多替换策略实现规范

> **重要**: L0Table 必须支持多种可配置的替换策略，通过构造函数参数选择。

### 5.1 替换策略枚举

```java
public enum ReplacementPolicy {
    CLOCK,       // 时钟算法 (推荐默认，最低开销)
    LRU,         // 最近最少使用 (9-bit 相对时间戳)
    LFU,         // 最少频率使用 (8-bit 频率计数)
    TINY_LFU,    // TinyLFU with decay (基于 LFU，周期性衰减)
    SAMPLED_LRU  // 随机采样 LRU (轻量级)
}
```

### 5.2 Extension Long 布局

每个 bucket 的第 8 个 long（偏移 56 字节）用于存储替换策略元数据：

```
┌─────────────────────────────────────────────────────────────────┐
│               Extension Long (64 bits) 布局                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  CLOCK 策略:                                                     │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │ bits 63-7: 保留 (57 bits)   │ bits 6-0: accessed flags (7b) ││
│  └──────────────────────────────────────────────────────────────┘│
│  每个 slot 使用 1 bit (bit i = slot i 的 accessed 标志)          │
│                                                                  │
│  LFU/TINY_LFU 策略:                                              │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │ bits 63-56: 保留 │ bits 55-0: 7 × 8-bit frequency counters   ││
│  └──────────────────────────────────────────────────────────────┘│
│  slot i 的频率: bits (i*8+7):(i*8)，范围 0-255                    │
│                                                                  │
│  LRU 策略:                                                       │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │ bit 63: 保留 │ bits 62-0: 7 × 9-bit timestamps               ││
│  └──────────────────────────────────────────────────────────────┘│
│  slot i 的时间戳: bits (i*9+8):(i*9)，范围 0-511                  │
│                                                                  │
│  SAMPLED_LRU 策略:                                               │
│  与 CLOCK 相同的布局，使用 accessed flags                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 构造函数

```java
/**
 * 使用 CLOCK 策略创建 L0Table（默认）
 */
public L0Table(L0MemoryAllocator l0Allocator, int bucketCountPow2) {
    this(l0Allocator, bucketCountPow2, ReplacementPolicy.CLOCK);
}

/**
 * 使用指定替换策略创建 L0Table
 */
public L0Table(L0MemoryAllocator l0Allocator, int bucketCountPow2, 
               ReplacementPolicy replacementPolicy) {
    this.replacementPolicy = replacementPolicy;
    this.random = (replacementPolicy == ReplacementPolicy.SAMPLED_LRU) 
                  ? new Random() : null;
    // ...
}
```

### 5.4 元数据操作方法

#### 5.4.1 访问时更新元数据

```java
private void updateAccessMetadata(MemorySegment segment, int bucketOffset, int slotIndex) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    long newExtLong;
    
    switch (replacementPolicy) {
        case CLOCK:
        case SAMPLED_LRU:
            // 设置 accessed bit
            newExtLong = extLong | (1L << slotIndex);
            break;
            
        case LFU:
        case TINY_LFU:
            // 增加频率 (饱和在 255)
            int shift = slotIndex * 8;
            int freq = (int) ((extLong >>> shift) & 0xFF);
            if (freq < 255) {
                newExtLong = (extLong & ~(0xFFL << shift)) | ((long)(freq + 1) << shift);
            } else {
                newExtLong = extLong;
            }
            // TinyLFU 需要额外处理衰减 (见 5.5)
            break;
            
        case LRU:
            // 更新时间戳
            int tsShift = slotIndex * 9;
            int ts = getRelativeTimestamp();
            newExtLong = (extLong & ~(0x1FFL << tsShift)) | ((long)(ts & 0x1FF) << tsShift);
            break;
            
        default:
            newExtLong = extLong;
    }
    
    segment.putLong(bucketOffset + EXTENSION_OFFSET, newExtLong);
}
```

#### 5.4.2 插入时初始化元数据

```java
private void initSlotMetadata(MemorySegment segment, int bucketOffset, int slotIndex) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    long newExtLong;
    
    switch (replacementPolicy) {
        case CLOCK:
        case SAMPLED_LRU:
            // 清除 accessed bit (新条目未被访问)
            newExtLong = extLong & ~(1L << slotIndex);
            break;
            
        case LFU:
        case TINY_LFU:
            // 初始化频率为 1
            int shift = slotIndex * 8;
            newExtLong = (extLong & ~(0xFFL << shift)) | (1L << shift);
            break;
            
        case LRU:
            // 设置当前时间戳
            int tsShift = slotIndex * 9;
            int ts = getRelativeTimestamp();
            newExtLong = (extLong & ~(0x1FFL << tsShift)) | ((long)(ts & 0x1FF) << tsShift);
            break;
            
        default:
            newExtLong = extLong;
    }
    
    segment.putLong(bucketOffset + EXTENSION_OFFSET, newExtLong);
}
```

### 5.5 Victim 选择方法

```java
private int selectVictimSlot(MemorySegment segment, int bucketOffset) {
    switch (replacementPolicy) {
        case CLOCK:
            return selectClockVictim(segment, bucketOffset);
        case LRU:
            return selectLruVictim(segment, bucketOffset);
        case LFU:
        case TINY_LFU:
            return selectLfuVictim(segment, bucketOffset);
        case SAMPLED_LRU:
            return selectSampledLruVictim(segment, bucketOffset);
        default:
            return 0;
    }
}

/**
 * CLOCK: 找空槽或未访问槽，否则清零并返回槽 0
 */
private int selectClockVictim(MemorySegment segment, int bucketOffset) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
        if (slot == 0) return i;
        if ((extLong & (1L << i)) == 0) return i;
    }
    
    segment.putLong(bucketOffset + EXTENSION_OFFSET, extLong & ~0x7FL);
    return 0;
}

/**
 * LRU: 找最旧时间戳的槽
 */
private int selectLruVictim(MemorySegment segment, int bucketOffset) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    int now = currentTimestamp & 0x1FF;
    int oldestSlot = 0;
    int oldestAge = -1;
    
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
        if (slot == 0) return i;
        
        int ts = (int) ((extLong >>> (i * 9)) & 0x1FF);
        int age = (now - ts) & 0x1FF;
        if (age > oldestAge) {
            oldestAge = age;
            oldestSlot = i;
        }
    }
    return oldestSlot;
}

/**
 * LFU: 找最低频率的槽
 */
private int selectLfuVictim(MemorySegment segment, int bucketOffset) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    int minSlot = 0;
    int minFreq = 256;
    
    for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
        long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
        if (slot == 0) return i;
        
        int freq = (int) ((extLong >>> (i * 8)) & 0xFF);
        if (freq < minFreq) {
            minFreq = freq;
            minSlot = i;
        }
    }
    return minSlot;
}

/**
 * SAMPLED_LRU: 随机采样 2 个槽，选择未访问的
 */
private int selectSampledLruVictim(MemorySegment segment, int bucketOffset) {
    long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
    
    int s1 = random.nextInt(SLOTS_PER_BUCKET);
    int s2 = random.nextInt(SLOTS_PER_BUCKET);
    
    long slot1 = segment.getLong(bucketOffset + s1 * SLOT_SIZE);
    if (slot1 == 0) return s1;
    
    long slot2 = segment.getLong(bucketOffset + s2 * SLOT_SIZE);
    if (slot2 == 0) return s2;
    
    boolean acc1 = (extLong & (1L << s1)) != 0;
    boolean acc2 = (extLong & (1L << s2)) != 0;
    
    if (!acc1 && acc2) return s1;
    if (acc1 && !acc2) return s2;
    return s1;
}
```

### 5.6 TinyLFU 衰减机制

```java
private static final long DECAY_INTERVAL = 10000;  // 每 10000 次访问衰减一次
private long tinyLfuAccessCount = 0;

// 在 updateAccessMetadata 中调用（仅 TINY_LFU 策略）
private void checkAndDecay() {
    if (++tinyLfuAccessCount >= DECAY_INTERVAL) {
        decayAllFrequencies();
        tinyLfuAccessCount = 0;
    }
}

// 所有频率除以 2
private void decayAllFrequencies() {
    for (int bucket = 0; bucket < bucketCount; bucket++) {
        MemorySegment segment = getSegmentForBucket(bucket);
        int bucketOffset = getBucketOffsetInSegment(bucket);
        long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
        
        long newExtLong = 0;
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            int freq = (int) ((extLong >>> (i * 8)) & 0xFF);
            newExtLong |= ((long)(freq >>> 1) << (i * 8));
        }
        segment.putLong(bucketOffset + EXTENSION_OFFSET, newExtLong);
    }
}
```

### 5.7 LRU 时间戳管理

```java
private int currentTimestamp = 0;

private int getRelativeTimestamp() {
    return (currentTimestamp++) & 0x1FF;  // 循环 0-511
}
```

### 5.8 公开 API

```java
public ReplacementPolicy getReplacementPolicy() {
    return replacementPolicy;
}
```

---

## 6. 统一 Slot 格式

MainTable 和 L0Table 使用**完全相同**的 slot 格式，便于理解：

### 5.1 Slot 编解码 (内联实现)

所有操作直接内联，不抽取方法：

```java
// 编码
long slot = ((long) hash << 32) | (ptr & 0xFFFFFFFFL);

// 解码
int hash = (int) (slot >>> 32);
int ptr = (int) slot;

// 判空
if (slot == 0) { /* 空槽 */ }

// 删除
chunk[offset + i] = 0;              // MainTable
segment.putLong(slotOffset, 0);     // L0Table
```

### 5.2 访问模式差异

| 组件 | 存储 | 访问方式 |
|------|------|----------|
| **MainTable** | `long[][]` 堆内数组 | 直接数组索引: `chunks[ci][offset + i]` |
| **L0Table** | MemorySegment (L0 内存) | MemorySegment API: `segment.getLong(offset)` |

### 5.3 Hash 计算统一

由于 slot 中存储完整的 32 位 hash，不再需要单独计算 tag：

```java
// ForL0StateMap 中
public S get(K key, N namespace) {
    int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());
    
    // L0Table 和 MainTable 使用相同的 hash
    long addr = l0Table.get(hash, key, namespace, heapEntryStore);
    if (addr > 0) return heapEntryStore.get(addr).getState();
    
    addr = mainTable.get(hash, key, namespace, heapEntryStore);
    // ...
}
```

---

## 7. 内存管理

### 6.1 MainTable: Chunked Long Array (堆内)

```
┌─────────────────────────────────────────────────────────────────┐
│                MainTable Chunked Long Array (堆内)               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   long[][] chunks:                                               │
│                                                                  │
│   chunks[0] → long[CHUNK_SIZE * 8]  // 65536 buckets × 8 longs  │
│   chunks[1] → long[CHUNK_SIZE * 8]                               │
│   chunks[2] → long[CHUNK_SIZE * 8]                               │
│   ...                                                            │
│   chunks[n] → long[CHUNK_SIZE * 8]                               │
│                                                                  │
│   每个 chunk: 65536 buckets = 4MB (65536 × 64B)                  │
│   由 GC 自动管理，无需手动释放                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 L0Table: MemorySegment (L0 内存)

```
┌─────────────────────────────────────────────────────────────────┐
│                L0Table MemorySegment Array (L0 内存)             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   MemorySegment[] memorySegments:                                │
│                                                                  │
│   memorySegments[0] → L0 内存块 (通过 L0MemoryAllocator 分配)    │
│   memorySegments[1] → L0 内存块                                  │
│   ...                                                            │
│                                                                  │
│   内存来源:                                                       │
│   - L0 模式: 鲲鹏 CPU L0 Cache (libl0mempool.so)                 │
│   - 模拟模式: malloc/free (macOS 开发环境)                       │
│                                                                  │
│   需要在 close() 时通过 L0MemoryAllocator.release() 释放         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 6.3 MainTable 扩展桶池

```
┌─────────────────────────────────────────────────────────────────┐
│                    Extension Bucket Pool                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   主桶区域:                                                       │
│   chunks[0..M] → 主桶 (bucketCount 个)                           │
│                                                                  │
│   扩展桶区域:                                                     │
│   chunks[M+1..] → 扩展桶池                                       │
│                                                                  │
│   每个主桶可分配最多 255 个扩展桶                                  │
│   使用 extensionBucketBaseIndices[] 记录基址                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 6.4 内存使用估算

| 组件 | 计算公式 | 示例 (1M 条目) |
|------|----------|----------------|
| MainTable 主桶 | bucketCount × 64B | 512KB (8K buckets) |
| MainTable 扩展桶 | 按需分配 | ~512KB (估算) |
| L0Table | l0BucketCount × 64B | 64KB (1K buckets) |
| **总计** | - | ~1.1MB |

### 6.5 MainTable Chunk 扩容策略

```java
private void ensureChunkCapacity(int requiredBuckets) {
    int requiredChunks = (requiredBuckets + CHUNK_SIZE - 1) >>> CHUNK_SIZE_BITS;
    if (requiredChunks > chunks.length) {
        int newLength = Math.max(chunks.length * 2, requiredChunks);
        chunks = Arrays.copyOf(chunks, newLength);
    }
    
    // 懒分配新 chunk
    for (int i = 0; i < requiredChunks; i++) {
        if (chunks[i] == null) {
            chunks[i] = new long[CHUNK_SIZE * BUCKET_SIZE_LONGS];
        }
    }
}
```

---

## 8. 性能优化要点

### 7.1 位操作优化

| 优化项 | 说明 |
|--------|------|
| **单次 long 读取** | 每个 slot 一次 `getLong` 即可获取 hash 和 ptr |
| **位移代替乘除** | `bucketIndex << 3` 代替 `bucketIndex * 8` |
| **位掩码代替取模** | `hash & (bucketCount - 1)` 代替 `hash % bucketCount` |
| **无分支编码** | slot 编码使用位操作，避免条件分支 |

### 7.2 内存访问优化

| 优化项 | 说明 |
|--------|------|
| **缓存行对齐** | 64B bucket 正好一个缓存行 |
| **顺序扫描** | 7 个 slot 顺序访问，利用 prefetch |
| **减少间接跳转** | 使用 `long[]` 而非 `MemorySegment` |

### 7.3 热路径优化

```java
// Hot path: 内联关键操作
public long get(int hash, K key, N namespace, HeapEntryStore<K, N, S> store) {
    int bucketIndex = hash & (bucketCount - 1);
    long[] chunk = chunks[bucketIndex >>> CHUNK_SIZE_BITS];
    int offset = (bucketIndex & CHUNK_MASK) << 3;  // * 8
    
    // 展开循环前几个 slot (最常命中位置)
    long slot0 = chunk[offset];
    if ((int) slot0 != 0 && (int) (slot0 >>> 32) == hash) {
        if (store.matches((int) slot0, key, namespace)) {
            return (int) slot0;
        }
    }
    
    long slot1 = chunk[offset + 1];
    // ... 类似处理
    
    // 剩余 slots 使用循环
    for (int i = 2; i < 7; i++) {
        long slot = chunk[offset + i];
        if ((int) slot == 0) continue;
        if ((int) (slot >>> 32) != hash) continue;
        if (store.matches((int) slot, key, namespace)) {
            return (int) slot;
        }
    }
    
    // 检查扩展桶...
    return searchExtensionBuckets(hash, key, namespace, store, chunk, offset);
}
```

### 7.4 避免的反模式

| 反模式 | 原因 | 替代方案 |
|--------|------|----------|
| 边界检查 | 热路径开销 | 保证调用方传入合法参数 |
| 对象分配 | GC 压力 | 使用基本类型返回值 |
| 方法调用 | 调用开销 | 内联关键方法 |
| 同步原语 | Flink 状态单线程 | 无锁实现 |

---

## 9. 改造影响分析

### 8.1 API 变化

| 接口 | 当前签名 | 新签名 | 说明 |
|------|----------|--------|------|
| `get` | `get(hash, tag, key, ns, store)` | `get(hash, key, ns, store)` | 移除 tag 参数 |
| `put` | `put(hash, tag, ptr, key, ns, store)` | `put(hash, ptr, key, ns, store)` | 移除 tag 参数 |
| `remove` | `remove(hash, tag, key, ns, store)` | `remove(hash, key, ns, store)` | 移除 tag 参数 |

### 8.2 移除的组件

| 组件 | 原因 |
|------|------|
| `MemoryManagerAllocator` | MainTable 改用堆内数组，无需 Flink MemoryManager |
| Tag 字段 | 使用完整 32 位 hash |
| Valid bitmap (L0Table) | 通过 ptr==0 判断 |

### 8.3 保留的组件

| 组件 | 说明 |
|------|------|
| `HeapEntryStore` | 不变，仍使用 chunked 对象数组 |
| `HeapStateEntry` | 不变 |
| `ForL0StateMap` | 调整调用签名 |
| `L0MemoryAllocator` | 保留，L0Table 仍需要 L0 内存 |
| `NativeL0Memory` (JNI) | 保留，用于 L0 内存分配 |
| 替换策略枚举 | 保留，实现方式调整 |

### 8.4 ForL0StateMap 改造

```java
// 旧代码
int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());
short tag = (short) (hash >>> 16);
l0Table.get(hash, tag, key, namespace, heapEntryStore);

// 新代码
int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());
l0Table.get(hash, key, namespace, heapEntryStore);
```

---

## 10. 实施计划

### 9.1 阶段划分

| 阶段 | 任务 | 预计时间 |
|------|------|----------|
| **阶段 1** | MainTable 改造 | 2-3 天 |
| **阶段 2** | L0Table 改造 | 1-2 天 |
| **阶段 3** | ForL0StateMap 适配 | 0.5 天 |
| **阶段 4** | ForL0StateTable 适配 | 0.5 天 |
| **阶段 5** | Backend 层适配 | 0.5 天 |
| **阶段 6** | 清理废弃代码 | 0.5 天 |
| **阶段 7** | 测试验证 | 1-2 天 |

### 9.2 阶段 1: MainTable 改造

1. 移除 `MemorySegment[]` 字段，改为 `long[][] chunks`
2. 移除 `MemoryManagerAllocator` 依赖，构造函数不再需要 allocator 参数
3. 移除 `import org.apache.flink.core.memory.MemorySegment`
4. 移除 `import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator`
5. 实现 chunked `long[][]` 存储和自动扩容
6. 改造 get/put/remove 方法 (slot 格式: hash:32 + ptr:32)
7. 移除 tag 参数，方法签名简化
8. 改造扩展桶管理 (使用 long 的低 32 位存储 4 个 8-bit 扩展指针)
9. 改造 resize 逻辑 (纯 Java 数组复制)
10. 更新 forEachEntry 迭代
11. 移除 close() 中对 allocator.close() 的调用

### 9.3 阶段 2: L0Table 布局改造

1. 调整 bucket 布局 (7 slots + 1 extension)
2. 统一使用 `getLong`/`putLong` 访问 slot (slot 格式: hash:32 + ptr:32)
3. 移除 tag 参数
4. 移除 ValidBitmap，通过 `slot == 0` 判断空槽
5. 重新实现替换算法 (CLOCK/LRU/LFU 使用 extension long)
6. 更新统计指标收集
7. **保留** L0MemoryAllocator 和 MemorySegment (L0 内存不变)

### 9.4 阶段 3: ForL0StateMap 适配

1. 移除 tag 计算 (`short tag = (short) (hash >>> 16)`)
2. 更新所有 MainTable/L0Table 调用签名 (移除 tag 参数)
3. 构造函数移除 `MemoryManagerAllocator` 参数
4. 直接创建 MainTable (无需 allocator)

### 9.5 阶段 4: ForL0StateTable 适配

涉及文件: `ForL0StateTable.java`

1. 移除 `MemoryManager` 相关字段和 ThreadLocal:
   - 移除 `private final MemoryManager memoryManager`
   - 移除 `private static final ThreadLocal<MemoryManager> MEMORY_MANAGER_HOLDER`
   - 移除 `currentMemoryManager` 字段
2. 简化工厂方法签名，移除 `MemoryManager` 参数:
   - `create(keyContext, metaInfo, keySerializer)` (不再需要 memoryManager)
   - `create(keyContext, metaInfo, keySerializer, config, sharedL0Allocator)`
3. 修改 `createStateMap()` 方法:
   - 不再创建 `MemoryManagerAllocator`
   - 直接调用 `new ForL0StateMap(...)` (无需 allocator 参数)

### 9.6 阶段 5: Backend 层适配

涉及文件: `ForL0KeyedStateBackendBuilder.java`, `ForL0KeyedStateBackend.java`, `ForL0StateBackend.java`

#### ForL0KeyedStateBackendBuilder

1. 移除 `private final MemoryManager memoryManager` 字段
2. 移除构造函数中的 `MemoryManager memoryManager` 参数
3. 移除 `MemoryManager` 相关 import
4. 修改 `StateTableFactory` 实现:
   ```java
   // 旧代码
   return ForL0StateTable.create(keyContext, metaInfo, keySerializer, mm, config, l0Allocator);
   
   // 新代码
   return ForL0StateTable.create(keyContext, metaInfo, keySerializer, config, l0Allocator);
   ```
5. 移除 `build()` 方法中传递给 `ForL0KeyedStateBackend` 的 `memoryManager` 参数

#### ForL0KeyedStateBackend

1. 移除 `private final MemoryManager memoryManager` 字段
2. 移除构造函数中的 `MemoryManager memoryManager` 参数
3. 移除 `import org.apache.flink.runtime.memory.MemoryManager`

#### ForL0StateBackend

1. 检查 `createKeyedStateBackend` 方法，移除传递 MemoryManager 的代码
2. 更新 `ForL0KeyedStateBackendBuilder` 构造调用

### 9.7 阶段 6: 清理废弃代码

#### 删除文件

| 文件 | 原因 |
|------|------|
| `space/MemoryManagerAllocator.java` | MainTable 改用堆内数组，不再需要 |
| `space/MemorySegmentSlice.java` | 当前无引用，可一并删除 |

#### 删除测试文件

| 文件 | 原因 |
|------|------|
| `space/MemoryManagerAllocatorTest.java` | 对应的类已删除 |

#### 可能删除的测试辅助类

| 文件 | 处理方式 |
|------|----------|
| `MemoryManagerBuilder.java` (test) | 如果改造后无测试使用 MemoryManager，可删除；否则保留供 minicluster 测试使用 |
| `OffHeapMemoryReleaseTest.java` | MainTable 不再使用堆外内存，此测试可能失去意义，评估后决定是否删除 |

#### 保留文件

| 文件 | 原因 |
|------|------|
| `space/L0MemoryAllocator.java` | L0Table 仍需要 |
| `space/NativeL0MemoryAllocator.java` | L0Table 仍需要 |
| `space/NativeL0Memory.java` | JNI 桥接类，L0Table 仍需要 |

#### 更新其他测试

| 测试文件 | 改动内容 |
|----------|----------|
| `IndexIntegrationTest.java` | 移除 MemoryManager 和 MemoryManagerAllocator |
| `MainTableTest.java` | 移除 MemoryManager，直接测试堆内数组版本 |
| `MainTableStressTest.java` | 同上 |
| `ForL0StateMapTest.java` | 移除 MemoryManager，更新构造函数调用 |
| `ForL0StateMapStressTest.java` | 同上 |
| `L0TableTest.java` | 调整为新的 bucket 布局 (7 slots) |
| `HeapEntryStoreTest.java` | 无需改动 (HeapEntryStore 不变) |

### 9.8 测试计划

| 测试类型 | 测试内容 |
|----------|----------|
| 单元测试 | 各操作的正确性 |
| 边界测试 | 空表、满表、扩容边界 |
| 集成测试 | ForL0StateMap + MainTable + L0Table + HeapEntryStore |
| 性能测试 | 吞吐量、延迟对比 |
| 压力测试 | 大量数据下的稳定性 |
| 回归测试 | 现有 WordCount benchmark 对比 |

### 9.9 改动文件汇总

#### 主代码 (src/main/java)

| 文件路径 | 改动类型 | 主要改动 |
|----------|----------|----------|
| `MainTable.java` | **重写** | 堆内 long[][] + 新 slot 格式 |
| `L0Table.java` | **重写** | 新 bucket 布局 + 新替换算法 |
| `ForL0StateMap.java` | 修改 | 移除 allocator + 移除 tag |
| `ForL0StateTable.java` | 修改 | 移除 MemoryManager 相关 |
| `ForL0KeyedStateBackend.java` | 修改 | 移除 MemoryManager 字段 |
| `ForL0KeyedStateBackendBuilder.java` | 修改 | 移除 MemoryManager 参数 |
| `ForL0StateBackend.java` | 修改 | 更新 builder 调用 |
| `space/MemoryManagerAllocator.java` | **删除** | 不再需要 |
| `space/MemorySegmentSlice.java` | **删除** | 不再需要 |

#### 测试代码 (src/test/java)

| 文件路径 | 改动类型 | 主要改动 |
|----------|----------|----------|
| `MainTableTest.java` | 修改 | 移除 MemoryManager |
| `MainTableStressTest.java` | 修改 | 移除 MemoryManager |
| `L0TableTest.java` | 修改 | 适配新 bucket 布局 |
| `ForL0StateMapTest.java` | 修改 | 移除 MemoryManager |
| `ForL0StateMapStressTest.java` | 修改 | 移除 MemoryManager |
| `IndexIntegrationTest.java` | 修改 | 移除 MemoryManagerAllocator |
| `space/MemoryManagerAllocatorTest.java` | **删除** | 对应类已删除 |
| `OffHeapMemoryReleaseTest.java` | **评估** | 可能删除或改造 |

---

## 附录 A: 新旧布局对比图

### MainTable Bucket 对比

```
┌─────────────────────────────────────────────────────────────────┐
│  旧布局 (64 字节, MemorySegment)                                 │
├─────────────────────────────────────────────────────────────────┤
│  [Tag:2B][Ptr:8B] × 6 = 60B  +  [ExtPtrs:4B] = 64B              │
│  Slot 数量: 6                                                    │
│  存储介质: 堆外 MemorySegment                                     │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  新布局 (64 字节 = 8 longs, 堆内)                                 │
├─────────────────────────────────────────────────────────────────┤
│  [Hash:4B|Ptr:4B] × 7 = 56B  +  [Reserved:4B|ExtPtrs:4B] = 64B  │
│  Slot 数量: 7 (+1)                                               │
│  存储介质: 堆内 long[][] 数组                                     │
└─────────────────────────────────────────────────────────────────┘
```

### L0Table Bucket 对比

```
┌─────────────────────────────────────────────────────────────────┐
│  旧布局 (64 字节, L0 内存)                                        │
├─────────────────────────────────────────────────────────────────┤
│  [ValidBitmap:4B] + [Tag:2B][Ext:5B][Ptr:8B] × 4 = 64B          │
│  Slot 数量: 4                                                    │
│  存储介质: L0 内存 (MemorySegment 包装)                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  新布局 (64 字节 = 8 longs, L0 内存)                              │
├─────────────────────────────────────────────────────────────────┤
│  [Hash:4B|Ptr:4B] × 7 = 56B  +  [Extension:8B] = 64B            │
│  Slot 数量: 7 (+3)                                               │
│  Valid 判断: ptr == 0                                            │
│  存储介质: L0 内存 (MemorySegment 包装, 不变)                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 附录 B: 关键位操作模板

### Slot 操作 (内联)

```java
// 编码
long slot = ((long) hash << 32) | (ptr & 0xFFFFFFFFL);

// 解码
int hash = (int) (slot >>> 32);
int ptr = (int) slot;

// 判空
if (slot == 0) { ... }

// 删除 (置零)
chunk[offset + i] = 0;
```

### MainTable 扩展指针操作

```java
// 低 32 位存储 4 个 8 位扩展指针
public static int getExtPtr(long extLong, int index) {
    return (int) ((extLong >>> (index << 3)) & 0xFF);
}

public static long setExtPtr(long extLong, int index, int value) {
    int shift = index << 3;
    return (extLong & ~(0xFFL << shift)) | ((long) (value & 0xFF) << shift);
}
```

### L0Table CLOCK 位操作

```java
// 低 7 位存储 accessed 标志
public static boolean isAccessed(long extLong, int slotIndex) {
    return (extLong & (1L << slotIndex)) != 0;
}

public static long setAccessed(long extLong, int slotIndex) {
    return extLong | (1L << slotIndex);
}

public static long clearAllAccessed(long extLong) {
    return extLong & ~0x7FL;
}
```
