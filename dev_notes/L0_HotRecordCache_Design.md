# L0 SwissTable 设计方案

## 1. 背景与动机

### 1.1 问题分析

VTune Profile 显示 SwissTable 的 **ctrl word 加载** 是主要瓶颈：

```
Line 156: int groupBase = group * LONGS_PER_GROUP;
Line 157: long ctrlWord = groupData[groupBase];   // 34% Memory Bound, 10.6% LLC Miss
```

原因：
- Hash 探测导致 `groupData[]` 访问位置随机
- 工作集超过 CPU Cache 容量
- CPU prefetcher 对随机访问无效

### 1.2 L0 的正确用法

L0 Cache 是**用户可控的高速存储**（~64KB，延迟 ~4-10 cycles）。

正确思路：**把 SwissTable 的 groupData 直接分配在 L0 内存中**，而不是缓存热点 key-value。

```
错误思路：L0 作为热点 key-value 缓存 ❌
正确思路：L0 作为 SwissTable 的存储介质 ✅
```

### 1.3 设计目标

创建 `SwissTableLongL0` —— 将 groupData 存储在 L0 内存中的 SwissTable 变体。

## 2. 核心设计

### 2.1 架构概览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ForL0KeyedStateBackend                          │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    L0MemoryManager (64KB)                       │   │
│   │                                                                  │   │
│   │   分配策略：                                                     │   │
│   │   - 小表 (groupData ≤ L0 剩余容量)：整个 groupData 放 L0        │   │
│   │   - 大表：groupData 放 Heap，L0 容量分配给更热的小表            │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                           │                                              │
│           ┌───────────────┴───────────────┐                              │
│           ▼                               ▼                              │
│   ┌───────────────────┐           ┌───────────────────┐                 │
│   │ SwissTableLongL0  │           │ SwissTableLong    │                 │
│   │ (groupData in L0) │           │ (groupData in Heap)│                 │
│   │                   │           │                   │                 │
│   │ 小表，高频访问     │           │ 大表或 L0 已满    │                 │
│   └───────────────────┘           └───────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 容量分析

每个 group 占用 72 bytes (9 longs × 8 bytes)：

| 表大小 (slots) | Groups | groupData 大小 | 可放入 64KB L0? |
|---------------|--------|---------------|----------------|
| 64 | 8 | 576 B | ✅ |
| 512 | 64 | 4.5 KB | ✅ |
| 2K | 256 | 18 KB | ✅ |
| 4K | 512 | 36 KB | ✅ |
| 8K | 1024 | 72 KB | ❌ 超出 |

**结论**：单表 ≤ 4K slots 时可完整放入 64KB L0。

### 2.3 多表共享 L0

实际场景中，一个 Backend 有多个 StateTable，每个 StateTable 有多个 KeyGroup：

```
典型配置：
- 3 个 StateTable
- 128 个 KeyGroup
- 每个 KeyGroup 一个 SwissTable

总表数: 3 × 128 = 384 个 SwissTable
```

L0 分配策略：
1. **按需分配**：新建 SwissTable 时申请 L0 空间
2. **空间不足时退回 Heap**：大表或 L0 满时使用 Heap 版本
3. **动态迁移（可选）**：冷表可从 L0 迁移到 Heap，腾出空间

## 3. SwissTableLongL0 实现

### 3.1 类设计

```java
/**
 * SwissTable with groupData stored in L0 memory.
 * 
 * Key difference from SwissTableLong:
 * - groupData is in L0 native memory, not Java heap
 * - Access via NativeL0Memory JNI calls
 * - values[] and hashes[] remain in Java heap
 */
public class SwissTableLongL0<S> {
    
    // L0 内存地址
    private long groupDataL0Addr;
    
    // Heap 存储（values 必须在 Heap，因为是 Object 引用）
    private Object[] values;
    private int[] hashes;
    
    // 元数据
    private int capacity;
    private int groupMask;
    private int used;
    private int tomb;
    private int growthLeft;
    
    public SwissTableLongL0(int slotCount, L0MemoryManager l0Manager) {
        int groupCount = slotCount >>> 3;
        long groupDataSize = groupCount * LONGS_PER_GROUP * 8L;
        
        // 在 L0 分配 groupData
        this.groupDataL0Addr = l0Manager.allocate(groupDataSize);
        if (groupDataL0Addr == 0) {
            throw new L0MemoryAllocationException("L0 allocation failed");
        }
        
        // 初始化 ctrl words 为 EMPTY
        initCtrlWords(groupCount);
        
        // values 和 hashes 仍在 Heap
        this.values = new Object[slotCount];
        this.hashes = new int[slotCount];
    }
}
```

### 3.2 核心操作 - get

```java
public S get(int hash, long key) {
    long h2Pattern = LSB * (hash & 0x7FL);
    int group = (hash >>> 7) & groupMask;
    int stride = 1;

    while (true) {
        // 从 L0 读取 ctrl word（关键优化点！）
        long ctrlOffset = groupDataL0Addr + (long) group * LONGS_PER_GROUP * 8;
        long ctrlWord = NativeL0Memory.getLong(ctrlOffset);

        // SWAR 并行匹配
        for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
            int lane = Long.numberOfTrailingZeros(match) >>> 3;
            // 从 L0 读取 key
            long keyOffset = ctrlOffset + ((1 + lane) << 3);
            long storedKey = NativeL0Memory.getLong(keyOffset);
            
            if (storedKey == key) {
                // 从 Heap 读取 value（这里仍有 pointer chasing，但频率低）
                return (S) values[(group << 3) + lane];
            }
        }

        // 检查 EMPTY
        if (matchEmpty(ctrlWord) != 0) {
            return null;
        }

        group = (group + stride) & groupMask;
        stride++;
    }
}
```

### 3.3 核心操作 - put

```java
public int put(int hash, long key) {
    long h2Pattern = LSB * (hash & 0x7FL);
    int group = (hash >>> 7) & groupMask;
    int stride = 1;
    int firstDeletedSlot = -1;

    while (true) {
        long ctrlOffset = groupDataL0Addr + (long) group * LONGS_PER_GROUP * 8;
        long ctrlWord = NativeL0Memory.getLong(ctrlOffset);

        // 查找已存在的 key
        for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
            int lane = Long.numberOfTrailingZeros(match) >>> 3;
            long keyOffset = ctrlOffset + ((1 + lane) << 3);
            if (NativeL0Memory.getLong(keyOffset) == key) {
                return (group << 3) + lane;  // 已存在
            }
        }

        // 查找 EMPTY 或 DELETED 槽位
        long emptyOrDeleted = matchEmptyOrDeleted(ctrlWord);
        if (emptyOrDeleted != 0) {
            int lane = Long.numberOfTrailingZeros(emptyOrDeleted) >>> 3;
            byte ctrlByte = getCtrlByte(ctrlWord, lane);
            
            if (ctrlByte == CTRL_EMPTY) {
                // 插入到 EMPTY 槽位
                int slot = (group << 3) + lane;
                
                // 写入 L0: key
                long keyOffset = ctrlOffset + ((1 + lane) << 3);
                NativeL0Memory.putLong(keyOffset, key);
                
                // 写入 L0: ctrl byte
                long newCtrlWord = setCtrlByte(ctrlWord, lane, (byte)(hash & 0x7F));
                NativeL0Memory.putLong(ctrlOffset, newCtrlWord);
                
                // 写入 Heap: hash
                hashes[slot] = hash;
                used++;
                growthLeft--;
                
                return slot | NEW_FLAG;
            }
            // ... DELETED 处理
        }

        group = (group + stride) & groupMask;
        stride++;
    }
}
```

## 4. L0 内存管理器

### 4.1 设计

```java
/**
 * Manages L0 memory allocation for SwissTables.
 * Shared at ForL0KeyedStateBackend level.
 */
public class L0MemoryManager implements AutoCloseable {
    
    private static final long L0_CAPACITY = 64 * 1024;  // 64KB
    
    private final long l0BaseAddr;
    private long allocated = 0;
    
    // 记录每个分配的 (addr, size) 用于释放
    private final List<AllocationRecord> allocations = new ArrayList<>();
    
    public L0MemoryManager() {
        this.l0BaseAddr = NativeL0Memory.malloc(L0_CAPACITY);
        if (l0BaseAddr == 0) {
            throw new L0MemoryAllocationException("Failed to allocate L0 pool");
        }
    }
    
    /**
     * 分配 L0 内存
     * @return L0 地址，或 0 表示空间不足
     */
    public synchronized long allocate(long size) {
        // 对齐到 8 字节
        size = (size + 7) & ~7L;
        
        if (allocated + size > L0_CAPACITY) {
            return 0;  // 空间不足
        }
        
        long addr = l0BaseAddr + allocated;
        allocated += size;
        allocations.add(new AllocationRecord(addr, size));
        return addr;
    }
    
    public long getAvailable() {
        return L0_CAPACITY - allocated;
    }
    
    @Override
    public void close() {
        NativeL0Memory.free(l0BaseAddr);
    }
}
```

### 4.2 分配策略

```java
/**
 * 根据表大小决定使用 L0 还是 Heap 版本
 */
public SwissTableLong<S> createTable(int slotCount, L0MemoryManager l0Manager) {
    int groupCount = slotCount >>> 3;
    long requiredSize = groupCount * LONGS_PER_GROUP * 8L;
    
    if (l0Manager != null && l0Manager.getAvailable() >= requiredSize) {
        // L0 有空间，使用 L0 版本
        return new SwissTableLongL0<>(slotCount, l0Manager);
    } else {
        // L0 已满或不可用，使用 Heap 版本
        return new SwissTableLong<>(slotCount);
    }
}
```

## 5. 与现有代码的集成

### 5.1 接口抽象

```java
/**
 * SwissTable 接口，统一 Heap 版本和 L0 版本
 */
public interface ISwissTableLong<S> {
    S get(int hash, long key);
    int put(int hash, long key);
    S remove(int hash, long key);
    void rehash();
    void grow();
    int size();
    boolean isEmpty();
    // ...
}

public class SwissTableLong<S> implements ISwissTableLong<S> { ... }
public class SwissTableLongL0<S> implements ISwissTableLong<S> { ... }
```

### 5.2 StateStore 集成

```java
public class ForL0StateStoreLong<N, S> extends ForL0StateStore<Long, N, S> {
    
    private final L0MemoryManager l0Manager;
    
    // VoidNamespace 模式
    private final ISwissTableLong<S>[] tablesLong;
    
    private ISwissTableLong<S> createOrGetTable(int idx) {
        ISwissTableLong<S> table = tablesLong[idx];
        if (table == null) {
            // 尝试用 L0，失败则用 Heap
            table = SwissTableFactory.create(INITIAL_CAPACITY, l0Manager);
            tablesLong[idx] = table;
        }
        return table;
    }
}
```

## 6. Grow 处理

当 SwissTableLongL0 需要 grow 时：

```java
public void grow() {
    int newCapacity = capacity * 2;
    long newGroupDataSize = (newCapacity >>> 3) * LONGS_PER_GROUP * 8L;
    
    // 尝试在 L0 分配新空间
    long newL0Addr = l0Manager.allocate(newGroupDataSize);
    
    if (newL0Addr != 0) {
        // L0 仍有空间，继续使用 L0
        migrateToNewL0(newL0Addr, newCapacity);
    } else {
        // L0 空间不足，迁移到 Heap
        // 返回一个 Heap 版本的 SwissTableLong
        throw new L0CapacityExceededException();
        // 或者：在外层处理，创建 Heap 版本并复制数据
    }
}
```

## 7. 性能预期

### 7.1 延迟对比

| 操作 | Heap 版本 (Cache Miss) | L0 版本 |
|------|----------------------|---------|
| 读取 ctrlWord | ~100 cycles | ~10 cycles |
| 读取 key | (同一 cache line) | ~10 cycles |
| 读取 value | ~100 cycles | ~100 cycles (仍在 Heap) |

假设平均探测 1.2 个 group：
- Heap 版本 (miss): 1.2 × 100 = **120 cycles**
- L0 版本: 1.2 × 10 + 100 = **112 cycles** (value 访问仍慢)

但如果 value 本身是热点（CPU cache 命中）：
- L0 版本: 1.2 × 10 + 20 = **32 cycles** (比 Heap 版本快 4x)

### 7.2 适用场景

| 场景 | 收益 |
|------|------|
| 小表 (≤ 4K slots) | ✅ 高，整表放 L0 |
| 大表 | ❌ 低，退回 Heap |
| 读多写少 | ✅ 高，ctrl 查找快 |
| Zipf 访问模式 | ✅ 高，热点 value 在 CPU cache |

## 8. 实现计划

### Phase 1: 基础框架
1. `L0MemoryManager` 类
2. `ISwissTableLong<S>` 接口
3. `SwissTableLongL0<S>` 基本实现

### Phase 2: 集成
1. 修改 `ForL0StateStoreLong` 使用工厂创建表
2. 处理 grow 时的 L0 → Heap 迁移

### Phase 3: 验证
1. 单元测试
2. Benchmark 对比
3. VTune 验证 Memory Bound 下降

## 9. 风险与缓解

| 风险 | 缓解 |
|------|------|
| L0 容量有限 | 自动退回 Heap 版本 |
| JNI 调用开销 | 批量操作 / 内联优化 |
| 复杂度增加 | 接口抽象，两个实现互不干扰 |
| Grow 时空间不足 | 迁移到 Heap 版本 |
