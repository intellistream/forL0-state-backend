# MainTable & Entry 扁平化优化改造计划

**日期**: 2025-12-24  
**分支**: feature/heap-object-store  
**目标**: 减少内存访问次数和 cache miss，提升状态访问性能

---

## 📋 改造目标

### 1. MainTable 扁平化
- **当前**: `long[][] chunks` - 二维分块数组
- **目标**: `long[] table` - 一维连续数组
- **初始容量**: 256K buckets (2M longs = 16MB)
- **扩展区**: 独立的 `long[] extensions` 数组

### 2. Entry 存储扁平化
- **当前**: `HeapStateEntry<K,N,S>[][]` - 二维分块数组，每个entry是对象
- **目标**: `Object[] entries` - 一维数组，K、N、S 三元组连续存储
- **布局**: `[K0, N0, S0, K1, N1, S1, ...]` (步长 3)
- **hash**: 单独的 `int[] hashes` 数组（仅在rehash时访问）

### 3. 移除 HeapStateEntry 类
- 删除 `HeapStateEntry.java` 文件
- 改为直接在 `entries[]` 数组中存储 K、N、S 引用
- 需要时通过匿名类或内部类临时构造 StateEntry 实现

---

## 🎯 预期收益

### 性能提升
- **减少内存访问**: 从 4 次（对象+3字段）减少到 3 次（数组3次）
- **空间局部性**: K、N、S 连续存储，提升 cache line 利用率
- **指针追踪**: 减少二维数组的基址解引用

### 内存优化
- **对象头开销**: 每个 entry 节省 16 字节对象头
- **256K entries**: 节省约 4MB 内存

### 预期指标
- Get 操作: **+10-15%**
- Put 操作: **+8-12%**
- Cache Miss: **-15-20%**

---

## 📂 涉及文件清单

### 核心实现文件
1. ✅ **MainTable.java** - 主改造文件
   - 替换 `long[][] chunks` → `long[] table`
   - 替换 `HeapStateEntry[][] entryChunks` → `Object[] entries + int[] hashes`
   - 修改所有 entry 访问代码
   - 更新索引计算逻辑

2. ✅ **L0Table.java**
   - 修改 `get()` 方法签名和参数
   - 从 `entryChunks[i][j]` 改为 `entries[idx*3+offset]`

3. ✅ **ForL0StateMap.java**
   - 修改所有 `HeapStateEntry` 类型引用
   - 更新迭代器实现
   - 调整直接访问 `mainTable.entryChunks` 的代码

4. ❌ **HeapStateEntry.java** - 删除此文件
   - 功能由 `entries[]` 数组直接承担
   - StateEntry 接口保留（Flink API 要求）

### 测试文件
5. ✅ **MainTableTest.java**
   - 移除所有 `HeapStateEntry` 类型引用
   - 使用 `mainTable.get()` 返回值（改为返回 ptr 或封装类）
   - 验证功能正确性

6. ✅ **MainTableStressTest.java**
   - 适配新的 API
   - 验证高负载场景

7. ✅ **ForL0StateMapTest.java**
   - 验证整体集成正确性

8. ✅ **ForL0StateMapStressTest.java**
   - 验证压力场景

---

## 🔧 详细改造方案

### Phase 1: MainTable.java 核心改造

#### 1.1 数据结构定义

```java
public class MainTable<K, N, S> {
    // ========== Bucket Index Storage ==========
    private long[] table;            // 主表 bucket 数组
    private long[] extensions;       // 树形扩展区
    private int bucketCount;
    private int bucketMask;
    
    // Extension management (保持不变)
    private int[] extensionBucketCounts;
    private int[] extensionBucketBaseIndices;
    private int extensionPoolCapacity;
    private int extensionPoolUsed;
    
    // ========== Entry Storage ==========
    private Object[] entries;        // [K0,N0,S0, K1,N1,S1, ...]
    private int[] hashes;            // [hash0, hash1, ...]
    private int entryCapacity;       // entries.length / 3
    private int nextEntryIndex;      // 下一个分配的索引
    
    // Free list (保持不变)
    private int[] freeList;
    private int freeCount;
    
    // Constants
    private static final int INITIAL_BUCKET_COUNT = 256 * 1024;  // 256K
    private static final int INITIAL_ENTRY_CAPACITY = 256 * 1024;
    private static final int ENTRY_STRIDE = 3;  // K, N, S
}
```

#### 1.2 构造函数修改

```java
public MainTable(double loadFactorThreshold, @Nullable L0Table<K, N, S> l0Table) {
    this.loadFactorThreshold = loadFactorThreshold;
    this.l0Table = l0Table;
    this.l0CacheEnabled = (l0Table != null);
    
    // 初始化 bucket table (256K buckets = 2M longs = 16MB)
    this.bucketCount = INITIAL_BUCKET_COUNT;
    this.bucketMask = INITIAL_BUCKET_COUNT - 1;
    this.table = new long[bucketCount * BUCKET_SIZE_LONGS];
    
    // 初始化扩展区管理
    this.extensionBucketBaseIndices = new int[bucketCount];
    this.extensionBucketCounts = new int[bucketCount];
    this.extensionPoolCapacity = 0;
    this.extensionPoolUsed = 0;
    
    // 初始化 entry 存储 (256K entries = 768K Object refs + 256K ints)
    this.entryCapacity = INITIAL_ENTRY_CAPACITY;
    this.entries = new Object[entryCapacity * ENTRY_STRIDE];
    this.hashes = new int[entryCapacity];
    this.nextEntryIndex = 0;
    this.freeList = new int[1024];
    this.freeCount = 0;
}
```

#### 1.3 get() 方法改造

```java
@Nullable
public S get(int hash, K key, N namespace) {
    // L0 cache check (如果启用)
    if (l0CacheEnabled) {
        int ptr = l0Table.get(hash, key, namespace, entries);
        if (ptr > 0) {
            int base = (ptr - 1) * ENTRY_STRIDE;
            return (S) entries[base + 2];  // 返回 state
        }
    }
    
    // MainTable 查找
    int bucketIndex = hash & bucketMask;
    int mainBucketIndex = bucketIndex;
    
    while (true) {
        int offset = bucketIndex * BUCKET_SIZE_LONGS;
        
        // 遍历 bucket slots
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = table[offset + i];
            if (slot == 0) break;  // Early exit
            if ((int)(slot >>> 32) != hash) continue;
            
            int ptr = (int) slot;
            int base = (ptr - 1) * ENTRY_STRIDE;
            K entryKey = (K) entries[base];
            N entryNs = (N) entries[base + 1];
            
            if (entryKey.equals(key) && 
                (entryNs == namespace || entryNs.equals(namespace))) {
                if (l0CacheEnabled) {
                    l0Table.put(hash, ptr);
                }
                return (S) entries[base + 2];  // 返回 state
            }
        }
        
        // 检查扩展 bucket
        long extLong = table[offset + EXTENSION_SLOT_INDEX];
        int extIndex = (hash >>> 29) & 0x7;
        int extOffset = (int)((extLong >>> (extIndex << 3)) & 0xFF);
        if (extOffset == 0) {
            return null;  // Not found
        }
        
        bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
    }
}
```

#### 1.4 put() 方法改造

```java
public int put(int hash, K key, N namespace) {
    if (needsResize) {
        resize();
    }
    
    int bucketIndex = hash & bucketMask;
    int mainBucketIndex = bucketIndex;
    
    while (true) {
        int offset = bucketIndex * BUCKET_SIZE_LONGS;
        
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = table[offset + i];
            
            if (slot == 0) {
                // 空槽位：分配新 entry
                int ptr = allocateEntry(key, namespace, null, hash);
                table[offset + i] = ((long) hash << 32) | (ptr & 0xFFFFFFFFL);
                if (l0CacheEnabled) {
                    l0Table.put(hash, ptr);
                }
                totalEntries++;
                checkResizeNeeded();
                return ptr;
            }
            
            if ((int)(slot >>> 32) != hash) continue;
            
            int ptr = (int) slot;
            int base = (ptr - 1) * ENTRY_STRIDE;
            K entryKey = (K) entries[base];
            N entryNs = (N) entries[base + 1];
            
            if (entryKey.equals(key) && 
                (entryNs == namespace || entryNs.equals(namespace))) {
                // 已存在：返回 ptr
                if (l0CacheEnabled) {
                    l0Table.put(hash, ptr);
                }
                return ptr;
            }
        }
        
        // 扩展 bucket 逻辑...
    }
}
```

#### 1.5 allocateEntry() 方法改造

```java
private int allocateEntry(K key, N namespace, S state, int hash) {
    int entryIndex;
    
    if (freeCount > 0) {
        entryIndex = freeList[--freeCount];
    } else {
        if (nextEntryIndex >= entryCapacity) {
            expandEntries();
        }
        entryIndex = nextEntryIndex++;
    }
    
    int base = entryIndex * ENTRY_STRIDE;
    entries[base] = key;
    entries[base + 1] = namespace;
    entries[base + 2] = state;
    hashes[entryIndex] = hash;
    
    return entryIndex + 1;  // ptr = index + 1
}
```

#### 1.6 removeEntry() 方法改造

```java
private void removeEntry(int ptr) {
    if (ptr <= 0) return;
    
    int entryIndex = ptr - 1;
    int base = entryIndex * ENTRY_STRIDE;
    
    entries[base] = null;
    entries[base + 1] = null;
    entries[base + 2] = null;
    // hashes[entryIndex] 不需要清理
    
    if (freeCount >= freeList.length) {
        freeList = Arrays.copyOf(freeList, freeList.length * 2);
    }
    freeList[freeCount++] = entryIndex;
}
```

#### 1.7 expandEntries() 方法

```java
private void expandEntries() {
    int newCapacity = entryCapacity * 2;
    entries = Arrays.copyOf(entries, newCapacity * ENTRY_STRIDE);
    hashes = Arrays.copyOf(hashes, newCapacity);
    entryCapacity = newCapacity;
}
```

#### 1.8 resize() 方法改造

```java
private void resize() {
    int newBucketCount = bucketCount * 2;
    long[] newTable = new long[newBucketCount * BUCKET_SIZE_LONGS];
    
    // Rehash 所有 entries
    for (int i = 0; i < nextEntryIndex; i++) {
        int base = i * ENTRY_STRIDE;
        if (entries[base] == null) continue;  // 已删除
        
        int hash = hashes[i];
        int ptr = i + 1;
        
        // 重新插入到 newTable
        // ... (插入逻辑)
    }
    
    table = newTable;
    bucketCount = newBucketCount;
    bucketMask = newBucketCount - 1;
    
    // 重建扩展区数组
    extensionBucketBaseIndices = new int[bucketCount];
    extensionBucketCounts = new int[bucketCount];
    // ... 重新分配扩展区
}
```

#### 1.9 迭代器改造

```java
public Iterator<StateEntry<K, N, S>> entryIterator() {
    return new Iterator<StateEntry<K, N, S>>() {
        private int currentIndex = 0;
        
        @Override
        public boolean hasNext() {
            // 跳过已删除的 entries
            while (currentIndex < nextEntryIndex) {
                if (entries[currentIndex * ENTRY_STRIDE] != null) {
                    return true;
                }
                currentIndex++;
            }
            return false;
        }
        
        @Override
        public StateEntry<K, N, S> next() {
            final int idx = currentIndex++;
            final int base = idx * ENTRY_STRIDE;
            
            // 返回匿名 StateEntry 实现
            return new StateEntry<K, N, S>() {
                @Override
                public K getKey() {
                    return (K) entries[base];
                }
                
                @Override
                public N getNamespace() {
                    return (N) entries[base + 1];
                }
                
                @Override
                public S getState() {
                    return (S) entries[base + 2];
                }
            };
        }
    };
}
```

---

### Phase 2: L0Table.java 适配

#### 2.1 修改 get() 方法签名

```java
// 原签名
public HeapStateEntry<K, N, S> get(int hash, K key, N namespace, 
                                   HeapStateEntry<K, N, S>[][] entryChunks)

// 新签名
public int get(int hash, K key, N namespace, Object[] entries)
```

#### 2.2 修改 entry 访问逻辑

```java
public int get(int hash, K key, N namespace, Object[] entries) {
    int slotIndex = hash & (slotCount - 1);
    int offset = slotIndex << SLOT_SIZE_BITS;
    
    // ... 查找 slot ...
    
    if (slot_hash == hash && slot_valid == 1) {
        int ptr = slot_ptr;
        int base = (ptr - 1) * 3;  // ENTRY_STRIDE = 3
        
        K entryKey = (K) entries[base];
        N entryNs = (N) entries[base + 1];
        
        if (entryKey.equals(key) && 
            (entryNs == namespace || entryNs.equals(namespace))) {
            return ptr;  // 返回 ptr 而不是 entry 对象
        }
    }
    
    return 0;  // Not found
}
```

---

### Phase 3: ForL0StateMap.java 适配

#### 3.1 移除 HeapStateEntry 类型引用

```java
// 原代码
private HeapStateEntry<K, N, S> putEntry(K key, N namespace) {
    HeapStateEntry<K, N, S> entry = mainTable.put(hash, key, namespace);
    if (entry.state == null) {
        size++;
    }
    return entry;
}

// 新代码
private int putEntry(K key, N namespace) {
    int hash = compositeHash(key, namespace);
    
    if (namespace.equals(lastNamespace)) {
        namespace = lastNamespace;
    } else {
        lastNamespace = namespace;
    }
    
    int ptr = mainTable.put(hash, key, namespace);
    int base = (ptr - 1) * 3;
    
    if (mainTable.entries[base + 2] == null) {  // state == null
        size++;
    }
    
    return ptr;
}
```

#### 3.2 修改 get() 方法

```java
@Override
public S get(K key, N namespace) {
    if (namespace.equals(lastNamespace)) {
        namespace = lastNamespace;
    } else {
        lastNamespace = namespace;
    }
    int hash = compositeHash(key, namespace);
    return mainTable.get(hash, key, namespace);  // 直接返回 S
}
```

#### 3.3 修改 put() 方法

```java
@Override
public void put(K key, N namespace, S state) {
    int ptr = putEntry(key, namespace);
    int base = (ptr - 1) * 3;
    mainTable.entries[base + 2] = state;
}
```

#### 3.4 修改 remove() 方法

```java
@Override
public void remove(K key, N namespace) {
    int hash = compositeHash(key, namespace);
    boolean removed = mainTable.remove(hash, key, namespace);
    if (removed) {
        size--;
    }
}
```

#### 3.5 修改迭代器

```java
@Override
public Iterator<StateEntry<K, N, S>> iterator() {
    return mainTable.entryIterator();  // 使用 MainTable 的迭代器
}
```

---

### Phase 4: 测试文件适配

#### 4.1 MainTableTest.java

```java
// 原代码
HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
assertNotNull(entry);
entry.state = value;

// 新代码
int ptr = mainTable.put(hash, key, namespace);
assertTrue(ptr > 0);
int base = (ptr - 1) * 3;
mainTable.entries[base + 2] = value;  // 直接设置 state

// 验证
String retrieved = mainTable.get(hash, key, namespace);
assertEquals(value, retrieved);
```

#### 4.2 ForL0StateMapTest.java

- 基本不需要修改（通过 ForL0StateMap API 测试）
- 只需验证功能正确性

---

## ✅ 验证检查清单

### 功能正确性
- [ ] get() 操作返回正确的 state
- [ ] put() 操作正确插入/更新
- [ ] remove() 操作正确删除
- [ ] 迭代器遍历所有 entries
- [ ] L0 cache 正确更新
- [ ] 扩展 bucket 逻辑正常工作
- [ ] resize/rehash 正确迁移数据

### 性能测试
- [ ] 运行 MainTableTest - 功能测试
- [ ] 运行 MainTableStressTest - 压力测试
- [ ] 运行 ForL0StateMapTest - 集成测试
- [ ] 运行 ForL0StateMapStressTest - 压力测试
- [ ] Benchmark 对比（扁平化前后）

### 内存验证
- [ ] 无内存泄漏（所有 remove 的 entry 正确清理）
- [ ] Free list 正常工作
- [ ] 对象引用正确释放

---

## 📊 性能基准测试计划

### 测试场景
1. **Random Get** - 随机读取测试
2. **Sequential Put** - 顺序插入测试
3. **Mixed Workload** - 混合读写测试
4. **Resize Stress** - 扩容压力测试

### 对比指标
- 吞吐量 (ops/sec)
- 平均延迟 (ns/op)
- P99 延迟
- 内存占用
- Cache Miss 率（Linux perf）

### 运行命令
```bash
cd benchmark/scripts
python run_benchmark.py --backend forl0 --profile
python generate_report.py
```

---

## 🚨 风险点与注意事项

### 1. 类型转换
- 所有从 `entries[]` 读取都需要 `@SuppressWarnings("unchecked")`
- 必须保证类型安全（不要混淆 K/N/S 的偏移）

### 2. 索引计算
- **关键**: `base = (ptr - 1) * 3`
- **Key**: `entries[base]`
- **Namespace**: `entries[base + 1]`
- **State**: `entries[base + 2]`
- **Hash**: `hashes[ptr - 1]`

### 3. Null 检查
- 使用 `entries[base] == null` 判断 entry 是否已删除
- State 可以为 null（需要与 entry 删除区分）

### 4. L0Table 集成
- L0 必须返回 `ptr` 而不是 entry 对象
- L0 get() 需要直接访问 `entries[]` 数组

### 5. 迭代器生命周期
- 迭代器返回的 StateEntry 是临时对象
- 不应该持有引用超出迭代周期

---

## 📅 实施步骤

### Step 1: MainTable 核心改造 (60%)
- 修改数据结构定义
- 改造 get/put/remove 方法
- 实现新的 allocateEntry/removeEntry
- 适配 resize 逻辑

### Step 2: L0Table 适配 (10%)
- 修改 get() 方法签名和实现

### Step 3: ForL0StateMap 适配 (15%)
- 修改所有 HeapStateEntry 引用
- 适配迭代器

### Step 4: 测试适配 (10%)
- 修改 MainTableTest
- 修改 ForL0StateMapTest
- 验证所有测试通过

---

## 📝 代码审查要点

1. **索引计算**: 确认所有 `base = (ptr-1)*3` 和 `base+offset` 计算正确
2. **类型转换**: 确认所有 `(K)`, `(N)`, `(S)` 转换位置正确
3. **Null 处理**: 确认删除逻辑和 null state 的区分
4. **L0 集成**: 确认 L0Table 正确处理新的 entries 数组
5. **扩容逻辑**: 确认 resize 正确迁移所有数据
6. **内存清理**: 确认 remove 正确清理所有三个引用

---

## 🎯 预期结果

完成改造后：
- ✅ 所有单元测试通过
- ✅ 所有压力测试通过
- ✅ Get 性能提升 10-15%
- ✅ Put 性能提升 8-12%
- ✅ 内存占用减少 5-8%
- ✅ Cache Miss 减少 15-20%
- ✅ 无内存泄漏
- ✅ 功能完全兼容

---

## 📌 附录：关键常量

```java
// MainTable.java
private static final int INITIAL_BUCKET_COUNT = 256 * 1024;     // 256K buckets
private static final int INITIAL_ENTRY_CAPACITY = 256 * 1024;   // 256K entries
private static final int ENTRY_STRIDE = 3;                      // K, N, S
private static final int BUCKET_SIZE_LONGS = 8;                 // 64 bytes
private static final int SLOTS_PER_BUCKET = 7;
private static final int EXTENSION_SLOT_INDEX = 7;

// 内存占用估算
// Table: 256K * 8 * 8 = 16 MB
// Entries: 256K * 3 * 8 = 6 MB (压缩指针)
// Hashes: 256K * 4 = 1 MB
// Total: ~23 MB (vs 当前 ~27 MB)
```

---

**检查完成后，请确认以下事项：**
1. 改造方案清晰完整
2. 所有涉及文件已列出
3. 风险点已识别
4. 测试计划充分
5. 性能预期合理

**确认后即可开始实施！**
