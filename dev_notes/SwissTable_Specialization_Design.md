# SwissTable 特化版本设计方案

## 1. 问题背景

VTune 分析显示 `SwissTable.put()` 中存在严重的 Memory Bound（~54%），热点指令：

```asm
mov rcx, qword ptr [rcx+rbx*8+0x18]  // 14.5s, 49.4% MB, 108M LLC Miss
mov rbx, qword ptr [rcx+0x8]         // 20.4s, 54.3% MB, 119M LLC Miss
```

对应源码：
```java
if (key.equals(keysNs[keyIdx]) && namespace.equals(keysNs[keyIdx + 1]))
```

**根因**：两层 Pointer Chasing
1. `keysNs[keyIdx]` → 加载 Object 引用
2. `Object.field` → 解引用访问对象内部字段

## 2. Go 特化方案参考

Go 1.24 Swiss Tables 提供特化版本：

| 文件 | Key 类型 | 核心优化 |
|------|----------|----------|
| `runtime_fast64.go` | uint64 | `key == *(*uint64)(slotKey)` 直接比较 |
| `runtime_faststr.go` | string | 长度 + 首尾 8 字节快速比较 |

**核心思想**：对于 primitive 类型，直接存储值而非对象引用。

## 3. 目标场景覆盖

| Benchmark | Key 类型 | Namespace 类型 | 特化类 |
|-----------|----------|----------------|--------|
| Nexmark Q4/9/18/19/20 | Long | VoidNamespace | `SwissTableLongVoid` |
| Nexmark Q5/7/8/11/12 | Long | TimeWindow | `SwissTableLongTimeWindow` |
| WordCount | String | TimeWindow | `SwissTableStringTimeWindow` |
| 通用 String State | String | VoidNamespace | `SwissTableStringVoid` |

**需要实现 4 个特化版本 + 1 个通用 fallback。**

## 4. 类型层次设计

**使用抽象类而非接口**：大部分逻辑（ctrl/SWAR/探测/grow/split）是公共的，只有 key/namespace 存储和比较不同。

```
AbstractSwissTable<K, N, S>                 // 抽象基类
    │
    │   // 公共状态
    │   byte[] ctrl, long[] hashes, Object[] values
    │   short capacity, used, tomb, growthLeft
    │   
    │   // 公共方法（完整实现）
    │   loadCtrlWord(), matchH2(), matchEmpty(), ...
    │   
    │   // 抽象方法（子类实现）
    │   abstract boolean keyEquals(int slot, K key, N namespace);
    │   abstract void storeKeyNs(int slot, K key, N namespace);
    │   abstract K getKey(int slot);
    │   abstract N getNamespace(int slot);
    │   abstract void initStorage(int capacity);
    │   abstract void copySlot(int fromSlot, AbstractSwissTable target, int toSlot);
    │
    ├── SwissTableLongVoid<S>               // long[] keys, 无 namespace
    ├── SwissTableLongTimeWindow<S>         // long[] keys, long[] namespaces
    ├── SwissTableStringVoid<S>             // String[] keys, 无 namespace
    ├── SwissTableStringTimeWindow<S>       // String[] keys, long[] namespaces
    └── SwissTableGeneric<K, N, S>          // Object[] keys, Object[] namespaces
```

### 为什么用抽象类而非接口？

| 方面 | 接口 | 抽象类 |
|------|------|--------|
| 公共状态 | ❌ 每个实现类重复定义 | ✅ 基类定义一次 |
| SWAR 逻辑 | ❌ 每个实现类重复 | ✅ 基类实现一次 |
| 探测/grow/split | ❌ 每个实现类重复 | ✅ 基类模板方法 |
| 子类工作量 | 大（全部实现） | **小（只实现差异部分）** |

## 5. 存储结构设计

### 5.1 公共存储（AbstractSwissTable）

```java
byte[] ctrl;           // 控制字节，SWAR 匹配
long[] hashes;         // 64-bit full hash
Object[] values;       // Value 对象（无法特化）
short capacity, used, tomb, growthLeft;
byte localDepth;
int index, groupMask;
```

### 5.2 特化存储

**统一布局原则**：key 和 namespace 分两个数组存储。

| 特化类 | Key 存储 | Namespace 存储 |
|--------|----------|----------------|
| `LongVoid` | `long[] keys` | 无 |
| `LongTimeWindow` | `long[] keys` | `long[] namespaces` (交替: ns[2i]=start, ns[2i+1]=end) |
| `StringVoid` | `String[] keys` | 无 |
| `StringTimeWindow` | `String[] keys` | `long[] namespaces` (交替: ns[2i]=start, ns[2i+1]=end) |
| `Generic` | `Object[] keys` | `Object[] namespaces` |

## 6. 抽象类设计

### 6.1 AbstractSwissTable 基类

```java
abstract class AbstractSwissTable<K, N, S> {
    
    // ===== 公共状态 =====
    byte[] ctrl;
    long[] hashes;
    Object[] values;
    short capacity, used, tomb, growthLeft;
    byte localDepth;
    int index, groupMask;
    
    // ===== 公共方法（完整实现）=====
    
    final long loadCtrlWord(int group) { ... }
    static long matchH2(long ctrlWord, int h2) { ... }
    static long matchEmpty(long ctrlWord) { ... }
    // ... 其他 SWAR 方法
    
    // ===== 模板方法 =====
    
    public final S get(long hash, K key, N namespace) {
        int h2Hash = h2(hash);
        int group = h1(hash) & groupMask;
        int stride = 1;
        
        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;
            
            long match = matchH2(ctrlWord, h2Hash);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                // 调用子类实现的比较方法
                if (keyEquals(slot, key, namespace)) {
                    return (S) values[slot];
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
    
    // put/remove 类似，调用子类的 keyEquals/storeKeyNs
    
    // ===== 抽象方法（子类实现）=====
    
    /** 比较 slot 位置的 key/namespace 是否匹配 */
    abstract boolean keyEquals(int slot, K key, N namespace);
    
    /** 存储 key/namespace 到 slot */
    abstract void storeKeyNs(int slot, K key, N namespace);
    
    /** 获取 slot 位置的 key（用于迭代器）*/
    abstract K getKey(int slot);
    
    /** 获取 slot 位置的 namespace（用于迭代器）*/
    abstract N getNamespace(int slot);
    
    /** 初始化 key/namespace 存储数组 */
    abstract void initStorage(int capacity);
    
    /** 复制 slot 数据到新表（用于 grow/split）*/
    abstract void copySlot(int fromSlot, AbstractSwissTable<K, N, S> target, int toSlot);
}
```

### 6.2 特化子类示例

```java
class SwissTableLongTimeWindow<S> extends AbstractSwissTable<Long, TimeWindow, S> {
    
    long[] keys;
    long[] namespaces;  // 交替: ns[2*i]=start, ns[2*i+1]=end
    
    @Override
    void initStorage(int capacity) {
        this.keys = new long[capacity];
        this.namespaces = new long[capacity * 2];
    }
    
    @Override
    boolean keyEquals(int slot, Long key, TimeWindow namespace) {
        int nsIdx = slot << 1;
        return keys[slot] == key.longValue() 
            && namespaces[nsIdx] == namespace.getStart()
            && namespaces[nsIdx + 1] == namespace.getEnd();
    }
    
    @Override
    void storeKeyNs(int slot, Long key, TimeWindow namespace) {
        keys[slot] = key.longValue();
        int nsIdx = slot << 1;
        namespaces[nsIdx] = namespace.getStart();
        namespaces[nsIdx + 1] = namespace.getEnd();
    }
    
    @Override
    Long getKey(int slot) {
        return keys[slot];  // 自动装箱
    }
    
    @Override
    TimeWindow getNamespace(int slot) {
        int nsIdx = slot << 1;
        return new TimeWindow(namespaces[nsIdx], namespaces[nsIdx + 1]);
    }
    
    @Override
    void copySlot(int fromSlot, AbstractSwissTable<Long, TimeWindow, S> target, int toSlot) {
        SwissTableLongTimeWindow<S> t = (SwissTableLongTimeWindow<S>) target;
        t.keys[toSlot] = this.keys[fromSlot];
        int fromNsIdx = fromSlot << 1;
        int toNsIdx = toSlot << 1;
        t.namespaces[toNsIdx] = this.namespaces[fromNsIdx];
        t.namespaces[toNsIdx + 1] = this.namespaces[fromNsIdx + 1];
    }
}
```

## 7. ForL0StateMap 集成

### 7.1 设计思路

**StateMap 持有 AbstractSwissTable<K, N, S>[]，构造时确定具体类型，运行时多态调用。**

### 7.2 实现

```java
public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> {
    
    private AbstractSwissTable<K, N, S>[] directory;
    
    @SuppressWarnings("unchecked")
    public ForL0StateMap(TypeSerializer<K> keySerializer, 
                         TypeSerializer<N> nsSerializer) {
        // 构造时根据类型创建对应的特化 SwissTable
        AbstractSwissTable<K, N, S> initTable = createTable(keySerializer, nsSerializer);
        this.directory = new AbstractSwissTable[] { initTable };
    }
    
    @Override
    public S get(K key, N namespace) {
        long hash = computeHash(key, namespace);
        return locateTable(hash).get(hash, key, namespace);  // 多态调用
    }
    
    @Override
    public void put(K key, N namespace, S state) {
        long hash = computeHash(key, namespace);
        // ... directory 中的特化 SwissTable 通过继承自动获得正确行为
    }
    
    // 工厂方法 - 根据类型选择特化实现
    private static <K, N, S> AbstractSwissTable<K, N, S> createTable(
            TypeSerializer<K> keySerializer, 
            TypeSerializer<N> nsSerializer) {
        
        if (keySerializer instanceof LongSerializer) {
            if (nsSerializer instanceof VoidNamespaceSerializer) {
                return (AbstractSwissTable<K, N, S>) new SwissTableLongVoid<>();
            }
            if (nsSerializer instanceof TimeWindowSerializer) {
                return (AbstractSwissTable<K, N, S>) new SwissTableLongTimeWindow<>();
            }
        }
        if (keySerializer instanceof StringSerializer) {
            if (nsSerializer instanceof VoidNamespaceSerializer) {
                return (AbstractSwissTable<K, N, S>) new SwissTableStringVoid<>();
            }
            if (nsSerializer instanceof TimeWindowSerializer) {
                return (AbstractSwissTable<K, N, S>) new SwissTableStringTimeWindow<>();
            }
        }
        return new SwissTableGeneric<>();
    }
}
```

## 8. Hash 计算

各特化版本内部实现对应的 hash 函数，无需暴露到外部。

## 9. 预期收益

| 特化类型 | Pointer Chasing | Memory Bound 预期 |
|----------|-----------------|-------------------|
| Generic（现有） | 4 层 | ~54%（基线） |
| LongVoid | 0 层 | **<5%** |
| LongTimeWindow | 0 层 | **<5%** |
| StringVoid | 1 层（String.equals内部） | ~15% |
| StringTimeWindow | 1 层（String.equals内部） | ~15% |

## 10. 文件结构

```
src/main/java/org/apache/flink/runtime/state/heap/
├── AbstractSwissTable.java              // 抽象基类（包含 SWAR、探测、ctrl 等公共逻辑）
├── SwissTableGeneric.java               // 通用实现（Object[] keys/namespaces）
├── SwissTableLongVoid.java              // Long key + VoidNamespace
├── SwissTableLongTimeWindow.java        // Long key + TimeWindow
├── SwissTableStringVoid.java            // String key + VoidNamespace
├── SwissTableStringTimeWindow.java      // String key + TimeWindow
└── ForL0StateMap.java                   // 持有 AbstractSwissTable<K,N,S>[]
```

## 11. 实施计划

### 11.1 Step 1：重构现有 SwissTable 为抽象基类（1天）

**目标**：将现有 SwissTable 重构为 AbstractSwissTable + SwissTableGeneric。

**变更**：
1. 创建 `AbstractSwissTable<K, N, S>` 抽象类
   - 保留 `ctrl[]`, `hashes[]`, `values[]`, `capacity`, `used`, `tomb` 等公共字段
   - 保留 SWAR 匹配方法 (`matchH2`, `matchEmpty`)
   - 保留探测序列逻辑
   - 将 key/namespace 比较和存储定义为抽象方法
2. 创建 `SwissTableGeneric` 继承 `AbstractSwissTable`
   - 将 `Object[] keysNs` 拆分为 `Object[] keys` + `Object[] namespaces`
   - 实现抽象方法

**验证**：运行现有单元测试，确保功能不变。

### 11.2 Step 2：实现 SwissTableLongVoid（1天）

**目标**：覆盖 Nexmark Q4/9/18/19/20。

**实现**：
1. 创建 `SwissTableLongVoid` 继承 `AbstractSwissTable`
2. 使用 `long[] keys`（无需 namespaces 数组）
3. 实现 `keyEquals()`：`keys[slot] == key.longValue()`
4. 实现 `storeKeyNs()`、`getKey()`、`getNamespace()`、`initStorage()`、`copySlot()`
5. 修改 `ForL0StateMap.createTable()` 添加类型检测

**验证**：
1. 新增 `SwissTableLongVoidTest` 单元测试
2. 运行 StateMapBenchmark 对比性能

### 11.3 Step 3：实现 SwissTableLongTimeWindow（1天）

**目标**：覆盖 Nexmark Q5/7/8/11/12。

**实现**：
1. 创建 `SwissTableLongTimeWindow` 继承 `AbstractSwissTable`
2. 使用 `long[] keys` + `long[] namespaces`（交替存储 start/end）
3. 实现比较逻辑：
   ```java
   int nsIdx = slot << 1;
   return keys[slot] == key.longValue() 
       && namespaces[nsIdx] == ns.getStart() 
       && namespaces[nsIdx + 1] == ns.getEnd();
   ```
4. 更新 `ForL0StateMap.createTable()`

**验证**：
1. 新增单元测试
2. 运行 Nexmark Q5 对比性能

### 11.4 Step 4：实现 SwissTableStringVoid（0.5天）

**目标**：通用 String Key State。

**实现**：
1. 创建 `SwissTableStringVoid` 继承 `AbstractSwissTable`
2. 使用 `String[] keys`（无需 namespaces 数组）
3. 比较逻辑：`key.equals(keys[slot])`（仍有 1 层 pointer chasing，但无法避免）

### 11.5 Step 5：实现 SwissTableStringTimeWindow（0.5天）

**目标**：覆盖 WordCount。

**实现**：
1. 创建 `SwissTableStringTimeWindow` 继承 `AbstractSwissTable`
2. 使用 `String[] keys` + `long[] namespaces`
3. 比较逻辑：`key.equals(keys[slot]) && ns[nsIdx] == nsStart && ns[nsIdx+1] == nsEnd`

### 11.6 Step 6：集成测试与性能验证（1天）

**验证项**：
1. 所有现有单元测试通过
2. MiniCluster ITCase 通过
3. VTune 分析确认 Memory Bound 下降
4. JMH StateMapBenchmark 性能对比：
   - ForL0 Generic vs 特化版本
   - 预期 Long 特化版本性能提升 50%+

### 11.7 时间线

| Step | 内容 | 预计时间 |
|------|------|----------|
| 1 | 重构 AbstractSwissTable + SwissTableGeneric | 1 天 |
| 2 | SwissTableLongVoid | 1 天 |
| 3 | SwissTableLongTimeWindow | 1 天 |
| 4 | SwissTableStringVoid | 0.5 天 |
| 5 | SwissTableStringTimeWindow | 0.5 天 |
| 6 | 集成测试与性能验证 | 1 天 |
| **总计** | | **5 天** |
