# Namespace 组织化的 StateStore 改造方案

> 创建日期: 2026-01-21  
> 状态: **设计中**  
> 目标: 按 Namespace 组织状态存储，降低 Cache Miss 和对象开销

---

## 一、改造范围

仅改造以下现有类，不新增类：

| 类 | 改造内容 |
|----|----------|
| `SwissTable` | `<K,N,S>` → `<K,S>`，去掉 Namespace 存储 |
| `ForL0StateStore` | 加一层 `HashMap<N, SwissTable>` 映射 |
| `ForL0StateStoreSnapshot` | 适配新的遍历方式 |
| `ForL0StateStoreKeyGroupReader` | 适配新的恢复逻辑 |

---

## 二、SwissTable 改造

### 2.1 泛型变化

```diff
-public class SwissTable<K, N, S> {
+public class SwissTable<K, S> {
```

### 2.2 存储变化

```diff
 // ========== Storage ==========
 byte[] ctrl;
-Object[] keysNs;     // length = capacity * 2, keysNs[2*i]=key, keysNs[2*i+1]=namespace
+Object[] keys;       // length = capacity, keys[i]=key
 Object[] values;
-long[] hashes;       // 64-bit hash
+int[] hashes;        // 32-bit hash (对齐 hash-smith SwissMap)
```

### 2.3 构造函数

```diff
 public SwissTable(int slotCount) {
     // ...
-    this.keysNs = new Object[slotCount * 2];
+    this.keys = new Object[slotCount];
     // ...
 }
```

### 2.4 get() 方法

```diff
-public S get(long hash, K key, N namespace) {
+public S get(int hash, K key) {
     // ...
     while (match != 0) {
         int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
-        int keyIdx = slot << 1;
-        if (key.equals(keysNs[keyIdx]) && namespace.equals(keysNs[keyIdx + 1])) {
+        if (key.equals(keys[slot])) {
             return (S) values[slot];
         }
         match = clearLowestBit(match);
     }
     // ...
 }
```

### 2.5 put() 方法

```diff
-public int put(long hash, K key, N namespace) {
+public int put(int hash, K key) {
     // 查找阶段
     while (match != 0) {
         int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
-        int keyIdx = slot << 1;
-        if (key.equals(keysNs[keyIdx]) && namespace.equals(keysNs[keyIdx + 1])) {
+        if (key.equals(keys[slot])) {
             return slot;
         }
         match = clearLowestBit(match);
     }
     
     // 插入阶段
-    int keyIdx = insertSlot << 1;
-    keysNs[keyIdx] = key;
-    keysNs[keyIdx + 1] = namespace;
+    keys[insertSlot] = key;
     hashes[insertSlot] = hash;
     // ...
 }
```

### 2.6 remove() / containsKey() 等方法

同样去掉 namespace 参数，只比较 key。

### 2.7 新增 isEmpty() 方法

```java
public boolean isEmpty() {
    return used == 0;
}
```

用于 `remove()` 后判断是否需要从 HashMap 中清理空的 namespace。

### 2.8 Entry 内部类

```diff
-public static class Entry<K, N, S> {
+public static class Entry<K, S> {
     private final K key;
-    private final N namespace;
     private final S state;
     
-    Entry(K key, N namespace, S state) {
+    Entry(K key, S state) {
         this.key = key;
-        this.namespace = namespace;
         this.state = state;
     }
     
     public K getKey() { return key; }
-    public N getNamespace() { return namespace; }
     public S getState() { return state; }
 }
```

### 2.8 rehash() / grow() 方法

```diff
 void rehash() {
     // ...
     for (int i = 0; i < oldCapacity; i++) {
         if (isFullSlot(oldCtrl[i])) {
-            putForRehash(oldHashes[i], oldKeysNs[2*i], oldKeysNs[2*i+1], oldValues[i]);
+            putForRehash(oldHashes[i], oldKeys[i], oldValues[i]);
         }
     }
 }
```

---

## 三、ForL0StateStore 改造

### 3.1 成员变量变化

```diff
 public class ForL0StateStore<K, N, S> implements StateSnapshotRestore {
 
-    /** Array of SwissTables, one per key group. */
-    private final SwissTable<K, N, S>[] tables;
+    /** 是否为 VoidNamespace 模式 */
+    private final boolean isVoidNamespace;
+    
+    /** VoidNamespace 模式: 直接持有 SwissTable[], 无 HashMap 层 */
+    private final SwissTable<K, S>[] tables;           // isVoidNamespace=true
+    
+    /** 通用 Namespace 模式: HashMap<N, SwissTable>[] */
+    private final Map<N, SwissTable<K, S>>[] namespaceMaps;  // isVoidNamespace=false
 
-    private N lastNamespace;
```

### 3.2 构造函数

```diff
 @SuppressWarnings("unchecked")
 public ForL0StateStore(
         KeyGroupRange keyGroupRange,
         TypeSerializer<K> keySerializer,
         RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
     this.keyGroupRange = keyGroupRange;
     this.keyGroupOffset = keyGroupRange.getStartKeyGroup();
     this.keySerializer = keySerializer;
     this.metaInfo = metaInfo;
     
     int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
-    this.tables = new SwissTable[numKeyGroups];
+    
+    // VoidNamespace 特化检测
+    this.isVoidNamespace = metaInfo.getNamespaceSerializer() 
+            instanceof VoidNamespaceSerializer;
+    
+    if (isVoidNamespace) {
+        // VoidNamespace: 直接持有 SwissTable[]，无 HashMap 开销
+        this.tables = new SwissTable[numKeyGroups];
+        this.namespaceMaps = null;
+    } else {
+        // 通用 Namespace: 使用 HashMap<N, SwissTable>[]
+        this.tables = null;
+        this.namespaceMaps = new HashMap[numKeyGroups];
+    }
 }
```

### 3.3 get() 方法

```diff
 public S get(K key, N namespace, int keyGroup) {
+    SwissTable<K, S> table;
+    
+    if (isVoidNamespace) {
+        // VoidNamespace: 直接访问
+        table = tables[keyGroup - keyGroupOffset];
+    } else {
+        // 通用 Namespace: 通过 HashMap 访问
+        Map<N, SwissTable<K, S>> nsMap = namespaceMaps[keyGroup - keyGroupOffset];
+        table = (nsMap == null) ? null : nsMap.get(namespace);
+    }
-    SwissTable<K, N, S> table = getTable(keyGroup);
     if (table == null) {
         return null;
     }
-    long hash = computeHash(key, namespace);
-    return table.get(hash, key, namespace);
+    int hash = computeKeyHash(key);
+    return table.get(hash, key);
 }
```

### 3.4 put() 方法

```diff
 public void put(K key, N namespace, S value, int keyGroup) {
+    int idx = keyGroup - keyGroupOffset;
+    SwissTable<K, S> table;
+    
+    if (isVoidNamespace) {
+        // VoidNamespace: 直接访问，无 HashMap 开销
+        table = tables[idx];
+        if (table == null) {
+            table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
+            tables[idx] = table;
+        }
+    } else {
+        // 通用 Namespace: 通过 HashMap 访问
+        Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
+        if (nsMap == null) {
+            nsMap = new HashMap<>(8);  // 小初始容量，Namespace 通常很少
+            namespaceMaps[idx] = nsMap;
+        }
+        table = nsMap.get(namespace);
+        if (table == null) {
+            table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
+            nsMap.put(namespace, table);  // Namespace 自动去重
+        }
+    }
-    SwissTable<K, N, S> table = getOrCreateTable(keyGroup);
-    namespace = deduplicateNamespace(namespace);
-    long hash = computeHash(key, namespace);
+    SwissTable<K, S> table = getOrCreateTable(keyGroup, namespace);
+    int hash = computeKeyHash(key);
     
     while (true) {
-        int result = table.put(hash, key, namespace);
+        int result = table.put(hash, key);
         
         if (result == SwissTable.NEED_REHASH) {
             table.rehash();
             continue;
         }
         if (result == SwissTable.NEED_GROW) {
             table.grow();
             continue;
         }
         
         int slot = result & SwissTable.SLOT_MASK;
         table.values[slot] = value;
         return;
     }
 }
```

### 3.5 remove() / containsKey() / transform()

同样的模式：先根据 `isVoidNamespace` 获取 table，再调用简化后的方法。

**重要**: `remove()` 需要在 SwissTable 变空后从 HashMap 中删除 namespace entry，避免窗口场景下 namespace 累积。

参考: Flink 1.14 的 `NestedStateMap.removeAndGetOld()` 也有相同的清理逻辑。

```java
public S remove(K key, N namespace, int keyGroup) {
    int idx = keyGroup - keyGroupOffset;
    
    if (isVoidNamespace) {
        SwissTable<K, S> table = tables[idx];
        if (table == null) {
            return null;
        }
        int hash = computeKeyHash(key);
        return table.remove(hash, key);
    } else {
        Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
        if (nsMap == null) {
            return null;
        }
        SwissTable<K, S> table = nsMap.get(namespace);
        if (table == null) {
            return null;
        }
        
        int hash = computeKeyHash(key);
        S removed = table.remove(hash, key);
        
        // 关键: 如果 SwissTable 变空，从 HashMap 中移除这个 namespace
        // 避免时间窗口场景下 namespace 在 HashMap 中累积
        if (table.isEmpty()) {
            nsMap.remove(namespace);
        }
        
        return removed;
    }
}
```

### 3.6 Hash 计算简化 (对齐 hash-smith SwissMap)

改用 32 位 hash，参考 hash-smith 的 smear 函数：

```diff
-private long computeHash(K key, N namespace) {
-    int keyHash = key.hashCode();
-    int nsHash = namespace.hashCode();
-    long h = ((long) keyHash << 32) | (nsHash & 0xFFFFFFFFL);
-    h ^= h >>> 33;
-    h *= 0xff51afd7ed558ccdL;
-    h ^= h >>> 33;
-    h *= 0xc4ceb9fe1a85ec53L;
-    h ^= h >>> 33;
-    return h;
-}
+/**
+ * 计算 key 的 32 位 hash (对齐 hash-smith SwissMap).
+ * 使用 Guava 风格的 smear 函数进行混合.
+ */
+private int computeKeyHash(K key) {
+    int h = key.hashCode();
+    // smear mixing (来自 hash-smith / Guava)
+    return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
+}
 
-private N deduplicateNamespace(N namespace) {
-    if (namespace.equals(lastNamespace)) {
-        return lastNamespace;
-    }
-    lastNamespace = namespace;
-    return namespace;
-}
```

SwissTable 内部的 H1/H2 分割：

```java
// SwissTable.java
static int h1(int hash) {
    return hash >>> 7;  // 高 25 位，探测起始 group
}

static int h2(int hash) {
    return hash & 0x7F;  // 低 7 位，存入 ctrl 字节
}
```

### 3.7 getKeys() 优化

```diff
 public Stream<K> getKeys(N namespace) {
     List<K> keys = new ArrayList<>();
+    
+    if (isVoidNamespace) {
+        // VoidNamespace: 所有 key 都属于同一个 namespace
+        for (SwissTable<K, S> table : tables) {
+            if (table != null) {
+                table.collectKeys(keys);
+            }
+        }
+    } else {
+        // 通用 Namespace: 只遍历对应的 SwissTable
-    for (SwissTable<K, N, S> table : tables) {
-        if (table != null) {
-            for (Iterator<SwissTable.Entry<K, N, S>> it = table.iterator(); it.hasNext(); ) {
-                SwissTable.Entry<K, N, S> entry = it.next();
-                if (Objects.equals(namespace, entry.getNamespace())) {
-                    keys.add(entry.getKey());
-                }
-            }
+        for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
+            if (nsMap != null) {
+                SwissTable<K, S> table = nsMap.get(namespace);
+                if (table != null) {
+                    table.collectKeys(keys);
+                }
+            }
         }
+    }
     return keys.stream();
 }
```

### 3.8 getKeysAndNamespaces()

```diff
 public Stream<Tuple2<K, N>> getKeysAndNamespaces() {
     List<Tuple2<K, N>> result = new ArrayList<>();
+    
+    if (isVoidNamespace) {
+        // VoidNamespace: 注入固定的 VoidNamespace.INSTANCE
+        @SuppressWarnings("unchecked")
+        N voidNs = (N) VoidNamespace.INSTANCE;
+        for (SwissTable<K, S> table : tables) {
+            if (table != null) {
+                for (Iterator<SwissTable.Entry<K, S>> it = table.iterator(); it.hasNext(); ) {
+                    result.add(Tuple2.of(it.next().getKey(), voidNs));
+                }
+            }
+        }
+    } else {
-    for (SwissTable<K, N, S> table : tables) {
-        if (table != null) {
-            for (Iterator<SwissTable.Entry<K, N, S>> it = table.iterator(); it.hasNext(); ) {
-                SwissTable.Entry<K, N, S> entry = it.next();
-                result.add(Tuple2.of(entry.getKey(), entry.getNamespace()));
+        for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
+            if (nsMap != null) {
+                for (Map.Entry<N, SwissTable<K, S>> nsEntry : nsMap.entrySet()) {
+                    N namespace = nsEntry.getKey();
+                    SwissTable<K, S> table = nsEntry.getValue();
+                    for (Iterator<SwissTable.Entry<K, S>> it = table.iterator(); it.hasNext(); ) {
+                        result.add(Tuple2.of(it.next().getKey(), namespace));
+                    }
+                }
             }
         }
+    }
     return result.stream();
 }
```

### 3.9 sizeOfNamespace() 优化

```diff
 public int sizeOfNamespace(Object namespace) {
+    if (isVoidNamespace) {
+        // VoidNamespace: 返回总 size
+        return size();
+    }
+    
+    // 通用 Namespace: O(keyGroups) 复杂度
     int count = 0;
-    for (SwissTable<K, N, S> table : tables) {
-        if (table != null) {
-            for (Iterator<SwissTable.Entry<K, N, S>> it = table.iterator(); it.hasNext(); ) {
-                SwissTable.Entry<K, N, S> entry = it.next();
-                if (Objects.equals(namespace, entry.getNamespace())) {
-                    count++;
-                }
-            }
+    for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
+        if (nsMap != null) {
+            @SuppressWarnings("unchecked")
+            SwissTable<K, S> table = nsMap.get((N) namespace);
+            if (table != null) {
+                count += table.size();
+            }
         }
     }
     return count;
 }
```

### 3.10 size() 方法

```diff
 public int size() {
     int total = 0;
+    
+    if (isVoidNamespace) {
+        for (SwissTable<K, S> table : tables) {
+            if (table != null) {
+                total += table.size();
+            }
+        }
+    } else {
-    for (SwissTable<K, N, S> table : tables) {
-        if (table != null) {
-            total += table.size();
+        for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
+            if (nsMap != null) {
+                for (SwissTable<K, S> table : nsMap.values()) {
+                    total += table.size();
+                }
+            }
         }
+    }
     return total;
 }
```

### 3.11 entries() 遍历

```java
public Iterable<StateEntry<K, N, S>> entries(int keyGroup) {
    int idx = keyGroup - keyGroupOffset;
    
    if (isVoidNamespace) {
        // VoidNamespace: 直接遍历 table，注入固定的 VoidNamespace.INSTANCE
        SwissTable<K, S> table = tables[idx];
        if (table == null || table.size() == 0) {
            return Collections.emptyList();
        }
        
        @SuppressWarnings("unchecked")
        N voidNs = (N) VoidNamespace.INSTANCE;
        
        return () -> new Iterator<StateEntry<K, N, S>>() {
            private final Iterator<SwissTable.Entry<K, S>> inner = table.iterator();
            
            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }
            
            @Override
            public StateEntry<K, N, S> next() {
                SwissTable.Entry<K, S> e = inner.next();
                return new SimpleStateEntry<>(e.getKey(), voidNs, e.getState());
            }
        };
    } else {
        // 通用 Namespace: 遍历 HashMap 中的所有 SwissTable
        Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
        if (nsMap == null || nsMap.isEmpty()) {
            return Collections.emptyList();
        }
        
        return () -> new Iterator<StateEntry<K, N, S>>() {
            private final Iterator<Map.Entry<N, SwissTable<K, S>>> nsIter = 
                    nsMap.entrySet().iterator();
            private N currentNamespace;
            private Iterator<SwissTable.Entry<K, S>> tableIter;
            
            @Override
            public boolean hasNext() {
                while ((tableIter == null || !tableIter.hasNext()) && nsIter.hasNext()) {
                    Map.Entry<N, SwissTable<K, S>> entry = nsIter.next();
                    currentNamespace = entry.getKey();
                    tableIter = entry.getValue().iterator();
                }
                return tableIter != null && tableIter.hasNext();
            }
            
            @Override
            public StateEntry<K, N, S> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                SwissTable.Entry<K, S> e = tableIter.next();
                return new SimpleStateEntry<>(e.getKey(), currentNamespace, e.getState());
            }
        };
    }
}
```

---

## 四、Snapshot 配合改造

### 4.1 ForL0StateStoreSnapshot

遍历时根据 `isVoidNamespace` 选择不同的遍历方式：

```java
// VoidNamespace 模式: 直接遍历 tables[]
if (stateStore.isVoidNamespace()) {
    SwissTable<K, S> table = stateStore.getTableDirect(keyGroup);
    if (table != null) {
        for (int slot = 0; slot < table.capacity; slot++) {
            if (isFullSlot(table.ctrl[slot])) {
                K key = table.keys[slot];
                S state = table.values[slot];
                // 写入 key, VoidNamespace.INSTANCE, state
                keySerializer.serialize(key, outputView);
                namespaceSerializer.serialize(VoidNamespace.INSTANCE, outputView);
                stateSerializer.serialize(state, outputView);
            }
        }
    }
} else {
    // 通用 Namespace 模式: 遍历 namespaceMaps[]
    Map<N, SwissTable<K, S>> nsMap = stateStore.getNamespaceMap(keyGroup);
    if (nsMap != null) {
        for (Map.Entry<N, SwissTable<K, S>> nsEntry : nsMap.entrySet()) {
            N namespace = nsEntry.getKey();
            SwissTable<K, S> table = nsEntry.getValue();
            for (int slot = 0; slot < table.capacity; slot++) {
                if (isFullSlot(table.ctrl[slot])) {
                    K key = table.keys[slot];
                    S state = table.values[slot];
                    // 写入 key, namespace, state
                    keySerializer.serialize(key, outputView);
                    namespaceSerializer.serialize(namespace, outputView);
                    stateSerializer.serialize(state, outputView);
                }
            }
        }
    }
}
```

### 4.2 ForL0StateStoreKeyGroupReader

恢复时根据 `isVoidNamespace` 选择不同的插入方式：

```java
// 读取 entry
K key = keySerializer.deserialize(inputView);
N namespace = namespaceSerializer.deserialize(inputView);
S state = stateSerializer.deserialize(inputView);

int hash = stateStore.computeKeyHash(key);

if (stateStore.isVoidNamespace()) {
    // VoidNamespace: 直接插入到 tables[]
    SwissTable<K, S> table = stateStore.getOrCreateTableDirect(keyGroup);
    int result = table.put(hash, key);
    int slot = result & SwissTable.SLOT_MASK;
    table.values[slot] = state;
} else {
    // 通用 Namespace: 通过 HashMap 插入
    SwissTable<K, S> table = stateStore.getOrCreateTable(keyGroup, namespace);
    int result = table.put(hash, key);
    int slot = result & SwissTable.SLOT_MASK;
    table.values[slot] = state;
}
```

---

## 五、实施计划

### Phase 1: SwissTable 改造 (1-2天)

- [ ] 修改泛型: `<K, N, S>` → `<K, S>`
- [ ] 修改存储: `keysNs[]` → `keys[]`, `long[] hashes` → `int[] hashes`
- [ ] 修改所有方法签名: 去掉 namespace 参数, hash 从 `long` 改为 `int`
- [ ] 修改 Entry 内部类
- [ ] 修改 rehash()/grow()
- [ ] 添加 collectKeys() 辅助方法
- [ ] 更新单元测试

### Phase 2: ForL0StateStore 改造 (2天)

- [ ] 添加 `isVoidNamespace` 标志
- [ ] 添加 `tables[]` (VoidNamespace) 和 `namespaceMaps[]` (通用)
- [ ] 修改 get/put/remove/transform/containsKey (两种模式)
- [ ] 简化 computeHash → computeKeyHash (64位 → 32位, 对齐 hash-smith smear)
- [ ] 删除 deduplicateNamespace (不再需要)
- [ ] 优化 getKeys()/sizeOfNamespace()/size()
- [ ] 修改 entries() 迭代器 (两种模式)
- [ ] 添加 getTableDirect()/getOrCreateTableDirect() 内部方法
- [ ] 更新单元测试

### Phase 3: Snapshot 改造 (1天)

- [ ] 修改 ForL0StateStoreSnapshot 遍历逻辑 (两种模式)
- [ ] 修改 ForL0StateStoreKeyGroupReader 恢复逻辑 (两种模式)
- [ ] 集成测试

### Phase 4: 验证 (1天)

- [ ] 更新相应的测试类
- [ ] 运行全量测试确保通过

---

## 六、预期收益

| 指标 | 改造前 | 改造后 (VoidNS) | 改造后 (通用NS) |
|------|--------|-----------------|-----------------|
| SwissTable 内存 | `keysNs[capacity*2]` | `keys[capacity]` (-50%) | `keys[capacity]` (-50%) |
| hashes[] 内存 | `long[capacity]` (8B/slot) | `int[capacity]` (4B/slot, -50%) | `int[capacity]` (4B/slot, -50%) |
| Hash 计算 | 64位 hash(key, ns) | 32位 smear(key) | 32位 smear(key) |
| Namespace 存储 | 每 Entry 一个引用 | 0 (无存储) | 每 Namespace 一个 HashMap entry |
| get/put 路径 | 直接访问 | 直接访问 (无 HashMap) | HashMap.get + 访问 |
| getKeys(ns) 复杂度 | O(全部entries) 扫描 | O(全部entries) | O(该namespace entries) |
| sizeOfNamespace() | O(全部entries) 扫描 | O(1) 直接返回 size() | O(keyGroups) |

---

## 七、注意事项

1. **VoidNamespace 特化**: 当检测到 VoidNamespaceSerializer 时，ForL0StateStore 直接持有 `SwissTable[]`，完全跳过 HashMap 层，零额外开销
2. **序列化兼容性**: Snapshot 格式不变 (key, namespace, state)，只是内存布局变了
3. **HashMap 初始容量**: 通用 Namespace 模式下使用小容量 (8)，因为 Namespace 数量通常很少
4. **单一成员变量**: `isVoidNamespace=true` 时 `namespaceMaps=null`；反之 `tables=null`，避免混淆
