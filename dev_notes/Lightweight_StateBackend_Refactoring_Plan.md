# ForL0 轻量级 StateBackend 重构方案

## 一、重构目标

将当前 `org.apache.flink.runtime.state.heap` 包下的 ForL0 StateBackend 实现迁移至独立包名 `org.apache.flink.state.forl0`，实现一个：

- **与 HashMapStateBackend 完全无关的**独立 StateBackend
- **与 forst、changelog 等平级的**轻量级实现
- **只依赖 Flink 公共接口**，不使用 heap 包的任何内部实现类
- **保持 State API 和 Checkpoint 功能**的最小化实现

## 二、设计原则

1. **Minimal**：只实现必要功能，不做过度抽象
2. **独立**：不继承 heap 包的 `StateTable`、`StateMap`、`AbstractHeapState` 等
3. **兼容**：使用 Flink 标准 checkpoint 格式，保证兼容性
4. **复用公共 API**：仅依赖 `flink-runtime` 的公共接口

## 三、与 Flink 公共 API 的对接

### 3.1 必须继承的基类

| 类 | 原因 |
|----|------|
| `AbstractKeyedStateBackend<K>` | 约 460 行，处理 key context、state 缓存、TTL 等通用逻辑，重写无收益 |

### 3.2 必须实现的接口

| 接口 | 用途 |
|------|------|
| `StateBackend` | StateBackend 入口 |
| `ConfigurableStateBackend` | 支持从配置创建 |
| `StateBackendFactory<T>` | SPI 工厂 |
| `SnapshotStrategy<S, SR>` | Checkpoint 策略 |
| `InternalValueState<K, N, V>` | ValueState 实现 |
| `InternalListState<K, N, V>` | ListState 实现 |
| `InternalMapState<K, N, UK, UV>` | MapState 实现 |
| `InternalReducingState<K, N, V>` | ReducingState 实现 |
| `InternalAggregatingState<K, N, IN, SV, OUT>` | AggregatingState 实现 |

### 3.3 复用的公共类

| 类 | 用途 |
|----|------|
| `KeyedBackendSerializationProxy` | Checkpoint 序列化格式 |
| `KeyGroupRangeAssignment` | KeyGroup 分配 |
| `CheckpointStreamFactory` | Checkpoint 输出流 |
| `KeyGroupsStateHandle` | Checkpoint 状态句柄 |
| `DefaultOperatorStateBackend` | OperatorState（直接复用） |
| `HeapPriorityQueueSetFactory` | 优先级队列工厂（直接复用） |
| `RegisteredKeyValueStateBackendMetaInfo` | 状态元信息（仅用于序列化） |

## 四、新目录结构

```
src/main/java/org/apache/flink/state/forl0/
│
├── ForL0StateBackend.java              # implements StateBackend, ConfigurableStateBackend
├── ForL0StateBackendFactory.java       # implements StateBackendFactory
├── ForL0Options.java                   # ConfigOption 定义 (合并原 Config + Options)
│
├── ForL0KeyedStateBackend.java         # extends AbstractKeyedStateBackend
├── ForL0KeyedStateBackendBuilder.java  # Builder 模式
│
├── ForL0StateStore.java                # 简单的状态存储: KeyGroup -> SwissTable
├── ForL0SnapshotStrategy.java          # implements SnapshotStrategy
├── ForL0SnapshotResources.java         # implements SnapshotResources
├── ForL0RestoreOperation.java          # 状态恢复
│
├── ForL0ValueState.java                # implements InternalValueState
├── ForL0ListState.java                 # implements InternalListState  
├── ForL0MapState.java                  # implements InternalMapState
├── ForL0ReducingState.java             # implements InternalReducingState
├── ForL0AggregatingState.java          # implements InternalAggregatingState
│
├── SwissTable.java                     # 核心数据结构 (从原 heap 包迁移)
│
└── space/                              # L0 内存管理 (保留)
    ├── L0MemoryAllocator.java
    ├── NativeL0Memory.java
    └── NativeL0MemoryAllocator.java
```

**总计：15 个核心 Java 文件 + space 子包**

## 五、核心类设计

### 5.1 ForL0StateStore (替代 StateTable + StateMap)

极简的状态存储，每个 KeyGroup 一个 SwissTable：

```java
public class ForL0StateStore<K, N, V> {
    private final SwissTable<K, N, V>[] tables;  // 按 KeyGroup 分区
    private final KeyGroupRange keyGroupRange;
    private final int keyGroupOffset;
    
    // 核心方法
    V get(K key, N namespace, int keyGroup);
    void put(K key, N namespace, V value, int keyGroup);
    V remove(K key, N namespace, int keyGroup);
    boolean containsKey(K key, N namespace, int keyGroup);
    
    // 遍历
    Iterable<StateEntry<K, N, V>> entries(int keyGroup);
    Stream<K> keys(N namespace);
    int size();
    
    // Snapshot 支持
    ForL0StateStoreSnapshot<K, N, V> snapshot();
}
```

### 5.2 ForL0ValueState (直接实现接口)

```java
public class ForL0ValueState<K, N, V> implements InternalValueState<K, N, V> {
    private final ForL0StateStore<K, N, V> store;
    private final ForL0KeyedStateBackend<K> backend;
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<V> valueSerializer;
    private final V defaultValue;
    private N currentNamespace;
    
    @Override
    public V value() {
        V val = store.get(backend.getCurrentKey(), currentNamespace, 
                          backend.getCurrentKeyGroupIndex());
        return val != null ? val : defaultValue;
    }
    
    @Override
    public void update(V value) {
        if (value == null) {
            clear();
            return;
        }
        store.put(backend.getCurrentKey(), currentNamespace, value,
                  backend.getCurrentKeyGroupIndex());
    }
    
    @Override
    public void clear() {
        store.remove(backend.getCurrentKey(), currentNamespace,
                     backend.getCurrentKeyGroupIndex());
    }
    
    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
    }
    
    // ... TypeSerializer getters
}
```

### 5.3 ForL0SnapshotStrategy

```java
public class ForL0SnapshotStrategy<K> 
        implements SnapshotStrategy<KeyedStateHandle, ForL0SnapshotResources<K>> {
    
    private final Map<String, ForL0StateStore<K, ?, ?>> registeredStores;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final KeyGroupRange keyGroupRange;
    private final TypeSerializer<K> keySerializer;
    private final StreamCompressionDecorator compressionDecorator;
    private final int totalKeyGroups;
    
    @Override
    public ForL0SnapshotResources<K> syncPrepareResources(long checkpointId) {
        // 收集所有 StateStore 的 snapshot
        Map<String, ForL0StateStoreSnapshot<K, ?, ?>> snapshots = new HashMap<>();
        for (Map.Entry<String, ForL0StateStore<K, ?, ?>> entry : registeredStores.entrySet()) {
            snapshots.put(entry.getKey(), entry.getValue().snapshot());
        }
        return new ForL0SnapshotResources<>(snapshots, registeredPQStates, 
                                             keyGroupRange, keySerializer, ...);
    }
    
    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            ForL0SnapshotResources<K> resources,
            long checkpointId, long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions) {
        
        return (closeableRegistry) -> {
            // 使用 Flink 标准格式写入
            // 1. 写 KeyedBackendSerializationProxy (元信息)
            // 2. 按 KeyGroup 写状态数据
            // 3. 返回 KeyGroupsStateHandle
        };
    }
}
```

### 5.4 ForL0KeyedStateBackend

```java
public class ForL0KeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
    
    private final Map<String, ForL0StateStore<K, ?, ?>> registeredStores;
    private final Map<String, InternalKvState<K, ?, ?>> createdStates;
    private final HeapPriorityQueuesManager priorityQueuesManager;
    private final ForL0SnapshotStrategy<K> snapshotStrategy;
    private final SnapshotExecutionType snapshotExecutionType;
    
    // 核心：创建 State
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
        
        ForL0StateStore<K, N, SV> store = getOrCreateStateStore(stateDesc.getName(), 
                                                                  namespaceSerializer,
                                                                  stateDesc.getSerializer());
        
        IS state = createState(stateDesc, store, namespaceSerializer);
        createdStates.put(stateDesc.getName(), (InternalKvState<K, ?, ?>) state);
        return state;
    }
    
    // Checkpoint
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId, long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions) throws Exception {
        
        return new SnapshotStrategyRunner<>(
                "ForL0 backend snapshot",
                snapshotStrategy,
                cancelStreamRegistry,
                snapshotExecutionType
        ).snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }
    
    // 优先级队列
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        return priorityQueuesManager.createOrUpdate(stateName, byteOrderedElementSerializer);
    }
}
```

## 六、不需要的组件

| 组件 | 原因 |
|------|------|
| `AbstractSwissTable` | 过度抽象 |
| `SwissTableGeneric` | 使用原始 SwissTable.java |
| `SwissTableLongTimeWindow` | 特化版本，不需要 |
| `SwissTableLongVoid` | 特化版本，不需要 |
| `SwissTableStringTimeWindow` | 特化版本，不需要 |
| `SwissTableStringVoid` | 特化版本，不需要 |
| `ForL0StateTable` | 不继承 StateTable |
| `ForL0StateTableSnapshot` | 合并到 SnapshotStrategy |
| `ForL0StateMap` | 不继承 StateMap |
| `ForL0StateMapSnapshot` | 合并到 SnapshotStrategy |
| `ForL0StateBackendConfig` | 合并到 ForL0Options |
| `ForL0StateBackendOptions` | 合并到 ForL0Options |

## 七、SPI 配置

### META-INF/services/org.apache.flink.runtime.state.StateBackendFactory

```
org.apache.flink.state.forl0.ForL0StateBackendFactory
```

## 八、Native 代码修改

JNI 函数名需要根据新包名修改：

```c
// 旧签名
JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_...

// 新签名
JNIEXPORT jlong JNICALL Java_org_apache_flink_state_forl0_space_NativeL0Memory_...
```

修改文件：`src/main/native/forl0_native.c`

## 九、迁移步骤

### Phase 1: 准备工作

| 步骤 | 任务 |
|------|------|
| 1.1 | 创建新分支 `refactor/lightweight-statebackend` |
| 1.2 | 创建新包结构 `org.apache.flink.state.forl0` |

### Phase 2: 核心迁移

| 步骤 | 任务 | 文件 |
|------|------|------|
| 2.1 | 迁移 SwissTable | `SwissTable.java` |
| 2.2 | 迁移 space 子包 | `space/*.java` |
| 2.3 | 实现 ForL0StateStore | 新建 |
| 2.4 | 实现 ForL0Options | 新建（合并原 Config + Options） |

### Phase 3: State 实现

| 步骤 | 任务 |
|------|------|
| 3.1 | 实现 ForL0ValueState |
| 3.2 | 实现 ForL0ListState |
| 3.3 | 实现 ForL0MapState |
| 3.4 | 实现 ForL0ReducingState |
| 3.5 | 实现 ForL0AggregatingState |

### Phase 4: Checkpoint

| 步骤 | 任务 |
|------|------|
| 4.1 | 实现 ForL0SnapshotResources |
| 4.2 | 实现 ForL0SnapshotStrategy |
| 4.3 | 实现 ForL0RestoreOperation |

### Phase 5: Backend 组装

| 步骤 | 任务 |
|------|------|
| 5.1 | 实现 ForL0KeyedStateBackend |
| 5.2 | 实现 ForL0KeyedStateBackendBuilder |
| 5.3 | 实现 ForL0StateBackend |
| 5.4 | 实现 ForL0StateBackendFactory |
| 5.5 | 更新 SPI 配置文件 |

### Phase 6: Native 更新

| 步骤 | 任务 |
|------|------|
| 6.1 | 更新 JNI 函数签名 |
| 6.2 | 重新编译 native 库 |

### Phase 7: 测试与清理

| 步骤 | 任务 |
|------|------|
| 7.1 | 迁移单元测试到新包 |
| 7.2 | 迁移集成测试 |
| 7.3 | 运行完整测试 |
| 7.4 | 删除旧的 heap 包下的 ForL0 代码 |
| 7.5 | 更新文档和 copilot-instructions.md |

## 十、工作量估算

| 任务 | 文件数 | 天数 |
|------|--------|------|
| Phase 1-2: 准备 + 核心迁移 | 5 | 2 |
| Phase 3: State 实现 | 5 | 2 |
| Phase 4: Checkpoint | 3 | 3 |
| Phase 5: Backend 组装 | 4 | 2 |
| Phase 6: Native 更新 | 1 | 0.5 |
| Phase 7: 测试与清理 | - | 2.5 |
| **总计** | **~18** | **~12 天** |

## 十一、验收标准

1. ✅ 所有代码位于 `org.apache.flink.state.forl0` 包下
2. ✅ 不依赖 `org.apache.flink.runtime.state.heap` 包的任何实现类
3. ✅ ValueState、ListState、MapState、ReducingState、AggregatingState 功能正常
4. ✅ Checkpoint/Savepoint 功能正常
5. ✅ 原有单元测试和集成测试全部通过
6. ✅ Native L0 内存功能正常（如已实现）

## 十二、风险点

| 风险 | 应对 |
|------|------|
| Checkpoint 格式兼容性 | 使用 Flink 标准的 KeyedBackendSerializationProxy |
| AbstractKeyedStateBackend 内部变化 | 依赖稳定的公共方法，避免使用 protected 成员 |
| 优先级队列集成 | 直接复用 HeapPriorityQueuesManager |
