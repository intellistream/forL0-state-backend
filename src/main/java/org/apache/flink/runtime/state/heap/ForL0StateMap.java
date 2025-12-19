package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.heap.entrystore.EntryStore;
import org.apache.flink.runtime.state.heap.io.SerializerPack;
import org.apache.flink.runtime.state.heap.io.FixedLengthTypeSupport;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
import org.apache.flink.runtime.state.heap.L0Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // Core storage components
    private final MemoryManagerAllocator allocator;  // For MainTable and EntryStore
    private final L0MemoryAllocator l0Allocator;     // For L0Table (nullable)
    private final EntryStore entryStore;
    // 直接管理表实例，移除 TableCore 中间层
    private final MainTable mainTable;
    private final L0Table l0Table; // nullable，仅在启用L0缓存时创建

    // 统一序列化打包器
    private final SerializerPack<K, N, S> serializerPack;

    // Fast-path support for fixed-length primitive types (Long, Int, Double, etc.)
    private final FixedLengthTypeSupport.TypeInfo stateTypeInfo;
    
    // Fast-path support for Tuple types composed of fixed-length fields
    private final FixedLengthTypeSupport.TupleTypeInfo tupleTypeInfo;
    
    // Fixed-length value size in bytes (for zero-copy MemorySegment operations)
    private final int fixedLengthValueSize;

    // Configuration
    private final boolean l0CacheEnabled;

    // Statistics
    private int size = 0;
    private long totalAccesses = 0;
    private long l0Hits = 0;
    private long mainTableHits = 0;

    // Resize coordination
    private volatile boolean resizeInProgress = false;
    // 移除时间节流，达条件即扩容

    // 复用反序列化对象，降低分配/反射开销
    private S reuseState;

    // Serialization cache for key and namespace to avoid repeated serialization
    private K lastKey;
    private N lastNamespace;
    private KeyNamespaceHash lastKeyNamespaceHash;

    /**
     * Constructor with basic parameters (backward compatible).
     * Uses default CLOCK replacement policy and 1.5 load factor threshold.
     */
    public ForL0StateMap(MemoryManagerAllocator allocator,
                         L0MemoryAllocator l0Allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled) {
        this(
            allocator,
            l0Allocator,
            mainTableInitPow2,
            l0CacheSizePow2,
            keySerializer,
            namespaceSerializer,
            stateSerializer,
            l0CacheEnabled,
            L0Table.ReplacementPolicy.CLOCK,
            1.5  // default load factor threshold
        );
    }

    /**
     * Constructor with replacement policy parameter (backward compatible).
     * Uses default 1.5 load factor threshold.
     */
    public ForL0StateMap(MemoryManagerAllocator allocator,
                         L0MemoryAllocator l0Allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy) {
        this(
            allocator,
            l0Allocator,
            mainTableInitPow2,
            l0CacheSizePow2,
            keySerializer,
            namespaceSerializer,
            stateSerializer,
            l0CacheEnabled,
            l0Policy,
            1.5  // default load factor threshold
        );
    }

    /**
     * Full constructor with all configurable parameters.
     *
     * @param allocator Memory manager allocator for MainTable and EntryStore
     * @param l0Allocator L0 memory allocator for L0Table (can be null if L0 disabled)
     * @param mainTableInitPow2 MainTable initial bucket count as power of 2
     * @param l0CacheSizePow2 L0Table bucket count as power of 2
     * @param keySerializer Key serializer
     * @param namespaceSerializer Namespace serializer
     * @param stateSerializer State serializer
     * @param l0CacheEnabled Whether L0 cache is enabled
     * @param l0Policy L0 cache replacement policy
     * @param loadFactorThreshold MainTable load factor threshold for resize
     */
    public ForL0StateMap(MemoryManagerAllocator allocator,
                         L0MemoryAllocator l0Allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy,
                         double loadFactorThreshold) {
        this(allocator, l0Allocator, mainTableInitPow2, l0CacheSizePow2,
             keySerializer, namespaceSerializer, stateSerializer,
             l0CacheEnabled, l0Policy, loadFactorThreshold, 0);
    }

    /**
     * Full constructor with all configurable parameters including arena pre-allocation.
     *
     * @param allocator Memory manager allocator for MainTable and EntryStore
     * @param l0Allocator L0 memory allocator for L0Table (can be null if L0 disabled)
     * @param mainTableInitPow2 MainTable initial bucket count as power of 2
     * @param l0CacheSizePow2 L0Table bucket count as power of 2
     * @param keySerializer Key serializer
     * @param namespaceSerializer Namespace serializer
     * @param stateSerializer State serializer
     * @param l0CacheEnabled Whether L0 cache is enabled
     * @param l0Policy L0 cache replacement policy
     * @param loadFactorThreshold MainTable load factor threshold for resize
     * @param arenaInitialSizeBytes Initial memory to pre-allocate for EntryStore (0 for no pre-allocation)
     */
    public ForL0StateMap(MemoryManagerAllocator allocator,
                         L0MemoryAllocator l0Allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy,
                         double loadFactorThreshold,
                         long arenaInitialSizeBytes) {
        this.allocator = allocator;
        this.l0Allocator = l0Allocator;
        // 直接使用 SerializerPack，移除冗余的序列化器引用
        this.serializerPack = new SerializerPack<>(keySerializer, namespaceSerializer, stateSerializer);
        this.l0CacheEnabled = l0CacheEnabled;
        this.entryStore = new EntryStore(allocator, arenaInitialSizeBytes);
        // MainTable 使用 MemoryManagerAllocator，使用配置的负载因子阈值
        this.mainTable = new MainTable(allocator, mainTableInitPow2, loadFactorThreshold);
        this.l0Table = (l0CacheEnabled && l0Allocator != null) 
            ? new L0Table(l0Allocator, l0CacheSizePow2, l0Policy) 
            : null;
        
        // Detect fixed-length type for fast-path optimization
        this.stateTypeInfo = FixedLengthTypeSupport.detect(stateSerializer);
        // Detect Tuple type composed of fixed-length fields
        this.tupleTypeInfo = (stateTypeInfo == null) 
            ? FixedLengthTypeSupport.detectTuple(stateSerializer) 
            : null;
        
        // Store fixed-length value size for zero-copy operations
        if (stateTypeInfo != null) {
            this.fixedLengthValueSize = stateTypeInfo.getByteSize();
            LOG.info("ForL0StateMap: Fast-path enabled for state type {} ({} bytes)",
                    stateTypeInfo.getType(), fixedLengthValueSize);
        } else if (tupleTypeInfo != null) {
            this.fixedLengthValueSize = tupleTypeInfo.getByteSize();
            LOG.info("ForL0StateMap: Fast-path enabled for Tuple{} ({} bytes)",
                    tupleTypeInfo.getArity(), fixedLengthValueSize);
        } else {
            this.fixedLengthValueSize = 0;
        }

        String fastPathDesc = stateTypeInfo != null ? stateTypeInfo.getType().toString() 
            : (tupleTypeInfo != null ? "Tuple" + tupleTypeInfo.getArity() : "none");
        LOG.debug("ForL0StateMap initialized with mainTable={} buckets (expandable), l0Cache={} buckets (fixed), " +
                  "cache={}, policy={}, loadFactor={}, arenaPreAlloc={} bytes, fastPath={}",
                1 << mainTableInitPow2, l0CacheEnabled ? 1 << l0CacheSizePow2 : 0, 
                l0CacheEnabled, l0Policy, loadFactorThreshold, arenaInitialSizeBytes,
                fastPathDesc);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Closing ForL0StateMap");
        if (mainTable != null) {
            mainTable.close();
        }
        if (l0Table != null) {
            l0Table.close();
        }
        if (entryStore != null) {
            entryStore.close();
        }
        LOG.debug("ForL0StateMap closed");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public S get(K key, N namespace) {
        if (key == null || namespace == null) { return null; }
        totalAccesses++;

        try {
            KeyNamespaceHash knh = serializeKeyNamespace(key, namespace);

            long addr;
            if (l0CacheEnabled && l0Table != null) {
                addr = l0Table.get(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, entryStore);
                if (addr > 0) {
                    l0Hits++;
                    return readValue(addr);
                }
            }
            addr = mainTable.get(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, entryStore);
            if (addr > 0) {
                mainTableHits++;
                updateL0Table(knh, addr); // promote to L0 cache
                return readValue(addr);
            }
            return null;
        } catch (IOException e) {
            LOG.error("Serialization error during get operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Get operation failed due to serialization error", e);
        }
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        if (key == null || namespace == null) { return false; }
        try {
            KeyNamespaceHash knh = serializeKeyNamespace(key, namespace);

            if (l0CacheEnabled && l0Table != null) {
                if (l0Table.get(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, entryStore) > 0)
                    return true;
            }

            return mainTable.get(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, entryStore) > 0;
        } catch (IOException e) {
            LOG.error("Error during containsKey operation for key={}, namespace={}", key, namespace, e);
            return false;
        }
    }

    @Override
    public void put(K key, N namespace, S state) {
        KeyNamespaceHash knh;
        try {
            knh = serializeKeyNamespace(key, namespace);
        } catch (IOException e) {
            LOG.error("Serialization error during put operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Put operation failed due to serialization error", e);
        }

        long result = putEntry(knh);

        if (result == 0) { // new entry
            long addr;
            if (stateTypeInfo != null) {
                // Zero-copy fast path: allocate entry with reserved value space, then write directly
                addr = entryStore.allocateEntry(knh.hash, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, null, fixedLengthValueSize);
                MemorySegmentSlice slice = entryStore.getValueSlice(addr);
                stateTypeInfo.write(slice.segment, slice.offset, state);
            } else if (tupleTypeInfo != null) {
                // Zero-copy fast path for Tuple types
                addr = entryStore.allocateEntry(knh.hash, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, null, fixedLengthValueSize);
                MemorySegmentSlice slice = entryStore.getValueSlice(addr);
                tupleTypeInfo.write(slice.segment, slice.offset, (org.apache.flink.api.java.tuple.Tuple) state);
            } else {
                // Normal path: use serializer
                try {
                    serializerPack.writeState(state);
                } catch (IOException e) {
                    throw new RuntimeException("Put operation failed due to serialization error", e);
                }
                addr = entryStore.allocateEntry(knh.hash, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, serializerPack.stateBuffer(), serializerPack.stateLength());
            }
            mainTable.setSlotPointer(addr);
            size++;
            updateL0Table(knh, addr);
        } else { // existing entry
            updateExistingEntry(result, state, knh);
        }
    }

    /**
     * Optimized update for fixed-length types: direct MemorySegment write.
     * For variable-length types, falls back to byte[] based update.
     */
    @SuppressWarnings("unchecked")
    private void updateExistingEntry(long existingAddr, S state, KeyNamespaceHash knh) {
        if (stateTypeInfo != null) {
            // Zero-copy: fixed-length types can always update in-place
            MemorySegmentSlice slice = entryStore.getValueSlice(existingAddr);
            stateTypeInfo.write(slice.segment, slice.offset, state);
        } else if (tupleTypeInfo != null) {
            // Zero-copy: fixed-length Tuple types can always update in-place
            MemorySegmentSlice slice = entryStore.getValueSlice(existingAddr);
            tupleTypeInfo.write(slice.segment, slice.offset, (org.apache.flink.api.java.tuple.Tuple) state);
        } else {
            // Variable-length: use byte[] based update
            try {
                serializerPack.writeState(state);
            } catch (IOException e) {
                throw new RuntimeException("Update operation failed due to serialization error", e);
            }
            // EntryStore.updateValue() guarantees address stability - no pointer switching needed
            entryStore.updateValue(existingAddr, serializerPack.stateBuffer(), serializerPack.stateLength());
        }
    }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        if (key == null || namespace == null) {
            return null;
        }
        S oldValue = get(key, namespace);
        put(key, namespace, state);

        return oldValue;
    }

    @Override
    public void remove(K key, N namespace) {
        if (key == null || namespace == null) { return; }
        if (allocator == null || entryStore == null || mainTable == null) { return; }

        try {
            KeyNamespaceHash knh = serializeKeyNamespace(key, namespace);
            removeEntry(knh);

        } catch (IOException e) {
            LOG.error("Serialization error during remove operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Remove operation failed due to serialization error", e);
        }
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        if (key == null || namespace == null) {
            return null;
        }
        S oldValue = get(key, namespace);
        remove(key, namespace);

        return oldValue;
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        if (entryStore == null || mainTable == null) {
            return 0;
        }
        int[] cnt = new int[1];
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            try {
                byte[] nb = entryStore.getNamespaceBytes(entryAddress);
                if (nb == null) { return; }
                N n = deserializeNamespace(nb);
                if ((namespace == null && n == null) || (namespace != null && namespace.equals(n))) {
                    cnt[0]++;
                }
            } catch (IOException ignore) {
            }
        });
        return cnt[0];
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        final int batchSize = Math.max(1, recommendedMaxNumberOfReturnedRecords);
        final Iterator<StateEntry<K, N, S>> iter = iterator();
        return new InternalKvState.StateIncrementalVisitor<K, N, S>() {
            @Override
            public java.util.List<StateEntry<K, N, S>> nextEntries() {
                java.util.ArrayList<StateEntry<K, N, S>> batch = new java.util.ArrayList<>(batchSize);
                int i = 0;
                while (i < batchSize && iter.hasNext()) {
                    batch.add(iter.next());
                    i++;
                }
                return batch;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public void update(StateEntry<K, N, S> entry, S newState) {
                if (entry == null) { return; }
                put(entry.getKey(), entry.getNamespace(), newState);
            }

            @Override
            public void remove(StateEntry<K, N, S> entry) {
                if (entry == null) { return; }
                ForL0StateMap.this.remove(entry.getKey(), entry.getNamespace());
            }

        };
    }


    @Nonnull
    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        // 构造一次性快照列表，避免并发修改影响
        ArrayList<StateEntry<K, N, S>> list = new ArrayList<>(Math.max(16, size));
        if (entryStore == null || mainTable == null) {
            return list.iterator();
        }
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            try {
                byte[] kb = entryStore.getKeyBytes(entryAddress);
                byte[] nb = entryStore.getNamespaceBytes(entryAddress);
                if (kb == null || nb == null) {
                    return; // skip entries with missing key or namespace
                }
                K k = deserializeKey(kb);
                N n = deserializeNamespace(nb);
                // Use optimized deserialization (consistent with get() method)
                S v = readValue(entryAddress);
                if (v != null) {
                    list.add(new StateEntry.SimpleStateEntry<>(k, n, v));
                }
            } catch (Exception e) {
                LOG.debug("Skipping corrupted entry during iteration", e);
            }
        });

        return list.iterator();
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateMapSnapshot<>(this);
    }

    @Override
    public Stream<K> getKeys(N namespace) {
        if (namespace == null || entryStore == null || mainTable == null) {
            return Stream.empty();
        }
        java.util.ArrayList<K> keys = new java.util.ArrayList<>();
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            try {
                byte[] nb = entryStore.getNamespaceBytes(entryAddress);
                if (nb == null) { return; }
                N n = deserializeNamespace(nb);
                if (n != null && n.equals(namespace)) {
                    byte[] kb = entryStore.getKeyBytes(entryAddress);
                    if (kb != null) {
                        K k = deserializeKey(kb);
                        keys.add(k);
                    }
                }
            } catch (Exception ignore) {
            }
        });
        return keys.stream();
    }

    @Override
    public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        KeyNamespaceHash knh = serializeKeyNamespace(key, namespace);
        long result = putEntry(knh);
        
        // Check fast-path availability
        final boolean useSimpleFastPath = stateTypeInfo != null;
        final boolean useTupleFastPath = tupleTypeInfo != null;

        if (result == 0) { // new entry
            S newState = transformation.apply(null, value);
            long addr;
            if (useSimpleFastPath) {
                // Zero-copy fast path: allocate entry with reserved value space, then write directly
                addr = entryStore.allocateEntry(knh.hash, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, null, fixedLengthValueSize);
                MemorySegmentSlice slice = entryStore.getValueSlice(addr);
                stateTypeInfo.write(slice.segment, slice.offset, newState);
            } else if (useTupleFastPath) {
                // Zero-copy fast path: allocate entry with reserved value space, then write directly
                addr = entryStore.allocateEntry(knh.hash, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, null, fixedLengthValueSize);
                MemorySegmentSlice slice = entryStore.getValueSlice(addr);
                tupleTypeInfo.write(slice.segment, slice.offset, (org.apache.flink.api.java.tuple.Tuple) newState);
            } else {
                // Normal path: use serializer
                serializerPack.writeState(newState);
                addr = entryStore.allocateEntry(knh.hash, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, serializerPack.stateBuffer(), serializerPack.stateLength());
            }
            mainTable.setSlotPointer(addr);
            size++;
            updateL0Table(knh, addr);
        } else { // existing entry
            S oldState;
            MemorySegmentSlice slice = null;
            if (useSimpleFastPath) {
                // Zero-copy fast path: read directly from MemorySegment
                slice = entryStore.getValueSlice(result);
                oldState = stateTypeInfo.read(slice.segment, slice.offset);
            } else if (useTupleFastPath) {
                // Zero-copy fast path: read directly from MemorySegment
                slice = entryStore.getValueSlice(result);
                oldState = (S) tupleTypeInfo.read(slice.segment, slice.offset);
            } else {
                // Normal path
                oldState = deserializeValueFromArena(result);
            }
            S newState = transformation.apply(oldState, value);
            if (useSimpleFastPath) {
                // Zero-copy fast path: fixed-length guarantees in-place update
                stateTypeInfo.write(slice.segment, slice.offset, newState);
            } else if (useTupleFastPath) {
                // Zero-copy fast path: fixed-length guarantees in-place update
                tupleTypeInfo.write(slice.segment, slice.offset, (org.apache.flink.api.java.tuple.Tuple) newState);
            } else {
                // Normal path: use serializer
                serializerPack.writeState(newState);
                entryStore.updateValue(result, serializerPack.stateBuffer(), serializerPack.stateLength());
            }
        }
    }

    /**
     * Optimized deserialization for fixed-length types and Tuples.
     * Reads value directly from MemorySegment without byte[] copy when fast-path is available.
     */
    @SuppressWarnings("unchecked")
    private S readValue(long entryAddress) {
        // Fast-path for primitive types: zero-copy direct MemorySegment read
        if (stateTypeInfo != null) {
            MemorySegmentSlice slice = entryStore.getValueSlice(entryAddress);
            if (slice == null || slice.length == 0) {
                return null;
            }
            return stateTypeInfo.read(slice.segment, slice.offset);
        }
        
        // Fast-path for Tuple types: zero-copy direct MemorySegment read
        if (tupleTypeInfo != null) {
            MemorySegmentSlice slice = entryStore.getValueSlice(entryAddress);
            if (slice == null || slice.length == 0) {
                return null;
            }
            return (S) tupleTypeInfo.read(slice.segment, slice.offset);
        }
        
        // Normal path: use TypeSerializer
        try {
            return deserializeValueFromArena(entryAddress);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize value from arena", e);
        }
    }

    /**
     * Puts an entry into the main table, performing resize if necessary.
     * @param knh key and namespace hash info
     * @return address of existing entry if found, 0 if new entry, -1 if failed (e.g. bucket pool full)
     */
    private long putEntry(KeyNamespaceHash knh) {
        if (mainTable.needsResize() && !resizeInProgress) {
            performResize();
        }
        long result = mainTable.put(knh.hash, knh.tag, 0, knh.keyBytes, knh.keyLength,
                knh.namespaceBytes, knh.namespaceLength, entryStore);
        
        // 经过 MainTable 优化后，put 返回 -1 表示严重错误（扩容逻辑失效）
        // 正常情况下 needsResize 标志应该在达到扩展桶上限前就被设置
        if (result == -1) {
            throw new IllegalStateException(
                "MainTable.put() returned -1 after resize check. " +
                "This indicates a critical failure in the resize mechanism. " +
                "LoadFactor=" + mainTable.getLoadFactor() + 
                ", MaxExtensionBuckets=" + mainTable.getMaxExtensionBucketsUsed() +
                ", NeedsResize=" + mainTable.needsResize());
        }
        return result;
    }

    /**
     * 执行条目删除操作，包括主表删除、L0缓存清理和内存释放
     *
     * @param knh 键命名空间哈希信息
     */
    private void removeEntry(KeyNamespaceHash knh) {
        long removedAddr = mainTable.remove(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                knh.namespaceBytes, knh.namespaceLength, entryStore);
        if (removedAddr > 0) {
            size--;
            if (l0CacheEnabled && l0Table != null) {
                l0Table.remove(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, entryStore);
            }
            entryStore.removeEntry(removedAddr);
        }
    }

    // ================== Private helper methods ==================


    /**
     * Performs the main table resize with L0 cache coordination.
     */
    private void performResize() {
        if (resizeInProgress || !mainTable.needsResize()) {
            return;
        }
        resizeInProgress = true;
        if (l0CacheEnabled && l0Table != null) {
            l0Table.clear();
        }
        mainTable.tryResize(entryStore);
        resizeInProgress = false;
    }

    private void updateL0Table(KeyNamespaceHash knh, long entryAddress) {
        if (l0CacheEnabled && l0Table != null && !resizeInProgress) {
            l0Table.put(knh.hash, knh.tag, entryAddress, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, entryStore);
        }
    }

    // 反序列化辅助：供迭代与 sizeOfNamespace 使用
    private K deserializeKey(byte[] keyBytes) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(keyBytes);
        return serializerPack.keySerializer().deserialize(in);
    }

    private N deserializeNamespace(byte[] namespaceBytes) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(namespaceBytes);
        return serializerPack.namespaceSerializer().deserialize(in);
    }

    private S deserializeValue(byte[] valueBytes) throws IOException {
        if (valueBytes == null || valueBytes.length == 0) {
            return null;
        }
        DataInputDeserializer in = new DataInputDeserializer(valueBytes);
        return serializerPack.stateSerializer().deserialize(in);
    }

    private S deserializeValueFromArena(long entryAddress) throws IOException {
        MemorySegmentSlice slice = entryStore.getValueSlice(entryAddress);
        if (slice == null || slice.length == 0) {
            return null;
        }
        // 使用 SerializerPack 提供的便捷方法：先重置输入视图再反序列化（支持复用实例）
        S reuse = reuseState;
        if (reuse == null) {
            reuse = serializerPack.stateSerializer().createInstance();
            if (reuse != null) {
                reuseState = reuse;
            }
        }

        return serializerPack.deserializeStateFrom(slice.segment, slice.offset, slice.length, reuse);
    }

    /**
     * Serializes key and namespace, computes hash and tag.
     * Returns a KeyNamespaceHash object containing all computed values.
     * This is a performance-critical method that should be inlined by JIT.
     */
    private KeyNamespaceHash serializeKeyNamespace(K key, N namespace) throws IOException {
        // Check cache first using identity comparison (fast path)
        if (key == lastKey && namespace == lastNamespace && lastKeyNamespaceHash != null) {
            return lastKeyNamespaceHash;
        }

        // Serialize and get lengths directly from write methods (avoids extra method calls)
        int klen = serializerPack.writeKey(key);
        int nlen = serializerPack.writeNamespace(namespace);
        byte[] kb = serializerPack.keyBuffer();
        byte[] nb = serializerPack.namespaceBuffer();
        int hash = HashFunctions.compositeHash(kb, klen, nb, nlen);
        short tag = (short) ((hash >> 16) ^ (hash & 0xFFFF)); // 取混合后的低16位作为tag

        // Update cache
        lastKey = key;
        lastNamespace = namespace;
        lastKeyNamespaceHash = new KeyNamespaceHash(kb, klen, nb, nlen, hash, tag);

        return lastKeyNamespaceHash;
    }

    /**
     * Immutable data class for key/namespace serialization results.
     * All fields are final to enable JIT optimizations.
     */
    private static final class KeyNamespaceHash {
        final byte[] keyBytes;
        final int keyLength;
        final byte[] namespaceBytes;
        final int namespaceLength;
        final int hash;
        final short tag;

        KeyNamespaceHash(byte[] keyBytes, int keyLength, byte[] namespaceBytes,
                        int namespaceLength, int hash, short tag) {
            this.keyBytes = keyBytes;
            this.keyLength = keyLength;
            this.namespaceBytes = namespaceBytes;
            this.namespaceLength = namespaceLength;
            this.hash = hash;
            this.tag = tag;
        }
    }

    // ================== For testing access ==================

    /**
     * Cache statistics for monitoring and debugging.
     */
    public static class CacheStats {
        public final long totalAccesses;
        public final long l0Hits;
        public final long mainTableHits;
        public final L0Table.L0TableStats l0Stats;
        public final int totalEntries;

        public CacheStats(long totalAccesses, long l0Hits, long mainTableHits,
                          L0Table.L0TableStats l0Stats, int totalEntries) {
            this.totalAccesses = totalAccesses;
            this.l0Hits = l0Hits;
            this.mainTableHits = mainTableHits;
            this.l0Stats = l0Stats;
            this.totalEntries = totalEntries;
        }

        public double getOverallHitRate() {
            return totalAccesses == 0 ? 0.0 : (double) (l0Hits + mainTableHits) / totalAccesses;
        }

        public double getL0HitRate() {
            return totalAccesses == 0 ? 0.0 : (double) l0Hits / totalAccesses;
        }

        @Override
        public String toString() {
            return String.format(
                    "CacheStats{totalAccesses=%d, l0Hits=%d, mainTableHits=%d, overallHitRate=%.3f, l0HitRate=%.3f, totalEntries=%d, l0Stats=%s}",
                    totalAccesses, l0Hits, mainTableHits, getOverallHitRate(), getL0HitRate(), totalEntries, l0Stats
            );
        }
    }

    /**
     * Gets cache statistics for monitoring and debugging.
     */
    public CacheStats getCacheStats() {
        L0Table.L0TableStats l0Stats = l0CacheEnabled && l0Table != null ? l0Table.getStats() : null;
        return new CacheStats(
                totalAccesses,
                l0Hits,
                mainTableHits,
                l0Stats,
                size
        );
    }

    /**
     * Gets detailed statistics about the state map including resize information.
     */
    public DetailedStats getDetailedStats() {
        MainTable.TableStats mainStats = mainTable.getStats();
        L0Table.L0TableStats l0Stats = l0CacheEnabled && l0Table != null ? l0Table.getStats() : null;

        return new DetailedStats(
                totalAccesses,
                l0Hits,
                mainTableHits,
                l0Stats,
                mainStats,
                size,
                resizeInProgress
        );
    }

    /**
     * Detailed statistics including resize information.
     */
    public static class DetailedStats extends CacheStats {
        public final MainTable.TableStats mainTableStats;
        public final boolean resizeInProgress;

        public DetailedStats(long totalAccesses, long l0Hits, long mainTableHits,
                             L0Table.L0TableStats l0Stats, MainTable.TableStats mainTableStats,
                             int totalEntries, boolean resizeInProgress) {
            super(totalAccesses, l0Hits, mainTableHits, l0Stats, totalEntries);
            this.mainTableStats = mainTableStats;
            this.resizeInProgress = resizeInProgress;
        }

        @Override
        public String toString() {
            return String.format(
                    "DetailedStats{%s, mainTable=%s, resizeInProgress=%s}",
                    super.toString(), mainTableStats, resizeInProgress
            );
        }
    }

}
