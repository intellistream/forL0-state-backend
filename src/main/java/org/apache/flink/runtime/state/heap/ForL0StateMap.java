package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.heap.io.SerializerPack;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // Core storage components - Phase 3: Heap Object Store
    private final MemoryManagerAllocator allocator;  // For MainTable (retained for compatibility)
    private final L0MemoryAllocator l0Allocator;     // For L0Table (nullable)
    private final HeapEntryStore<K, N, S> heapEntryStore;  // Heap-based entry storage (zero serialization)
    private final MainTable<K, N, S> mainTable;            // Generic MainTable with object comparison
    private final L0Table<K, N, S> l0Table;                // nullable, Generic L0Table with object comparison

    // 序列化器（仅 Checkpoint/Restore 使用，热路径不序列化）
    private final SerializerPack<K, N, S> serializerPack;

    // Configuration
    private final boolean l0CacheEnabled;

    // Statistics
    private int size = 0;
    private long totalAccesses = 0;
    private long l0Hits = 0;
    private long mainTableHits = 0;

    // Resize coordination
    private volatile boolean resizeInProgress = false;

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
     * @param allocator Memory manager allocator for MainTable (retained for compatibility)
     * @param l0Allocator L0 memory allocator for L0Table (can be null if L0 disabled)
     * @param mainTableInitPow2 MainTable initial bucket count as power of 2
     * @param l0CacheSizePow2 L0Table bucket count as power of 2
     * @param keySerializer Key serializer
     * @param namespaceSerializer Namespace serializer
     * @param stateSerializer State serializer
     * @param l0CacheEnabled Whether L0 cache is enabled
     * @param l0Policy L0 cache replacement policy
     * @param loadFactorThreshold MainTable load factor threshold for resize
     * @param arenaInitialSizeBytes Ignored (kept for backward compatibility)
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
        // 序列化器仅用于 Checkpoint/Restore，热路径不序列化
        this.serializerPack = new SerializerPack<>(keySerializer, namespaceSerializer, stateSerializer);
        this.l0CacheEnabled = l0CacheEnabled;
        
        // Phase 3: 使用 HeapEntryStore 存储堆对象，零序列化
        this.heapEntryStore = new HeapEntryStore<>();
        // MainTable/L0Table 使用泛型和对象比较
        this.mainTable = new MainTable<>(allocator, mainTableInitPow2, loadFactorThreshold);
        this.l0Table = (l0CacheEnabled && l0Allocator != null) 
            ? new L0Table<>(l0Allocator, l0CacheSizePow2, l0Policy) 
            : null;

        LOG.debug("ForL0StateMap initialized (Phase 3: Heap Object Store) with mainTable={} buckets, " +
                  "l0Cache={} buckets, cache={}, policy={}, loadFactor={}",
                1 << mainTableInitPow2, l0CacheEnabled ? 1 << l0CacheSizePow2 : 0, 
                l0CacheEnabled, l0Policy, loadFactorThreshold);
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
        if (heapEntryStore != null) {
            heapEntryStore.close();
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

        // Phase 3: 热路径零序列化 - 使用对象哈希和对象比较
        int hash = compositeHash(key, namespace);
        short tag = (short) (hash >>> 16);  // 与 HeapStateEntry.getTag() 保持一致

        long addr;
        if (l0CacheEnabled && l0Table != null) {
            addr = l0Table.get(hash, tag, key, namespace, heapEntryStore);
            if (addr > 0) {
                l0Hits++;
                HeapStateEntry<K, N, S> entry = heapEntryStore.get(addr);
                return entry != null ? entry.getState() : null;
            }
        }
        addr = mainTable.get(hash, tag, key, namespace, heapEntryStore);
        if (addr > 0) {
            mainTableHits++;
            updateL0Table(hash, tag, key, namespace, addr);
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(addr);
            return entry != null ? entry.getState() : null;
        }
        return null;
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        if (key == null || namespace == null) { return false; }
        
        // Phase 3: 热路径零序列化
        int hash = compositeHash(key, namespace);
        short tag = (short) (hash >>> 16);  // 与 HeapStateEntry.getTag() 保持一致

        if (l0CacheEnabled && l0Table != null) {
            if (l0Table.get(hash, tag, key, namespace, heapEntryStore) > 0)
                return true;
        }

        return mainTable.get(hash, tag, key, namespace, heapEntryStore) > 0;
    }

    @Override
    public void put(K key, N namespace, S state) {
        if (key == null || namespace == null) { return; }
        
        // Phase 3: 热路径零序列化
        int hash = compositeHash(key, namespace);
        short tag = (short) (hash >>> 16);  // 与 HeapStateEntry.getTag() 保持一致

        // 检查是否需要 resize
        if (mainTable.needsResize() && !resizeInProgress) {
            performResize();
        }

        // 尝试在 MainTable 中查找或插入
        long existingAddr = mainTable.put(hash, tag, 0, key, namespace, heapEntryStore);
        
        if (existingAddr == 0) {
            // 新条目：在 HeapEntryStore 中分配并存储对象
            long addr = heapEntryStore.allocate(key, namespace, state);
            mainTable.setSlotPointer(addr);
            size++;
            updateL0Table(hash, tag, key, namespace, addr);
        } else if (existingAddr == -1) {
            // MainTable 满了，强制 resize 后重试
            performResize();
            put(key, namespace, state);
        } else {
            // 更新现有条目：直接更新 HeapEntryStore 中的对象
            heapEntryStore.updateState(existingAddr, state);
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
        if (mainTable == null) { return; }

        // Phase 3: 热路径零序列化
        int hash = compositeHash(key, namespace);
        short tag = (short) (hash >>> 16);  // 与 HeapStateEntry.getTag() 保持一致
        
        long removedAddr = mainTable.remove(hash, tag, key, namespace, heapEntryStore);
        if (removedAddr > 0) {
            size--;
            if (l0CacheEnabled && l0Table != null) {
                l0Table.remove(hash, tag, key, namespace, heapEntryStore);
            }
            heapEntryStore.remove(removedAddr);
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
        if (heapEntryStore == null || mainTable == null) {
            return 0;
        }
        int[] cnt = new int[1];
        // Phase 3: 直接使用对象比较，无需序列化
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(entryAddress);
            if (entry != null) {
                N n = entry.getNamespace();
                if ((namespace == null && n == null) || (namespace != null && namespace.equals(n))) {
                    cnt[0]++;
                }
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
        if (heapEntryStore == null || mainTable == null) {
            return list.iterator();
        }
        // Phase 3: 直接从 HeapEntryStore 读取对象，无需序列化
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(entryAddress);
            if (entry != null && entry.getState() != null) {
                list.add(new StateEntry.SimpleStateEntry<>(
                    entry.getKey(), entry.getNamespace(), entry.getState()));
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
        if (namespace == null || heapEntryStore == null || mainTable == null) {
            return Stream.empty();
        }
        java.util.ArrayList<K> keys = new java.util.ArrayList<>();
        // Phase 3: 直接使用对象比较，无需序列化
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(entryAddress);
            if (entry != null) {
                N n = entry.getNamespace();
                if (n != null && n.equals(namespace)) {
                    keys.add(entry.getKey());
                }
            }
        });
        return keys.stream();
    }

    @Override
    public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        if (key == null || namespace == null) { return; }
        
        // Phase 3: 热路径零序列化
        int hash = compositeHash(key, namespace);
        short tag = (short) (hash >>> 16);  // 与 HeapStateEntry.getTag() 保持一致

        // 检查是否需要 resize
        if (mainTable.needsResize() && !resizeInProgress) {
            performResize();
        }

        // 尝试在 MainTable 中查找或插入
        long existingAddr = mainTable.put(hash, tag, 0, key, namespace, heapEntryStore);
        
        if (existingAddr == 0) {
            // 新条目：应用转换函数并存储
            S newState = transformation.apply(null, value);
            long addr = heapEntryStore.allocate(key, namespace, newState);
            mainTable.setSlotPointer(addr);
            size++;
            updateL0Table(hash, tag, key, namespace, addr);
        } else if (existingAddr == -1) {
            // MainTable 满了，强制 resize 后重试
            performResize();
            transform(key, namespace, value, transformation);
        } else {
            // 更新现有条目：读取旧值，应用转换，更新
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(existingAddr);
            S oldState = entry != null ? entry.getState() : null;
            S newState = transformation.apply(oldState, value);
            heapEntryStore.updateState(existingAddr, newState);
        }
    }

    // ================== Private helper methods ==================

    /**
     * Computes composite hash from key and namespace using Object.hashCode().
     * This is the hot path - no serialization needed.
     */
    private static int compositeHash(Object key, Object namespace) {
        return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
    }

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
        mainTable.tryResize(heapEntryStore);
        resizeInProgress = false;
    }

    /**
     * Updates L0 cache with the entry (promotes hot keys).
     */
    private void updateL0Table(int hash, short tag, K key, N namespace, long entryAddress) {
        if (l0CacheEnabled && l0Table != null && !resizeInProgress) {
            l0Table.put(hash, tag, entryAddress, key, namespace, heapEntryStore);
        }
    }

    /**
     * Returns the HeapEntryStore for Checkpoint/Snapshot access.
     * Package-private for use by ForL0StateMapSnapshot.
     */
    HeapEntryStore<K, N, S> getHeapEntryStore() {
        return heapEntryStore;
    }

    /**
     * Returns the SerializerPack for Checkpoint/Snapshot serialization.
     * Package-private for use by ForL0StateMapSnapshot.
     */
    SerializerPack<K, N, S> getSerializerPack() {
        return serializerPack;
    }

    // ================== REMOVED LEGACY METHODS ==================
    // The following methods have been removed as they are no longer needed:
    // - readValue(long) - replaced by direct HeapEntryStore.get()
    // - putEntry(KeyNamespaceHash) - replaced by inline code in put()
    // - removeEntry(KeyNamespaceHash) - replaced by inline code in remove()
    // - updateL0Table(KeyNamespaceHash, long) - replaced by updateL0Table(hash, tag, key, namespace, addr)
    // - deserializeKey/Namespace/Value - not needed for hot path
    // - deserializeValueFromArena - not needed for hot path
    // - serializeKeyNamespace - not needed for hot path
    // - KeyNamespaceHash class - not needed for hot path
    // ============================================================

    // ================== Backward compatibility - UNUSED ==================
    // These methods are kept for reference but should not be called:
    @SuppressWarnings("unused")
    private S UNUSED_readValue(long entryAddress) {
        // This method is no longer used - values are read directly from HeapEntryStore
        throw new UnsupportedOperationException("readValue is no longer used in Phase 3");
    }
    // =====================================================================

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
