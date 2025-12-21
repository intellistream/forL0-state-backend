package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.apache.flink.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // Core storage components
    private final HeapEntryStore<K, N, S> heapEntryStore;  // Heap-based entry storage (zero serialization)
    private final MainTable<K, N, S> mainTable;            // Generic MainTable with object comparison
    private final L0Table<K, N, S> l0Table;                // nullable, Generic L0Table with object comparison

    // Configuration
    private final boolean l0CacheEnabled;

    // Size tracking
    private int size = 0;

    /**
     * The last namespace that was actually inserted. This is a small optimization to reduce
     * duplicate namespace objects and improve identity comparison hit rate.
     */
    private N lastNamespace;

    // Resize coordination
    private volatile boolean resizeInProgress = false;

    /**
     * Constructor with basic parameters (backward compatible).
     * Uses default CLOCK replacement policy and 1.5 load factor threshold.
     */
    public ForL0StateMap(L0MemoryAllocator l0Allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled) {
        this(
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
    public ForL0StateMap(L0MemoryAllocator l0Allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy) {
        this(
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
     * @param l0Allocator L0 memory allocator for L0Table (can be null if L0 disabled)
     * @param mainTableInitPow2 MainTable initial bucket count as power of 2
     * @param l0CacheSizePow2 L0Table bucket count as power of 2
     * @param keySerializer Key serializer (unused, kept for API compatibility)
     * @param namespaceSerializer Namespace serializer (unused, kept for API compatibility)
     * @param stateSerializer State serializer (unused, kept for API compatibility)
     * @param l0CacheEnabled Whether L0 cache is enabled
     * @param l0Policy L0 cache replacement policy
     * @param loadFactorThreshold MainTable load factor threshold for resize
     */
    public ForL0StateMap(L0MemoryAllocator l0Allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy,
                         double loadFactorThreshold) {
        this.l0CacheEnabled = l0CacheEnabled;

        this.heapEntryStore = new HeapEntryStore<>();
        this.mainTable = new MainTable<>(mainTableInitPow2, loadFactorThreshold);
        this.l0Table = (l0CacheEnabled && l0Allocator != null) 
            ? new L0Table<>(l0Allocator, l0CacheSizePow2, l0Policy) 
            : null;

        LOG.debug("ForL0StateMap initialized with mainTable={} buckets, " +
                  "l0Cache={} buckets, cache={}, policy={}, loadFactor={}",
                1 << mainTableInitPow2, l0CacheEnabled ? 1 << l0CacheSizePow2 : 0, 
                l0CacheEnabled, l0Policy, loadFactorThreshold);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Closing ForL0StateMap");
        mainTable.close();
        if (l0Table != null) {
            l0Table.close();
        }
        heapEntryStore.close();
        LOG.debug("ForL0StateMap closed");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public S get(K key, N namespace) {
        if (key == null || namespace == null) { return null; }

        // Hot path: inline hash computation, zero serialization
        int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());

        int ptr;
        if (l0CacheEnabled) {
            ptr = l0Table.get(hash, key, namespace, heapEntryStore);
            if (ptr > 0) {
                return heapEntryStore.get(ptr).getState();
            }
        }
        ptr = mainTable.get(hash, key, namespace, heapEntryStore);
        if (ptr > 0) {
            if (l0CacheEnabled) {
                l0Table.put(hash, ptr, key, namespace, heapEntryStore);
            }
            return heapEntryStore.get(ptr).getState();
        }
        return null;
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        if (key == null || namespace == null) { return false; }

        int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());

        if (l0CacheEnabled && l0Table.get(hash, key, namespace, heapEntryStore) > 0) {
            return true;
        }
        return mainTable.get(hash, key, namespace, heapEntryStore) > 0;
    }

    @Override
    public void put(K key, N namespace, S state) {
        if (key == null || namespace == null) { return; }

        int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());

        if (mainTable.needsResize() && !resizeInProgress) {
            performResize();
        }

        int existingPtr = mainTable.put(hash, 0, key, namespace, heapEntryStore);

        if (existingPtr == 0) {
            // Deduplicate namespace object to improve identity comparison hit rate
            if (namespace.equals(lastNamespace)) {
                namespace = lastNamespace;
            } else {
                lastNamespace = namespace;
            }
            // New entry: allocate in HeapEntryStore
            int ptr = (int) heapEntryStore.allocate(key, namespace, state);
            mainTable.setSlot(hash, ptr);
            size++;
            if (l0CacheEnabled && !resizeInProgress) {
                l0Table.put(hash, ptr, key, namespace, heapEntryStore);
            }
        } else if (existingPtr == -1) {
            // MainTable full, resize and retry
            performResize();
            put(key, namespace, state);
        } else {
            // Update existing entry
            heapEntryStore.updateState(existingPtr, state);
        }
    }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        if (key == null || namespace == null) {
            return null;
        }

        int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());

        if (mainTable.needsResize() && !resizeInProgress) {
            performResize();
        }

        int existingPtr = mainTable.put(hash, 0, key, namespace, heapEntryStore);

        if (existingPtr == 0) {
            // Deduplicate namespace object
            if (namespace.equals(lastNamespace)) {
                namespace = lastNamespace;
            } else {
                lastNamespace = namespace;
            }
            int ptr = (int) heapEntryStore.allocate(key, namespace, state);
            mainTable.setSlot(hash, ptr);
            size++;
            if (l0CacheEnabled && !resizeInProgress) {
                l0Table.put(hash, ptr, key, namespace, heapEntryStore);
            }
            return null;
        } else if (existingPtr == -1) {
            performResize();
            return putAndGetOld(key, namespace, state);
        } else {
            // Update existing entry, return old value
            S oldValue = heapEntryStore.get(existingPtr).getState();
            heapEntryStore.updateState(existingPtr, state);
            return oldValue;
        }
    }

    @Override
    public void remove(K key, N namespace) {
        if (key == null || namespace == null) { return; }

        int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());

        int removedPtr = mainTable.remove(hash, key, namespace, heapEntryStore);
        if (removedPtr > 0) {
            size--;
            if (l0CacheEnabled) {
                l0Table.remove(hash, key, namespace, heapEntryStore);
            }
            heapEntryStore.remove(removedPtr);
        }
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        if (key == null || namespace == null) {
            return null;
        }

        int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());

        int removedPtr = mainTable.remove(hash, key, namespace, heapEntryStore);
        if (removedPtr > 0) {
            size--;
            if (l0CacheEnabled) {
                l0Table.remove(hash, key, namespace, heapEntryStore);
            }
            S oldValue = heapEntryStore.get(removedPtr).getState();
            heapEntryStore.remove(removedPtr);
            return oldValue;
        }
        return null;
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        int[] cnt = new int[1];
        mainTable.forEachEntry((ptr, hash) -> {
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(ptr);
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
        mainTable.forEachEntry((ptr, hash) -> {
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(ptr);
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
        if (namespace == null) {
            return Stream.empty();
        }
        java.util.ArrayList<K> keys = new java.util.ArrayList<>();
        mainTable.forEachEntry((ptr, hash) -> {
            HeapStateEntry<K, N, S> entry = heapEntryStore.get(ptr);
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

        int hash = MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());

        if (mainTable.needsResize() && !resizeInProgress) {
            performResize();
        }

        int existingPtr = mainTable.put(hash, 0, key, namespace, heapEntryStore);

        if (existingPtr == 0) {
            // Deduplicate namespace object
            if (namespace.equals(lastNamespace)) {
                namespace = lastNamespace;
            } else {
                lastNamespace = namespace;
            }
            S newState = transformation.apply(null, value);
            int ptr = (int) heapEntryStore.allocate(key, namespace, newState);
            mainTable.setSlot(hash, ptr);
            size++;
            if (l0CacheEnabled && !resizeInProgress) {
                l0Table.put(hash, ptr, key, namespace, heapEntryStore);
            }
        } else if (existingPtr == -1) {
            performResize();
            transform(key, namespace, value, transformation);
        } else {
            S oldState = heapEntryStore.get(existingPtr).getState();
            S newState = transformation.apply(oldState, value);
            heapEntryStore.updateState(existingPtr, newState);
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
        if (l0CacheEnabled) {
            l0Table.clear();
        }
        mainTable.tryResize(heapEntryStore);
        resizeInProgress = false;
    }

    /**
     * Returns the HeapEntryStore for Checkpoint/Snapshot access.
     * Package-private for use by ForL0StateMapSnapshot.
     */
    HeapEntryStore<K, N, S> getHeapEntryStore() {
        return heapEntryStore;
    }

    // ================== Statistics (for benchmark/testing) ==================

    /**
     * Gets L0 Table statistics for benchmark monitoring.
     * Returns null if L0 cache is disabled.
     */
    public L0Table.L0TableStats getL0Stats() {
        return l0CacheEnabled ? l0Table.getStats() : null;
    }

    /**
     * Gets MainTable statistics for benchmark monitoring.
     */
    public MainTable.TableStats getMainTableStats() {
        return mainTable.getStats();
    }

    /**
     * Gets detailed statistics about the state map.
     */
    public DetailedStats getDetailedStats() {
        return new DetailedStats(
                l0CacheEnabled ? l0Table.getStats() : null,
                mainTable.getStats(),
                size,
                resizeInProgress
        );
    }

    /**
     * Detailed statistics for monitoring.
     */
    public static class DetailedStats {
        public final L0Table.L0TableStats l0Stats;
        public final MainTable.TableStats mainTableStats;
        public final int totalEntries;
        public final boolean resizeInProgress;

        public DetailedStats(L0Table.L0TableStats l0Stats, MainTable.TableStats mainTableStats,
                             int totalEntries, boolean resizeInProgress) {
            this.l0Stats = l0Stats;
            this.mainTableStats = mainTableStats;
            this.totalEntries = totalEntries;
            this.resizeInProgress = resizeInProgress;
        }

        @Override
        public String toString() {
            return String.format(
                    "DetailedStats{l0Stats=%s, mainTable=%s, entries=%d, resizing=%s}",
                    l0Stats, mainTableStats, totalEntries, resizeInProgress
            );
        }
    }

}
