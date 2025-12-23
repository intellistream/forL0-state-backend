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
import java.util.Iterator;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // Core storage component (MainTable now manages entry storage internally)
    private final MainTable<K, N, S> mainTable;

    // Size tracking
    private int size = 0;

    /**
     * The last namespace that was actually inserted. This is a small optimization to reduce
     * duplicate namespace objects and improve identity comparison hit rate.
     */
    private N lastNamespace;

    /**
     * Constructor with basic parameters (backward compatible).
     * Uses default CLOCK replacement policy and 1.5 load factor threshold.
     */
    public ForL0StateMap(L0MemoryAllocator l0Allocator,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled) {
        this(
            l0Allocator,
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
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy) {
        this(
            l0Allocator,
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
     * @param l0CacheSizePow2 L0Table bucket count as power of 2
     * @param keySerializer Key serializer (unused, kept for API compatibility)
     * @param namespaceSerializer Namespace serializer (unused, kept for API compatibility)
     * @param stateSerializer State serializer (unused, kept for API compatibility)
     * @param l0CacheEnabled Whether L0 cache is enabled
     * @param l0Policy L0 cache replacement policy
     * @param loadFactorThreshold MainTable load factor threshold for resize
     */
    public ForL0StateMap(L0MemoryAllocator l0Allocator,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy,
                         double loadFactorThreshold) {
        // Create L0Table if enabled, then pass to MainTable
        L0Table<K, N, S> l0Table = (l0CacheEnabled && l0Allocator != null) 
            ? new L0Table<>(l0Allocator, l0CacheSizePow2, l0Policy) 
            : null;
        this.mainTable = new MainTable<>(loadFactorThreshold, l0Table);

        LOG.debug("ForL0StateMap initialized with mainTable=65536 buckets, " +
                  "l0Cache={} buckets, cache={}, policy={}, loadFactor={}",
                l0CacheEnabled ? 1 << l0CacheSizePow2 : 0, 
                l0CacheEnabled, l0Policy, loadFactorThreshold);
    }

    @Override
    public void close() throws Exception {
        LOG.info("ForL0StateMap closing - {}", mainTable.getStats());
        mainTable.close();  // MainTable.close() is idempotent
        LOG.debug("ForL0StateMap closed");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public S get(K key, N namespace) {
        // Deduplicate namespace object to improve identity comparison hit rate
        if (namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        int hash = compositeHash(key, namespace);
        HeapStateEntry<K, N, S> entry = mainTable.get(hash, key, namespace);
        return entry != null ? entry.state : null;
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        // Deduplicate namespace object to improve identity comparison hit rate
        if (namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        int hash = compositeHash(key, namespace);
        return mainTable.get(hash, key, namespace) != null;
    }

    /**
     * Helper method that is the basis for operations that add mappings.
     * Returns the entry (existing or newly created with state=null).
     * If a new entry is created, size is incremented.
     */
    private HeapStateEntry<K, N, S> putEntry(K key, N namespace) {
        int hash = compositeHash(key, namespace);
        
        // Deduplicate namespace object to improve identity comparison hit rate
        if (namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        
        HeapStateEntry<K, N, S> entry = mainTable.put(hash, key, namespace);
        if (entry.state == null) {
            // New entry (put creates Entry with state=null)
            size++;
        }
        return entry;
    }

    @Override
    public void put(K key, N namespace, S state) {
        HeapStateEntry<K, N, S> entry = putEntry(key, namespace);
        entry.state = state;
    }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        HeapStateEntry<K, N, S> entry = putEntry(key, namespace);
        S oldValue = entry.state;
        entry.state = state;
        return oldValue;
    }

    @Override
    public void remove(K key, N namespace) {
        int hash = compositeHash(key, namespace);
        HeapStateEntry<K, N, S> removed = mainTable.remove(hash, key, namespace);
        if (removed != null) {
            size--;
        }
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        int hash = compositeHash(key, namespace);
        HeapStateEntry<K, N, S> removed = mainTable.remove(hash, key, namespace);
        if (removed != null) {
            size--;
            return removed.state;
        }
        return null;
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        int[] cnt = new int[1];
        mainTable.forEachEntry((ptr, hash) -> {
            int idx = ptr - 1;
            HeapStateEntry<K, N, S> entry = mainTable.entryChunks[idx >> MainTable.ENTRY_CHUNK_BITS][idx & MainTable.ENTRY_CHUNK_MASK];
            if (entry != null) {
                N n = entry.namespace;
                if (namespace != null && namespace.equals(n)) {
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
        // Return a lazy iterator that traverses mainTable directly
        return new MainTableIterator();
    }

    /**
     * Lazy iterator over all entries in the MainTable.
     */
    private class MainTableIterator implements Iterator<StateEntry<K, N, S>> {
        private final int[] ptrs;
        private int index = 0;
        
        MainTableIterator() {
            // Collect all pointers first (just ints, minimal allocation)
            ptrs = new int[size];
            int[] count = {0};
            mainTable.forEachEntry((ptr, hash) -> {
                if (count[0] < ptrs.length) {
                    ptrs[count[0]++] = ptr;
                }
            });
        }
        
        @Override
        public boolean hasNext() {
            return index < ptrs.length;
        }
        
        @Override
        public StateEntry<K, N, S> next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            int ptr = ptrs[index++];
            int idx = ptr - 1;
            return mainTable.entryChunks[idx >> MainTable.ENTRY_CHUNK_BITS][idx & MainTable.ENTRY_CHUNK_MASK];
        }
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateMapSnapshot<>(this);
    }

    @Override
    public Stream<K> getKeys(N namespace) {
        java.util.ArrayList<K> keys = new java.util.ArrayList<>();
        mainTable.forEachEntry((ptr, hash) -> {
            int idx = ptr - 1;
            HeapStateEntry<K, N, S> entry = mainTable.entryChunks[idx >> MainTable.ENTRY_CHUNK_BITS][idx & MainTable.ENTRY_CHUNK_MASK];
            if (entry != null) {
                N n = entry.namespace;
                if (n.equals(namespace)) {
                    keys.add(entry.key);
                }
            }
        });
        return keys.stream();
    }

    @Override
    public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        HeapStateEntry<K, N, S> entry = putEntry(key, namespace);
        entry.state = transformation.apply(entry.state, value);
    }

    // ================== Statistics (for benchmark/testing) ==================

    /**
     * Gets L0 Table statistics for benchmark monitoring.
     * Returns null if L0 cache is disabled.
     */
    public L0Table.L0TableStats getL0Stats() {
        return mainTable.getL0Stats();
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
                mainTable.getL0Stats(),
                mainTable.getStats(),
                size
        );
    }

    /**
     * Detailed statistics for monitoring.
     */
    public static class DetailedStats {
        public final L0Table.L0TableStats l0Stats;
        public final MainTable.TableStats mainTableStats;
        public final int totalEntries;

        public DetailedStats(L0Table.L0TableStats l0Stats, MainTable.TableStats mainTableStats,
                             int totalEntries) {
            this.l0Stats = l0Stats;
            this.mainTableStats = mainTableStats;
            this.totalEntries = totalEntries;
        }

        @Override
        public String toString() {
            return String.format(
                    "DetailedStats{l0Stats=%s, mainTable=%s, entries=%d}",
                    l0Stats, mainTableStats, totalEntries
            );
        }
    }

    /**
     * Computes composite hash for key and namespace.
     * Uses double bitMix with XOR for better hash distribution.
     * 
     * @param key the key object
     * @param namespace the namespace object
     * @return scrambled composite hash value
     */
    private static int compositeHash(Object key, Object namespace) {
        // Apply bitMix to each hash separately, then XOR them
        // This provides better distribution than single bitMix on combined value
        return MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());
    }

}
