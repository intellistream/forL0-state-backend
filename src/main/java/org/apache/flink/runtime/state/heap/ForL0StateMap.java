package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // Core storage component (MainTable now manages entry storage internally)
    private final MainTable<K, N, S> mainTable;

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
        return mainTable.size();
    }

    @Override
    public S get(K key, N namespace) {
        return mainTable.get(key, namespace);
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        return get(key, namespace) != null;
    }

    @Override
    public void put(K key, N namespace, S state) {
        // Deduplicate namespace object to reduce memory and improve identity comparison
        if (namespace == lastNamespace || namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        
        int ptr = mainTable.put(key, namespace);
        mainTable.states[ptr - 1] = state;
    }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        // Deduplicate namespace object to reduce memory and improve identity comparison
        if (namespace == lastNamespace || namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        
        int ptr = mainTable.put(key, namespace);
        S oldValue = (S) mainTable.states[ptr - 1];
        mainTable.states[ptr - 1] = state;
        return oldValue;
    }

    @Override
    public void remove(K key, N namespace) {
        mainTable.remove(key, namespace);
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        return mainTable.remove(key, namespace);
    }

    @Override
    public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        // Deduplicate namespace object to reduce memory and improve identity comparison
        if (namespace == lastNamespace || namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        
        int ptr = mainTable.put(key, namespace);
        S oldState = (S) mainTable.states[ptr - 1];
        mainTable.states[ptr - 1] = transformation.apply(oldState, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int sizeOfNamespace(Object namespace) {
        int[] cnt = new int[1];
        mainTable.forEachEntry((ptr, hash) -> {
            int base = (ptr - 1) * 2;
            N n = (N) mainTable.keyNs[base + 1];  // namespace
            if (n != null && namespace != null && namespace.equals(n)) {
                cnt[0]++;
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
                    // Copy current entry data into lightweight POJO
                    // (iterator returns reusable view, must copy here)
                    StateEntry<K, N, S> entry = iter.next();
                    batch.add(new StateEntryImpl<>(entry.getKey(), entry.getNamespace(), entry.getState()));
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
     * Lightweight StateEntry implementation for batch operations.
     * Used by StateIncrementalVisitor to avoid anonymous class overhead.
     */
    private static class StateEntryImpl<K, N, S> implements StateEntry<K, N, S> {
        private final K key;
        private final N namespace;
        private final S state;

        StateEntryImpl(K key, N namespace, S state) {
            this.key = key;
            this.namespace = namespace;
            this.state = state;
        }

        @Override
        public K getKey() { return key; }

        @Override
        public N getNamespace() { return namespace; }

        @Override
        public S getState() { return state; }
    }

    /**
     * Cursor-based iterator over all entries in the MainTable (zero pre-allocation).
     * Uses simple state machine: scan main buckets sequentially, then their extensions.
     */
    private class MainTableIterator implements Iterator<StateEntry<K, N, S>> {
        private int bucketIdx = 0;        // Current main bucket index
        private int slotIdx = 0;          // Current slot in bucket (0-6 for data)
        private int extPath = -1;         // Current extension path (-1 = in main bucket)
        private int extBucket = 0;        // Current extension bucket in chain
        private long extSlot = 0;         // Cached extension slot for current bucket
        
        private int nextPtr = 0;
        private boolean hasNextCached = false;
        private final ReusableEntry reusableEntry = new ReusableEntry();
        
        @Override
        public boolean hasNext() {
            if (hasNextCached) return true;
            nextPtr = findNext();
            hasNextCached = (nextPtr > 0);
            return hasNextCached;
        }
        
        @Override
        public StateEntry<K, N, S> next() {
            if (!hasNext()) throw new java.util.NoSuchElementException();
            hasNextCached = false;
            reusableEntry.pointTo(nextPtr);
            return reusableEntry;
        }
        
        /**
         * Reusable entry view. WARNING: reused across next() calls.
         * Caches index calculations to avoid repeated computation.
         */
        private class ReusableEntry implements StateEntry<K, N, S> {
            private int stateIndex;
            private int base;
            
            void pointTo(int ptr) {
                this.stateIndex = ptr - 1;
                this.base = stateIndex << 1;  // (ptr - 1) * 2
            }
            
            @Override @SuppressWarnings("unchecked")
            public K getKey() { return (K) mainTable.keyNs[base]; }
            
            @Override @SuppressWarnings("unchecked")
            public N getNamespace() { return (N) mainTable.keyNs[base + 1]; }
            
            @Override @SuppressWarnings("unchecked")
            public S getState() { return (S) mainTable.states[stateIndex]; }
        }
        
        /**
         * Finds next non-zero ptr by advancing through buckets and extensions.
         */
        private int findNext() {
            while (bucketIdx < mainTable.bucketCount) {
                // Try main bucket first
                if (extPath < 0) {
                    int ptr = scanBucket(mainTable.table, bucketIdx << 3);
                    if (ptr > 0) return ptr;
                    
                    // Read and cache extension slot for this bucket
                    extSlot = mainTable.table[(bucketIdx << 3) + 7];
                    if (extSlot == 0) {
                        // All 8 paths empty, skip to next main bucket
                        bucketIdx++;
                        slotIdx = 0;
                        continue;
                    }
                    
                    // Find first non-zero path
                    extPath = 0;
                    while (extPath < 8 && ((extSlot >>> (extPath << 3)) & 0xFF) == 0) {
                        extPath++;
                    }
                    extBucket = 0;
                    slotIdx = 0;
                }
                
                // Scan current path
                if (extPath < 8) {
                    int ptr = scanExtensionPath();
                    if (ptr > 0) return ptr;
                    
                    // Find next non-zero path
                    extPath++;
                    while (extPath < 8 && ((extSlot >>> (extPath << 3)) & 0xFF) == 0) {
                        extPath++;
                    }
                    extBucket = 0;
                    slotIdx = 0;
                    continue;
                }
                
                // Next main bucket
                bucketIdx++;
                extPath = -1;
                slotIdx = 0;
            }
            return 0;
        }
        
        /**
         * Scans one bucket (main or extension) starting from current slotIdx.
         */
        private int scanBucket(long[] table, int offset) {
            while (slotIdx < 7) {
                long slot = table[offset + slotIdx++];
                if (slot == 0) { slotIdx = 7; break; }  // Early exit
                return (int) slot;  // Found non-zero slot
            }
            return 0;
        }
        
        /**
         * Scans current extension path (follows chain of buckets).
         * Assumes extSlot is already cached and extPath points to a non-zero path.
         */
        private int scanExtensionPath() {
            // Get first/next bucket in this path
            if (extBucket == 0) {
                // Use cached extSlot instead of re-reading from table
                int offset = (int)((extSlot >>> (extPath << 3)) & 0xFF);
                if (offset == 0) return 0;  // Should not happen (findNext ensures non-zero)
                extBucket = mainTable.extensionBucketBaseIndices[bucketIdx] + offset - 1;
            }
            
            // Scan extension buckets in chain
            while (extBucket >= mainTable.bucketCount) {
                int ptr = scanBucket(mainTable.extensions, (extBucket - mainTable.bucketCount) << 3);
                if (ptr > 0) return ptr;
                
                // Follow chain
                long extPtr = mainTable.extensions[(extBucket - mainTable.bucketCount) << 3 | 7];
                int offset = (int)((extPtr >>> (extPath << 3)) & 0xFF);
                if (offset == 0) break;
                extBucket = mainTable.extensionBucketBaseIndices[bucketIdx] + offset - 1;
                slotIdx = 0;
            }
            return 0;
        }
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateMapSnapshot<>(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<K> getKeys(N namespace) {
        java.util.ArrayList<K> keys = new java.util.ArrayList<>();
        mainTable.forEachEntry((ptr, hash) -> {
            int base = (ptr - 1) * 2;
            K k = (K) mainTable.keyNs[base];        // key
            N n = (N) mainTable.keyNs[base + 1];     // namespace
            if (n != null && n.equals(namespace)) {
                keys.add(k);
            }
        });
        return keys.stream();
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
                mainTable.size()
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

}
