package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.heap.io.MemorySegmentDataInputView;
import org.apache.flink.runtime.state.heap.io.ReusableBufferDataOutputView;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // 当使用 FREE_LIST 策略时，仅回收大于该阈值的旧 entry，避免 free list 过长导致热路径变慢
    private static final int FREE_LIST_RECYCLE_MIN_SIZE = 0; // bytes, 总是回收以促进复用

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;

    // Core storage components
    private final MemoryManagerAllocator allocator;
    private final EntryArena entryArena;
    private final MainTable mainTable;
    private final L0Table l0Table;

    // Configuration
    private final boolean l0CacheEnabled;

    // Statistics
    private int size = 0;
    private long totalAccesses = 0;
    private long l0Hits = 0;
    private long mainTableHits = 0;

    // Resize coordination
    private volatile boolean resizeInProgress = false;
    private long lastResizeTime = 0;
    private static final long MIN_RESIZE_INTERVAL_MS = 0; // 立即扩容，避免更新���内存峰值

    // Serialization helpers (zero-copy)
    private final ReusableBufferDataOutputView keyOut = new ReusableBufferDataOutputView(128);
    private final ReusableBufferDataOutputView namespaceOut = new ReusableBufferDataOutputView(128);
    private final ReusableBufferDataOutputView stateOut = new ReusableBufferDataOutputView(128);
    private final MemorySegmentDataInputView segInput = new MemorySegmentDataInputView();

    public ForL0StateMap(MemoryManagerAllocator allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled) {
        this(allocator, mainTableInitPow2, l0CacheSizePow2, keySerializer, namespaceSerializer,
             stateSerializer, l0CacheEnabled, EntryArena.AllocationStrategy.FREE_LIST);
    }

    /**
     * Creates a ForL0StateMap with configurable EntryArena allocation strategy.
     */
    public ForL0StateMap(MemoryManagerAllocator allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         EntryArena.AllocationStrategy arenaAllocationStrategy) {
        this.allocator = allocator;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.stateSerializer = stateSerializer;
        this.l0CacheEnabled = l0CacheEnabled;

        try {
            // Initialize storage components with configurable allocation strategy
            this.entryArena = new EntryArena(allocator, arenaAllocationStrategy);
            this.mainTable = new MainTable(allocator, mainTableInitPow2, 0.75);
            // L0Table大小固定，不会扩容
            this.l0Table = l0CacheEnabled ? new L0Table(allocator, l0CacheSizePow2, L0Table.ReplacementPolicy.RANDOM) : null;

            LOG.debug("ForL0StateMap initialized with mainTable={} buckets (expandable), l0Cache={} buckets (fixed), cache={}, arenaStrategy={}",
                    1 << mainTableInitPow2, l0CacheEnabled ? 1 << l0CacheSizePow2 : 0, l0CacheEnabled, arenaAllocationStrategy);

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ForL0StateMap", e);
        }
    }

//    public ForL0StateMap(int initPow2,
//                         TypeSerializer<K> keySerializer,
//                         TypeSerializer<N> namespaceSerializer,
//                         TypeSerializer<S> stateSerializer) {
//        this.keySerializer = keySerializer;
//        this.namespaceSerializer = namespaceSerializer;
//        this.stateSerializer = stateSerializer;
//        this.allocator = null;
//        this.entryArena = null;
//        this.mainTable = null;
//        this.l0Table = null;
//        this.l0CacheEnabled = false;
//        // This constructor is for compatibility, but core functionality requires proper initialization
//    }

    @Override
    public void close() throws Exception {
        LOG.debug("Closing ForL0StateMap");

        if (l0Table != null) {
            l0Table.close();
        }
        if (mainTable != null) {
            mainTable.close();
        }
        if (entryArena != null) {
            entryArena.close();
        }

        LOG.debug("ForL0StateMap closed");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public S get(K key, N namespace) {
        if (key == null || namespace == null) {
            return null;
        }

        // Check if properly initialized
        if (allocator == null || entryArena == null || mainTable == null) {
            return null;
        }

        totalAccesses++;

        try {
            // Serialize key and namespace for lookup into reusable buffers
            serializeKeyToBuffer(key);
            serializeNamespaceToBuffer(namespace);
            byte[] kb = keyOut.getBuffer();
            int klen = keyOut.getLength();
            byte[] nb = namespaceOut.getBuffer();
            int nlen = namespaceOut.getLength();

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(kb, 0, klen);
            int namespaceHash = HashFunctions.murmurHash3(nb, 0, nlen);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher for exact key/namespace matching (zero-copy compare)
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, kb, klen, nb, nlen);

            long entryAddress;

            // Try L0 cache first if enabled
            if (l0CacheEnabled && l0Table != null) {
                entryAddress = l0Table.get(hash, tag, matcher::matches);
                if (entryAddress > 0) {
                    l0Hits++;
                    LOG.trace("L0 cache hit for key={}, namespace={}", key, namespace);
                    return deserializeValueFromArena(entryAddress);
                }
            }

            // L0 miss or cache disabled, try main table
            entryAddress = mainTable.get(hash, tag, matcher);
            if (entryAddress > 0) {
                mainTableHits++;
                LOG.trace("Main table hit for key={}, namespace={}", key, namespace);

                // Promote to L0 cache if enabled
                if (l0CacheEnabled && l0Table != null) {
                    long evictedAddress = l0Table.put(hash, tag, entryAddress, matcher::matches);
                    if (evictedAddress > 0) {
                        LOG.trace("L0 cache evicted entry at address {}", evictedAddress);
                    }
                }

                return deserializeValueFromArena(entryAddress);
            }

            LOG.trace("Key not found: key={}, namespace={}", key, namespace);
            return null;

        } catch (Exception e) {
            LOG.error("Error during get operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Get operation failed", e);
        }
    }

    private S deserializeValueFromArena(long entryAddress) throws IOException {
        EntryArena.Slice slice = entryArena.getValueSlice(entryAddress);
        if (slice == null || slice.length == 0) {
            return null;
        }
        segInput.reset(slice.segment, slice.offset, slice.length);
        return stateSerializer.deserialize(segInput);
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        if (key == null || namespace == null) {
            return false;
        }

        // Check if properly initialized
        if (allocator == null || entryArena == null || mainTable == null) {
            return false;
        }

        try {
            // Serialize key and namespace for lookup
            serializeKeyToBuffer(key);
            serializeNamespaceToBuffer(namespace);
            byte[] kb = keyOut.getBuffer();
            int klen = keyOut.getLength();
            byte[] nb = namespaceOut.getBuffer();
            int nlen = namespaceOut.getLength();

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(kb, 0, klen);
            int namespaceHash = HashFunctions.murmurHash3(nb, 0, nlen);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher for exact key/namespace matching
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, kb, klen, nb, nlen);

            // Try L0 cache first if enabled
            if (l0CacheEnabled && l0Table != null) {
                long entryAddress = l0Table.get(hash, tag, matcher::matches);
                if (entryAddress > 0) {
                    return true;
                }
            }

            // Try main table
            long entryAddress = mainTable.get(hash, tag, matcher);
            return entryAddress > 0;

        } catch (Exception e) {
            LOG.error("Error during containsKey operation for key={}, namespace={}", key, namespace, e);
            return false;
        }
    }

    @Override
    public void put(K key, N namespace, S state) {
        if (key == null || namespace == null) {
            // For compatibility, silently ignore null key/namespace instead of throwing exception
            return;
        }

        // Check if properly initialized
        if (allocator == null || entryArena == null || mainTable == null) {
            return;
        }

        try {
            // Check if resize is needed before insertion
            checkAndTriggerResize();

            // Serialize all components into reusable buffers
            serializeKeyToBuffer(key);
            serializeNamespaceToBuffer(namespace);
            serializeValueToBuffer(state);

            byte[] kb = keyOut.getBuffer();
            int klen = keyOut.getLength();
            byte[] nb = namespaceOut.getBuffer();
            int nlen = namespaceOut.getLength();
            byte[] vb = stateOut.getBuffer();
            int vlen = stateOut.getLength();

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(kb, 0, klen);
            int namespaceHash = HashFunctions.murmurHash3(nb, 0, nlen);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, kb, klen, nb, nlen);

            // Store new entry in arena (zero-copy from buffers)
            long newEntryAddress = entryArena.putEntry(kb, klen, nb, nlen, vb, vlen);

            // Update main table (with resize handling)
            long oldMainAddress = putWithResizeHandling(hash, tag, newEntryAddress, matcher);

            if (oldMainAddress == 0) {
                // New entry
                size++;
                LOG.trace("Inserted new entry: key={}, namespace={}, size={}", key, namespace, size);
            } else {
                // Updated existing entry：按阈值回收旧 entry，避免 free list 膨胀拖慢热路径
                if (entryArena.getAllocationStrategy() == EntryArena.AllocationStrategy.FREE_LIST) {
                    try {
                        if (oldMainAddress != newEntryAddress) {
                            int oldSize = entryArena.getEntrySize(oldMainAddress);
                            if (oldSize >= FREE_LIST_RECYCLE_MIN_SIZE) {
                                entryArena.removeEntry(oldMainAddress);
                            }
                        }
                    } catch (Exception ex) {
                        LOG.debug("Skip recycle old entry {} due to exception", oldMainAddress, ex);
                    }
                }
                LOG.trace("Updated existing entry: key={}, namespace={}", key, namespace);
            }

            // Update L0 cache if enabled (after successful main table update)
            if (l0CacheEnabled && l0Table != null && !resizeInProgress) {
                long oldL0Address = l0Table.put(hash, tag, newEntryAddress, matcher::matches);
                if (oldL0Address > 0 && oldL0Address != oldMainAddress) {
                    LOG.trace("L0 cache updated/evicted for key={}, namespace={}", key, namespace);
                }
            }

        } catch (Exception e) {
            LOG.error("Error during put operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Put operation failed", e);
        }
    }

    /**
     * Puts an entry into main table with resize handling.
     * If resize is needed and put fails, triggers resize and retries.
     */
    private long putWithResizeHandling(int hash, short tag, long entryAddress, MainTable.EntryMatcher matcher)
            throws Exception {
        try {
            return mainTable.put(hash, tag, entryAddress, matcher);
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("Table is full - resize needed")) {
                performResize();
                return mainTable.put(hash, tag, entryAddress, matcher);
            }
            throw e;
        }
    }

    /**
     * Checks if resize should be triggered and performs it if necessary.
     */
    private void checkAndTriggerResize() throws Exception {
        if (mainTable.needsResize() && !resizeInProgress) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastResizeTime >= MIN_RESIZE_INTERVAL_MS) {
                performResize();
            }
        }
    }

    /**
     * Performs the main table resize with L0 cache coordination.
     */
    private synchronized void performResize() throws Exception {
        if (resizeInProgress) {
            // Another thread is already resizing, wait for it to complete
            return;
        }

        if (!mainTable.needsResize()) {
            // Resize no longer needed
            return;
        }

        resizeInProgress = true;
        long resizeStartTime = System.currentTimeMillis();

        try {
            LOG.info("Starting ForL0StateMap resize operation");

            // Step 1: Clear L0 cache to avoid stale entries
            if (l0CacheEnabled && l0Table != null) {
                LOG.debug("Clearing L0 cache before resize");
                l0Table.clear();
            }

            // Step 2: Resize main table
            boolean resizePerformed = mainTable.tryResize(entryArena);

            if (resizePerformed) {
                if (l0CacheEnabled && l0Table != null) {
                    l0Table.clear();
                }
                long resizeEndTime = System.currentTimeMillis();
                lastResizeTime = resizeEndTime;

                LOG.info("ForL0StateMap resize completed successfully in {}ms",
                         resizeEndTime - resizeStartTime);

                // Log statistics after resize
                MainTable.TableStats stats = mainTable.getStats();
                LOG.info("Post-resize stats: {}", stats);
            }
        } catch (Exception e) {
            LOG.error("ForL0StateMap resize failed", e);
            throw new Exception("Resize operation failed", e);
        } finally {
            resizeInProgress = false;
        }
    }

    /**
     * Forces a resize operation for testing purposes.
     *
     * @throws Exception if resize fails
     */
    public void forceResize() throws Exception {
        mainTable.forceResize(entryArena);

        // Clear L0 cache after forced resize
        if (l0CacheEnabled && l0Table != null) {
            l0Table.clear();
        }

        lastResizeTime = System.currentTimeMillis();
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
            resizeInProgress,
            lastResizeTime
        );
    }

    /**
     * Detailed statistics including resize information.
     */
    public static class DetailedStats extends CacheStats {
        public final MainTable.TableStats mainTableStats;
        public final boolean resizeInProgress;
        public final long lastResizeTime;

        public DetailedStats(long totalAccesses, long l0Hits, long mainTableHits,
                           L0Table.L0TableStats l0Stats, MainTable.TableStats mainTableStats,
                           int totalEntries, boolean resizeInProgress, long lastResizeTime) {
            super(totalAccesses, l0Hits, mainTableHits, l0Stats, totalEntries);
            this.mainTableStats = mainTableStats;
            this.resizeInProgress = resizeInProgress;
            this.lastResizeTime = lastResizeTime;
        }

        @Override
        public String toString() {
            return String.format(
                "DetailedStats{%s, mainTable=%s, resizeInProgress=%s, lastResize=%dms ago}",
                super.toString(), mainTableStats, resizeInProgress,
                System.currentTimeMillis() - lastResizeTime
            );
        }
    }
    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        if (key == null || namespace == null) {
            // For compatibility, return null for null key/namespace instead of throwing exception
            return null;
        }

        try {
            // Get old value first
            S oldValue = get(key, namespace);

            // Put new value
            put(key, namespace, state);

            return oldValue;

        } catch (Exception e) {
            LOG.error("Error during putAndGetOld operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("PutAndGetOld operation failed", e);
        }
    }

    @Override
    public void remove(K key, N namespace) {
        if (key == null || namespace == null) {
            return;
        }

        // Check if properly initialized
        if (allocator == null || entryArena == null || mainTable == null) {
            return;
        }

        try {
            // Serialize key and namespace for lookup
            serializeKeyToBuffer(key);
            serializeNamespaceToBuffer(namespace);
            byte[] kb = keyOut.getBuffer();
            int klen = keyOut.getLength();
            byte[] nb = namespaceOut.getBuffer();
            int nlen = namespaceOut.getLength();

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(kb, 0, klen);
            int namespaceHash = HashFunctions.murmurHash3(nb, 0, nlen);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, kb, klen, nb, nlen);

            // Remove from main table
            long removedAddress = mainTable.remove(hash, tag, matcher);

            if (removedAddress > 0) {
                size--;
                LOG.trace("Removed entry: key={}, namespace={}, size={}", key, namespace, size);

                // Remove from L0 cache if enabled
                if (l0CacheEnabled && l0Table != null) {
                    l0Table.remove(hash, tag, matcher::matches);
                }

                // Free the removed entry block back to arena (enables reuse under FREE_LIST strategy)
                try {
                    entryArena.removeEntry(removedAddress);
                } catch (Exception ex) {
                    LOG.debug("Failed to free removed entry address {}", removedAddress, ex);
                }
            } else {
                LOG.trace("Entry not found for removal: key={}, namespace={}", key, namespace);
            }

        } catch (Exception e) {
            LOG.error("Error during remove operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Remove operation failed", e);
        }
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        if (key == null || namespace == null) {
            return null;
        }

        try {
            // Get old value first
            S oldValue = get(key, namespace);

            // Remove the entry
            remove(key, namespace);

            return oldValue;

        } catch (Exception e) {
            LOG.error("Error during removeAndGetOld operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("RemoveAndGetOld operation failed", e);
        }
    }

    // Serialization helper methods (zero-copy)
    private void serializeKeyToBuffer(K key) throws IOException {
        keyOut.clear();
        keySerializer.serialize(key, keyOut);
    }

    private void serializeNamespaceToBuffer(N namespace) throws IOException {
        namespaceOut.clear();
        namespaceSerializer.serialize(namespace, namespaceOut);
    }

    private void serializeValueToBuffer(S value) throws IOException {
        stateOut.clear();
        if (value != null) {
            stateSerializer.serialize(value, stateOut);
        }
    }

    private K deserializeKey(byte[] keyBytes) throws IOException {
        org.apache.flink.core.memory.DataInputDeserializer in = new org.apache.flink.core.memory.DataInputDeserializer(keyBytes);
        return keySerializer.deserialize(in);
    }

    private N deserializeNamespace(byte[] namespaceBytes) throws IOException {
        org.apache.flink.core.memory.DataInputDeserializer in = new org.apache.flink.core.memory.DataInputDeserializer(namespaceBytes);
        return namespaceSerializer.deserialize(in);
    }

    private S deserializeValue(byte[] valueBytes) throws IOException {
        if (valueBytes == null || valueBytes.length == 0) {
            return null;
        }
        org.apache.flink.core.memory.DataInputDeserializer in = new org.apache.flink.core.memory.DataInputDeserializer(valueBytes);
        return stateSerializer.deserialize(in);
    }

    // Additional ForL0 specific functionality

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
                "CacheStats{totalAccesses=%d, l0Hits=%d, mainTableHits=%d, " +
                "overallHitRate=%.3f, l0HitRate=%.3f, totalEntries=%d, l0Stats=%s}",
                totalAccesses, l0Hits, mainTableHits, getOverallHitRate(),
                getL0HitRate(), totalEntries, l0Stats
            );
        }
    }

    @Override
    public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
        if (key == null || namespace == null || transformation == null) {
            return;
        }
        try {
            // 读取旧值
            S previous = get(key, namespace);
            // 计算新值
            S updated = transformation.apply(previous, value);
            if (updated == null) {
                // 返回 null 表示清空该状态
                if (previous != null) {
                    remove(key, namespace);
                }
            } else {
                // 写回新值（内部包含必要的扩容与缓存更新）
                put(key, namespace, updated);
            }
        } catch (Exception e) {
            LOG.error("Error during transform operation for key={}, namespace={}", key, namespace, e);
            throw e;
        }
    }

    @Override
    public Stream<K> getKeys(N namespace) {
        if (namespace == null || entryArena == null || mainTable == null) {
            return Stream.empty();
        }
        java.util.ArrayList<K> keys = new java.util.ArrayList<>();
        try {
            mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
                try {
                    byte[] nb = entryArena.getNamespaceBytes(entryAddress);
                    if (nb == null) { return; }
                    N n = deserializeNamespace(nb);
                    if ((n == null && namespace == null) || (n != null && n.equals(namespace))) {
                        byte[] kb = entryArena.getKeyBytes(entryAddress);
                        if (kb != null) {
                            K k = deserializeKey(kb);
                            keys.add(k);
                        }
                    }
                } catch (Exception ignore) {
                }
            });
        } catch (Exception e) {
            return keys.stream();
        }
        return keys.stream();
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

            // Some Flink versions require an update method on the visitor; implement minimal in-place update via put.
            @Override
            public void update(StateEntry<K, N, S> entry, S newState) {
                if (entry == null) { return; }
                put(entry.getKey(), entry.getNamespace(), newState);
            }

            // Some Flink versions require a remove method on the visitor; implement via map remove.
            @Override
            public void remove(StateEntry<K, N, S> entry) {
                if (entry == null) { return; }
                ForL0StateMap.this.remove(entry.getKey(), entry.getNamespace());
            }

            // close() might not be part of the interface in certain Flink versions; keep as no-op without @Override for compatibility.
            public void close() {
                // no resources to release in current implementation
            }
        };
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateMapSnapshot<>(this);
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        if (entryArena == null || mainTable == null) {
            return 0;
        }
        int[] cnt = new int[1];
        try {
            mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
                try {
                    byte[] nb = entryArena.getNamespaceBytes(entryAddress);
                    if (nb == null) { return; }
                    N n = deserializeNamespace(nb);
                    if ((n == null && namespace == null) || (n != null && n.equals(namespace))) {
                        cnt[0]++;
                    }
                } catch (Exception ignore) {
                }
            });
        } catch (Exception e) {
            // fall through
        }
        return cnt[0];
    }

    private static final class SimpleStateEntry<K, N, S> implements StateEntry<K, N, S> {
        private final K key;
        private final N namespace;
        private final S state;
        SimpleStateEntry(K key, N namespace, S state) {
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

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        // 构造一次性快照列表，避免并发修改影响
        java.util.ArrayList<StateEntry<K, N, S>> list = new java.util.ArrayList<>(Math.max(16, size));
        try {
            if (entryArena == null || mainTable == null) {
                return list.iterator();
            }
            mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
                try {
                    byte[] kb = entryArena.getKeyBytes(entryAddress);
                    byte[] nb = entryArena.getNamespaceBytes(entryAddress);
                    byte[] vb = entryArena.getValueBytes(entryAddress);
                    if (kb == null || nb == null) {
                        return; // skip entries with missing key or namespace
                    }
                    K k = deserializeKey(kb);
                    N n = deserializeNamespace(nb);
                    // Handle null/empty values correctly for checkpoint compatibility
                    S v = null;
                    if (vb != null && vb.length > 0) {
                        v = deserializeValue(vb);
                    }
                    // Only include entries with non-null values in the snapshot
                    // This matches Flink's standard behavior for checkpoints
                    if (v != null) {
                        list.add(new SimpleStateEntry<>(k, n, v));
                    }
                } catch (Exception e) {
                    // Skip broken entries but log the issue
                    LOG.debug("Skipping corrupted entry during iteration", e);
                }
            });
        } catch (Exception e) {
            LOG.warn("Error during ForL0StateMap iteration, returning partial results", e);
        }
        return list.iterator();
    }

}
