package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
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

    // Serialization helpers
    private final DataOutputSerializer keyOutputSerializer = new DataOutputSerializer(128);
    private final DataOutputSerializer namespaceOutputSerializer = new DataOutputSerializer(128);
    private final DataOutputSerializer stateOutputSerializer = new DataOutputSerializer(128);
    private final DataInputDeserializer keyInputDeserializer = new DataInputDeserializer();
    private final DataInputDeserializer namespaceInputDeserializer = new DataInputDeserializer();
    private final DataInputDeserializer stateInputDeserializer = new DataInputDeserializer();

    public ForL0StateMap(MemoryManagerAllocator allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled) {
        this.allocator = allocator;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.stateSerializer = stateSerializer;
        this.l0CacheEnabled = l0CacheEnabled;

        try {
            // Initialize storage components
            this.entryArena = new EntryArena(allocator);
            // 使用更高的负载因子阈值以支持压力测试
            this.mainTable = new MainTable(allocator, mainTableInitPow2, 0.95);
            // L0Table大小固定，不会扩容
            this.l0Table = l0CacheEnabled ? new L0Table(allocator, l0CacheSizePow2, L0Table.ReplacementPolicy.LRU) : null;

            LOG.debug("ForL0StateMap initialized with mainTable={} buckets (expandable), l0Cache={} buckets (fixed), cache={}",
                    1 << mainTableInitPow2, l0CacheEnabled ? 1 << l0CacheSizePow2 : 0, l0CacheEnabled);

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
            // Serialize key and namespace for lookup
            byte[] keyBytes = serializeKey(key);
            byte[] namespaceBytes = serializeNamespace(namespace);

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(keyBytes);
            int namespaceHash = HashFunctions.murmurHash3(namespaceBytes);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher for exact key/namespace matching
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, keyBytes, namespaceBytes);

            long entryAddress;

            // Try L0 cache first if enabled
            if (l0CacheEnabled && l0Table != null) {
                entryAddress = l0Table.get(hash, tag, matcher::matches);
                if (entryAddress > 0) {
                    l0Hits++;
                    LOG.trace("L0 cache hit for key={}, namespace={}", key, namespace);
                    return deserializeValue(entryArena.getValueBytes(entryAddress));
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

                return deserializeValue(entryArena.getValueBytes(entryAddress));
            }

            LOG.trace("Key not found: key={}, namespace={}", key, namespace);
            return null;

        } catch (Exception e) {
            LOG.error("Error during get operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Get operation failed", e);
        }
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
            byte[] keyBytes = serializeKey(key);
            byte[] namespaceBytes = serializeNamespace(namespace);

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(keyBytes);
            int namespaceHash = HashFunctions.murmurHash3(namespaceBytes);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher for exact key/namespace matching
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, keyBytes, namespaceBytes);

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
            // Serialize all components
            byte[] keyBytes = serializeKey(key);
            byte[] namespaceBytes = serializeNamespace(namespace);
            byte[] valueBytes = state != null ? serializeValue(state) : new byte[0];

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(keyBytes);
            int namespaceHash = HashFunctions.murmurHash3(namespaceBytes);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, keyBytes, namespaceBytes);

            // Store new entry in arena
            long newEntryAddress = entryArena.putEntry(keyBytes, namespaceBytes, valueBytes);

            // Update main table
            long oldMainAddress = mainTable.put(hash, tag, newEntryAddress, matcher);

            if (oldMainAddress == 0) {
                // New entry
                size++;
                LOG.trace("Inserted new entry: key={}, namespace={}, size={}", key, namespace, size);
            } else {
                // Updated existing entry
                LOG.trace("Updated existing entry: key={}, namespace={}", key, namespace);
            }

            // Update L0 cache if enabled
            if (l0CacheEnabled && l0Table != null) {
                long oldL0Address = l0Table.put(hash, tag, newEntryAddress, matcher::matches);
                if (oldL0Address > 0 && oldL0Address != oldMainAddress) {
                    LOG.trace("L0 cache updated for key={}, namespace={}", key, namespace);
                }
            }

        } catch (Exception e) {
            LOG.error("Error during put operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Put operation failed", e);
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
            byte[] keyBytes = serializeKey(key);
            byte[] namespaceBytes = serializeNamespace(namespace);

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(keyBytes);
            int namespaceHash = HashFunctions.murmurHash3(namespaceBytes);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, keyBytes, namespaceBytes);

            // Remove from main table
            long removedAddress = mainTable.remove(hash, tag, matcher);

            if (removedAddress > 0) {
                size--;
                LOG.trace("Removed entry: key={}, namespace={}, size={}", key, namespace, size);

                // Remove from L0 cache if enabled
                if (l0CacheEnabled && l0Table != null) {
                    l0Table.remove(hash, tag, matcher::matches);
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

    // Serialization helper methods
    private byte[] serializeKey(K key) throws IOException {
        keyOutputSerializer.clear();
        keySerializer.serialize(key, keyOutputSerializer);
        return keyOutputSerializer.getCopyOfBuffer();
    }

    private byte[] serializeNamespace(N namespace) throws IOException {
        namespaceOutputSerializer.clear();
        namespaceSerializer.serialize(namespace, namespaceOutputSerializer);
        return namespaceOutputSerializer.getCopyOfBuffer();
    }

    private byte[] serializeValue(S value) throws IOException {
        stateOutputSerializer.clear();
        stateSerializer.serialize(value, stateOutputSerializer);
        return stateOutputSerializer.getCopyOfBuffer();
    }

    private K deserializeKey(byte[] keyBytes) throws IOException {
        keyInputDeserializer.setBuffer(keyBytes);
        return keySerializer.deserialize(keyInputDeserializer);
    }

    private N deserializeNamespace(byte[] namespaceBytes) throws IOException {
        namespaceInputDeserializer.setBuffer(namespaceBytes);
        return namespaceSerializer.deserialize(namespaceInputDeserializer);
    }

    private S deserializeValue(byte[] valueBytes) throws IOException {
        if (valueBytes == null || valueBytes.length == 0) {
            return null;
        }
        stateInputDeserializer.setBuffer(valueBytes);
        return stateSerializer.deserialize(stateInputDeserializer);
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
        // TODO: Implement transform functionality in later phase
    }

    @Override
    public Stream<K> getKeys(N namespace) {
        // TODO: Implement getKeys functionality in later phase
        return Stream.empty();
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        // TODO: Implement visitor functionality in later phase
        return null;
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        // TODO: Implement snapshot functionality in later phase
        return new ForL0StateMapSnapshot<>(this);
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        // TODO: Implement namespace size calculation in later phase
        return 0;
    }

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        // TODO: Implement iterator functionality in later phase
        return null;
    }

}
