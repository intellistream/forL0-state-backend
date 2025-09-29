package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.heap.io.SerializerPack;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
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
    private final MemoryManagerAllocator allocator;
    private final EntryArena entryArena;
    // 直接管理表实例，移除 TableCore 中间层
    private final MainTable mainTable;
    private final L0Table l0Table; // nullable，仅在启用L0缓存时创建

    // 统一序列化打包器
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
    // 移除时间节流，达条件即扩容

    // 复用反序列化对象，降低分配/反射开销
    // 在 Flink 的单线程处理模型中，直接使用成员变量即可，无需 ThreadLocal
    private S reuseState;

    // Serialization cache for key and namespace to avoid repeated serialization
    private K lastKey;
    private N lastNamespace;
    private KeyNamespaceHash lastKeyNamespaceHash;

    public ForL0StateMap(MemoryManagerAllocator allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled) {
        // 委托到带策略参数的构造函数，默认使用 RANDOM 策略
        this(
            allocator,
            mainTableInitPow2,
            l0CacheSizePow2,
            keySerializer,
            namespaceSerializer,
            stateSerializer,
            l0CacheEnabled,
            L0Table.ReplacementPolicy.RANDOM
        );
    }

    // 允许注入 L0 替换策略的构造函数
    public ForL0StateMap(MemoryManagerAllocator allocator,
                         int mainTableInitPow2,
                         int l0CacheSizePow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         boolean l0CacheEnabled,
                         L0Table.ReplacementPolicy l0Policy) {
        this.allocator = allocator;
        // 直接使用 SerializerPack，移除冗余的序列化器引用
        this.serializerPack = new SerializerPack<>(keySerializer, namespaceSerializer, stateSerializer);
        this.l0CacheEnabled = l0CacheEnabled;
        this.entryArena = new EntryArena(allocator);
        // 分别创建 MainTable 和 L0Table，移除 TableCore 中间层
        this.mainTable = new MainTable(allocator, mainTableInitPow2, 1.5); // 负载因子阈值
        this.l0Table = l0CacheEnabled ? new L0Table(allocator, l0CacheSizePow2, l0Policy) : null;

        LOG.debug("ForL0StateMap initialized with mainTable={} buckets (expandable), l0Cache={} buckets (fixed), cache={}",
                1 << mainTableInitPow2, l0CacheEnabled ? 1 << l0CacheSizePow2 : 0, l0CacheEnabled);
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
        if (key == null || namespace == null) { return null; }
        totalAccesses++;

        try {
            KeyNamespaceHash knh = serializeKeyNamespace(key, namespace);

            long addr;
            if (l0CacheEnabled && l0Table != null) {
                addr = l0Table.get(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, entryArena);
                if (addr > 0) { l0Hits++; return deserializeValueFromArena(addr); }
            }
            addr = mainTable.get(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, entryArena);
            if (addr > 0) {
                mainTableHits++;
                updateL0Table(knh, addr); // promote to L0 cache
                return deserializeValueFromArena(addr);
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
                        knh.namespaceBytes, knh.namespaceLength, entryArena) > 0)
                    return true;
            }

            return mainTable.get(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, entryArena) > 0;
        } catch (IOException e) {
            LOG.error("Error during containsKey operation for key={}, namespace={}", key, namespace, e);
            return false;
        }
    }

    @Override
    public void put(K key, N namespace, S state) {
        KeyNamespaceHash knh;
        try{
            knh = serializeKeyNamespace(key, namespace);
            serializerPack.writeState(state);
        } catch (IOException e) {
            LOG.error("Serialization error during put operation for key={}, namespace={}", key, namespace, e);
            throw new RuntimeException("Put operation failed due to serialization error", e);
        }

        byte[] vb = serializerPack.stateBuffer();
        int vlen = serializerPack.stateLength();

        long result = putEntry(knh);

        if (result == 0) { // new entry
            long addr = entryArena.putEntry(knh.hash, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, vb, vlen);
            mainTable.setSlotPointer(addr);
            size++;
            updateL0Table(knh, addr);
        } else { // existing entry
            updateExistingEntry(result, vb, vlen, knh);
        }
    }

    private void updateExistingEntry(long result, byte[] vb, int vlen, KeyNamespaceHash knh) {
        // 先尝试就地更新，避免指针切换
        if (entryArena.updateValueInPlace(result, vb, vlen)) {
            return;
        }
        // 分配新entry，先切换指针与L0，再释放旧entry，避免L0访问悬垂指针
        long newAddr = entryArena.putEntry(knh.hash, knh.keyBytes, knh.keyLength,
                knh.namespaceBytes, knh.namespaceLength, vb, vlen);
        if (newAddr != 0) {
            mainTable.setSlotPointer(newAddr);
            updateL0Table(knh, newAddr);
            entryArena.removeEntry(result);
        } else {
            // 回退：无法分配新块，尝试使用旧逻辑（可能触发重分配+立即释放，但这是降级路径）
            long addr = entryArena.updateEntry(result, vb, vlen);
            if (addr != result) {
                mainTable.setSlotPointer(addr);
                updateL0Table(knh, addr);
            }
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
        if (allocator == null || entryArena == null || mainTable == null) { return; }

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
        if (entryArena == null || mainTable == null) {
            return 0;
        }
        int[] cnt = new int[1];
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            try {
                byte[] nb = entryArena.getNamespaceBytes(entryAddress);
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
                S v = null;
                if (vb != null && vb.length > 0) {
                    v = deserializeValue(vb);
                }
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
        if (namespace == null || entryArena == null || mainTable == null) {
            return Stream.empty();
        }
        java.util.ArrayList<K> keys = new java.util.ArrayList<>();
        mainTable.forEachEntry((entryAddress, keyHash, tag) -> {
            try {
                byte[] nb = entryArena.getNamespaceBytes(entryAddress);
                if (nb == null) { return; }
                N n = deserializeNamespace(nb);
                if (n != null && n.equals(namespace)) {
                    byte[] kb = entryArena.getKeyBytes(entryAddress);
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

        if (result == 0) { // new entry
            S newState = transformation.apply(null, value);
            serializerPack.writeState(newState);
            byte[] vb = serializerPack.stateBuffer();
            int vlen = serializerPack.stateLength();
            long addr = entryArena.putEntry(knh.hash, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, vb, vlen);
            mainTable.setSlotPointer(addr);
            size++;
            updateL0Table(knh, addr);
        } else { // existing entry
            S state = transformation.apply(deserializeValueFromArena(result), value);
            serializerPack.writeState(state);
            byte[] vb = serializerPack.stateBuffer();
            int vlen = serializerPack.stateLength();
            // 先尝试就地更新
            updateExistingEntry(result, vb, vlen, knh);
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
                knh.namespaceBytes, knh.namespaceLength, entryArena);
        // TODO: 这时扩展桶池不应该满，需要改进扩容逻辑
        if (result == -1) {
            // 扩展桶池满，强制扩容并重试
            performResize();
            result = mainTable.put(knh.hash, knh.tag, 0, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, entryArena);
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
                knh.namespaceBytes, knh.namespaceLength, entryArena);
        if (removedAddr > 0) {
            size--;
            if (l0CacheEnabled && l0Table != null) {
                l0Table.remove(knh.hash, knh.tag, knh.keyBytes, knh.keyLength,
                        knh.namespaceBytes, knh.namespaceLength, entryArena);
            }
            entryArena.removeEntry(removedAddr);
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
        mainTable.tryResize(entryArena);
        resizeInProgress = false;
    }

    private void updateL0Table(KeyNamespaceHash knh, long entryAddress) {
        if (l0CacheEnabled && l0Table != null && !resizeInProgress) {
            l0Table.put(knh.hash, knh.tag, entryAddress, knh.keyBytes, knh.keyLength,
                    knh.namespaceBytes, knh.namespaceLength, entryArena);
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
        MemorySegmentSlice slice = entryArena.getValueSlice(entryAddress);
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
        // Check cache first
        if (key == lastKey && namespace == lastNamespace && lastKeyNamespaceHash != null) {
            return lastKeyNamespaceHash;
        }

        serializerPack.writeKey(key);
        serializerPack.writeNamespace(namespace);
        byte[] kb = serializerPack.keyBuffer();
        int klen = serializerPack.keyLength();
        byte[] nb = serializerPack.namespaceBuffer();
        int nlen = serializerPack.namespaceLength();
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
