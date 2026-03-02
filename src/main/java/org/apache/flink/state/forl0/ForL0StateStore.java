package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * A simple state store that maps KeyGroup -> SwissTable (or HashMap<Namespace, SwissTable>).
 * 
 * <p>This is a lightweight replacement for the complex StateTable hierarchy.
 * State is organized by namespace to reduce cache misses.
 * 
 * <p>Two storage modes:
 * <ul>
 *   <li>VoidNamespace mode: Direct SwissTable[] access, zero HashMap overhead</li>
 *   <li>General Namespace mode: HashMap<N, SwissTable>[] for namespace routing</li>
 * </ul>
 *
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateStore<K, N, S> implements StateSnapshotRestore {

    /** Initial capacity for each SwissTable. */
    private static final int INITIAL_TABLE_CAPACITY = 64;

    /** The key group range covered by this store. */
    private final KeyGroupRange keyGroupRange;

    /** Offset for translating key group to array index. */
    private final int keyGroupOffset;

    /** Serializer for keys. */
    private final TypeSerializer<K> keySerializer;

    /** Meta information about this state. */
    private RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo;

    /** Whether this store uses VoidNamespace mode (no HashMap layer). */
    private final boolean isVoidNamespace;

    /** VoidNamespace mode: direct SwissTable array, one per key group. */
    private final SwissTable<K, S>[] tables;

    /** General Namespace mode: HashMap per key group for namespace routing. */
    private final Map<N, SwissTable<K, S>>[] namespaceMaps;

    /**
     * Creates a new ForL0StateStore.
     *
     * @param keyGroupRange the key group range
     * @param keySerializer the key serializer
     * @param metaInfo the state meta info
     */
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
        
        // VoidNamespace specialization detection
        this.isVoidNamespace = metaInfo.getNamespaceSerializer() instanceof VoidNamespaceSerializer;
        
        if (isVoidNamespace) {
            // VoidNamespace: direct SwissTable[], no HashMap overhead
            this.tables = new SwissTable[numKeyGroups];
            this.namespaceMaps = null;
        } else {
            // General Namespace: use HashMap<N, SwissTable>[]
            this.tables = null;
            this.namespaceMaps = new HashMap[numKeyGroups];
        }
    }

    // ========== Core Operations ==========

    /**
     * Gets the value for the given key and namespace.
     *
     * @param key the key
     * @param namespace the namespace
     * @param keyGroup the key group
     * @return the state value, or null if not found
     */
    public S get(K key, N namespace, int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        SwissTable<K, S> table;
        
        if (isVoidNamespace) {
            // VoidNamespace: direct access
            table = tables[idx];
        } else {
            // General Namespace: via HashMap
            Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return null;
        }
        int hash = computeKeyHash(key);
        return table.get(hash, key);
    }

    /**
     * Puts a value for the given key and namespace.
     *
     * @param key the key
     * @param namespace the namespace
     * @param value the value
     * @param keyGroup the key group
     */
    public void put(K key, N namespace, S value, int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        SwissTable<K, S> table;
        
        if (isVoidNamespace) {
            // VoidNamespace: direct access, no HashMap overhead
            table = tables[idx];
            if (table == null) {
                table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
                tables[idx] = table;
            }
        } else {
            // General Namespace: via HashMap
            Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);  // Small initial capacity, Namespace count is usually small
                namespaceMaps[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        int hash = computeKeyHash(key);
        
        while (true) {
            int result = table.put(hash, key);
            
            if (result == SwissTable.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTable.NEED_GROW) {
                table.grow();
                continue;
            }
            
            // Normal insert or update
            int slot = result & SwissTable.SLOT_MASK;
            table.entries[(slot << 1) + 1] = value;
            return;
        }
    }

    /**
     * Transforms a value for the given key and namespace using a transformation function.
     * This is more efficient than get + transform + put as it only performs one lookup.
     *
     * @param key the key
     * @param namespace the namespace
     * @param value the input value for transformation
     * @param transformation the transformation function
     * @param keyGroup the key group
     * @param <T> the type of the input value
     */
    @SuppressWarnings("unchecked")
    public <T> void transform(
            K key, N namespace, T value, 
            StateTransformationFunction<S, T> transformation,
            int keyGroup) throws Exception {
        int idx = keyGroup - keyGroupOffset;
        SwissTable<K, S> table;
        
        if (isVoidNamespace) {
            table = tables[idx];
            if (table == null) {
                table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
                tables[idx] = table;
            }
        } else {
            Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);
                namespaceMaps[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        int hash = computeKeyHash(key);
        
        while (true) {
            int result = table.put(hash, key);
            
            if (result == SwissTable.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTable.NEED_GROW) {
                table.grow();
                continue;
            }
            
            // Get slot and apply transformation directly
            int slot = result & SwissTable.SLOT_MASK;
            int valueIdx = (slot << 1) + 1;
            S oldState = (S) table.entries[valueIdx];
            table.entries[valueIdx] = transformation.apply(oldState, value);
            return;
        }
    }

    /**
     * Removes the value for the given key and namespace.
     *
     * @param key the key
     * @param namespace the namespace
     * @param keyGroup the key group
     * @return the removed value, or null if not found
     */
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
            
            // Critical: Remove empty namespace from HashMap to avoid accumulation in windowed scenarios
            if (table.isEmpty()) {
                nsMap.remove(namespace);
            }
            
            return removed;
        }
    }

    /**
     * Checks if a key/namespace pair exists.
     */
    public boolean containsKey(K key, N namespace, int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        SwissTable<K, S> table;
        
        if (isVoidNamespace) {
            table = tables[idx];
        } else {
            Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return false;
        }
        int hash = computeKeyHash(key);
        return table.containsKey(hash, key);
    }

    // ========== Hash Computation (aligned with hash-smith SwissMap) ==========

    /**
     * Computes 32-bit hash for key using smear function.
     * Aligned with hash-smith SwissMap.
     */
    int computeKeyHash(K key) {
        int h = key.hashCode();
        // smear mixing (from hash-smith / Guava)
        return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
    }

    // ========== Iteration and Queries ==========

    /**
     * Returns a stream of all keys for the given namespace.
     */
    public Stream<K> getKeys(N namespace) {
        List<K> keys = new ArrayList<>();
        
        if (isVoidNamespace) {
            // VoidNamespace: all keys belong to the same namespace
            for (SwissTable<K, S> table : tables) {
                if (table != null) {
                    table.collectKeys(keys);
                }
            }
        } else {
            // General Namespace: only traverse the matching SwissTable
            for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
                if (nsMap != null) {
                    SwissTable<K, S> table = nsMap.get(namespace);
                    if (table != null) {
                        table.collectKeys(keys);
                    }
                }
            }
        }
        return keys.stream();
    }

    /**
     * Returns a stream of all keys and namespaces.
     */
    @SuppressWarnings("unchecked")
    public Stream<org.apache.flink.api.java.tuple.Tuple2<K, N>> getKeysAndNamespaces() {
        List<org.apache.flink.api.java.tuple.Tuple2<K, N>> result = new ArrayList<>();
        
        if (isVoidNamespace) {
            // VoidNamespace: inject fixed VoidNamespace.INSTANCE
            N voidNs = (N) VoidNamespace.INSTANCE;
            for (SwissTable<K, S> table : tables) {
                if (table != null) {
                    for (Iterator<SwissTable.Entry<K, S>> it = table.iterator(); it.hasNext(); ) {
                        result.add(org.apache.flink.api.java.tuple.Tuple2.of(it.next().getKey(), voidNs));
                    }
                }
            }
        } else {
            for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
                if (nsMap != null) {
                    for (Map.Entry<N, SwissTable<K, S>> nsEntry : nsMap.entrySet()) {
                        N namespace = nsEntry.getKey();
                        SwissTable<K, S> table = nsEntry.getValue();
                        for (Iterator<SwissTable.Entry<K, S>> it = table.iterator(); it.hasNext(); ) {
                            result.add(org.apache.flink.api.java.tuple.Tuple2.of(it.next().getKey(), namespace));
                        }
                    }
                }
            }
        }
        return result.stream();
    }

    /**
     * Returns an iterable over entries for a specific key group.
     */
    @SuppressWarnings("unchecked")
    public Iterable<StateEntry<K, N, S>> entries(int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        
        if (isVoidNamespace) {
            // VoidNamespace: directly traverse table, inject fixed VoidNamespace.INSTANCE
            SwissTable<K, S> table = tables[idx];
            if (table == null || table.isEmpty()) {
                return Collections.emptyList();
            }
            
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
            // General Namespace: traverse all SwissTables in HashMap
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

    /**
     * Gets the total number of entries across all key groups.
     */
    public int size() {
        int total = 0;
        
        if (isVoidNamespace) {
            for (SwissTable<K, S> table : tables) {
                if (table != null) {
                    total += table.size();
                }
            }
        } else {
            for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
                if (nsMap != null) {
                    for (SwissTable<K, S> table : nsMap.values()) {
                        total += table.size();
                    }
                }
            }
        }
        return total;
    }

    // ========== Getters ==========

    /**
     * Gets the entry count for a specific key group (used by snapshot writer to avoid double iteration).
     */
    public int getEntryCount(int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        
        if (isVoidNamespace) {
            SwissTable<K, S> table = tables[idx];
            return (table == null) ? 0 : table.size();
        } else {
            Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
            if (nsMap == null) {
                return 0;
            }
            int count = 0;
            for (SwissTable<K, S> table : nsMap.values()) {
                count += table.size();
            }
            return count;
        }
    }

    /**
     * Functional interface for zero-allocation snapshot traversal.
     * Receives namespace, key, state for each entry without any object allocation.
     */
    @FunctionalInterface
    public interface StateEntryConsumer<K, N, S> {
        void accept(K key, N namespace, S state) throws IOException;
    }

    /**
     * Iterates over all entries in a key group without allocating iterator/entry objects.
     * This is the zero-allocation path used by the snapshot writer.
     *
     * @param keyGroup the key group
     * @param consumer callback for each (key, namespace, state) triple
     */
    @SuppressWarnings("unchecked")
    public void forEachInKeyGroup(int keyGroup, StateEntryConsumer<K, N, S> consumer) throws IOException {
        int idx = keyGroup - keyGroupOffset;
        
        if (isVoidNamespace) {
            SwissTable<K, S> table = tables[idx];
            if (table == null || table.isEmpty()) {
                return;
            }
            N voidNs = (N) VoidNamespace.INSTANCE;
            try {
                table.<IOException>forEachEntry((key, value) -> consumer.accept(key, voidNs, value));
            } catch (Exception e) {
                throwAsIOException(e);
            }
        } else {
            Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
            if (nsMap == null || nsMap.isEmpty()) {
                return;
            }
            for (Map.Entry<N, SwissTable<K, S>> nsEntry : nsMap.entrySet()) {
                N namespace = nsEntry.getKey();
                SwissTable<K, S> table = nsEntry.getValue();
                try {
                    table.<IOException>forEachEntry((key, value) -> consumer.accept(key, namespace, value));
                } catch (Exception e) {
                    throwAsIOException(e);
                }
            }
        }
    }

    private static void throwAsIOException(Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        throw new IOException(e);
    }

    public String getStateName() {
        return metaInfo.getName();
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return metaInfo.getNamespaceSerializer();
    }

    public TypeSerializer<S> getStateSerializer() {
        return metaInfo.getStateSerializer();
    }

    public RegisteredKeyValueStateBackendMetaInfo<N, S> getMetaInfo() {
        return metaInfo;
    }

    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        this.metaInfo = metaInfo;
    }

    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    // ========== StateSnapshotRestore Implementation ==========

    @Override
    @Nonnull
    public StateSnapshot stateSnapshot() {
        return new ForL0StateStoreSnapshot<>(this);
    }

    @Override
    @Nonnull
    public StateSnapshotKeyGroupReader keyGroupReader(int readVersionHint) {
        return ForL0StateStoreKeyGroupReader.create(this);
    }

    /**
     * Gets the count of entries for a specific namespace.
     */
    @SuppressWarnings("unchecked")
    public int sizeOfNamespace(Object namespace) {
        if (isVoidNamespace) {
            // VoidNamespace: return total size
            return size();
        }
        
        // General Namespace: O(keyGroups) complexity
        int count = 0;
        for (Map<N, SwissTable<K, S>> nsMap : namespaceMaps) {
            if (nsMap != null) {
                SwissTable<K, S> table = nsMap.get((N) namespace);
                if (table != null) {
                    count += table.size();
                }
            }
        }
        return count;
    }

    // ========== Internal Helpers ==========

    /**
     * Checks if this store uses VoidNamespace mode.
     */
    public boolean isVoidNamespace() {
        return isVoidNamespace;
    }

    /**
     * Gets the direct table for a key group (VoidNamespace mode only).
     */
    SwissTable<K, S> getTableDirect(int keyGroup) {
        return tables[keyGroup - keyGroupOffset];
    }

    /**
     * Gets the namespace map for a key group (General Namespace mode only).
     */
    Map<N, SwissTable<K, S>> getNamespaceMap(int keyGroup) {
        return namespaceMaps[keyGroup - keyGroupOffset];
    }

    /**
     * Gets or creates a direct table for a key group (VoidNamespace mode only).
     */
    SwissTable<K, S> getOrCreateTableDirect(int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        SwissTable<K, S> table = tables[idx];
        if (table == null) {
            table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
            tables[idx] = table;
        }
        return table;
    }

    /**
     * Gets or creates a table for a key group and namespace (General Namespace mode only).
     */
    SwissTable<K, S> getOrCreateTable(int keyGroup, N namespace) {
        int idx = keyGroup - keyGroupOffset;
        Map<N, SwissTable<K, S>> nsMap = namespaceMaps[idx];
        if (nsMap == null) {
            nsMap = new HashMap<>(8);
            namespaceMaps[idx] = nsMap;
        }
        SwissTable<K, S> table = nsMap.get(namespace);
        if (table == null) {
            table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
            nsMap.put(namespace, table);
        }
        return table;
    }

    /**
     * Simple StateEntry implementation.
     */
    private static class SimpleStateEntry<K, N, S> implements StateEntry<K, N, S> {
        private final K key;
        private final N namespace;
        private final S state;

        SimpleStateEntry(K key, N namespace, S state) {
            this.key = key;
            this.namespace = namespace;
            this.state = state;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public N getNamespace() {
            return namespace;
        }

        @Override
        public S getState() {
            return state;
        }
    }
}
