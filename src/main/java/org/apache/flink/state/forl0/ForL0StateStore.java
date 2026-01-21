/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateTransformationFunction;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A simple state store that maps KeyGroup -> SwissTable.
 * 
 * <p>This is a lightweight replacement for the complex StateTable hierarchy.
 * Each KeyGroup has one SwissTable for storing key/namespace/value triplets.
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

    /** Array of SwissTables, one per key group. */
    private final SwissTable<K, N, S>[] tables;

    /**
     * The last namespace that was actually used. This is a small optimization to reduce
     * duplicate namespace objects, aligned with CopyOnWriteStateMap's lastNamespace optimization.
     */
    private N lastNamespace;

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
        this.tables = new SwissTable[numKeyGroups];
        
        // Lazily initialize tables
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
        SwissTable<K, N, S> table = getTable(keyGroup);
        if (table == null) {
            return null;
        }
        long hash = computeHash(key, namespace);
        return table.get(hash, key, namespace);
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
        SwissTable<K, N, S> table = getOrCreateTable(keyGroup);
        // Deduplicate namespace to reduce memory usage
        namespace = deduplicateNamespace(namespace);
        long hash = computeHash(key, namespace);
        
        while (true) {
            int result = table.put(hash, key, namespace);
            
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
            table.values[slot] = value;
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
        SwissTable<K, N, S> table = getOrCreateTable(keyGroup);
        // Deduplicate namespace to reduce memory usage
        namespace = deduplicateNamespace(namespace);
        long hash = computeHash(key, namespace);
        
        while (true) {
            int result = table.put(hash, key, namespace);
            
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
            S oldState = (S) table.values[slot];
            table.values[slot] = transformation.apply(oldState, value);
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
        SwissTable<K, N, S> table = getTable(keyGroup);
        if (table == null) {
            return null;
        }
        long hash = computeHash(key, namespace);
        return table.remove(hash, key, namespace);
    }

    /**
     * Checks if a key/namespace pair exists.
     */
    public boolean containsKey(K key, N namespace, int keyGroup) {
        SwissTable<K, N, S> table = getTable(keyGroup);
        if (table == null) {
            return false;
        }
        long hash = computeHash(key, namespace);
        return table.containsKey(hash, key, namespace);
    }

    // ========== Table Management ==========

    private SwissTable<K, N, S> getTable(int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        return tables[idx];
    }

    private SwissTable<K, N, S> getOrCreateTable(int keyGroup) {
        int idx = keyGroup - keyGroupOffset;
        SwissTable<K, N, S> table = tables[idx];
        if (table == null) {
            table = new SwissTable<>(INITIAL_TABLE_CAPACITY);
            tables[idx] = table;
        }
        return table;
    }

    // ========== Hash Computation ==========

    /**
     * Deduplicates namespace objects to reduce memory usage.
     * Aligned with CopyOnWriteStateMap's lastNamespace optimization.
     */
    private N deduplicateNamespace(N namespace) {
        if (namespace.equals(lastNamespace)) {
            return lastNamespace;
        }
        lastNamespace = namespace;
        return namespace;
    }

    private long computeHash(K key, N namespace) {
        // Use a combination of key and namespace hash
        int keyHash = key.hashCode();
        int nsHash = namespace.hashCode();
        // Mix the hashes - using MurmurHash3 finalizer style mixing
        long h = ((long) keyHash << 32) | (nsHash & 0xFFFFFFFFL);
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    // ========== Iteration and Queries ==========

    /**
     * Returns a stream of all keys for the given namespace.
     */
    public Stream<K> getKeys(N namespace) {
        List<K> keys = new ArrayList<>();
        for (SwissTable<K, N, S> table : tables) {
            if (table != null) {
                for (Iterator<SwissTable.Entry<K, N, S>> it = table.iterator(); it.hasNext(); ) {
                    SwissTable.Entry<K, N, S> entry = it.next();
                    if (Objects.equals(namespace, entry.getNamespace())) {
                        keys.add(entry.getKey());
                    }
                }
            }
        }
        return keys.stream();
    }

    /**
     * Returns a stream of all keys and namespaces.
     */
    public Stream<org.apache.flink.api.java.tuple.Tuple2<K, N>> getKeysAndNamespaces() {
        List<org.apache.flink.api.java.tuple.Tuple2<K, N>> result = new ArrayList<>();
        for (SwissTable<K, N, S> table : tables) {
            if (table != null) {
                for (Iterator<SwissTable.Entry<K, N, S>> it = table.iterator(); it.hasNext(); ) {
                    SwissTable.Entry<K, N, S> entry = it.next();
                    result.add(org.apache.flink.api.java.tuple.Tuple2.of(entry.getKey(), entry.getNamespace()));
                }
            }
        }
        return result.stream();
    }

    /**
     * Returns an iterable over entries for a specific key group.
     */
    public Iterable<StateEntry<K, N, S>> entries(int keyGroup) {
        SwissTable<K, N, S> table = getTable(keyGroup);
        if (table == null) {
            return java.util.Collections.emptyList();
        }
        
        return () -> new Iterator<StateEntry<K, N, S>>() {
            private final Iterator<SwissTable.Entry<K, N, S>> inner = table.iterator();
            
            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }
            
            @Override
            public StateEntry<K, N, S> next() {
                SwissTable.Entry<K, N, S> e = inner.next();
                return new SimpleStateEntry<>(e.getKey(), e.getNamespace(), e.getState());
            }
        };
    }

    /**
     * Gets the total number of entries across all key groups.
     */
    public int size() {
        int total = 0;
        for (SwissTable<K, N, S> table : tables) {
            if (table != null) {
                total += table.size();
            }
        }
        return total;
    }

    // ========== Getters ==========

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
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (SwissTable<K, N, S> table : tables) {
            if (table != null) {
                for (Iterator<SwissTable.Entry<K, N, S>> it = table.iterator(); it.hasNext(); ) {
                    SwissTable.Entry<K, N, S> entry = it.next();
                    if (Objects.equals(namespace, entry.getNamespace())) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    // ========== Internal Helpers ==========

    /**
     * Gets the raw table array for snapshot purposes.
     */
    SwissTable<K, N, S>[] getTables() {
        return tables;
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
