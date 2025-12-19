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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.util.MathUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Heap-based state entry that directly stores object references.
 * 
 * <p>This class is similar to {@code CopyOnWriteStateMap.StateMapEntry}, but simplified
 * for use with the ForL0 off-heap index + on-heap storage architecture. Unlike the COW
 * version, this entry does not require copy-on-write metadata (entryVersion, stateVersion)
 * or a linked list pointer (next).
 * 
 * <p>Key Design Points:
 * <ul>
 *   <li><b>Zero Serialization</b>: Key, namespace, and state are stored as direct object references</li>
 *   <li><b>Hash Caching</b>: The composite hash is computed once and cached for fast lookups</li>
 *   <li><b>Tag Derivation</b>: Tag is derived from hash as {@code (short)(hash >>> 16)}, not stored separately</li>
 *   <li><b>Immutable Identity</b>: Key and namespace are final; only state can be updated</li>
 * </ul>
 * 
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public final class HeapStateEntry<K, N, S> implements StateEntry<K, N, S> {
    
    /** The key. Assumed to be immutable and not null. */
    @Nonnull
    private final K key;
    
    /** The namespace. Assumed to be immutable and not null. */
    @Nonnull
    private final N namespace;
    
    /** The state value. Can be null and can be updated in place. */
    @Nullable
    private S state;
    
    /** 
     * The cached composite hash value, computed as {@code MathUtils.bitMix(key.hashCode() ^ namespace.hashCode())}.
     * This provides good bit distribution for hash table indexing.
     */
    private final int hash;
    
    // ========== Constructors ==========
    
    /**
     * Creates a new heap state entry.
     * 
     * @param key the key (must not be null)
     * @param namespace the namespace (must not be null)
     * @param state the state value (can be null)
     */
    public HeapStateEntry(@Nonnull K key, @Nonnull N namespace, @Nullable S state) {
        this.key = key;
        this.namespace = namespace;
        this.state = state;
        // Use the same hash computation as CopyOnWriteStateMap for consistency
        this.hash = MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
    }
    
    // ========== StateEntry Interface ==========
    
    @Nonnull
    @Override
    public K getKey() {
        return key;
    }
    
    @Nonnull
    @Override
    public N getNamespace() {
        return namespace;
    }
    
    @Nullable
    @Override
    public S getState() {
        return state;
    }
    
    // ========== State Update ==========
    
    /**
     * Updates the state value in place.
     * 
     * <p>This operation does not change the entry's address in HeapEntryStore,
     * allowing index pointers to remain stable.
     * 
     * @param state the new state value (can be null)
     */
    public void setState(@Nullable S state) {
        this.state = state;
    }
    
    // ========== Hash and Tag ==========
    
    /**
     * Returns the cached composite hash value.
     * 
     * @return the hash value
     */
    public int getHash() {
        return hash;
    }
    
    /**
     * Computes and returns the tag from the hash.
     * 
     * <p>The tag is the high 16 bits of the hash, used for fast filtering
     * in the off-heap index before doing full key/namespace comparison.
     * 
     * @return the tag (high 16 bits of hash)
     */
    public short getTag() {
        return (short) (hash >>> 16);
    }
    
    // ========== Matching ==========
    
    /**
     * Checks if this entry matches the given key and namespace.
     * 
     * <p>Uses {@code Object.equals()} for comparison, which is the standard
     * Java equality check. This is much faster than comparing serialized bytes.
     * 
     * @param key the key to match (must not be null)
     * @param namespace the namespace to match (must not be null)
     * @return true if both key and namespace match
     */
    public boolean matches(@Nonnull K key, @Nonnull N namespace) {
        return this.key.equals(key) && this.namespace.equals(namespace);
    }
    
    // ========== Object Methods ==========
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StateEntry)) {
            return false;
        }
        StateEntry<?, ?, ?> that = (StateEntry<?, ?, ?>) o;
        return key.equals(that.getKey()) 
            && namespace.equals(that.getNamespace())
            && (state == null ? that.getState() == null : state.equals(that.getState()));
    }
    
    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + namespace.hashCode();
        result = 31 * result + (state != null ? state.hashCode() : 0);
        return result;
    }
    
    @Override
    public String toString() {
        return "HeapStateEntry{" +
            "key=" + key +
            ", namespace=" + namespace +
            ", state=" + state +
            ", hash=" + hash +
            '}';
    }
}
