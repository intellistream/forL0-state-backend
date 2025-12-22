package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;

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
    final K key;
    
    /** The namespace. Assumed to be immutable and not null. */
    @Nonnull
    final N namespace;
    
    /** The state value. Can be null and can be updated in place. */
    @Nullable
    S state;
    
    /** 
     * The cached composite hash value, computed as {@code MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode())}.
     * This provides good bit distribution for hash table indexing by mixing each hashCode separately before XOR.
     */
    final int hash;
    
    // ========== Constructors ==========
    
    /**
     * Creates a new heap state entry with pre-computed hash.
     * 
     * @param key the key (must not be null)
     * @param namespace the namespace (must not be null)
     * @param state the state value (can be null)
     * @param hash the pre-computed composite hash
     */
    public HeapStateEntry(@Nonnull K key, @Nonnull N namespace, @Nullable S state, int hash) {
        this.key = key;
        this.namespace = namespace;
        this.state = state;
        this.hash = hash;
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
