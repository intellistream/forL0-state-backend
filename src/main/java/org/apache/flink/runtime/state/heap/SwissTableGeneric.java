package org.apache.flink.runtime.state.heap;

/**
 * Generic SwissTable implementation using Object arrays for key and namespace storage.
 * 
 * <p>This is the fallback implementation used when no specialized version matches
 * the key/namespace types. It uses Object references, which may incur pointer chasing
 * overhead during key comparison.
 * 
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
class SwissTableGeneric<K, N, S> extends AbstractSwissTable<K, N, S> {

    /** Key storage: keys[slot] = key */
    K[] keys;
    
    /** Namespace storage: namespaces[slot] = namespace */
    N[] namespaces;

    /**
     * Creates a new generic SwissTable.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    SwissTableGeneric(int slotCount, byte localDepth, int index) {
        super(slotCount, localDepth, index);
    }

    @Override
    @SuppressWarnings("unchecked")
    void initStorage(int capacity) {
        this.keys = (K[]) new Object[capacity];
        this.namespaces = (N[]) new Object[capacity];
    }

    @Override
    boolean keyEquals(int slot, K key, N namespace) {
        return key.equals(keys[slot]) && namespace.equals(namespaces[slot]);
    }

    @Override
    void storeKeyNs(int slot, K key, N namespace) {
        keys[slot] = key;
        namespaces[slot] = namespace;
    }

    @Override
    K getKey(int slot) {
        return keys[slot];
    }

    @Override
    N getNamespace(int slot) {
        return namespaces[slot];
    }

    @Override
    void clearSlot(int slot) {
        keys[slot] = null;
        namespaces[slot] = null;
    }

    @Override
    void copySlot(int fromSlot, AbstractSwissTable<K, N, S> target, int toSlot) {
        SwissTableGeneric<K, N, S> t = (SwissTableGeneric<K, N, S>) target;
        t.keys[toSlot] = this.keys[fromSlot];
        t.namespaces[toSlot] = this.namespaces[fromSlot];
    }

    @Override
    AbstractSwissTable<K, N, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableGeneric<>(slotCount, localDepth, index);
    }

    @Override
    void copyStorageFrom(AbstractSwissTable<K, N, S> other) {
        SwissTableGeneric<K, N, S> o = (SwissTableGeneric<K, N, S>) other;
        this.keys = o.keys;
        this.namespaces = o.namespaces;
    }
}
