package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.VoidNamespace;

/**
 * Specialized SwissTable for String keys with VoidNamespace.
 * 
 * <p>This implementation stores keys directly as String references.
 * While there's still one level of pointer chasing (String.equals accesses
 * the internal char array), it's unavoidable for String comparison.
 * VoidNamespace is not stored since it's a singleton.
 * 
 * <p>Target: General String key state operations
 * 
 * @param <S> state type
 */
class SwissTableStringVoid<S> extends AbstractSwissTable<String, VoidNamespace, S> {

    /** Key storage: String references */
    String[] keys;

    /**
     * Creates a new specialized SwissTable for String keys with VoidNamespace.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    SwissTableStringVoid(int slotCount, byte localDepth, int index) {
        super(slotCount, localDepth, index);
    }

    @Override
    void initStorage(int capacity) {
        this.keys = new String[capacity];
    }

    @Override
    boolean keyEquals(int slot, String key, VoidNamespace namespace) {
        // String.equals has one level of pointer chasing (unavoidable)
        return key.equals(keys[slot]);
    }

    @Override
    void storeKeyNs(int slot, String key, VoidNamespace namespace) {
        keys[slot] = key;
        // VoidNamespace is singleton, no need to store
    }

    @Override
    String getKey(int slot) {
        return keys[slot];
    }

    @Override
    VoidNamespace getNamespace(int slot) {
        return VoidNamespace.INSTANCE;
    }

    @Override
    void clearSlot(int slot) {
        keys[slot] = null;
    }

    @Override
    void copySlot(int fromSlot, AbstractSwissTable<String, VoidNamespace, S> target, int toSlot) {
        SwissTableStringVoid<S> t = (SwissTableStringVoid<S>) target;
        t.keys[toSlot] = this.keys[fromSlot];
    }

    @Override
    AbstractSwissTable<String, VoidNamespace, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableStringVoid<>(slotCount, localDepth, index);
    }

    @Override
    void copyStorageFrom(AbstractSwissTable<String, VoidNamespace, S> other) {
        SwissTableStringVoid<S> o = (SwissTableStringVoid<S>) other;
        this.keys = o.keys;
    }
}
