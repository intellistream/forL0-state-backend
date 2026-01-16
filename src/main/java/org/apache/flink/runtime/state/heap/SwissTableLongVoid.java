package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.VoidNamespace;

/**
 * Specialized SwissTable for Long keys with VoidNamespace.
 * 
 * <p>This implementation stores keys as primitive long values, eliminating
 * pointer chasing during key comparison. VoidNamespace is not stored since
 * it's a singleton.
 * 
 * <p>Target benchmarks: Nexmark Q4/9/18/19/20
 * 
 * @param <S> state type
 */
class SwissTableLongVoid<S> extends AbstractSwissTable<Long, VoidNamespace, S> {

    /** Key storage: primitive long array for zero-overhead comparison */
    long[] keys;

    /**
     * Creates a new specialized SwissTable for Long keys with VoidNamespace.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    SwissTableLongVoid(int slotCount, byte localDepth, int index) {
        super(slotCount, localDepth, index);
    }

    @Override
    void initStorage(int capacity) {
        this.keys = new long[capacity];
    }

    @Override
    boolean keyEquals(int slot, Long key, VoidNamespace namespace) {
        // Direct primitive comparison - no pointer chasing
        return keys[slot] == key.longValue();
    }

    @Override
    void storeKeyNs(int slot, Long key, VoidNamespace namespace) {
        keys[slot] = key.longValue();
        // VoidNamespace is singleton, no need to store
    }

    @Override
    Long getKey(int slot) {
        return keys[slot];  // Auto-boxing
    }

    @Override
    VoidNamespace getNamespace(int slot) {
        return VoidNamespace.INSTANCE;
    }

    @Override
    void clearSlot(int slot) {
        keys[slot] = 0L;
    }

    @Override
    void copySlot(int fromSlot, AbstractSwissTable<Long, VoidNamespace, S> target, int toSlot) {
        SwissTableLongVoid<S> t = (SwissTableLongVoid<S>) target;
        t.keys[toSlot] = this.keys[fromSlot];
    }

    @Override
    AbstractSwissTable<Long, VoidNamespace, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableLongVoid<>(slotCount, localDepth, index);
    }

    @Override
    void copyStorageFrom(AbstractSwissTable<Long, VoidNamespace, S> other) {
        SwissTableLongVoid<S> o = (SwissTableLongVoid<S>) other;
        this.keys = o.keys;
    }
}
