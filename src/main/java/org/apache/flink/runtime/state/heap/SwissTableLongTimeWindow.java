package org.apache.flink.runtime.state.heap;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Specialized SwissTable for Long keys with TimeWindow namespace.
 * 
 * <p>This implementation stores:
 * <ul>
 *   <li>Keys as primitive long values</li>
 *   <li>TimeWindow as two primitive longs (start/end) in interleaved layout</li>
 * </ul>
 * 
 * <p>Memory layout for namespaces:
 * <pre>
 * namespaces[slot * 2]     = window.start
 * namespaces[slot * 2 + 1] = window.end
 * </pre>
 * 
 * <p>Target benchmarks: Nexmark Q5/7/8/11/12
 * 
 * @param <S> state type
 */
class SwissTableLongTimeWindow<S> extends AbstractSwissTable<Long, TimeWindow, S> {

    /** Key storage: primitive long array */
    long[] keys;
    
    /** Namespace storage: interleaved [start, end, start, end, ...] */
    long[] namespaces;

    /**
     * Creates a new specialized SwissTable for Long keys with TimeWindow namespace.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    SwissTableLongTimeWindow(int slotCount, byte localDepth, int index) {
        super(slotCount, localDepth, index);
    }

    @Override
    void initStorage(int capacity) {
        this.keys = new long[capacity];
        this.namespaces = new long[capacity * 2];  // 2 longs per TimeWindow
    }

    @Override
    boolean keyEquals(int slot, Long key, TimeWindow namespace) {
        // Direct primitive comparison - no pointer chasing
        int nsIdx = slot << 1;
        return keys[slot] == key.longValue()
            && namespaces[nsIdx] == namespace.getStart()
            && namespaces[nsIdx + 1] == namespace.getEnd();
    }

    @Override
    void storeKeyNs(int slot, Long key, TimeWindow namespace) {
        keys[slot] = key.longValue();
        int nsIdx = slot << 1;
        namespaces[nsIdx] = namespace.getStart();
        namespaces[nsIdx + 1] = namespace.getEnd();
    }

    @Override
    Long getKey(int slot) {
        return keys[slot];  // Auto-boxing
    }

    @Override
    TimeWindow getNamespace(int slot) {
        int nsIdx = slot << 1;
        return new TimeWindow(namespaces[nsIdx], namespaces[nsIdx + 1]);
    }

    @Override
    void clearSlot(int slot) {
        keys[slot] = 0L;
        int nsIdx = slot << 1;
        namespaces[nsIdx] = 0L;
        namespaces[nsIdx + 1] = 0L;
    }

    @Override
    void copySlot(int fromSlot, AbstractSwissTable<Long, TimeWindow, S> target, int toSlot) {
        SwissTableLongTimeWindow<S> t = (SwissTableLongTimeWindow<S>) target;
        t.keys[toSlot] = this.keys[fromSlot];
        int fromNsIdx = fromSlot << 1;
        int toNsIdx = toSlot << 1;
        t.namespaces[toNsIdx] = this.namespaces[fromNsIdx];
        t.namespaces[toNsIdx + 1] = this.namespaces[fromNsIdx + 1];
    }

    @Override
    AbstractSwissTable<Long, TimeWindow, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableLongTimeWindow<>(slotCount, localDepth, index);
    }

    @Override
    void copyStorageFrom(AbstractSwissTable<Long, TimeWindow, S> other) {
        SwissTableLongTimeWindow<S> o = (SwissTableLongTimeWindow<S>) other;
        this.keys = o.keys;
        this.namespaces = o.namespaces;
    }
}
