package org.apache.flink.runtime.state.heap;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Specialized SwissTable for String keys with TimeWindow namespace.
 * 
 * <p>This implementation stores:
 * <ul>
 *   <li>Keys as String references</li>
 *   <li>TimeWindow as two primitive longs (start/end) in interleaved layout</li>
 * </ul>
 * 
 * <p>Memory layout for namespaces:
 * <pre>
 * namespaces[slot * 2]     = window.start
 * namespaces[slot * 2 + 1] = window.end
 * </pre>
 * 
 * <p>Target benchmarks: WordCount
 * 
 * @param <S> state type
 */
class SwissTableStringTimeWindow<S> extends AbstractSwissTable<String, TimeWindow, S> {

    /** Key storage: String references */
    String[] keys;
    
    /** Namespace storage: interleaved [start, end, start, end, ...] */
    long[] namespaces;

    /**
     * Creates a new specialized SwissTable for String keys with TimeWindow namespace.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    SwissTableStringTimeWindow(int slotCount, byte localDepth, int index) {
        super(slotCount, localDepth, index);
    }

    @Override
    void initStorage(int capacity) {
        this.keys = new String[capacity];
        this.namespaces = new long[capacity * 2];  // 2 longs per TimeWindow
    }

    @Override
    boolean keyEquals(int slot, String key, TimeWindow namespace) {
        // String comparison + primitive comparison for TimeWindow
        int nsIdx = slot << 1;
        return key.equals(keys[slot])
            && namespaces[nsIdx] == namespace.getStart()
            && namespaces[nsIdx + 1] == namespace.getEnd();
    }

    @Override
    void storeKeyNs(int slot, String key, TimeWindow namespace) {
        keys[slot] = key;
        int nsIdx = slot << 1;
        namespaces[nsIdx] = namespace.getStart();
        namespaces[nsIdx + 1] = namespace.getEnd();
    }

    @Override
    String getKey(int slot) {
        return keys[slot];
    }

    @Override
    TimeWindow getNamespace(int slot) {
        int nsIdx = slot << 1;
        return new TimeWindow(namespaces[nsIdx], namespaces[nsIdx + 1]);
    }

    @Override
    void clearSlot(int slot) {
        keys[slot] = null;
        int nsIdx = slot << 1;
        namespaces[nsIdx] = 0L;
        namespaces[nsIdx + 1] = 0L;
    }

    @Override
    void copySlot(int fromSlot, AbstractSwissTable<String, TimeWindow, S> target, int toSlot) {
        SwissTableStringTimeWindow<S> t = (SwissTableStringTimeWindow<S>) target;
        t.keys[toSlot] = this.keys[fromSlot];
        int fromNsIdx = fromSlot << 1;
        int toNsIdx = toSlot << 1;
        t.namespaces[toNsIdx] = this.namespaces[fromNsIdx];
        t.namespaces[toNsIdx + 1] = this.namespaces[fromNsIdx + 1];
    }

    @Override
    AbstractSwissTable<String, TimeWindow, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableStringTimeWindow<>(slotCount, localDepth, index);
    }

    @Override
    void copyStorageFrom(AbstractSwissTable<String, TimeWindow, S> other) {
        SwissTableStringTimeWindow<S> o = (SwissTableStringTimeWindow<S>) other;
        this.keys = o.keys;
        this.namespaces = o.namespaces;
    }
}
