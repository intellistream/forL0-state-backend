package org.apache.flink.runtime.state.heap;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Specialized SwissTable for Long keys with TimeWindow namespace.
 * 
 * <p>This implementation uses Group-Interleaved layout for optimal cache performance:
 * <ul>
 *   <li>groupData[]: long array with ctrl word + (key, start, end) per slot</li>
 *   <li>Layout: [ctrl|k0,s0,e0|k1,s1,e1|...|k7,s7,e7] per group</li>
 *   <li>Each group occupies 25 longs (200 bytes): 1 ctrl + 8 slots × 3 longs</li>
 * </ul>
 * 
 * <p>Target benchmarks: Nexmark Q5/7/8/11/12
 * 
 * @param <S> state type
 */
class SwissTableLongTimeWindow<S> extends AbstractSwissTable<Long, TimeWindow, S> {

    /** Number of longs per group: 1 ctrl word + 8 slots × 3 longs (key + start + end) */
    private static final int LONGS_PER_GROUP = 25;
    /** Number of longs per slot: key + start + end */
    private static final int LONGS_PER_SLOT = 3;

    /** Group-Interleaved storage: [ctrl|k0,s0,e0|k1,s1,e1|...|k7,s7,e7] per group */
    long[] groupData;

    /**
     * Creates a new specialized SwissTable for Long keys with TimeWindow namespace.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    SwissTableLongTimeWindow(int slotCount, byte localDepth, int index) {
        if (slotCount < 8 || (slotCount & (slotCount - 1)) != 0 || (slotCount & 7) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2 and >= 8");
        }
        this.capacity = (short) slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.localDepth = localDepth;
        this.index = index;
        this.used = 0;
        this.tomb = 0;
        this.growthLeft = (short) maxOcc(slotCount);
        
        // Initialize Group-Interleaved storage
        int groupCount = slotCount >>> 3;
        this.groupData = new long[groupCount * LONGS_PER_GROUP];
        // Set ctrl words to EMPTY (only ctrl words, not slot data)
        for (int g = 0; g < groupCount; g++) {
            groupData[g * LONGS_PER_GROUP] = EMPTY_CTRL_WORD;
        }
        this.values = new Object[slotCount];
        this.hashes = new long[slotCount];
    }

    /** Computes group base offset: group * 25 */
    private static int groupBase(int group) {
        return group * LONGS_PER_GROUP;
    }

    /** Gets the offset for a slot's data within a group: base + 1 + lane * 3 */
    private static int slotOffset(int groupBase, int lane) {
        return groupBase + 1 + lane * LONGS_PER_SLOT;
    }

    // ========== Core Operations (fully inlined for performance) ==========

    @Override
    @SuppressWarnings("unchecked")
    S get(long hash, Long key, TimeWindow namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                int offset = slotOffset(groupBase, lane);
                if (groupData[offset] == key.longValue()
                    && groupData[offset + 1] == namespace.getStart()
                    && groupData[offset + 2] == namespace.getEnd()) {
                    return (S) values[(group << 3) + lane];
                }
            }

            if (matchEmpty(ctrlWord) != 0) return null;

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @Override
    int put(long hash, Long key, TimeWindow namespace, int maxTableCapacity) {
        int group = h1(hash) & groupMask;
        int stride = 1;
        int firstDeletedSlot = -1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            // 1. Look for existing key
            for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                int offset = slotOffset(groupBase, lane);
                if (groupData[offset] == key.longValue()
                    && groupData[offset + 1] == namespace.getStart()
                    && groupData[offset + 2] == namespace.getEnd()) {
                    return (group << 3) + lane;
                }
            }

            // 2. Check for empty or deleted
            long emptyOrDeleted = matchEmptyOrDeleted(ctrlWord);
            if (emptyOrDeleted == 0) {
                group = (group + stride) & groupMask;
                stride++;
                continue;
            }

            // 3. Check first candidate
            int lane = Long.numberOfTrailingZeros(emptyOrDeleted) >>> 3;
            if (getCtrlByte(ctrlWord, lane) == CTRL_DELETED) {
                if (firstDeletedSlot == -1) firstDeletedSlot = (group << 3) + lane;
                group = (group + stride) & groupMask;
                stride++;
                continue;
            }

            // 4. Found EMPTY - insert
            int insertSlot = (firstDeletedSlot != -1) ? firstDeletedSlot : (group << 3) + lane;
            boolean useDeleted = firstDeletedSlot != -1;

            if (!useDeleted && growthLeft == 0) {
                if (tomb > 0) return NEED_REHASH;
                if (capacity < maxTableCapacity) return NEED_GROW;
                return NEED_SPLIT;
            }

            // Write to insert slot
            int insertBase = groupBase(insertSlot >>> 3);
            int insertLane = insertSlot & 7;
            int insertOffset = slotOffset(insertBase, insertLane);
            groupData[insertOffset] = key.longValue();
            groupData[insertOffset + 1] = namespace.getStart();
            groupData[insertOffset + 2] = namespace.getEnd();
            groupData[insertBase] = setCtrlByte(groupData[insertBase], insertLane, (byte) h2(hash));
            hashes[insertSlot] = hash;
            used++;
            if (useDeleted) tomb--; else growthLeft--;
            return insertSlot | NEW_FLAG;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    S remove(long hash, Long key, TimeWindow namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                int offset = slotOffset(groupBase, lane);
                if (groupData[offset] == key.longValue()
                    && groupData[offset + 1] == namespace.getStart()
                    && groupData[offset + 2] == namespace.getEnd()) {
                    int slot = (group << 3) + lane;
                    S oldValue = (S) values[slot];
                    groupData[offset] = 0L;
                    groupData[offset + 1] = 0L;
                    groupData[offset + 2] = 0L;
                    values[slot] = null;
                    boolean hasEmpty = matchEmpty(ctrlWord) != 0;
                    groupData[groupBase] = setCtrlByte(ctrlWord, lane, hasEmpty ? CTRL_EMPTY : CTRL_DELETED);
                    used--;
                    if (hasEmpty) growthLeft++; else tomb++;
                    return oldValue;
                }
            }

            if (matchEmpty(ctrlWord) != 0) return null;

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @Override
    void putDirect(long hash, Long key, TimeWindow namespace, S value) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int lane = Long.numberOfTrailingZeros(emptyMask) >>> 3;
                int slot = (group << 3) + lane;
                int offset = slotOffset(groupBase, lane);
                groupData[offset] = key.longValue();
                groupData[offset + 1] = namespace.getStart();
                groupData[offset + 2] = namespace.getEnd();
                groupData[groupBase] = setCtrlByte(ctrlWord, lane, (byte) h2(hash));
                values[slot] = value;
                hashes[slot] = hash;
                used++;
                growthLeft--;
                return;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    void rehash() {
        int cap = capacity;
        SwissTableLongTimeWindow<S> newTable = new SwissTableLongTimeWindow<>(cap, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = cap >>> 3;
        for (int g = 0; g < groupCount; g++) {
            // Fuse base calculation: g * 25 = (g << 4) + (g << 3) + g
            int base = (g << 4) + (g << 3) + g;
            long ctrlWord = groupData[base];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                int off = base + 1 + lane * LONGS_PER_SLOT;
                
                long hash = hashes[slot];
                long keyVal = groupData[off];
                long start = groupData[off + 1];
                long end = groupData[off + 2];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, new TimeWindow(start, end), value);
            }
        }

        // Copy new table state
        this.groupData = newTable.groupData;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(cap) - used);
    }

    @Override
    @SuppressWarnings("unchecked")
    void grow() {
        int newCapacity = capacity * 2;
        SwissTableLongTimeWindow<S> newTable = new SwissTableLongTimeWindow<>(newCapacity, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            // Fuse base calculation: g * 25 = (g << 4) + (g << 3) + g
            int base = (g << 4) + (g << 3) + g;
            long ctrlWord = groupData[base];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                int off = base + 1 + lane * LONGS_PER_SLOT;
                
                long hash = hashes[slot];
                long keyVal = groupData[off];
                long start = groupData[off + 1];
                long end = groupData[off + 2];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, new TimeWindow(start, end), value);
            }
        }

        // Copy new table state
        this.capacity = (short) newCapacity;
        this.groupMask = newTable.groupMask;
        this.groupData = newTable.groupData;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(newCapacity) - used);
    }

    // ========== Iteration and Query Methods ==========

    @Override
    int countNamespace(Object namespace) {
        if (!(namespace instanceof TimeWindow)) {
            return 0;
        }
        TimeWindow tw = (TimeWindow) namespace;
        long start = tw.getStart();
        long end = tw.getEnd();
        
        int count = 0;
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            // Fuse base calculation: g * 25 = (g << 4) + (g << 3) + g
            int base = (g << 4) + (g << 3) + g;
            long ctrlWord = groupData[base];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int off = base + 1 + lane * LONGS_PER_SLOT;
                if (groupData[off + 1] == start && groupData[off + 2] == end) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    Long getKey(int slot) {
        int group = slot >>> 3;
        int lane = slot & 7;
        // Fuse base calculation: group * 25 = (group << 4) + (group << 3) + group
        int off = (group << 4) + (group << 3) + group + 1 + lane * LONGS_PER_SLOT;
        return groupData[off];
    }

    @Override
    TimeWindow getNamespace(int slot) {
        int group = slot >>> 3;
        int lane = slot & 7;
        // Fuse base calculation: group * 25 = (group << 4) + (group << 3) + group
        int off = (group << 4) + (group << 3) + group + 1 + lane * LONGS_PER_SLOT;
        return new TimeWindow(groupData[off + 1], groupData[off + 2]);
    }

    @Override
    boolean isSlotFull(int slot) {
        int group = slot >>> 3;
        int lane = slot & 7;
        // Fuse base calculation: group * 25 = (group << 4) + (group << 3) + group
        long ctrlWord = groupData[(group << 4) + (group << 3) + group];
        byte ctrl = getCtrlByte(ctrlWord, lane);
        return isFull(ctrl);
    }

    @Override
    AbstractSwissTable<Long, TimeWindow, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableLongTimeWindow<>(slotCount, localDepth, index);
    }
}
