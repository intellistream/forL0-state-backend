package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.VoidNamespace;

/**
 * Specialized SwissTable for Long keys with VoidNamespace.
 * 
 * <p>This implementation uses Group-Interleaved layout for optimal cache performance:
 * <ul>
 *   <li>groupData[]: long array with ctrl word + 8 keys per group</li>
 *   <li>Layout: [ctrl0|k0|k1|...|k7][ctrl1|k0|k1|...|k7]...</li>
 *   <li>Each group occupies 9 longs (72 bytes)</li>
 * </ul>
 * 
 * <p>VoidNamespace is not stored since it's a singleton.
 * 
 * <p>Target benchmarks: Nexmark Q4/9/18/19/20
 * 
 * @param <S> state type
 */
class SwissTableLongVoid<S> extends AbstractSwissTable<Long, VoidNamespace, S> {

    /** Number of longs per group: 1 ctrl word + 8 keys */
    private static final int LONGS_PER_GROUP = 9;

    /** Group-Interleaved storage: [ctrl|k0|k1|...|k7] per group */
    long[] groupData;

    /**
     * Creates a new specialized SwissTable for Long keys with VoidNamespace.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    SwissTableLongVoid(int slotCount, byte localDepth, int index) {
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
        // Set ctrl words to EMPTY (only ctrl words, not key slots)
        for (int g = 0; g < groupCount; g++) {
            groupData[g * LONGS_PER_GROUP] = EMPTY_CTRL_WORD;
        }
        this.values = new Object[slotCount];
        this.hashes = new long[slotCount];
    }

    /** Computes group base offset: group * 9 */
    private static int groupBase(int group) {
        return group * LONGS_PER_GROUP;
    }

    // ========== Core Operations (fully inlined for performance) ==========

    @Override
    @SuppressWarnings("unchecked")
    S get(long hash, Long key, VoidNamespace namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (groupData[groupBase + 1 + lane] == key.longValue()) {
                    return (S) values[(group << 3) + lane];
                }
            }

            if (matchEmpty(ctrlWord) != 0) return null;

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @Override
    int put(long hash, Long key, VoidNamespace namespace, int maxTableCapacity) {
        int group = h1(hash) & groupMask;
        int stride = 1;
        int firstDeletedSlot = -1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            // 1. Look for existing key
            for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (groupData[groupBase + 1 + lane] == key.longValue()) {
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
            groupData[insertBase + 1 + insertLane] = key.longValue();
            groupData[insertBase] = setCtrlByte(groupData[insertBase], insertLane, (byte) h2(hash));
            hashes[insertSlot] = hash;
            used++;
            if (useDeleted) tomb--; else growthLeft--;
            return insertSlot | NEW_FLAG;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    S remove(long hash, Long key, VoidNamespace namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            for (long match = matchH2(ctrlWord, h2(hash)); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (groupData[groupBase + 1 + lane] == key.longValue()) {
                    int slot = (group << 3) + lane;
                    S oldValue = (S) values[slot];
                    groupData[groupBase + 1 + lane] = 0L;
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
    void putDirect(long hash, Long key, VoidNamespace namespace, S value) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            int groupBase = groupBase(group);
            long ctrlWord = groupData[groupBase];

            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int lane = Long.numberOfTrailingZeros(emptyMask) >>> 3;
                int slot = (group << 3) + lane;
                groupData[groupBase + 1 + lane] = key.longValue();
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
        SwissTableLongVoid<S> newTable = new SwissTableLongVoid<>(cap, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = cap >>> 3;
        for (int g = 0; g < groupCount; g++) {
            // Fuse base calculation: g * 9 = (g << 3) + g
            int groupBase = (g << 3) + g;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                
                long hash = hashes[slot];
                long keyVal = groupData[groupBase + 1 + lane];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, VoidNamespace.INSTANCE, value);
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
        SwissTableLongVoid<S> newTable = new SwissTableLongVoid<>(newCapacity, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            // Fuse base calculation: g * 9 = (g << 3) + g
            int groupBase = (g << 3) + g;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                
                long hash = hashes[slot];
                long keyVal = groupData[groupBase + 1 + lane];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, VoidNamespace.INSTANCE, value);
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
        // All entries have VoidNamespace, just return used count
        return used;
    }

    @Override
    Long getKey(int slot) {
        int group = slot >>> 3;
        int lane = slot & 7;
        // Fuse base calculation: group * 9 = (group << 3) + group
        return groupData[(group << 3) + group + 1 + lane];
    }

    @Override
    VoidNamespace getNamespace(int slot) {
        return VoidNamespace.INSTANCE;
    }

    @Override
    boolean isSlotFull(int slot) {
        int group = slot >>> 3;
        int lane = slot & 7;
        // Fuse base calculation: group * 9 = (group << 3) + group
        long ctrlWord = groupData[(group << 3) + group];
        byte ctrl = getCtrlByte(ctrlWord, lane);
        return isFull(ctrl);
    }

    @Override
    AbstractSwissTable<Long, VoidNamespace, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableLongVoid<>(slotCount, localDepth, index);
    }
}
