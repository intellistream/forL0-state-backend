package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.VoidNamespace;

/**
 * Specialized SwissTable for String keys with VoidNamespace.
 * 
 * <p>This implementation uses a hybrid layout:
 * <ul>
 *   <li>groupData[]: long array with ctrl word only (no namespace data for VoidNamespace)</li>
 *   <li>keys[]: String array for key references (required for GC)</li>
 *   <li>Layout: [ctrl] per group</li>
 * </ul>
 * 
 * <p>VoidNamespace is not stored since it's a singleton.
 * While there's still one level of pointer chasing (String.equals accesses
 * the internal char array), it's unavoidable for String comparison.
 * 
 * <p>Target: General String key state operations
 * 
 * @param <S> state type
 */
class SwissTableStringVoid<S> extends AbstractSwissTable<String, VoidNamespace, S> {

    /** Group-Interleaved storage: just ctrl word per group (VoidNamespace needs no data) */
    long[] groupData;
    
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
        
        // Initialize group data (1 ctrl word per group)
        int groupCount = slotCount >>> 3;
        this.groupData = new long[groupCount];
        java.util.Arrays.fill(groupData, EMPTY_CTRL_WORD);
        this.keys = new String[slotCount];
        this.values = new Object[slotCount];
        this.hashes = new long[slotCount];
    }

    // ========== Core Operations (fully inlined for performance) ==========

    @Override
    @SuppressWarnings("unchecked")
    S get(long hash, String key, VoidNamespace namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrl = groupData[group];

            for (long m = matchH2(ctrl, h2(hash)); m != 0; m &= m - 1) {
                int slot = (group << 3) | (Long.numberOfTrailingZeros(m) >>> 3);
                if (keys[slot] == key || key.equals(keys[slot])) {
                    return (S) values[slot];
                }
            }

            if ((ctrl & ~(ctrl << 6) & MSB) != 0) return null;

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @Override
    int put(long hash, String key, VoidNamespace namespace, int maxTableCapacity) {
        int group = h1(hash) & groupMask;
        int stride = 1;
        int delSlot = -1;

        while (true) {
            long ctrl = groupData[group];

            // 1. Look for existing key
            for (long m = matchH2(ctrl, h2(hash)); m != 0; m &= m - 1) {
                int slot = (group << 3) | (Long.numberOfTrailingZeros(m) >>> 3);
                if (keys[slot] == key || key.equals(keys[slot])) {
                    return slot;
                }
            }

            // 2. Check for empty or deleted
            long eod = ctrl & MSB;
            if (eod == 0) {
                group = (group + stride) & groupMask;
                stride++;
                continue;
            }

            // 3. Check first candidate
            int lane = Long.numberOfTrailingZeros(eod) >>> 3;
            if (((ctrl >>> (lane << 3)) & 0xFF) == (CTRL_DELETED & 0xFF)) {
                if (delSlot == -1) delSlot = (group << 3) | lane;
                group = (group + stride) & groupMask;
                stride++;
                continue;
            }

            // 4. Found EMPTY - insert
            int slot = (delSlot != -1) ? delSlot : (group << 3) | lane;
            boolean useDel = delSlot != -1;

            if (!useDel && growthLeft == 0) {
                if (tomb > 0) return NEED_REHASH;
                if (capacity < maxTableCapacity) return NEED_GROW;
                return NEED_SPLIT;
            }

            int ig = slot >>> 3, il = slot & 7;
            keys[slot] = key;
            groupData[ig] = setCtrlByte(groupData[ig], il, (byte) h2(hash));
            hashes[slot] = hash;
            used++;
            if (useDel) tomb--; else growthLeft--;
            return slot | NEW_FLAG;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    S remove(long hash, String key, VoidNamespace namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrl = groupData[group];

            for (long m = matchH2(ctrl, h2(hash)); m != 0; m &= m - 1) {
                int slot = (group << 3) | (Long.numberOfTrailingZeros(m) >>> 3);
                if (keys[slot] == key || key.equals(keys[slot])) {
                    S old = (S) values[slot];
                    keys[slot] = null;
                    values[slot] = null;
                    boolean hasEmpty = (ctrl & ~(ctrl << 6) & MSB) != 0;
                    int lane = slot & 7;
                    groupData[group] = setCtrlByte(ctrl, lane, hasEmpty ? CTRL_EMPTY : CTRL_DELETED);
                    used--;
                    if (hasEmpty) growthLeft++; else tomb++;
                    return old;
                }
            }

            if ((ctrl & ~(ctrl << 6) & MSB) != 0) return null;

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @Override
    void putDirect(long hash, String key, VoidNamespace namespace, S value) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrl = groupData[group];

            long em = ctrl & ~(ctrl << 6) & MSB;
            if (em != 0) {
                int lane = Long.numberOfTrailingZeros(em) >>> 3;
                int slot = (group << 3) | lane;
                keys[slot] = key;
                groupData[group] = setCtrlByte(ctrl, lane, (byte) h2(hash));
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
        SwissTableStringVoid<S> newTable = new SwissTableStringVoid<>(cap, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = cap >>> 3;
        for (int g = 0; g < groupCount; g++) {
            long ctrlWord = groupData[g];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                
                long hash = hashes[slot];
                String keyVal = keys[slot];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, VoidNamespace.INSTANCE, value);
            }
        }

        // Copy new table state
        this.groupData = newTable.groupData;
        this.keys = newTable.keys;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(cap) - used);
    }

    @Override
    @SuppressWarnings("unchecked")
    void grow() {
        int newCapacity = capacity * 2;
        SwissTableStringVoid<S> newTable = new SwissTableStringVoid<>(newCapacity, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            long ctrlWord = groupData[g];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                
                long hash = hashes[slot];
                String keyVal = keys[slot];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, VoidNamespace.INSTANCE, value);
            }
        }

        // Copy new table state
        this.capacity = (short) newCapacity;
        this.groupMask = newTable.groupMask;
        this.groupData = newTable.groupData;
        this.keys = newTable.keys;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(newCapacity) - used);
    }

    // ========== Iteration and Query Methods ==========

    @Override
    int countNamespace(Object namespace) {
        // VoidNamespace is singleton, all entries belong to it
        return used;
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
    boolean isSlotFull(int slot) {
        int group = slot >>> 3;
        int lane = slot & 7;
        long ctrlWord = groupData[group];
        byte ctrl = getCtrlByte(ctrlWord, lane);
        return isFull(ctrl);
    }

    @Override
    AbstractSwissTable<String, VoidNamespace, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableStringVoid<>(slotCount, localDepth, index);
    }
}
