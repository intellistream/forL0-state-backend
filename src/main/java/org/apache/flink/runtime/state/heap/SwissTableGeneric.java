package org.apache.flink.runtime.state.heap;

/**
 * Generic SwissTable implementation using Object arrays for key and namespace storage.
 * 
 * <p>This is the fallback implementation used when no specialized version matches
 * the key/namespace types. It uses Object references, which may incur pointer chasing
 * overhead during key comparison.
 * 
 * <p>This implementation uses a simple layout:
 * <ul>
 *   <li>groupData[]: long array with ctrl word only</li>
 *   <li>keys[]: Object array for key references</li>
 *   <li>namespaces[]: Object array for namespace references</li>
 * </ul>
 * 
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
class SwissTableGeneric<K, N, S> extends AbstractSwissTable<K, N, S> {

    /** Group data: ctrl word per group */
    long[] groupData;
    
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
    @SuppressWarnings("unchecked")
    SwissTableGeneric(int slotCount, byte localDepth, int index) {
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
        this.keys = (K[]) new Object[slotCount];
        this.namespaces = (N[]) new Object[slotCount];
        this.values = new Object[slotCount];
        this.hashes = new long[slotCount];
    }

    // ========== Core Operations (fully inlined for performance) ==========

    @Override
    @SuppressWarnings("unchecked")
    S get(long hash, K key, N namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrl = groupData[group];

            for (long m = matchH2(ctrl, h2(hash)); m != 0; m &= m - 1) {
                int slot = (group << 3) | (Long.numberOfTrailingZeros(m) >>> 3);
                if ((keys[slot] == key || key.equals(keys[slot]))
                    && namespace.equals(namespaces[slot])) {
                    return (S) values[slot];
                }
            }

            if ((ctrl & ~(ctrl << 6) & MSB) != 0) return null;

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @Override
    int put(long hash, K key, N namespace, int maxTableCapacity) {
        int group = h1(hash) & groupMask;
        int stride = 1;
        int delSlot = -1;

        while (true) {
            long ctrl = groupData[group];

            // 1. Look for existing key
            for (long m = matchH2(ctrl, h2(hash)); m != 0; m &= m - 1) {
                int slot = (group << 3) | (Long.numberOfTrailingZeros(m) >>> 3);
                if ((keys[slot] == key || key.equals(keys[slot]))
                    && namespace.equals(namespaces[slot])) {
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
            namespaces[slot] = namespace;
            groupData[ig] = setCtrlByte(groupData[ig], il, (byte) h2(hash));
            hashes[slot] = hash;
            used++;
            if (useDel) tomb--; else growthLeft--;
            return slot | NEW_FLAG;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    S remove(long hash, K key, N namespace) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrl = groupData[group];

            for (long m = matchH2(ctrl, h2(hash)); m != 0; m &= m - 1) {
                int slot = (group << 3) | (Long.numberOfTrailingZeros(m) >>> 3);
                if ((keys[slot] == key || key.equals(keys[slot]))
                    && namespace.equals(namespaces[slot])) {
                    S old = (S) values[slot];
                    keys[slot] = null;
                    namespaces[slot] = null;
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
    void putDirect(long hash, K key, N namespace, S value) {
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrl = groupData[group];

            long em = ctrl & ~(ctrl << 6) & MSB;
            if (em != 0) {
                int lane = Long.numberOfTrailingZeros(em) >>> 3;
                int slot = (group << 3) | lane;
                keys[slot] = key;
                namespaces[slot] = namespace;
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
        SwissTableGeneric<K, N, S> newTable = new SwissTableGeneric<>(cap, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = cap >>> 3;
        for (int g = 0; g < groupCount; g++) {
            long ctrlWord = groupData[g];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                
                long hash = hashes[slot];
                K keyVal = keys[slot];
                N nsVal = namespaces[slot];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, nsVal, value);
            }
        }

        // Copy new table state
        this.groupData = newTable.groupData;
        this.keys = newTable.keys;
        this.namespaces = newTable.namespaces;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(cap) - used);
    }

    @Override
    @SuppressWarnings("unchecked")
    void grow() {
        int newCapacity = capacity * 2;
        SwissTableGeneric<K, N, S> newTable = new SwissTableGeneric<>(newCapacity, localDepth, index);

        // Reinsert all FULL entries
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            long ctrlWord = groupData[g];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                
                long hash = hashes[slot];
                K keyVal = keys[slot];
                N nsVal = namespaces[slot];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, nsVal, value);
            }
        }

        // Copy new table state
        this.capacity = (short) newCapacity;
        this.groupMask = newTable.groupMask;
        this.groupData = newTable.groupData;
        this.keys = newTable.keys;
        this.namespaces = newTable.namespaces;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(newCapacity) - used);
    }

    // ========== Iteration and Query Methods ==========

    @Override
    int countNamespace(Object namespace) {
        int count = 0;
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            long ctrlWord = groupData[g];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask = clearLowestBit(fullMask)) {
                int tz = Long.numberOfTrailingZeros(fullMask);
                int lane = tz >>> 3;  // Inlined laneFromTz
                int slot = (g << 3) + lane;
                if (namespace.equals(namespaces[slot])) {
                    count++;
                }
            }
        }
        return count;
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
    boolean isSlotFull(int slot) {
        int group = slot >>> 3;
        int lane = slot & 7;
        long ctrlWord = groupData[group];
        byte ctrl = getCtrlByte(ctrlWord, lane);
        return isFull(ctrl);
    }

    @Override
    AbstractSwissTable<K, N, S> createNew(int slotCount, byte localDepth, int index) {
        return new SwissTableGeneric<>(slotCount, localDepth, index);
    }
}
