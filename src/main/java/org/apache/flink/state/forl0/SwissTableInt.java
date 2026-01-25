package org.apache.flink.state.forl0;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Specialized SwissTable for Integer keys with Group-Interleaved layout.
 * 
 * <p>This implementation uses packed int storage:
 * <ul>
 *   <li>groupData[]: long array with ctrl word + 4 key pairs per group</li>
 *   <li>Layout: [ctrl|k0k1|k2k3|k4k5|k6k7] per group</li>
 *   <li>Each group occupies 5 longs (40 bytes)</li>
 *   <li>Keys are packed: 2 ints per long</li>
 * </ul>
 * 
 * @param <S> state type
 */
public class SwissTableInt<S> {

    // ========== Return value encoding for put() ==========
    public static final int NEW_FLAG = 1 << 16;
    public static final int SLOT_MASK = 0xFFFF;
    public static final int NEED_REHASH = -1;
    public static final int NEED_GROW = -2;

    // ========== Control byte values ==========
    private static final byte CTRL_EMPTY = (byte) 0x80;
    private static final byte CTRL_DELETED = (byte) 0xFE;

    // ========== SWAR constants ==========
    private static final long LSB = 0x0101010101010101L;
    private static final long MSB = 0x8080808080808080L;
    private static final long EMPTY_CTRL_WORD = 0x8080808080808080L;

    /** Number of longs per group: 1 ctrl word + 4 key-pair longs (8 keys packed as 2 per long). */
    private static final int LONGS_PER_GROUP = 5;

    // ========== Counts ==========
    int used;
    int tomb;
    int capacity;
    int growthLeft;
    int groupMask;

    // ========== Group-Interleaved Storage ==========
    /** [ctrl|k0k1|k2k3|k4k5|k6k7] per group, each group 5 longs. */
    long[] groupData;
    Object[] values;
    int[] hashes;

    public SwissTableInt(int slotCount) {
        if (slotCount < 8 || (slotCount & (slotCount - 1)) != 0 || (slotCount & 7) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2 and >= 8");
        }
        this.capacity = slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.used = 0;
        this.tomb = 0;
        this.growthLeft = maxOcc(slotCount);
        
        int groupCount = slotCount >>> 3;
        this.groupData = new long[groupCount * LONGS_PER_GROUP];
        for (int g = 0; g < groupCount; g++) {
            groupData[g * LONGS_PER_GROUP] = EMPTY_CTRL_WORD;
        }
        this.values = new Object[slotCount];
        this.hashes = new int[slotCount];
    }

    @SuppressWarnings("unchecked")
    public S get(int hash, int key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (getKey(groupBase, lane) == key) {
                    return (S) values[(group << 3) + lane];
                }
            }

            if (matchEmpty(ctrlWord) != 0) {
                return null;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    public int put(int hash, int key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;
        int firstDeletedSlot = -1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (getKey(groupBase, lane) == key) {
                    return (group << 3) + lane;
                }
            }

            long emptyOrDeleted = matchEmptyOrDeleted(ctrlWord);
            if (emptyOrDeleted == 0) {
                group = (group + stride) & groupMask;
                stride++;
                continue;
            }

            int lane = Long.numberOfTrailingZeros(emptyOrDeleted) >>> 3;
            byte ctrlByte = getCtrlByte(ctrlWord, lane);
            
            if (ctrlByte == CTRL_DELETED) {
                if (firstDeletedSlot == -1) {
                    firstDeletedSlot = (group << 3) + lane;
                }
                group = (group + stride) & groupMask;
                stride++;
                continue;
            }

            int insertSlot = (firstDeletedSlot != -1) ? firstDeletedSlot : (group << 3) + lane;
            boolean useDeleted = firstDeletedSlot != -1;

            if (!useDeleted && growthLeft == 0) {
                if (tomb > 0) return NEED_REHASH;
                return NEED_GROW;
            }

            int insertGroup = insertSlot >>> 3;
            int insertLane = insertSlot & 7;
            int insertBase = insertGroup * LONGS_PER_GROUP;
            
            setKey(insertBase, insertLane, key);
            
            groupData[insertBase] = setCtrlByte(groupData[insertBase], insertLane, (byte) (hash & 0x7F));
            hashes[insertSlot] = hash;
            used++;
            if (useDeleted) {
                tomb--;
            } else {
                growthLeft--;
            }
            return insertSlot | NEW_FLAG;
        }
    }

    @SuppressWarnings("unchecked")
    public S remove(int hash, int key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (getKey(groupBase, lane) == key) {
                    int slot = (group << 3) + lane;
                    S oldValue = (S) values[slot];
                    
                    setKey(groupBase, lane, 0);
                    values[slot] = null;
                    
                    boolean hasEmpty = matchEmpty(ctrlWord) != 0;
                    if (hasEmpty) {
                        groupData[groupBase] = setCtrlByte(ctrlWord, lane, CTRL_EMPTY);
                        growthLeft++;
                    } else {
                        groupData[groupBase] = setCtrlByte(ctrlWord, lane, CTRL_DELETED);
                        tomb++;
                    }
                    used--;
                    
                    return oldValue;
                }
            }

            if (matchEmpty(ctrlWord) != 0) {
                return null;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    void putDirect(int hash, int key, S value) {
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int lane = Long.numberOfTrailingZeros(emptyMask) >>> 3;
                int slot = (group << 3) + lane;
                
                setKey(groupBase, lane, key);
                
                groupData[groupBase] = setCtrlByte(ctrlWord, lane, (byte) (hash & 0x7F));
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

    @SuppressWarnings("unchecked")
    public void rehash() {
        int cap = capacity;
        SwissTableInt<S> newTable = new SwissTableInt<>(cap);

        int groupCount = cap >>> 3;
        for (int g = 0; g < groupCount; g++) {
            int groupBase = g * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = Long.numberOfTrailingZeros(fullMask) >>> 3;
                int slot = (g << 3) + lane;
                
                int hash = hashes[slot];
                int keyVal = getKey(groupBase, lane);
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, value);
            }
        }

        this.groupData = newTable.groupData;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = maxOcc(cap) - used;
    }

    @SuppressWarnings("unchecked")
    public void grow() {
        int newCapacity = capacity * 2;
        SwissTableInt<S> newTable = new SwissTableInt<>(newCapacity);

        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            int groupBase = g * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = Long.numberOfTrailingZeros(fullMask) >>> 3;
                int slot = (g << 3) + lane;
                
                int hash = hashes[slot];
                int keyVal = getKey(groupBase, lane);
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, value);
            }
        }

        this.capacity = newCapacity;
        this.groupMask = newTable.groupMask;
        this.groupData = newTable.groupData;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = maxOcc(newCapacity) - used;
    }

    public int size() {
        return used;
    }

    public boolean isEmpty() {
        return used == 0;
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean containsKey(int hash, int key) {
        return get(hash, key) != null;
    }

    public void collectKeys(java.util.List<Integer> result) {
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            int groupBase = g * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = Long.numberOfTrailingZeros(fullMask) >>> 3;
                result.add(getKey(groupBase, lane));
            }
        }
    }

    public Iterator<Entry<S>> iterator() {
        return new EntryIterator();
    }

    public static class Entry<S> {
        private final int key;
        private final S state;

        Entry(int key, S state) {
            this.key = key;
            this.state = state;
        }

        public int getKey() { return key; }
        public Integer getKeyBoxed() { return key; }
        public S getState() { return state; }
    }

    private class EntryIterator implements Iterator<Entry<S>> {
        private int currentGroup = 0;
        private long currentFullMask;
        private int groupCount;

        EntryIterator() {
            this.groupCount = capacity >>> 3;
            advanceToNextFullSlot();
        }

        private void advanceToNextFullSlot() {
            while (currentGroup < groupCount) {
                if (currentFullMask == 0) {
                    int groupBase = currentGroup * LONGS_PER_GROUP;
                    currentFullMask = matchFull(groupData[groupBase]);
                }
                if (currentFullMask != 0) {
                    return;
                }
                currentGroup++;
            }
        }

        @Override
        public boolean hasNext() {
            return currentFullMask != 0;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<S> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            
            int lane = Long.numberOfTrailingZeros(currentFullMask) >>> 3;
            int groupBase = currentGroup * LONGS_PER_GROUP;
            int slot = (currentGroup << 3) + lane;
            
            int key = getKey(groupBase, lane);
            S value = (S) values[slot];
            
            currentFullMask &= currentFullMask - 1;
            if (currentFullMask == 0) {
                currentGroup++;
                advanceToNextFullSlot();
            }
            
            return new Entry<>(key, value);
        }
    }

    // ========== SWAR Matching ==========

    private static long matchH2(long ctrlWord, long h2Pattern) {
        long x = ctrlWord ^ h2Pattern;
        return (x - LSB) & ~x & MSB;
    }

    private static long matchEmptyOrDeleted(long ctrlWord) {
        return ctrlWord & MSB;
    }

    private static long matchFull(long ctrlWord) {
        return ~ctrlWord & MSB;
    }

    private static long matchEmpty(long ctrlWord) {
        return (ctrlWord & ~(ctrlWord << 6)) & MSB;
    }

    private static byte getCtrlByte(long ctrlWord, int lane) {
        return (byte) (ctrlWord >>> (lane << 3));
    }

    private static long setCtrlByte(long ctrlWord, int lane, byte value) {
        int shift = lane << 3;
        long mask = 0xFFL << shift;
        return (ctrlWord & ~mask) | ((value & 0xFFL) << shift);
    }

    private static int maxOcc(int capacity) {
        return (capacity * 7) >>> 3;
    }

    // ========== Key Access (packed 2 ints per long) ==========

    private int getKey(int groupBase, int lane) {
        return (int) (groupData[groupBase + 1 + (lane >>> 1)] >>> ((lane & 1) << 5));
    }

    private void setKey(int groupBase, int lane, int key) {
        int i = groupBase + 1 + (lane >>> 1), s = (lane & 1) << 5;
        groupData[i] = (groupData[i] & ~(0xFFFFFFFFL << s)) | ((key & 0xFFFFFFFFL) << s);
    }
}
