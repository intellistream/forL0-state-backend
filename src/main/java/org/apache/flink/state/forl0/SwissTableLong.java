package org.apache.flink.state.forl0;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Specialized SwissTable for Long keys with Group-Interleaved layout.
 * 
 * <p>This implementation eliminates Pointer Chasing by storing keys directly as primitives:
 * <ul>
 *   <li>groupData[]: long array with ctrl word + 8 keys per group</li>
 *   <li>Layout: [ctrl0|k0|k1|...|k7][ctrl1|k0|k1|...|k7]...</li>
 *   <li>Each group occupies 9 longs (72 bytes, fits in cache line)</li>
 * </ul>
 * 
 * <p>Key comparison uses primitive == instead of Object.equals(), avoiding:
 * <ul>
 *   <li>Pointer Chasing to fetch key object</li>
 *   <li>Virtual method call to equals()</li>
 * </ul>
 * 
 * @param <S> state type
 */
public class SwissTableLong<S> {

    // ========== Return value encoding for put() ==========
    /** Flag indicating a new entry was inserted (high bit). */
    public static final int NEW_FLAG = 1 << 16;
    /** Mask to extract slot index from return value. */
    public static final int SLOT_MASK = 0xFFFF;
    /** Special return value indicating the table needs to rehash (clear tombstones). */
    public static final int NEED_REHASH = -1;
    /** Special return value indicating the table needs to grow. */
    public static final int NEED_GROW = -2;

    // ========== Control byte values ==========
    private static final byte CTRL_EMPTY = (byte) 0x80;
    private static final byte CTRL_DELETED = (byte) 0xFE;
    // FULL: (byte) h2, range 0x00..0x7F

    // ========== SWAR constants for parallel matching ==========
    private static final long LSB = 0x0101010101010101L;
    private static final long MSB = 0x8080808080808080L;
    
    /** All-empty ctrl word constant (8 CTRL_EMPTY bytes packed). */
    private static final long EMPTY_CTRL_WORD = 0x8080808080808080L;

    /** Number of longs per group: 1 ctrl word + 8 keys. */
    private static final int LONGS_PER_GROUP = 9;

    // ========== Counts ==========
    int used;          // FULL slot count
    int tomb;          // DELETED slot count (for O(1) tombstone check)
    int capacity;      // total slots (power of 2, multiple of 8)
    int growthLeft;    // remaining budget for inserting into EMPTY slots
    
    // ========== Group mask for probing ==========
    int groupMask;     // (capacity / 8) - 1

    // ========== Group-Interleaved Storage ==========
    /** [ctrl|k0|k1|...|k7] per group, each group 9 longs. */
    long[] groupData;
    
    /** State values, slot i → values[i]. */
    Object[] values;
    
    /** 32-bit hash for each slot, used by rehash/grow. */
    int[] hashes;

    /**
     * Creates a new SwissTableLong with the given slot count.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     */
    public SwissTableLong(int slotCount) {
        if (slotCount < 8 || (slotCount & (slotCount - 1)) != 0 || (slotCount & 7) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2 and >= 8");
        }
        this.capacity = slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.used = 0;
        this.tomb = 0;
        this.growthLeft = maxOcc(slotCount);
        
        // Initialize Group-Interleaved storage
        int groupCount = slotCount >>> 3;
        this.groupData = new long[groupCount * LONGS_PER_GROUP];
        // Set ctrl words to EMPTY (only ctrl words, not key slots)
        for (int g = 0; g < groupCount; g++) {
            groupData[g * LONGS_PER_GROUP] = EMPTY_CTRL_WORD;
        }
        this.values = new Object[slotCount];
        this.hashes = new int[slotCount];
    }

    // ========== Core Operations ==========

    /**
     * Gets the state value for the given hash and key.
     * 
     * @param hash 32-bit hash
     * @param key the primitive long key
     * @return state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public S get(int hash, long key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            // SWAR parallel matching: find all slots with matching h2
            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (groupData[groupBase + 1 + lane] == key) {
                    return (S) values[(group << 3) + lane];
                }
            }

            // Check for empty (early stop)
            if (matchEmpty(ctrlWord) != 0) {
                return null;
            }

            // Continue probing
            group = (group + stride) & groupMask;
            stride++;
        }
    }

    /**
     * Finds or inserts an entry for the given key.
     * 
     * <p>Return value encoding:
     * <ul>
     *   <li>slot | NEW_FLAG: new entry inserted, caller should write values[slot]</li>
     *   <li>slot: existing entry found, caller can read/write values[slot]</li>
     *   <li>NEED_REHASH: table needs rehash, caller should call rehash() and retry</li>
     *   <li>NEED_GROW: table needs grow, caller should call grow() and retry</li>
     * </ul>
     * 
     * @param hash 32-bit hash
     * @param key the primitive long key
     * @return slot encoding or NEED_REHASH/NEED_GROW
     */
    public int put(int hash, long key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;
        int firstDeletedSlot = -1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            // 1. Look for existing key
            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (groupData[groupBase + 1 + lane] == key) {
                    return (group << 3) + lane;  // Existing, return slot
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
            byte ctrlByte = getCtrlByte(ctrlWord, lane);
            
            if (ctrlByte == CTRL_DELETED) {
                if (firstDeletedSlot == -1) {
                    firstDeletedSlot = (group << 3) + lane;
                }
                group = (group + stride) & groupMask;
                stride++;
                continue;
            }

            // 4. Found EMPTY - insert
            int insertSlot = (firstDeletedSlot != -1) ? firstDeletedSlot : (group << 3) + lane;
            boolean useDeleted = firstDeletedSlot != -1;

            if (!useDeleted && growthLeft == 0) {
                if (tomb > 0) return NEED_REHASH;
                return NEED_GROW;
            }

            // Write to insert slot
            int insertGroup = insertSlot >>> 3;
            int insertLane = insertSlot & 7;
            int insertBase = insertGroup * LONGS_PER_GROUP;
            
            groupData[insertBase + 1 + insertLane] = key;
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

    /**
     * Removes the entry for the given key.
     * 
     * @param hash 32-bit hash
     * @param key the primitive long key
     * @return the removed state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public S remove(int hash, long key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            // Match H2
            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int lane = Long.numberOfTrailingZeros(match) >>> 3;
                if (groupData[groupBase + 1 + lane] == key) {
                    int slot = (group << 3) + lane;
                    S oldValue = (S) values[slot];
                    
                    // Clear slot data
                    groupData[groupBase + 1 + lane] = 0L;
                    values[slot] = null;
                    
                    // Determine delete strategy
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

            // Check for empty (early stop)
            if (matchEmpty(ctrlWord) != 0) {
                return null;
            }

            // Continue probing
            group = (group + stride) & groupMask;
            stride++;
        }
    }

    /**
     * Direct insert, skipping duplicate check. 
     * Only used for rehash/grow data migration.
     * 
     * @param hash 32-bit hash
     * @param key the primitive long key
     * @param value the value
     */
    void putDirect(int hash, long key, S value) {
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            int groupBase = group * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];

            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int lane = Long.numberOfTrailingZeros(emptyMask) >>> 3;
                int slot = (group << 3) + lane;
                
                groupData[groupBase + 1 + lane] = key;
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

    // ========== Expansion Operations ==========

    /**
     * Rehash with same capacity to clear tombstones.
     */
    @SuppressWarnings("unchecked")
    public void rehash() {
        int cap = capacity;
        SwissTableLong<S> newTable = new SwissTableLong<>(cap);

        // Reinsert all FULL entries
        int groupCount = cap >>> 3;
        for (int g = 0; g < groupCount; g++) {
            int groupBase = g * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = Long.numberOfTrailingZeros(fullMask) >>> 3;
                int slot = (g << 3) + lane;
                
                int hash = hashes[slot];
                long keyVal = groupData[groupBase + 1 + lane];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, value);
            }
        }

        // Copy new table state
        this.groupData = newTable.groupData;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = maxOcc(cap) - used;
    }

    /**
     * Grow table to double capacity.
     */
    @SuppressWarnings("unchecked")
    public void grow() {
        int newCapacity = capacity * 2;
        SwissTableLong<S> newTable = new SwissTableLong<>(newCapacity);

        // Reinsert all FULL entries
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            int groupBase = g * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = Long.numberOfTrailingZeros(fullMask) >>> 3;
                int slot = (g << 3) + lane;
                
                int hash = hashes[slot];
                long keyVal = groupData[groupBase + 1 + lane];
                S value = (S) values[slot];
                
                newTable.putDirect(hash, keyVal, value);
            }
        }

        // Copy new table state
        this.capacity = newCapacity;
        this.groupMask = newTable.groupMask;
        this.groupData = newTable.groupData;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = maxOcc(newCapacity) - used;
    }

    // ========== Getters ==========

    /**
     * Gets the number of entries in this table.
     */
    public int size() {
        return used;
    }

    /**
     * Checks if this table is empty.
     */
    public boolean isEmpty() {
        return used == 0;
    }

    /**
     * Gets the capacity of this table.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Checks if a key exists.
     */
    public boolean containsKey(int hash, long key) {
        return get(hash, key) != null;
    }

    // ========== Iteration Support ==========

    /**
     * Collects all keys into the given list.
     * Keys are boxed to Long for iteration.
     */
    public void collectKeys(java.util.List<Long> result) {
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            int groupBase = g * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];
            
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = Long.numberOfTrailingZeros(fullMask) >>> 3;
                result.add(groupData[groupBase + 1 + lane]);
            }
        }
    }

    /**
     * Functional interface for zero-allocation entry traversal (Long keys).
     */
    @FunctionalInterface
    public interface EntryConsumer<S> {
        void accept(long key, S value) throws Exception;
    }

    /**
     * Iterates over all entries without allocating any objects.
     * Uses SWAR parallel matching to skip empty/deleted groups efficiently.
     *
     * @param consumer callback for each key-value pair
     */
    @SuppressWarnings("unchecked")
    public <E extends Exception> void forEachEntry(EntryConsumer<S> consumer) throws E {
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            int groupBase = g * LONGS_PER_GROUP;
            long ctrlWord = groupData[groupBase];
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = Long.numberOfTrailingZeros(fullMask) >>> 3;
                int slot = (g << 3) + lane;
                long key = groupData[groupBase + 1 + lane];
                try {
                    consumer.accept(key, (S) values[slot]);
                } catch (Exception e) {
                    throw (E) e;
                }
            }
        }
    }

    /**
     * Returns an iterator over all entries in this table.
     */
    public Iterator<Entry<S>> iterator() {
        return new EntryIterator();
    }

    /**
     * Entry class for iteration.
     */
    public static class Entry<S> {
        private final long key;
        private final S state;

        Entry(long key, S state) {
            this.key = key;
            this.state = state;
        }

        public long getKey() { return key; }
        public Long getKeyBoxed() { return key; }
        public S getState() { return state; }
    }

    /**
     * Iterator over all entries in the table.
     */
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
            
            long key = groupData[groupBase + 1 + lane];
            S value = (S) values[slot];
            
            // Clear this bit and advance
            currentFullMask &= currentFullMask - 1;
            if (currentFullMask == 0) {
                currentGroup++;
                advanceToNextFullSlot();
            }
            
            return new Entry<>(key, value);
        }
    }

    // ========== SWAR Matching Algorithms ==========

    /**
     * H2 exact match: SWAR equality comparison.
     * Returns a bitmask with MSB set for each matching lane.
     */
    private static long matchH2(long ctrlWord, long h2Pattern) {
        long x = ctrlWord ^ h2Pattern;
        return (x - LSB) & ~x & MSB;
    }

    /**
     * Match empty or deleted slots: MSB=1 (bit trick).
     */
    private static long matchEmptyOrDeleted(long ctrlWord) {
        return ctrlWord & MSB;
    }

    /**
     * Match full slots: MSB=0.
     */
    private static long matchFull(long ctrlWord) {
        return ~ctrlWord & MSB;
    }

    /**
     * Match empty slots: bit7=1 and bit1=0 (bit trick).
     */
    private static long matchEmpty(long ctrlWord) {
        return (ctrlWord & ~(ctrlWord << 6)) & MSB;
    }

    /**
     * Gets a control byte from the control word.
     */
    private static byte getCtrlByte(long ctrlWord, int lane) {
        return (byte) (ctrlWord >>> (lane << 3));
    }

    /**
     * Sets a control byte in the control word.
     */
    private static long setCtrlByte(long ctrlWord, int lane, byte value) {
        int shift = lane << 3;
        long mask = 0xFFL << shift;
        return (ctrlWord & ~mask) | ((value & 0xFFL) << shift);
    }

    /**
     * Calculates maxOcc (maximum occupancy) for a given capacity.
     * maxOcc = capacity * 7 / 8 = 87.5% load factor.
     */
    private static int maxOcc(int capacity) {
        return (capacity * 7) >>> 3;
    }
}
