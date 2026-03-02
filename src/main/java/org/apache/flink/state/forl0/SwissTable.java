package org.apache.flink.state.forl0;

import org.apache.flink.state.forl0.utils.UnsafeAccess;

import sun.misc.Unsafe;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * SwissTable implementation for ForL0 State Backend.
 * 
 * <p>This is a self-contained hash table unit using Swiss Tables architecture (aligned with Go 1.24).
 * Multiple SwissTables are managed by ForL0StateStore, organized by namespace.
 * 
 * <p>Key features:
 * <ul>
 *   <li>SWAR (SIMD Within A Register) parallel matching of 8 slots per group</li>
 *   <li>Control bytes for fast presence/absence checking</li>
 *   <li>Quadratic probing for collision resolution</li>
 *   <li>Direct key/value storage per slot (namespace managed externally)</li>
 *   <li>32-bit hash aligned with hash-smith SwissMap</li>
 * </ul>
 * 
 * @param <K> key type
 * @param <S> state type
 */
@SuppressWarnings("restriction")
public class SwissTable<K, S> {

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
    static final byte CTRL_EMPTY = (byte) 0x80;
    static final byte CTRL_DELETED = (byte) 0xFE;
    // FULL: (byte) h2, range 0x00..0x7F

    // ========== SWAR constants for parallel matching ==========
    private static final long LSB = 0x0101010101010101L;
    private static final long MSB = 0x8080808080808080L;

    // ========== Unsafe for fast group loading ==========
    private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
    private static final long BYTE_ARRAY_BASE_OFFSET = UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    // ========== Counts ==========
    int used;          // FULL slot count
    int tomb;          // DELETED slot count (for O(1) tombstone check)
    int capacity;      // total slots (power of 2, multiple of 8)
    int growthLeft;    // remaining budget for inserting into EMPTY slots
    
    // ========== Group mask for probing ==========
    int groupMask;       // (capacity / 8) - 1
    
    // ========== Storage (AoS layout for cache locality) ==========
    byte[] ctrl;         // control bytes, length = capacity
    Object[] entries;    // interleaved [k0,v0,k1,v1,...], length = capacity * 2
    int[] hashes;        // 32-bit hash for each slot, used by rehash/grow (aligned with hash-smith)

    /**
     * Creates a new SwissTable with the given slot count.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     */
    public SwissTable(int slotCount) {
        if (slotCount < 8 || (slotCount & (slotCount - 1)) != 0 || (slotCount & 7) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2 and >= 8");
        }
        this.capacity = slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.ctrl = new byte[slotCount];
        Arrays.fill(this.ctrl, CTRL_EMPTY);  // Must initialize to 0x80
        this.entries = new Object[slotCount << 1];  // AoS: k0,v0,k1,v1,...
        this.hashes = new int[slotCount];
        this.used = 0;
        this.tomb = 0;
        // maxOcc = capacity * 7 / 8 = 87.5% load factor (aligned with Go 1.24)
        this.growthLeft = maxOcc(slotCount);
    }

    // ========== Hash Layout (aligned with hash-smith SwissMap) ==========
    // H1 = hash >>> 7  (high 25 bits for probe starting group)
    // H2 = hash & 0x7F (low 7 bits stored in ctrl byte)

    // ========== Core Operations ==========

    /**
     * Gets the state value for the given hash and key.
     * 
     * @param hash 32-bit hash
     * @param key the key
     * @return state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public S get(int hash, K key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            // Match H2 with precomputed pattern
            long match = matchH2(ctrlWord, h2Pattern);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                int idx = slot << 1;
                if (key.equals(entries[idx])) {
                    return (S) entries[idx + 1];
                }
                match = clearLowestBit(match);
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
     * <p>Return value encoding (aligned with hash-smith SwissMap):
     * <ul>
     *   <li>slot | NEW_FLAG: new entry inserted, caller should write values[slot]</li>
     *   <li>slot: existing entry found, caller can read/write values[slot]</li>
     *   <li>NEED_REHASH: table needs rehash to clear tombstones, caller should call rehash() and retry</li>
     *   <li>NEED_GROW: table needs to grow, caller should call grow() and retry</li>
     * </ul>
     * 
     * <p>Note: For new insertions, keys and ctrl are already filled.
     * The caller should write values[slot].
     * 
     * @param hash 32-bit hash
     * @param key the key
     * @return slot encoding or NEED_REHASH/NEED_GROW
     */
    public int put(int hash, K key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;
        int stride = 1;
        int firstDeletedSlot = -1;

        while (true) {  // Probe loop (aligned with hash-smith: single loop, signals for retry)
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            // 1. Look for existing key (with precomputed pattern)
            long match = matchH2(ctrlWord, h2Pattern);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                if (key.equals(entries[slot << 1])) {
                    // Found - return slot (no NEW_FLAG)
                    return slot;
                }
                match = clearLowestBit(match);
            }

            // 2. Record first DELETED position
            if (firstDeletedSlot == -1) {
                long delMask = matchDeleted(ctrlWord);
                if (delMask != 0) {
                    firstDeletedSlot = base + laneFromTz(Long.numberOfTrailingZeros(delMask));
                }
            }

            // 3. Check EMPTY (probe termination)
            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int emptySlot = base + laneFromTz(Long.numberOfTrailingZeros(emptyMask));

                // Determine insert position
                int insertSlot;
                boolean useDeleted;

                if (firstDeletedSlot != -1) {
                    insertSlot = firstDeletedSlot;
                    useDeleted = true;
                } else {
                    insertSlot = emptySlot;
                    useDeleted = false;

                    // Using EMPTY requires budget check
                    if (growthLeft == 0) {
                        // O(1) check for tombstones - return signal for caller to handle
                        if (tomb > 0) {
                            return NEED_REHASH;
                        }
                        // No tombstones, need to grow
                        return NEED_GROW;
                    }
                }

                // Execute insert (fill key/ctrl/hash, caller writes entries[(slot<<1)+1])
                int insertIdx = insertSlot << 1;
                entries[insertIdx] = key;
                // entries[insertIdx + 1] is set by caller
                ctrl[insertSlot] = (byte) (hash & 0x7F);
                hashes[insertSlot] = hash;  // Store hash for rehash/grow
                used++;

                if (useDeleted) {
                    tomb--;  // Reusing tombstone, growthLeft unchanged
                } else {
                    growthLeft--;
                }

                return insertSlot | NEW_FLAG;  // Return slot + new insert flag
            }

            // 4. Continue probing
            group = (group + stride) & groupMask;
            stride++;
        }
    }

    /**
     * Removes the entry for the given key.
     * 
     * @param hash 32-bit hash
     * @param key the key
     * @return the removed state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public S remove(int hash, K key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            // Match H2 with precomputed pattern
            long match = matchH2(ctrlWord, h2Pattern);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                int idx = slot << 1;
                if (key.equals(entries[idx])) {
                    // Found - determine delete strategy BEFORE modifying ctrl
                    boolean hasEmpty = matchEmpty(ctrlWord) != 0;

                    // Get old value
                    S oldValue = (S) entries[idx + 1];

                    // Clear slot data
                    entries[idx] = null;
                    entries[idx + 1] = null;

                    // Update ctrl and counts
                    if (hasEmpty) {
                        ctrl[slot] = CTRL_EMPTY;
                        used--;
                        growthLeft++;
                    } else {
                        ctrl[slot] = CTRL_DELETED;
                        used--;
                        tomb++;
                    }

                    return oldValue;
                }
                match = clearLowestBit(match);
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
     * <p>Assumption: key does not exist, and growthLeft > 0
     * 
     * @param hash 32-bit hash
     * @param key the key
     * @param value the value
     */
    public void putDirect(int hash, K key, S value) {
        int group = (hash >>> 7) & groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int slot = (group << 3) + laneFromTz(Long.numberOfTrailingZeros(emptyMask));
                int idx = slot << 1;
                entries[idx] = key;
                entries[idx + 1] = value;
                ctrl[slot] = (byte) (hash & 0x7F);  // h2
                hashes[slot] = hash;  // Store hash
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
     * Uses stored hashes[] for proper placement.
     */
    public void rehash() {
        int cap = capacity;
        byte[] newCtrl = new byte[cap];
        Arrays.fill(newCtrl, CTRL_EMPTY);
        Object[] newEntries = new Object[cap << 1];
        int[] newHashes = new int[cap];
        int newGroupMask = groupMask;

        // Reinsert all FULL entries using stored hash
        for (int i = 0; i < cap; i++) {
            if (isFull(ctrl[i])) {
                int idx = i << 1;
                Object key = entries[idx];
                Object value = entries[idx + 1];
                int hash = hashes[i];

                insertForRehash(newCtrl, newEntries, newHashes, newGroupMask, hash, key, value);
            }
        }

        ctrl = newCtrl;
        entries = newEntries;
        hashes = newHashes;
        tomb = 0;
        growthLeft = maxOcc(cap) - used;
    }

    /**
     * Insert entry into new arrays during rehash/grow.
     * Assumes slot does not exist and there is room.
     */
    private void insertForRehash(byte[] newCtrl, Object[] newEntries, int[] newHashes,
                                  int newGroupMask, int hash, Object key, Object value) {
        int group = (hash >>> 7) & newGroupMask;
        int stride = 1;

        while (true) {
            int base = group << 3;
            for (int j = 0; j < 8; j++) {
                int slot = base + j;
                if (newCtrl[slot] == CTRL_EMPTY) {
                    int idx = slot << 1;
                    newEntries[idx] = key;
                    newEntries[idx + 1] = value;
                    newHashes[slot] = hash;
                    newCtrl[slot] = (byte) (hash & 0x7F);  // h2
                    return;
                }
            }
            group = (group + stride) & newGroupMask;
            stride++;
        }
    }

    /**
     * Grow table to double capacity.
     * Uses stored hashes[] for proper placement.
     */
    public void grow() {
        int newCapacity = capacity * 2;
        byte[] newCtrl = new byte[newCapacity];
        Arrays.fill(newCtrl, CTRL_EMPTY);
        Object[] newEntries = new Object[newCapacity << 1];
        int[] newHashes = new int[newCapacity];
        int newGroupMask = (newCapacity >>> 3) - 1;

        // Reinsert all FULL entries using stored hash
        for (int i = 0; i < capacity; i++) {
            if (isFull(ctrl[i])) {
                int idx = i << 1;
                Object key = entries[idx];
                Object value = entries[idx + 1];
                int hash = hashes[i];

                insertForRehash(newCtrl, newEntries, newHashes, newGroupMask, hash, key, value);
            }
        }

        capacity = newCapacity;
        groupMask = newGroupMask;
        ctrl = newCtrl;
        entries = newEntries;
        hashes = newHashes;
        tomb = 0;
        growthLeft = maxOcc(newCapacity) - used;
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
    public boolean containsKey(int hash, K key) {
        return get(hash, key) != null;
    }

    // ========== Iteration Support ==========

    /**
     * Functional interface for zero-allocation entry traversal.
     */
    @FunctionalInterface
    public interface EntryConsumer<K, S> {
        void accept(K key, S value) throws Exception;
    }

    /**
     * Iterates over all entries without allocating any objects.
     * Uses SWAR parallel matching to skip empty/deleted groups efficiently.
     *
     * @param consumer callback for each key-value pair
     */
    @SuppressWarnings("unchecked")
    public <E extends Exception> void forEachEntry(EntryConsumer<K, S> consumer) throws E {
        int groupCount = capacity >>> 3;
        for (int g = 0; g < groupCount; g++) {
            long ctrlWord = loadCtrlWord(g);
            for (long fullMask = matchFull(ctrlWord); fullMask != 0; fullMask &= fullMask - 1) {
                int lane = (int) (Long.numberOfTrailingZeros(fullMask) >>> 3);
                int slot = (g << 3) + lane;
                int idx = slot << 1;
                try {
                    consumer.accept((K) entries[idx], (S) entries[idx + 1]);
                } catch (Exception e) {
                    throw (E) e;
                }
            }
        }
    }

    /**
     * Collects all keys into the given list.
     */
    @SuppressWarnings("unchecked")
    public void collectKeys(java.util.List<K> result) {
        for (int i = 0; i < capacity; i++) {
            if (isFull(ctrl[i])) {
                result.add((K) entries[i << 1]);
            }
        }
    }

    /**
     * Returns an iterator over all entries in this table.
     */
    public Iterator<Entry<K, S>> iterator() {
        return new EntryIterator();
    }

    /**
     * Entry class for iteration.
     */
    public static class Entry<K, S> {
        private final K key;
        private final S state;

        Entry(K key, S state) {
            this.key = key;
            this.state = state;
        }

        public K getKey() { return key; }
        public S getState() { return state; }
    }

    /**
     * Iterator over all entries in the table.
     * Uses SWAR parallel matching to efficiently skip empty/deleted groups.
     */
    private class EntryIterator implements Iterator<Entry<K, S>> {
        private int currentGroup = 0;
        private long currentFullMask;
        private final int groupCount;

        EntryIterator() {
            this.groupCount = capacity >>> 3;
            advanceToNextFullSlot();
        }

        private void advanceToNextFullSlot() {
            while (currentGroup < groupCount) {
                if (currentFullMask == 0) {
                    currentFullMask = matchFull(loadCtrlWord(currentGroup));
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
        public Entry<K, S> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int lane = (int) (Long.numberOfTrailingZeros(currentFullMask) >>> 3);
            int slot = (currentGroup << 3) + lane;
            int idx = slot << 1;
            K key = (K) entries[idx];
            S value = (S) entries[idx + 1];
            
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
     * Loads a control word (8 control bytes) from the given group index.
     * Uses Unsafe for efficient unaligned load.
     */
    long loadCtrlWord(int groupIdx) {
        return UNSAFE.getLong(ctrl, BYTE_ARRAY_BASE_OFFSET + ((long) groupIdx << 3));
    }

    /**
     * H2 exact match: SWAR equality comparison with precomputed pattern.
     * pattern = LSB * (h2 & 0xFFL), should be computed outside the probe loop.
     * Returns a bitmask with MSB set for each matching lane.
     */
    static long matchH2(long ctrlWord, long pattern) {
        long x = ctrlWord ^ pattern;
        return (x - LSB) & ~x & MSB;
    }

    /**
     * Match empty or deleted slots: MSB=1 (bit trick).
     */
    static long matchEmptyOrDeleted(long ctrlWord) {
        return ctrlWord & MSB;
    }

    /**
     * Match full slots: MSB=0.
     */
    static long matchFull(long ctrlWord) {
        return ~ctrlWord & MSB;
    }

    /**
     * Match empty slots: bit7=1 and bit1=0 (bit trick).
     */
    static long matchEmpty(long ctrlWord) {
        return (ctrlWord & ~(ctrlWord << 6)) & MSB;
    }

    /**
     * Match deleted slots: bit7=1 and bit1=1 (bit trick).
     */
    static long matchDeleted(long ctrlWord) {
        return (ctrlWord & (ctrlWord << 6)) & MSB;
    }

    /**
     * Extracts the lane index (0-7) from a match bitmask.
     */
    static int laneFromTz(int trailingZeros) {
        return trailingZeros >>> 3;
    }

    /**
     * Clears the lowest set bit in the mask.
     */
    static long clearLowestBit(long mask) {
        return mask & (mask - 1);
    }

    /**
     * Calculates maxOcc (maximum occupancy) for a given capacity.
     * maxOcc = capacity * 7 / 8 = 87.5% load factor (aligned with Go 1.24 SwissTable).
     */
    static int maxOcc(int capacity) {
        return (capacity * 7) >> 3;
    }

    /**
     * Checks if a control byte represents a FULL slot.
     */
    static boolean isFull(byte ctrl) {
        return (ctrl & 0x80) == 0;
    }

    /**
     * Checks if a control byte represents an EMPTY slot.
     */
    static boolean isEmpty(byte ctrl) {
        return ctrl == CTRL_EMPTY;
    }

    /**
     * Checks if a control byte represents a DELETED slot.
     */
    static boolean isDeleted(byte ctrl) {
        return ctrl == CTRL_DELETED;
    }
}
