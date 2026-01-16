package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.utils.UnsafeAccess;

import sun.misc.Unsafe;

import java.util.Arrays;

/**
 * Abstract base class for SwissTable implementations.
 * 
 * <p>This class contains all the common SWAR matching, probing, and expansion logic.
 * Subclasses only need to implement key/namespace storage and comparison methods.
 * 
 * <p>Key features:
 * <ul>
 *   <li>SWAR (SIMD Within A Register) parallel matching of 8 slots per group</li>
 *   <li>Control bytes for fast presence/absence checking</li>
 *   <li>Quadratic probing for collision resolution</li>
 *   <li>Direct key/namespace/value storage per slot</li>
 *   <li>Support for incremental expansion (split)</li>
 * </ul>
 * 
 * <p>Memory layout:
 * <ul>
 *   <li>ctrl[]: byte array, one control byte per slot (EMPTY=0x80, DELETED=0xFE, FULL=0x00-0x7F)</li>
 *   <li>values[]: Object array, slot i → values[i]</li>
 *   <li>Key/namespace storage is defined by subclasses</li>
 * </ul>
 * 
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
@SuppressWarnings("restriction")
abstract class AbstractSwissTable<K, N, S> {

    // ========== Return value encoding for put() ==========
    /** Flag indicating a new entry was inserted (high bit) */
    static final int NEW_FLAG = 1 << 16;
    /** Mask to extract slot index from return value */
    static final int SLOT_MASK = 0xFFFF;
    /** Special return value indicating the table needs to split */
    static final int NEED_SPLIT = -1;
    /** Special return value indicating the table needs to rehash (clear tombstones) */
    static final int NEED_REHASH = -2;
    /** Special return value indicating the table needs to grow */
    static final int NEED_GROW = -3;

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
    short used;          // FULL slot count
    short tomb;          // DELETED slot count (for O(1) tombstone check)
    short capacity;      // total slots (power of 2, multiple of 8)
    short growthLeft;    // remaining budget for inserting into EMPTY slots
    
    // ========== Directory info (Go style) ==========
    byte localDepth;     // local depth for directory split
    int index;           // starting index in the directory (Go style deduplication)
    
    // ========== Group mask for probing ==========
    int groupMask;       // (capacity / 8) - 1
    
    // ========== Storage (direct slot indexing) ==========
    byte[] ctrl;         // control bytes, length = capacity
    Object[] values;     // length = capacity
    long[] hashes;       // full 64-bit hash for each slot, used by rehash/grow

    /**
     * Creates a new SwissTable with the given slot count and local depth.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     */
    AbstractSwissTable(int slotCount, byte localDepth, int index) {
        if (slotCount < 8 || (slotCount & (slotCount - 1)) != 0 || (slotCount & 7) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2 and >= 8");
        }
        this.capacity = (short) slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.localDepth = localDepth;
        this.index = index;
        this.ctrl = new byte[slotCount];
        Arrays.fill(this.ctrl, CTRL_EMPTY);  // Must initialize to 0x80
        this.values = new Object[slotCount];
        this.hashes = new long[slotCount];
        this.used = 0;
        this.tomb = 0;
        // maxOcc = capacity * 7 / 8
        this.growthLeft = (short) maxOcc(slotCount);
        
        // Initialize subclass-specific storage
        initStorage(slotCount);
    }

    // ========== Abstract methods for key/namespace handling ==========

    /**
     * Compares the key and namespace at the given slot with the provided key and namespace.
     * 
     * @param slot the slot index
     * @param key the key to compare
     * @param namespace the namespace to compare
     * @return true if the slot contains the same key and namespace
     */
    abstract boolean keyEquals(int slot, K key, N namespace);

    /**
     * Stores the key and namespace at the given slot.
     * 
     * @param slot the slot index
     * @param key the key to store
     * @param namespace the namespace to store
     */
    abstract void storeKeyNs(int slot, K key, N namespace);

    /**
     * Gets the key at the given slot.
     * 
     * @param slot the slot index
     * @return the key at the slot
     */
    abstract K getKey(int slot);

    /**
     * Gets the namespace at the given slot.
     * 
     * @param slot the slot index
     * @return the namespace at the slot
     */
    abstract N getNamespace(int slot);

    /**
     * Initializes the key/namespace storage arrays for the given capacity.
     * Called during construction.
     * 
     * @param capacity the number of slots
     */
    abstract void initStorage(int capacity);

    /**
     * Copies key/namespace data from one slot in this table to another slot in the target table.
     * Used during rehash/grow/split operations.
     * 
     * @param fromSlot the source slot in this table
     * @param target the target table
     * @param toSlot the destination slot in the target table
     */
    abstract void copySlot(int fromSlot, AbstractSwissTable<K, N, S> target, int toSlot);

    /**
     * Clears the key/namespace data at the given slot.
     * 
     * @param slot the slot index
     */
    abstract void clearSlot(int slot);

    /**
     * Creates a new table of the same type with the given parameters.
     * Used during grow/split operations.
     * 
     * @param slotCount number of slots
     * @param localDepth local depth for directory management
     * @param index starting index in the directory
     * @return a new table of the same specialized type
     */
    abstract AbstractSwissTable<K, N, S> createNew(int slotCount, byte localDepth, int index);

    // ========== Hash functions (aligned with Go 1.24) ==========

    /**
     * H1: High 57 bits, used for probe starting group.
     * Aligns with Go 1.24: h1(hash) = hash >>> 7
     */
    static int h1(long hash) {
        return (int)(hash >>> 7);
    }

    /**
     * H2: Low 7 bits, stored in ctrl byte.
     * Aligns with Go 1.24: h2(hash) = hash & 0x7F
     */
    static int h2(long hash) {
        return (int)(hash & 0x7F);
    }

    // ========== Core Operations ==========

    /**
     * Gets the state value for the given hash, key and namespace.
     * 
     * @param hash full 64-bit hash
     * @param key the key
     * @param namespace the namespace
     * @return state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    S get(long hash, K key, N namespace) {
        int h2Hash = h2(hash);
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            // Match H2
            long match = matchH2(ctrlWord, h2Hash);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                if (keyEquals(slot, key, namespace)) {
                    return (S) values[slot];
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
     * Finds or inserts an entry for the given key and namespace.
     * 
     * <p>Return value encoding (aligned with Go 1.24 Swiss Tables):
     * <ul>
     *   <li>slot | NEW_FLAG: new entry inserted, caller should write values[slot]</li>
     *   <li>slot: existing entry found, caller can read/write values[slot]</li>
     *   <li>NEED_REHASH: table needs rehash to clear tombstones, caller should call rehash() and retry</li>
     *   <li>NEED_GROW: table needs to grow, caller should call grow() and retry</li>
     *   <li>NEED_SPLIT: table is at max capacity and needs to split</li>
     * </ul>
     * 
     * <p>Note: For new insertions, key/ns and ctrl are already filled.
     * The caller should write values[slot].
     * 
     * @param hash full 64-bit hash
     * @param key the key
     * @param namespace the namespace
     * @param maxTableCapacity maximum allowed table capacity before split
     * @return slot encoding or NEED_REHASH/NEED_GROW/NEED_SPLIT
     */
    int put(long hash, K key, N namespace, int maxTableCapacity) {
        int h2Hash = h2(hash);
        int group = h1(hash) & groupMask;
        int stride = 1;
        int firstDeletedSlot = -1;

        while (true) {  // Probe loop (aligned with Go 1.24: single loop, signals for retry)
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            // 1. Look for existing key
            long match = matchH2(ctrlWord, h2Hash);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                if (keyEquals(slot, key, namespace)) {
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
                        if (capacity < maxTableCapacity) {
                            return NEED_GROW;
                        }
                        // Already at max capacity, need split
                        return NEED_SPLIT;
                    }
                }

                // Execute insert (fill key/ns/ctrl/hash, caller writes value)
                storeKeyNs(insertSlot, key, namespace);
                // values[insertSlot] is set by caller
                ctrl[insertSlot] = (byte) h2Hash;
                hashes[insertSlot] = hash;  // Store full hash for rehash/grow
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
     * Removes the entry for the given key and namespace.
     * 
     * @param hash full 64-bit hash
     * @param key the key
     * @param namespace the namespace
     * @return the removed state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    S remove(long hash, K key, N namespace) {
        int h2Hash = h2(hash);
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            // Match H2
            long match = matchH2(ctrlWord, h2Hash);
            while (match != 0) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                if (keyEquals(slot, key, namespace)) {
                    // Found - determine delete strategy BEFORE modifying ctrl
                    boolean hasEmpty = matchEmpty(ctrlWord) != 0;

                    // Get old value
                    S oldValue = (S) values[slot];

                    // Clear slot data
                    clearSlot(slot);
                    values[slot] = null;

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
     * Only used for split/rehash/grow data migration.
     * 
     * <p>Assumption: key does not exist, and growthLeft > 0
     * 
     * @param hash full 64-bit hash
     * @param key the key
     * @param namespace the namespace
     * @param value the value
     */
    void putDirect(long hash, K key, N namespace, S value) {
        int h2Hash = h2(hash);
        int group = h1(hash) & groupMask;
        int stride = 1;

        while (true) {
            int base = group << 3;
            for (int j = 0; j < 8; j++) {
                int slot = base + j;
                if (ctrl[slot] == CTRL_EMPTY) {
                    storeKeyNs(slot, key, namespace);
                    values[slot] = value;
                    ctrl[slot] = (byte) h2Hash;
                    hashes[slot] = hash;  // Store full hash
                    used++;
                    growthLeft--;
                    return;
                }
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
    @SuppressWarnings("unchecked")
    void rehash() {
        int cap = capacity;
        AbstractSwissTable<K, N, S> newTable = createNew(cap, localDepth, index);

        // Reinsert all FULL entries using stored hash
        for (int i = 0; i < cap; i++) {
            if (isFull(ctrl[i])) {
                long hash = hashes[i];
                K key = getKey(i);
                N ns = getNamespace(i);
                S value = (S) values[i];
                
                insertForRehash(newTable, hash, key, ns, value);
            }
        }

        // Copy new table state to this table
        this.ctrl = newTable.ctrl;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        copyStorageFrom(newTable);
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(cap) - used);
    }

    /**
     * Insert entry into new table during rehash/grow.
     * Assumes slot does not exist and there is room.
     */
    private void insertForRehash(AbstractSwissTable<K, N, S> newTable, long hash, K key, N ns, S value) {
        int h2Hash = h2(hash);
        int group = h1(hash) & newTable.groupMask;
        int stride = 1;

        while (true) {
            int base = group << 3;
            for (int j = 0; j < 8; j++) {
                int slot = base + j;
                if (newTable.ctrl[slot] == CTRL_EMPTY) {
                    newTable.storeKeyNs(slot, key, ns);
                    newTable.values[slot] = value;
                    newTable.hashes[slot] = hash;
                    newTable.ctrl[slot] = (byte) h2Hash;
                    return;
                }
            }
            group = (group + stride) & newTable.groupMask;
            stride++;
        }
    }

    /**
     * Copies storage arrays from another table. Used after rehash/grow.
     * Subclasses should override this to copy their specific storage arrays.
     */
    abstract void copyStorageFrom(AbstractSwissTable<K, N, S> other);

    /**
     * Grow table to double capacity.
     * Uses stored hashes[] for proper placement.
     */
    @SuppressWarnings("unchecked")
    void grow() {
        int newCapacity = capacity * 2;
        AbstractSwissTable<K, N, S> newTable = createNew(newCapacity, localDepth, index);

        // Reinsert all FULL entries using stored hash
        for (int i = 0; i < capacity; i++) {
            if (isFull(ctrl[i])) {
                long hash = hashes[i];
                K key = getKey(i);
                N ns = getNamespace(i);
                S value = (S) values[i];

                insertForRehash(newTable, hash, key, ns, value);
            }
        }

        // Copy new table state to this table
        this.capacity = (short) newCapacity;
        this.groupMask = newTable.groupMask;
        this.ctrl = newTable.ctrl;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        copyStorageFrom(newTable);
        this.tomb = 0;
        this.growthLeft = (short) (maxOcc(newCapacity) - used);
    }

    // ========== Utility Methods for Iteration ==========

    /**
     * Counts entries in this table matching the given namespace.
     */
    int countNamespace(Object namespace) {
        int count = 0;
        for (int i = 0; i < capacity; i++) {
            if (isFull(ctrl[i])) {
                if (namespace.equals(getNamespace(i))) {
                    count++;
                }
            }
        }
        return count;
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
     * H2 exact match: SWAR equality comparison.
     * Returns a bitmask with MSB set for each matching lane.
     * 
     * @param ctrlWord 8 control bytes packed as a long
     * @param h2 hash fragment (0..127)
     * @return bitmask with bit 7, 15, 23, ... set for matching lanes
     */
    static long matchH2(long ctrlWord, int h2) {
        long pattern = LSB * (h2 & 0xFFL);
        long x = ctrlWord ^ pattern;
        return (x - LSB) & ~x & MSB;
    }

    /**
     * Match empty or deleted slots: MSB=1 (bit trick).
     * Both EMPTY (0x80) and DELETED (0xFE) have their MSB set.
     */
    static long matchEmptyOrDeleted(long ctrlWord) {
        return ctrlWord & MSB;
    }

    /**
     * Match full slots: MSB=0.
     * FULL slots have h2 values 0x00-0x7F, all with MSB=0.
     */
    static long matchFull(long ctrlWord) {
        return ~ctrlWord & MSB;
    }

    /**
     * Match empty slots: bit7=1 and bit1=0 (bit trick).
     * EMPTY = 0x80 = 0b1000_0000 (bit1=0)
     * DELETED = 0xFE = 0b1111_1110 (bit1=1)
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
     * The bitmask has the MSB of each matching byte set.
     * 
     * @param trailingZeros result of Long.numberOfTrailingZeros(mask)
     * @return lane index 0-7
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
     * maxOcc = capacity * 11 / 16 ≈ 68.75% load factor.
     */
    static int maxOcc(int capacity) {
        return capacity * 11 / 16;
    }

    /**
     * Checks if a control byte represents a FULL slot.
     * FULL slots have h2 values 0x00-0x7F (MSB=0).
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
