package org.apache.flink.runtime.state.heap;

/**
 * Abstract base class for SwissTable implementations.
 * 
 * <p>This class defines the interface contract for SwissTable.
 * Subclasses implement the actual storage layout and core operations.
 * 
 * <p>Key features:
 * <ul>
 *   <li>SWAR (SIMD Within A Register) parallel matching of 8 slots per group</li>
 *   <li>Control bytes for fast presence/absence checking</li>
 *   <li>Quadratic probing for collision resolution</li>
 *   <li>Support for incremental expansion (split)</li>
 * </ul>
 * 
 * <p>Subclasses should implement Group-Interleaved layout for optimal cache performance:
 * <ul>
 *   <li>groupData[]: long array with ctrl word + key/namespace data per group</li>
 *   <li>values[]: Object array, slot i → values[i]</li>
 *   <li>hashes[]: long array for rehash/grow operations</li>
 * </ul>
 * 
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
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

    // ========== SWAR constants for parallel matching (protected for subclass use) ==========
    protected static final long LSB = 0x0101010101010101L;
    protected static final long MSB = 0x8080808080808080L;
    
    /** All-empty ctrl word constant (8 CTRL_EMPTY bytes packed) */
    protected static final long EMPTY_CTRL_WORD = 0x8080808080808080L;

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

    // ========== Storage shared by all implementations ==========
    Object[] values;     // length = capacity
    long[] hashes;       // full 64-bit hash for each slot, used by rehash/grow

    // ========== Core Operations (abstract, implemented by subclasses) ==========

    /**
     * Gets the state value for the given hash, key and namespace.
     * 
     * @param hash full 64-bit hash
     * @param key the key
     * @param namespace the namespace
     * @return state value, or null if not found
     */
    abstract S get(long hash, K key, N namespace);

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
     * @param hash full 64-bit hash
     * @param key the key
     * @param namespace the namespace
     * @param maxTableCapacity maximum allowed table capacity before split
     * @return slot encoding or NEED_REHASH/NEED_GROW/NEED_SPLIT
     */
    abstract int put(long hash, K key, N namespace, int maxTableCapacity);

    /**
     * Removes the entry for the given key and namespace.
     * 
     * @param hash full 64-bit hash
     * @param key the key
     * @param namespace the namespace
     * @return the removed state value, or null if not found
     */
    abstract S remove(long hash, K key, N namespace);

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
    abstract void putDirect(long hash, K key, N namespace, S value);

    /**
     * Rehash with same capacity to clear tombstones.
     */
    abstract void rehash();

    /**
     * Grow table to double capacity.
     */
    abstract void grow();

    // ========== Iteration and Query Methods ==========

    /**
     * Counts entries in this table matching the given namespace.
     */
    abstract int countNamespace(Object namespace);

    /**
     * Gets the key at the given global slot index.
     * 
     * @param slot the global slot index
     * @return the key at the slot
     */
    abstract K getKey(int slot);

    /**
     * Gets the namespace at the given global slot index.
     * 
     * @param slot the global slot index
     * @return the namespace at the slot
     */
    abstract N getNamespace(int slot);

    /**
     * Checks if the slot at the given global index is full (contains data).
     * 
     * @param slot the global slot index
     * @return true if the slot is full
     */
    abstract boolean isSlotFull(int slot);

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

    // ========== SWAR Matching Algorithms (static, for subclass use) ==========

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

    // ========== Ctrl byte manipulation helpers for Group-Interleaved layout ==========

    /**
     * Gets the ctrl byte for a specific lane from a ctrl word.
     * 
     * @param ctrlWord the packed ctrl word (8 bytes as long)
     * @param lane the lane index (0-7)
     * @return the ctrl byte at that lane
     */
    static byte getCtrlByte(long ctrlWord, int lane) {
        return (byte) ((ctrlWord >>> (lane << 3)) & 0xFF);
    }

    /**
     * Sets the ctrl byte for a specific lane in a ctrl word.
     * 
     * @param ctrlWord the original ctrl word
     * @param lane the lane index (0-7)
     * @param ctrl the new ctrl byte value
     * @return the updated ctrl word
     */
    static long setCtrlByte(long ctrlWord, int lane, byte ctrl) {
        int shift = lane << 3;
        long mask = 0xFFL << shift;
        return (ctrlWord & ~mask) | ((ctrl & 0xFFL) << shift);
    }
}
