package org.apache.flink.runtime.state.heap;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * SwissTable implementation for ForL0 State Backend.
 * 
 * <p>This is a single hash table unit in the Swiss Tables architecture.
 * Multiple SwissTables are managed by a directory in {@link SwissMap}.
 * 
 * <p>Key features:
 * <ul>
 *   <li>SWAR (SIMD Within A Register) parallel matching of 8 slots per group</li>
 *   <li>Control bytes for fast presence/absence checking</li>
 *   <li>Quadratic probing for collision resolution</li>
 *   <li>Support for incremental expansion (split)</li>
 * </ul>
 * 
 * <p>Memory layout:
 * <ul>
 *   <li>ctrl[]: byte array, one control byte per slot (EMPTY=0x80, DELETED=0xFE, FULL=0x00-0x7F)</li>
 *   <li>slots[]: int array, stores entryId+1 (0 means empty/deleted)</li>
 * </ul>
 */
@SuppressWarnings("restriction")
class SwissTable {

    // Control byte values
    static final byte CTRL_EMPTY = (byte) 0x80;
    static final byte CTRL_DELETED = (byte) 0xFE;
    // FULL: (byte) h2, range 0x00..0x7F

    // SWAR constants for parallel matching
    private static final long LSB = 0x0101010101010101L;
    private static final long MSB = 0x8080808080808080L;

    // Unsafe for fast group loading
    private static final Unsafe UNSAFE;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Unsafe instance", e);
        }
    }

    // Counts
    short used;          // FULL slot count
    short tomb;          // DELETED slot count
    short capacity;      // slotCount = groupCount * 8
    short growthLeft;    // remaining budget for inserting into EMPTY slots
    int groupMask;       // groupCount - 1, for group index modulo
    byte localDepth;     // local depth for directory split

    // Storage
    byte[] ctrl;         // control bytes, length = capacity
    int[] slots;         // slot pointers, length = capacity, value = entryId+1 (0 = invalid)

    /**
     * Creates a new SwissTable with the given slot count.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     */
    SwissTable(int slotCount) {
        this(slotCount, (byte) 0);
    }

    /**
     * Creates a new SwissTable with the given slot count and local depth.
     * 
     * @param slotCount number of slots, must be a power of 2 and multiple of 8
     * @param localDepth local depth for directory management
     */
    SwissTable(int slotCount, byte localDepth) {
        if (slotCount < 8 || (slotCount & (slotCount - 1)) != 0 || (slotCount & 7) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2 and >= 8");
        }
        this.capacity = (short) slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.localDepth = localDepth;
        this.ctrl = new byte[slotCount];
        Arrays.fill(this.ctrl, CTRL_EMPTY);  // Must initialize to 0x80
        this.slots = new int[slotCount];
        this.used = 0;
        this.tomb = 0;
        // maxOcc = capacity * 7 / 8
        this.growthLeft = (short) (slotCount * 7 / 8);
    }

    /**
     * Loads a control word (8 control bytes) from the given group index.
     * Uses Unsafe for efficient unaligned load.
     */
    long loadCtrlWord(int groupIdx) {
        return UNSAFE.getLong(ctrl, BYTE_ARRAY_BASE_OFFSET + ((long) groupIdx << 3));
    }

    // ========== SWAR Matching Algorithms ==========

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
     * maxOcc = capacity * 7 / 8 = 87.5% load factor.
     */
    static int maxOcc(int capacity) {
        return capacity * 7 / 8;
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
