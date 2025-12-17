package org.apache.flink.runtime.state.heap.entrystore;

/**
 * Size classes for ValuePool allocation.
 * Each size class represents a fixed slot size for efficient memory management.
 * 
 * <p>Design principles:
 * <ul>
 *   <li>Small values use fine-grained size classes to minimize internal fragmentation</li>
 *   <li>Size classes double in size for predictable growth</li>
 *   <li>Values larger than 4KB are handled by LargeObjectPool separately</li>
 * </ul>
 */
public enum ValueSizeClass {
    
    /** Values ≤ 32 bytes, slot size 32B */
    VS_32(0, 32, 32),
    
    /** Values 33-64 bytes, slot size 64B */
    VS_64(32, 64, 64),
    
    /** Values 65-128 bytes, slot size 128B */
    VS_128(64, 128, 128),
    
    /** Values 129-256 bytes, slot size 256B */
    VS_256(128, 256, 256),
    
    /** Values 257-384 bytes, slot size 384B */
    VS_384(256, 384, 384),
    
    /** Values 385-512 bytes, slot size 512B */
    VS_512(384, 512, 512),
    
    /** Values 513-1024 bytes, slot size 1KB */
    VS_1K(512, 1024, 1024),
    
    /** Values 1025-2048 bytes, slot size 2KB */
    VS_2K(1024, 2048, 2048),
    
    /** Values 2049-4096 bytes, slot size 4KB */
    VS_4K(2048, 4096, 4096),
    
    /** Values > 4KB, handled by LargeObjectPool */
    LARGE(4096, Integer.MAX_VALUE, -1);
    
    /** Minimum size (exclusive) for this class */
    private final int minSize;
    
    /** Maximum size (inclusive) for this class */
    private final int maxSize;
    
    /** Fixed slot size for this class (-1 for LARGE) */
    private final int slotSize;
    
    ValueSizeClass(int minSize, int maxSize, int slotSize) {
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.slotSize = slotSize;
    }
    
    /**
     * Gets the minimum size (exclusive) for this class.
     */
    public int getMinSize() {
        return minSize;
    }
    
    /**
     * Gets the maximum size (inclusive) for this class.
     */
    public int getMaxSize() {
        return maxSize;
    }
    
    /**
     * Gets the fixed slot size for this class.
     * Returns -1 for LARGE class (variable size allocation).
     */
    public int getSlotSize() {
        return slotSize;
    }
    
    /**
     * Returns true if this is a small/medium value class with fixed slots.
     */
    public boolean isFixedSlot() {
        return slotSize > 0;
    }
    
    /**
     * Gets the number of slots that fit in a run of given size.
     * 
     * @param runSize the size of the run in bytes
     * @return number of slots, or 0 for LARGE class
     */
    public int getSlotsPerRun(int runSize) {
        if (slotSize <= 0) {
            return 0;
        }
        return runSize / slotSize;
    }
    
    // ========== Static Methods ==========
    
    /** Cached values array for fast lookup */
    private static final ValueSizeClass[] VALUES = values();
    
    /** Number of fixed-size classes (excluding LARGE) */
    public static final int FIXED_SIZE_CLASS_COUNT = VALUES.length - 1;
    
    /**
     * Gets the appropriate size class for a given total size (including VALUE_ENTRY_HEADER_SIZE).
     * Uses bit manipulation for fast lookup.
     * 
     * @param totalSize total size in bytes (header + value)
     * @return the appropriate size class
     */
    public static ValueSizeClass getSizeClass(int totalSize) {
        if (totalSize <= 0) {
            return VS_32;
        }
        if (totalSize <= 32) {
            return VS_32;
        }
        if (totalSize <= 64) {
            return VS_64;
        }
        if (totalSize <= 128) {
            return VS_128;
        }
        if (totalSize <= 256) {
            return VS_256;
        }
        if (totalSize <= 384) {
            return VS_384;
        }
        if (totalSize <= 512) {
            return VS_512;
        }
        if (totalSize <= 1024) {
            return VS_1K;
        }
        if (totalSize <= 2048) {
            return VS_2K;
        }
        if (totalSize <= 4096) {
            return VS_4K;
        }
        return LARGE;
    }
    
    /**
     * Gets size class by ordinal index.
     * 
     * @param ordinal the ordinal index
     * @return the size class, or null if out of range
     */
    public static ValueSizeClass byOrdinal(int ordinal) {
        if (ordinal >= 0 && ordinal < VALUES.length) {
            return VALUES[ordinal];
        }
        return null;
    }
}
