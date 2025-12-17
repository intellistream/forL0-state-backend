package org.apache.flink.runtime.state.heap.entrystore;

/**
 * Size classes for ValuePool allocation.
 * 
 * <p>Fine-grained design with 28 fixed size classes + LARGE:
 * <ul>
 *   <li>0-128B: 8 classes with 16B step → ~12% max fragmentation</li>
 *   <li>128-256B: 4 classes with 32B step → ~14% max fragmentation</li>
 *   <li>256-512B: 4 classes with 64B step → ~17% max fragmentation</li>
 *   <li>512-1KB: 4 classes with 128B step → ~17% max fragmentation</li>
 *   <li>1-2KB: 4 classes with 256B step → ~17% max fragmentation</li>
 *   <li>2-4KB: 4 classes with 512B step → ~17% max fragmentation</li>
 *   <li>&gt;4KB: LARGE (separate allocation)</li>
 * </ul>
 * 
 * <p>Uses lookup table for O(1) size class determination.
 */
public enum ValueSizeClass {
    
    // ===== Small values: 16B step (8 classes) =====
    
    /** Values ≤ 16 bytes */
    VS_16(0, 16, 16),
    
    /** Values 17-32 bytes */
    VS_32(16, 32, 32),
    
    /** Values 33-48 bytes */
    VS_48(32, 48, 48),
    
    /** Values 49-64 bytes */
    VS_64(48, 64, 64),
    
    /** Values 65-80 bytes */
    VS_80(64, 80, 80),
    
    /** Values 81-96 bytes */
    VS_96(80, 96, 96),
    
    /** Values 97-112 bytes */
    VS_112(96, 112, 112),
    
    /** Values 113-128 bytes */
    VS_128(112, 128, 128),
    
    // ===== Medium values: 32B step (4 classes) =====
    
    /** Values 129-160 bytes */
    VS_160(128, 160, 160),
    
    /** Values 161-192 bytes */
    VS_192(160, 192, 192),
    
    /** Values 193-224 bytes */
    VS_224(192, 224, 224),
    
    /** Values 225-256 bytes */
    VS_256(224, 256, 256),
    
    // ===== Medium-large values: 64B step (4 classes) =====
    
    /** Values 257-320 bytes */
    VS_320(256, 320, 320),
    
    /** Values 321-384 bytes */
    VS_384(320, 384, 384),
    
    /** Values 385-448 bytes */
    VS_448(384, 448, 448),
    
    /** Values 449-512 bytes */
    VS_512(448, 512, 512),
    
    // ===== Large values: 128B step (4 classes) =====
    
    /** Values 513-640 bytes */
    VS_640(512, 640, 640),
    
    /** Values 641-768 bytes */
    VS_768(640, 768, 768),
    
    /** Values 769-896 bytes */
    VS_896(768, 896, 896),
    
    /** Values 897-1024 bytes */
    VS_1024(896, 1024, 1024),
    
    // ===== Extra-large values: 256B step (4 classes) =====
    
    /** Values 1025-1280 bytes */
    VS_1280(1024, 1280, 1280),
    
    /** Values 1281-1536 bytes */
    VS_1536(1280, 1536, 1536),
    
    /** Values 1537-1792 bytes */
    VS_1792(1536, 1792, 1792),
    
    /** Values 1793-2048 bytes */
    VS_2048(1792, 2048, 2048),
    
    // ===== Huge values: 512B step (4 classes) =====
    
    /** Values 2049-2560 bytes */
    VS_2560(2048, 2560, 2560),
    
    /** Values 2561-3072 bytes */
    VS_3072(2560, 3072, 3072),
    
    /** Values 3073-3584 bytes */
    VS_3584(3072, 3584, 3584),
    
    /** Values 3585-4096 bytes */
    VS_4096(3584, 4096, 4096),
    
    // ===== Large objects =====
    
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
     * Lookup table for O(1) size class determination.
     * Each entry covers a 16-byte range: index i covers sizes (i*16+1) to ((i+1)*16).
     * Total 256 entries to cover 0-4096 bytes.
     */
    private static final ValueSizeClass[] LOOKUP_TABLE = new ValueSizeClass[256];
    
    static {
        // Build lookup table
        // Index i covers sizes: i*16 < size <= (i+1)*16
        // So LOOKUP_TABLE[0] covers 1-16, LOOKUP_TABLE[1] covers 17-32, etc.
        
        for (int i = 0; i < 256; i++) {
            int upperBound = (i + 1) * 16;  // max size for this index
            LOOKUP_TABLE[i] = findSizeClassForSize(upperBound);
        }
    }
    
    /**
     * Helper to find size class for a specific size (used during table init).
     */
    private static ValueSizeClass findSizeClassForSize(int size) {
        // Small values: 16B step (0-128)
        if (size <= 16) return VS_16;
        if (size <= 32) return VS_32;
        if (size <= 48) return VS_48;
        if (size <= 64) return VS_64;
        if (size <= 80) return VS_80;
        if (size <= 96) return VS_96;
        if (size <= 112) return VS_112;
        if (size <= 128) return VS_128;
        
        // Medium values: 32B step (128-256)
        if (size <= 160) return VS_160;
        if (size <= 192) return VS_192;
        if (size <= 224) return VS_224;
        if (size <= 256) return VS_256;
        
        // Medium-large: 64B step (256-512)
        if (size <= 320) return VS_320;
        if (size <= 384) return VS_384;
        if (size <= 448) return VS_448;
        if (size <= 512) return VS_512;
        
        // Large: 128B step (512-1024)
        if (size <= 640) return VS_640;
        if (size <= 768) return VS_768;
        if (size <= 896) return VS_896;
        if (size <= 1024) return VS_1024;
        
        // Extra-large: 256B step (1024-2048)
        if (size <= 1280) return VS_1280;
        if (size <= 1536) return VS_1536;
        if (size <= 1792) return VS_1792;
        if (size <= 2048) return VS_2048;
        
        // Huge: 512B step (2048-4096)
        if (size <= 2560) return VS_2560;
        if (size <= 3072) return VS_3072;
        if (size <= 3584) return VS_3584;
        if (size <= 4096) return VS_4096;
        
        return LARGE;
    }
    
    /**
     * Gets the appropriate size class for a given total size (including VALUE_ENTRY_HEADER_SIZE).
     * Uses lookup table for O(1) performance.
     * 
     * @param totalSize total size in bytes (header + value)
     * @return the appropriate size class
     */
    public static ValueSizeClass getSizeClass(int totalSize) {
        if (totalSize <= 0) {
            return VS_16;
        }
        if (totalSize > 4096) {
            return LARGE;
        }
        // Lookup table: index = (totalSize - 1) / 16
        // This maps sizes 1-16 to index 0, 17-32 to index 1, etc.
        return LOOKUP_TABLE[(totalSize - 1) >> 4];
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
