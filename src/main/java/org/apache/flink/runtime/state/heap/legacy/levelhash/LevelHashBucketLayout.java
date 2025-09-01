package org.apache.flink.runtime.state.heap.legacy.levelhash;

/**
 * 64‑byte bucket layout utilities – offsets, masks and helpers.
 */
final class LevelHashBucketLayout {
    private LevelHashBucketLayout() {}

    /** Number of logical slots in a bucket (tag + ptr). */
    public static final int SLOT_COUNT = 4;

    // --- byte offsets within the 64‑byte bucket ------------------------------------------------
    public static final int TAG0_OFFSET = 0;           // uint32
    public static final int TAG1_OFFSET = TAG0_OFFSET + 4;
    public static final int TAG2_OFFSET = TAG1_OFFSET + 4;
    public static final int TAG3_OFFSET = TAG2_OFFSET + 4;

    public static final int PTR0_OFFSET = 16;          // uint64
    public static final int PTR1_OFFSET = PTR0_OFFSET + 8;
    public static final int PTR2_OFFSET = PTR1_OFFSET + 8;
    public static final int PTR3_OFFSET = PTR2_OFFSET + 8;

    public static final int MASK_OFFSET     = 48;      // uint8   – 0..3 used, 4th bit="next" flag
    public static final int VERSION_OFFSET  = 49;      // uint8
    public static final int HOTCNT_OFFSET   = 50;      // uint16 (optional)

    public static final int BUCKET_SIZE     = 64;

    // helper
    public static int tagOffset(int slot) { return TAG0_OFFSET + (slot << 2); }
    public static int ptrOffset(int slot) { return PTR0_OFFSET + (slot << 3); }
}
