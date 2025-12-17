package org.apache.flink.runtime.state.heap.entrystore;

/**
 * Constants for EntryStore components.
 * Defines memory layout, alignment, and sizing constants.
 */
public final class EntryStoreConstants {
    
    private EntryStoreConstants() {
        // Utility class, no instantiation
    }
    
    // ========== Memory Alignment ==========
    
    /** Memory alignment in bytes (8 bytes for better performance) */
    public static final int ALIGNMENT = 8;
    
    // ========== Segment Configuration ==========
    
    /** Default segment size (64KB, page-aligned) */
    public static final int DEFAULT_SEGMENT_SIZE = 64 * 1024;
    
    /** Default run size for ValuePool (64KB per run) */
    public static final int DEFAULT_RUN_SIZE = 64 * 1024;
    
    // ========== KeyNsPool Entry Layout ==========
    // Layout (with inline support):
    // [hash(4B)][modeKeyLen(4B)][nsLen(2B)][padding(2B)][valueHandle/inlineValue(8B)][key][namespace]
    //
    // modeKeyLen encoding (32 bits):
    // - bit 31: mode (0=pointer, 1=inline)
    // - bits 30-27: inlineLen (0-8, only valid when mode=1)
    // - bits 26-0: keyLen (max 128MB)
    
    /** Offset of hash field in KeyNsPool entry */
    public static final int KEY_HASH_OFFSET = 0;
    
    /** Offset of mode+keyLen combined field */
    public static final int MODE_KEY_LEN_OFFSET = 4;
    
    /** Offset of namespace length field (2 bytes) */
    public static final int NS_LEN_OFFSET = 8;
    
    /** Offset of padding (2 bytes, reserved) */
    public static final int PADDING_OFFSET = 10;
    
    /** Offset of value handle / inline value field (8 bytes) */
    public static final int VALUE_HANDLE_OFFSET = 12;
    
    /** Offset where key data begins */
    public static final int KEY_DATA_OFFSET = 20;
    
    /** Total header size for KeyNsPool entry */
    public static final int KEY_ENTRY_HEADER_SIZE = 20;
    
    // ========== Mode+KeyLen Encoding Constants ==========
    
    /** Mode bit mask: bit 31 (0x80000000) */
    public static final int MODE_MASK = 0x80000000;
    
    /** Inline length mask: bits 30-27 (0x78000000) */
    public static final int INLINE_LEN_MASK = 0x78000000;
    
    /** Key length mask: bits 26-0 (0x07FFFFFF) */
    public static final int KEY_LEN_MASK = 0x07FFFFFF;
    
    /** Mode bit shift (bit 31) */
    public static final int MODE_SHIFT = 31;
    
    /** Inline length shift (bits 30-27) */
    public static final int INLINE_LEN_SHIFT = 27;
    
    /** Mode value for pointer mode */
    public static final int MODE_POINTER = 0;
    
    /** Mode value for inline mode */
    public static final int MODE_INLINE = 1;
    
    /** Maximum inline value size (8 bytes - fits in valueHandle field) */
    public static final int INLINE_THRESHOLD = 8;
    
    // ========== Legacy Constants (for backward compatibility in transition) ==========
    
    /** @deprecated Use MODE_KEY_LEN_OFFSET instead */
    @Deprecated
    public static final int KEY_LEN_OFFSET = MODE_KEY_LEN_OFFSET;
    
    // ========== ValuePool Entry Layout ==========
    // Layout: [valueLen(4B)][value]
    
    /** Offset of value length field */
    public static final int VALUE_LEN_OFFSET = 0;
    
    /** Total header size for ValuePool entry */
    public static final int VALUE_ENTRY_HEADER_SIZE = 4;
    
    // ========== Size Limits ==========
    
    /** Maximum key size (16KB) */
    public static final int MAX_KEY_SIZE = 16 * 1024;
    
    /** Maximum namespace size (16KB) */
    public static final int MAX_NAMESPACE_SIZE = 16 * 1024;
    
    /** Maximum value size (256KB) */
    public static final int MAX_VALUE_SIZE = 256 * 1024;
    
    /** Large object threshold (4KB) - values larger than this use LargeObjectPool */
    public static final int LARGE_OBJECT_THRESHOLD = 4 * 1024;
    
    /** Page size for large object alignment */
    public static final int PAGE_SIZE = 4096;
    
    // ========== Address Encoding ==========
    // KeyNsPool address: (segmentIndex + 1) << 32 | (offset + 1)
    // ValuePool address uses pool type marker in high bits
    
    /** Pool type marker for KeyNsPool (0x00) */
    public static final byte POOL_TYPE_KEY_NS = 0x00;
    
    /** Pool type marker for ValuePool small/medium (0x01) */
    public static final byte POOL_TYPE_VALUE = 0x01;
    
    /** Pool type marker for LargeObjectPool (0x02) */
    public static final byte POOL_TYPE_LARGE = 0x02;
    
    /** Null/invalid handle value */
    public static final long NULL_HANDLE = 0L;
    
    // ========== Initial Capacity ==========
    
    /** Initial segment array capacity */
    public static final int INITIAL_SEGMENT_CAPACITY = 16;
    
    /** Initial run array capacity per size class */
    public static final int INITIAL_RUN_CAPACITY = 8;
    
    // ========== Utility Methods ==========
    
    /**
     * Aligns a size to 8-byte boundary.
     * 
     * @param size the size to align
     * @return the aligned size
     */
    public static int align8(int size) {
        return (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    }
    
    /**
     * Aligns a size to page boundary.
     * 
     * @param size the size to align
     * @return the aligned size
     */
    public static int alignToPage(int size) {
        return (size + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
    }
    
    /**
     * Encodes a KeyNsPool address from segment index and offset.
     * Uses +1 encoding to ensure 0 is never a valid address.
     * 
     * @param segmentIndex the segment index
     * @param offset the offset within the segment
     * @return the encoded address
     */
    public static long encodeKeyNsAddress(int segmentIndex, int offset) {
        return ((long) (segmentIndex + 1) << 32) | (offset + 1);
    }
    
    /**
     * Decodes segment index from a KeyNsPool address.
     * 
     * @param address the encoded address
     * @return the segment index
     */
    public static int decodeSegmentIndex(long address) {
        return (int) (address >>> 32) - 1;
    }
    
    /**
     * Decodes offset from a KeyNsPool address.
     * 
     * @param address the encoded address
     * @return the offset within the segment
     */
    public static int decodeOffset(long address) {
        return (int) (address & 0xFFFFFFFFL) - 1;
    }
}
