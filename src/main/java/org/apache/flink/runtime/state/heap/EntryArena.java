package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.ArrayList;
import java.util.List;

/**
 * Entry Arena manages the actual storage of key-value entries.
 * Stores serialized key/namespace/value byte arrays directly in off-heap memory.
 * Entry format: [keyLen(4B)][namespaceLen(4B)][valueLen(4B)][key][namespace][value]
 *
 * Simplified to single allocation strategy: FREE_LIST with size classes for reuse.
 * Safe implementation: uses Flink MemorySegment instead of Unsafe operations.
 */
public class EntryArena implements AutoCloseable {

    // Entry layout constants
    private static final int KEY_LEN_OFFSET = 0;        // 4 bytes
    private static final int NAMESPACE_LEN_OFFSET = 4;  // 4 bytes
    private static final int VALUE_LEN_OFFSET = 8;      // 4 bytes
    private static final int ENTRY_HEADER_SIZE = 12;    // Total header size

    // Memory alignment (8 bytes for better performance)
    private static final int ALIGNMENT = 8;
    private static final int MIN_ENTRY_SIZE = ENTRY_HEADER_SIZE + ALIGNMENT;

    // Memory segment management
    private static final int SEGMENT_SIZE = 64 * 1024;  // 64KB per slab

    // Free list constants
    private static final int FREE_BLOCK_HEADER_SIZE = 8;  // next_pointer(8B)
    private static final int MIN_FREE_BLOCK_SIZE = FREE_BLOCK_HEADER_SIZE + ALIGNMENT;

    // Limit free list scan to avoid O(n) overhead in hot path
    private static final int MAX_FREE_LIST_SCAN = 16;

    /**
     * Size classes for free list allocation strategy.
     */
    private enum SizeClass {
        TINY(0, 32),           // <= 32 bytes
        SMALL(32, 128),        // 33-128 bytes
        MEDIUM(128, 512),      // 129-512 bytes
        LARGE(512, 2048),      // 513-2048 bytes
        XLARGE(2048, Integer.MAX_VALUE);  // > 2048 bytes

        final int minSize;
        final int maxSize;

        SizeClass(int minSize, int maxSize) {
            this.minSize = minSize;
            this.maxSize = maxSize;
        }

        static SizeClass getSizeClass(int size) {
            if (size <= TINY.maxSize) return TINY;
            if (size <= SMALL.maxSize) return SMALL;
            if (size <= MEDIUM.maxSize) return MEDIUM;
            if (size <= LARGE.maxSize) return LARGE;
            return XLARGE;
        }
    }

    /**
     * Free block representation for FREE_LIST strategy.
     */
    private static class FreeBlock {
        final long address;
        final int size;
        FreeBlock next;

        FreeBlock(long address, int size) {
            this.address = address;
            this.size = size;
            this.next = null;
        }
    }

    private final MemoryManagerAllocator allocator;
    private final List<MemorySegment> segments;
    private final List<List<MemorySegment>> originalAllocations;

    // Current allocation segment
    private MemorySegment currentSegment;
    private int currentOffset;

    // Free list data structures
    private final FreeBlock[] freeListHeads; // 使用数组按ordinal索引
    private long totalFreedMemory;
    private int totalFreeBlocks;

    // Statistics
    private long totalAllocated;
    private int activeEntries;
    private boolean closed;

    /**
     * Slice view for zero-copy read of key/namespace/value in a single segment.
     */
    public static final class Slice {
        public final MemorySegment segment;
        public final int offset;
        public final int length;
        public Slice(MemorySegment segment, int offset, int length) {
            this.segment = segment;
            this.offset = offset;
            this.length = length;
        }
    }

    /**
     * Creates an Entry Arena (FREE_LIST allocation strategy).
     */
    public EntryArena(MemoryManagerAllocator allocator) {
        this.allocator = allocator;
        this.segments = new ArrayList<>();
        this.originalAllocations = new ArrayList<>();
        this.totalAllocated = 0;
        this.activeEntries = 0;
        this.closed = false;

        // Initialize free list structures
        this.freeListHeads = new FreeBlock[SizeClass.values().length];
        for (int i = 0; i < freeListHeads.length; i++) {
            freeListHeads[i] = null;
        }
        this.totalFreedMemory = 0;
        this.totalFreeBlocks = 0;

        // Allocate initial segment
        allocateNewSegment();
    }

    /**
     * Stores a new entry and returns its address.
     *
     * @param keyBytes Serialized key bytes
     * @param namespaceBytes Serialized namespace bytes
     * @param valueBytes Serialized value bytes
     * @return Entry address (slab_index << 32 | offset), or 0 if allocation failed
     */
    public long putEntry(byte[] keyBytes, byte[] namespaceBytes, byte[] valueBytes) {
        if (closed || keyBytes == null || namespaceBytes == null || valueBytes == null) {
            return 0;
        }

        // Validate input sizes to prevent overflow
        if (keyBytes.length > 16 * 1024 || namespaceBytes.length > 16 * 1024 || valueBytes.length > 256 * 1024) {
            return 0;
        }

        int entrySize = calculateEntrySize(keyBytes.length, namespaceBytes.length, valueBytes.length);
        long address = allocateEntry(entrySize);

        if (address == 0) {
            return 0;
        }

        try {
            // Decode address: subtract 1 from both slabIndex and offset
            int slabIndex = (int)(address >>> 32) - 1;
            int offset = (int)(address & 0xFFFFFFFFL) - 1;

            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return 0;
            }

            MemorySegment segment = segments.get(slabIndex);

            // Write entry header
            segment.putInt(offset + KEY_LEN_OFFSET, keyBytes.length);
            segment.putInt(offset + NAMESPACE_LEN_OFFSET, namespaceBytes.length);
            segment.putInt(offset + VALUE_LEN_OFFSET, valueBytes.length);

            // Write entry data
            int dataOffset = offset + ENTRY_HEADER_SIZE;
            segment.put(dataOffset, keyBytes);
            dataOffset += keyBytes.length;
            segment.put(dataOffset, namespaceBytes);
            dataOffset += namespaceBytes.length;
            segment.put(dataOffset, valueBytes);

            activeEntries++;
            return address;
        } catch (Exception e) {
            // If write fails, just return 0 - memory will be "lost" but won't crash
            return 0;
        }
    }

    /**
     * Stores a new entry and returns its address (buffer+length 重载，避免复制 DataOutputSerializer 缓冲区）。
     */
    public long putEntry(byte[] keyBuffer, int keyLen, byte[] namespaceBuffer, int namespaceLen, byte[] valueBuffer, int valueLen) {
        if (closed || keyBuffer == null || namespaceBuffer == null || valueBuffer == null) {
            return 0;
        }
        // Validate input sizes and lengths
        if (keyLen < 0 || keyLen > keyBuffer.length || namespaceLen < 0 || namespaceLen > namespaceBuffer.length ||
                valueLen < 0 || valueLen > valueBuffer.length) {
            return 0;
        }
        if (keyLen > 16 * 1024 || namespaceLen > 16 * 1024 || valueLen > 256 * 1024) {
            return 0;
        }
        int entrySize = calculateEntrySize(keyLen, namespaceLen, valueLen);
        long address = allocateEntry(entrySize);
        if (address == 0) {
            return 0;
        }
        try {
            int slabIndex = (int)(address >>> 32) - 1;
            int offset = (int)(address & 0xFFFFFFFFL) - 1;
            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return 0;
            }
            MemorySegment segment = segments.get(slabIndex);
            // header
            segment.putInt(offset + KEY_LEN_OFFSET, keyLen);
            segment.putInt(offset + NAMESPACE_LEN_OFFSET, namespaceLen);
            segment.putInt(offset + VALUE_LEN_OFFSET, valueLen);
            // body
            int dataOffset = offset + ENTRY_HEADER_SIZE;
            segment.put(dataOffset, keyBuffer, 0, keyLen);
            dataOffset += keyLen;
            segment.put(dataOffset, namespaceBuffer, 0, namespaceLen);
            dataOffset += namespaceLen;
            segment.put(dataOffset, valueBuffer, 0, valueLen);
            activeEntries++;
            return address;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Updates an existing entry's value.
     * Always allocates new entry and frees the old one into free list.
     */
    public long updateEntry(long address, byte[] valueBytes) {
        if (closed || address == 0 || valueBytes == null) {
            return 0;
        }

        try {
            byte[] keyBytes = getKeyBytes(address);
            byte[] namespaceBytes = getNamespaceBytes(address);

            if (keyBytes == null || namespaceBytes == null) {
                return 0;
            }

            // Always allocate new entry
            long newAddress = putEntry(keyBytes, namespaceBytes, valueBytes);

            if (newAddress != 0) {
                // Free the old entry into free list
                int oldSize = getEntrySize(address);
                if (oldSize > 0) {
                    addToFreeList(address, oldSize);
                }
                // Decrement active entries count
                activeEntries--;
            }

            return newAddress;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Reads an entry's key bytes.
     */
    public byte[] getKeyBytes(long address) {
        if (closed || address == 0) {
            return null;
        }

        try {
            // Decode address: subtract 1 from both slabIndex and offset
            int slabIndex = (int)(address >>> 32) - 1;
            int offset = (int)(address & 0xFFFFFFFFL) - 1;

            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return null;
            }

            MemorySegment segment = segments.get(slabIndex);
            int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);

            if (keyLen <= 0 || keyLen > 16 * 1024) {
                return keyLen == 0 ? new byte[0] : null;
            }

            byte[] keyBytes = new byte[keyLen];
            segment.get(offset + ENTRY_HEADER_SIZE, keyBytes);
            return keyBytes;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Reads an entry's namespace bytes.
     */
    public byte[] getNamespaceBytes(long address) {
        if (closed || address == 0) {
            return null;
        }

        try {
            // Decode address: subtract 1 from both slabIndex and offset
            int slabIndex = (int)(address >>> 32) - 1;
            int offset = (int)(address & 0xFFFFFFFFL) - 1;

            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return null;
            }

            MemorySegment segment = segments.get(slabIndex);
            int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
            int namespaceLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);

            if (namespaceLen <= 0 || namespaceLen > 16 * 1024) {
                return namespaceLen == 0 ? new byte[0] : null;
            }

            byte[] namespaceBytes = new byte[namespaceLen];
            segment.get(offset + ENTRY_HEADER_SIZE + keyLen, namespaceBytes);
            return namespaceBytes;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Reads an entry's value bytes.
     */
    public byte[] getValueBytes(long address) {
        if (closed || address == 0) {
            return null;
        }

        try {
            // Decode address: subtract 1 from both slabIndex and offset
            int slabIndex = (int)(address >>> 32) - 1;
            int offset = (int)(address & 0xFFFFFFFFL) - 1;

            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return null;
            }

            MemorySegment segment = segments.get(slabIndex);
            int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
            int namespaceLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
            int valueLen = segment.getInt(offset + VALUE_LEN_OFFSET);

            if (valueLen <= 0 || valueLen > 256 * 1024) {
                return valueLen == 0 ? new byte[0] : null;
            }

            byte[] valueBytes = new byte[valueLen];
            segment.get(offset + ENTRY_HEADER_SIZE + keyLen + namespaceLen, valueBytes);
            return valueBytes;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Checks if key and namespace match for given entry.
     */
    public boolean matchesKey(long address, byte[] keyBytes, byte[] namespaceBytes) {
        if (address == 0 || keyBytes == null || namespaceBytes == null) {
            return false;
        }

        try {
            byte[] entryKey = getKeyBytes(address);
            byte[] entryNamespace = getNamespaceBytes(address);

            return java.util.Arrays.equals(keyBytes, entryKey) &&
                   java.util.Arrays.equals(namespaceBytes, entryNamespace);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Zero-copy matches check with external buffers and explicit lengths.
     */
    public boolean matchesKey(long address, byte[] keyBuffer, int keyLen, byte[] namespaceBuffer, int namespaceLen) {
        if (address == 0 || keyBuffer == null || namespaceBuffer == null) {
            return false;
        }
        try {
            int slabIndex = (int)(address >>> 32) - 1;
            int base = (int)(address & 0xFFFFFFFFL) - 1;
            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return false;
            }
            MemorySegment segment = segments.get(slabIndex);
            int storedKeyLen = segment.getInt(base + KEY_LEN_OFFSET);
            int storedNsLen = segment.getInt(base + NAMESPACE_LEN_OFFSET);
            if (storedKeyLen != keyLen || storedNsLen != namespaceLen) {
                return false;
            }
            int dataOffset = base + ENTRY_HEADER_SIZE;
            // compare key
            if (!equalsSegmentBytes(segment, dataOffset, keyBuffer, 0, keyLen)) {
                return false;
            }
            dataOffset += storedKeyLen;
            // compare namespace
            return equalsSegmentBytes(segment, dataOffset, namespaceBuffer, 0, namespaceLen);
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean equalsSegmentBytes(MemorySegment seg, int segOffset, byte[] arr, int arrOff, int len) {
        int i = 0;
        // compare 8 bytes at a time when possible
        int limit8 = len & ~7;
        for (; i < limit8; i += 8) {
            long a = seg.getLong(segOffset + i);
            long b = (((long)arr[arrOff + i] & 0xFF)      ) |
                     (((long)arr[arrOff + i + 1] & 0xFF) << 8) |
                     (((long)arr[arrOff + i + 2] & 0xFF) << 16) |
                     (((long)arr[arrOff + i + 3] & 0xFF) << 24) |
                     (((long)arr[arrOff + i + 4] & 0xFF) << 32) |
                     (((long)arr[arrOff + i + 5] & 0xFF) << 40) |
                     (((long)arr[arrOff + i + 6] & 0xFF) << 48) |
                     (((long)arr[arrOff + i + 7] & 0xFF) << 56);
            if (a != b) {
                return false;
            }
        }
        for (; i < len; i++) {
            if (seg.get(segOffset + i) != arr[arrOff + i]) {
                return false;
            }
        }
        return true;
    }

    // ==== allocation helpers ====
    private int calculateEntrySize(int keyLen, int namespaceLen, int valueLen) {
        int dataSize = ENTRY_HEADER_SIZE + keyLen + namespaceLen + valueLen;
        return alignSize(Math.max(dataSize, MIN_ENTRY_SIZE));
    }

    private static int alignSize(int size) {
        return (size + ALIGNMENT - 1) & (-ALIGNMENT);
    }

    private boolean allocateNewSegment() {
        if (closed) {
            return false;
        }
        try {
            List<MemorySegment> newSegments = allocator.allocate(SEGMENT_SIZE);
            if (newSegments == null || newSegments.isEmpty()) {
                return false;
            }
            originalAllocations.add(new ArrayList<>(newSegments));
            long totalSize = 0;
            for (MemorySegment seg : newSegments) { totalSize += seg.size(); }
            if (totalSize < SEGMENT_SIZE) {
                allocator.release(newSegments);
                originalAllocations.remove(originalAllocations.size() - 1);
                return false;
            }
            MemorySegment largest = newSegments.get(0);
            for (MemorySegment seg : newSegments) { if (seg.size() > largest.size()) largest = seg; }
            segments.add(largest);
            currentSegment = largest;
            currentOffset = 0;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private long allocateEntry(int size) {
        if (closed || size <= 0) { return 0; }
        long reused = allocateFromFreeListBounded(size, MAX_FREE_LIST_SCAN);
        if (reused != 0) { return reused; }
        if (currentSegment != null && currentOffset + size <= currentSegment.size()) {
            int slabIndex = segments.size() - 1;
            long addr = ((long)(slabIndex + 1) << 32) | (currentOffset + 1);
            currentOffset += size;
            totalAllocated += size;
            return addr;
        }
        return linearAllocate(size);
    }

    private long linearAllocate(int size) {
        if (currentSegment != null && currentOffset + size <= currentSegment.size()) {
            int slabIndex = segments.size() - 1;
            long addr = ((long)(slabIndex + 1) << 32) | (currentOffset + 1);
            currentOffset += size;
            totalAllocated += size;
            return addr;
        }
        if (size > SEGMENT_SIZE) { return 0; }
        if (!allocateNewSegment()) { return 0; }
        if (currentSegment != null && currentOffset + size <= currentSegment.size()) {
            int slabIndex = segments.size() - 1;
            long addr = ((long)(slabIndex + 1) << 32) | (currentOffset + 1);
            currentOffset += size;
            totalAllocated += size;
            return addr;
        }
        return 0;
    }

    private long allocateFromFreeListBounded(int size, int maxScan) {
        SizeClass sc = SizeClass.getSizeClass(size);
        long addr = scanFreeList(sc, size, maxScan);
        if (addr != 0) { return addr; }
        int remainProbe = Math.max(1, maxScan / 4);
        for (SizeClass larger : SizeClass.values()) {
            if (larger.ordinal() > sc.ordinal()) {
                addr = scanFreeList(larger, size, remainProbe);
                if (addr != 0) { return addr; }
            }
        }
        return 0;
    }

    private long scanFreeList(SizeClass sc, int size, int maxScan) {
        FreeBlock head = freeListHeads[sc.ordinal()];
        FreeBlock prev = null, cur = head;
        int scanned = 0;
        while (cur != null && scanned < maxScan) {
            if (cur.size >= size) {
                if (prev == null) { freeListHeads[sc.ordinal()] = cur.next; } else { prev.next = cur.next; }
                totalFreeBlocks--; totalFreedMemory -= cur.size;
                int remaining = cur.size - size;
                if (remaining >= MIN_FREE_BLOCK_SIZE) { addToFreeList(cur.address + size, remaining); }
                return cur.address;
            }
            prev = cur; cur = cur.next; scanned++;
        }
        return 0;
    }

    private void addToFreeList(long address, int size) {
        if (size < MIN_FREE_BLOCK_SIZE) { return; }
        SizeClass sc = SizeClass.getSizeClass(size);
        FreeBlock nb = new FreeBlock(address, size);
        nb.next = freeListHeads[sc.ordinal()];
        freeListHeads[sc.ordinal()] = nb;
        totalFreeBlocks++;
        totalFreedMemory += size;
    }

    public int getEntrySize(long address) {
        if (closed || address == 0) { return 0; }
        try {
            int slabIndex = (int)(address >>> 32) - 1;
            int offset = (int)(address & 0xFFFFFFFFL) - 1;
            if (slabIndex < 0 || slabIndex >= segments.size()) { return 0; }
            MemorySegment seg = segments.get(slabIndex);
            int k = seg.getInt(offset + KEY_LEN_OFFSET);
            int n = seg.getInt(offset + NAMESPACE_LEN_OFFSET);
            int v = seg.getInt(offset + VALUE_LEN_OFFSET);
            return calculateEntrySize(k, n, v);
        } catch (Exception e) { return 0; }
    }

    public void removeEntry(long address) {
        if (closed || address == 0) { return; }
        try {
            int sz = getEntrySize(address);
            if (sz > 0) { addToFreeList(address, sz); }
            activeEntries--;
        } catch (Exception ignore) { }
    }

    /**
     * Returns a Slice of the value for zero-copy deserialization.
     */
    public Slice getValueSlice(long address) {
        if (closed || address == 0) {
            return null;
        }
        try {
            int slabIndex = (int)(address >>> 32) - 1;
            int base = (int)(address & 0xFFFFFFFFL) - 1;
            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return null;
            }
            MemorySegment seg = segments.get(slabIndex);
            int keyLen = seg.getInt(base + KEY_LEN_OFFSET);
            int nsLen = seg.getInt(base + NAMESPACE_LEN_OFFSET);
            int valLen = seg.getInt(base + VALUE_LEN_OFFSET);
            if (valLen < 0 || valLen > 256 * 1024) {
                return null;
            }
            int off = base + ENTRY_HEADER_SIZE + keyLen + nsLen;
            return new Slice(seg, off, valLen);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Returns a Slice of the key (optional helper for iterations).
     */
    public Slice getKeySlice(long address) {
        if (closed || address == 0) {
            return null;
        }
        try {
            int slabIndex = (int)(address >>> 32) - 1;
            int base = (int)(address & 0xFFFFFFFFL) - 1;
            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return null;
            }
            MemorySegment seg = segments.get(slabIndex);
            int keyLen = seg.getInt(base + KEY_LEN_OFFSET);
            if (keyLen < 0 || keyLen > 16 * 1024) {
                return null;
            }
            int off = base + ENTRY_HEADER_SIZE;
            return new Slice(seg, off, keyLen);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Returns a Slice of the namespace (optional helper for iterations).
     */
    public Slice getNamespaceSlice(long address) {
        if (closed || address == 0) {
            return null;
        }
        try {
            int slabIndex = (int)(address >>> 32) - 1;
            int base = (int)(address & 0xFFFFFFFFL) - 1;
            if (slabIndex < 0 || slabIndex >= segments.size()) {
                return null;
            }
            MemorySegment seg = segments.get(slabIndex);
            int keyLen = seg.getInt(base + KEY_LEN_OFFSET);
            int nsLen = seg.getInt(base + NAMESPACE_LEN_OFFSET);
            if (nsLen < 0 || nsLen > 16 * 1024) {
                return null;
            }
            int off = base + ENTRY_HEADER_SIZE + keyLen;
            return new Slice(seg, off, nsLen);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;

        // Clear free lists
        for (int i = 0; i < freeListHeads.length; i++) {
            freeListHeads[i] = null;
        }
        totalFreedMemory = 0;
        totalFreeBlocks = 0;

        // Free all original allocations - this ensures proper memory accounting
        for (List<MemorySegment> allocation : originalAllocations) {
            try {
                allocator.release(allocation);
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }

        originalAllocations.clear();
        segments.clear();
        currentSegment = null;
        currentOffset = 0;

        // Reset statistics
        totalAllocated = 0;
        activeEntries = 0;
    }

    /**
     * Gets memory usage statistics.
     */
    public ArenaStats getStats() {
        long systemMemory = (long) segments.size() * SEGMENT_SIZE;
        return new ArenaStats(
            systemMemory,
            totalAllocated,
            totalFreedMemory,
            activeEntries,
            totalFreeBlocks,
            calculateMemoryEfficiency()
        );
    }

    /**
     * Calculates memory efficiency as a percentage.
     */
    private double calculateMemoryEfficiency() {
        long systemMemory = (long) segments.size() * SEGMENT_SIZE;
        if (systemMemory == 0) {
            return 0.0;
        }
        return (double) totalAllocated / systemMemory * 100.0;
    }

    /**
     * Compacts the arena by attempting to coalesce adjacent free blocks.
     * Placeholder for potential future enhancement.
     */
    public void compact() {
        // No-op simple placeholder; free list already enables reuse.
        if (totalFreeBlocks > 0) {
            // potential defragmentation opportunity exists
        }
    }

    /**
     * Statistics class for arena memory usage.
     */
    public static class ArenaStats {
        public final long totalSystemMemory;
        public final long totalAllocated;
        public final long totalFreed;
        public final int activeAllocations;
        public final int freeBlocks;
        public final double memoryEfficiency;
        public final double fragmentation;

        public ArenaStats(long totalSystemMemory, long totalAllocated,
                         long totalFreed, int activeAllocations, int freeBlocks, double memoryEfficiency) {
            this.totalSystemMemory = totalSystemMemory;
            this.totalAllocated = totalAllocated;
            this.totalFreed = totalFreed;
            this.activeAllocations = activeAllocations;
            this.freeBlocks = freeBlocks;
            this.memoryEfficiency = memoryEfficiency;
            this.fragmentation = freeBlocks > 0 ? (double) freeBlocks / (activeAllocations + freeBlocks) * 100.0 : 0.0;
        }

        @Override
        public String toString() {
            return String.format(
                "ArenaStats{systemMemory=%d, allocated=%d, freed=%d, " +
                "activeEntries=%d, freeBlocks=%d, efficiency=%.2f%%, fragmentation=%.2f%%}",
                totalSystemMemory, totalAllocated, totalFreed,
                activeAllocations, freeBlocks, memoryEfficiency, fragmentation
            );
        }
    }
}

