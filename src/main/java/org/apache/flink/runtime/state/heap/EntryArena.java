package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Entry Arena manages the actual storage of key-value entries.
 * Stores serialized key/namespace/value byte arrays directly in off-heap memory.
 * Entry format: [keyLen(4B)][namespaceLen(4B)][valueLen(4B)][key][namespace][value]
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
        Arrays.fill(freeListHeads, null);
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
        if (keyBytes == null || namespaceBytes == null || valueBytes == null) {
            return 0;
        }
        // Delegate to the more efficient buffer+length version
        return putEntry(keyBytes, keyBytes.length, namespaceBytes, namespaceBytes.length, valueBytes, valueBytes.length);
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

        AddressInfo info = resolveAddress(address);
        // header
        info.segment.putInt(info.offset + KEY_LEN_OFFSET, keyLen);
        info.segment.putInt(info.offset + NAMESPACE_LEN_OFFSET, namespaceLen);
        info.segment.putInt(info.offset + VALUE_LEN_OFFSET, valueLen);
        // body
        int dataOffset = info.offset + ENTRY_HEADER_SIZE;
        info.segment.put(dataOffset, keyBuffer, 0, keyLen);
        dataOffset += keyLen;
        info.segment.put(dataOffset, namespaceBuffer, 0, namespaceLen);
        dataOffset += namespaceLen;
        info.segment.put(dataOffset, valueBuffer, 0, valueLen);
        activeEntries++;
        return address;
    }

    /**
     * Updates an existing entry's value.
     * First attempts in-place update if the new value fits in existing space,
     * otherwise allocates new entry and frees the old one into free list.
     */
    public long updateEntry(long address, byte[] valueBytes) {
        if (valueBytes == null) {
            return 0;
        }
        // Delegate to the more efficient buffer+length version
        return updateEntry(address, valueBytes, valueBytes.length);
    }

    /**
     * Updates an existing entry's value using buffer+length (避免复制 DataOutputSerializer 缓冲区).
     * First attempts in-place update if the new value fits in existing space,
     * otherwise allocates new entry and frees the old one into free list.
     */
    public long updateEntry(long address, byte[] valueBuffer, int valueLen) {
        if (address == 0 || valueBuffer == null) {
            return 0;
        }

        if (valueLen > 256 * 1024) {
            return 0;
        }

        // First try in-place update
        if (updateValueInPlace(address, valueBuffer, valueLen)) {
            // In-place update succeeded, return the same address
            return address;
        }

        // In-place update failed, fall back to traditional approach
        byte[] keyBytes = getKeyBytes(address);
        byte[] namespaceBytes = getNamespaceBytes(address);

        // Allocate new entry using buffer version
        long newAddress = putEntry(keyBytes, keyBytes.length, namespaceBytes, namespaceBytes.length, valueBuffer, valueLen);

        if (newAddress != 0) {
            // Free the old entry into free list
            removeEntry(address);
        }

        return newAddress;
    }

    /**
     * Attempts to update the value in place using buffer+length if the new value fits in the existing space.
     * Returns true if successful, false if a new allocation is needed.
     */
    public boolean updateValueInPlace(long address, byte[] newValueBuffer, int newValueLen) {
        if (closed || address == 0 || newValueBuffer == null) {
            return false;
        }

        // Validate input
        if (newValueLen < 0 || newValueLen > newValueBuffer.length) {
            return false;
        }

        AddressInfo info = resolveAddress(address);
        int keyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        int namespaceLen = info.segment.getInt(info.offset + NAMESPACE_LEN_OFFSET);
        int currentValueLen = info.segment.getInt(info.offset + VALUE_LEN_OFFSET);

        // Check if new value fits in available space
        if (newValueLen <= currentValueLen) {
            // Update value length
            info.segment.putInt(info.offset + VALUE_LEN_OFFSET, newValueLen);

            // Update value data
            int valueOffset = info.offset + ENTRY_HEADER_SIZE + keyLen + namespaceLen;
            info.segment.put(valueOffset, newValueBuffer, 0, newValueLen);

            return true;
        }

        return false;
    }

    /**
     * Reads an entry's key bytes.
     */
    public byte[] getKeyBytes(long address) {
        AddressInfo info = resolveAddress(address);

        int keyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        if (keyLen <= 0 || keyLen > 16 * 1024) {
            return keyLen == 0 ? new byte[0] : null;
        }

        byte[] keyBytes = new byte[keyLen];
        info.segment.get(info.offset + ENTRY_HEADER_SIZE, keyBytes);
        return keyBytes;
    }

    /**
     * Reads an entry's namespace bytes.
     */
    public byte[] getNamespaceBytes(long address) {
        AddressInfo info = resolveAddress(address);

        int keyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        int namespaceLen = info.segment.getInt(info.offset + NAMESPACE_LEN_OFFSET);

        if (namespaceLen <= 0 || namespaceLen > 16 * 1024) {
            return namespaceLen == 0 ? new byte[0] : null;
        }

        byte[] namespaceBytes = new byte[namespaceLen];
        info.segment.get(info.offset + ENTRY_HEADER_SIZE + keyLen, namespaceBytes);
        return namespaceBytes;
    }

    /**
     * Reads an entry's value bytes.
     */
    public byte[] getValueBytes(long address) {
        AddressInfo info = resolveAddress(address);

        int keyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        int namespaceLen = info.segment.getInt(info.offset + NAMESPACE_LEN_OFFSET);
        int valueLen = info.segment.getInt(info.offset + VALUE_LEN_OFFSET);

        if (valueLen <= 0 || valueLen > 256 * 1024) {
            return valueLen == 0 ? new byte[0] : null;
        }

        byte[] valueBytes = new byte[valueLen];
        info.segment.get(info.offset + ENTRY_HEADER_SIZE + keyLen + namespaceLen, valueBytes);
        return valueBytes;
    }

    /**
     * Checks if key and namespace match for given entry.
     *
     * @deprecated Use {@link #matchesKey(long, byte[], int, byte[], int)} instead for better performance.
     *             This method creates temporary arrays which impacts performance.
     */
    @Deprecated
    public boolean matchesKey(long address, byte[] keyBytes, byte[] namespaceBytes) {
        if (address == 0 || keyBytes == null || namespaceBytes == null) {
            return false;
        }
        // Delegate to the zero-copy version for better performance
        return matchesKey(address, keyBytes, keyBytes.length, namespaceBytes, namespaceBytes.length);
    }

    /**
     * Zero-copy matches check with external buffers and explicit lengths.
     */
    public boolean matchesKey(long address, byte[] keyBuffer, int keyLen, byte[] namespaceBuffer, int namespaceLen) {
        AddressInfo info = resolveAddress(address);

        int storedKeyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        int storedNsLen = info.segment.getInt(info.offset + NAMESPACE_LEN_OFFSET);
        if (storedKeyLen != keyLen || storedNsLen != namespaceLen) {
            return false;
        }
        int dataOffset = info.offset + ENTRY_HEADER_SIZE;
        // compare key
        if (!equalsSegmentBytes(info.segment, dataOffset, keyBuffer, keyLen)) {
            return false;
        }
        dataOffset += storedKeyLen;
        // compare namespace
        return equalsSegmentBytes(info.segment, dataOffset, namespaceBuffer, namespaceLen);
    }

    private static boolean equalsSegmentBytes(MemorySegment seg, int segOffset, byte[] arr, int len) {
        // Use Flink's optimized memory comparison instead of manual byte-by-byte comparison
        MemorySegment arrSeg = MemorySegmentFactory.wrap(arr);
        return seg.equalTo(arrSeg, segOffset, 0, len);
    }

    // ==== allocation helpers ====
    private int calculateEntrySize(int keyLen, int namespaceLen, int valueLen) {
        int dataSize = ENTRY_HEADER_SIZE + keyLen + namespaceLen + valueLen;
        // Align to 8 bytes for better memory performance
        return (Math.max(dataSize, MIN_ENTRY_SIZE) + ALIGNMENT - 1) & (-ALIGNMENT);
    }

    private boolean allocateNewSegment() {
        if (closed) {
            return false;
        }
        try {
            List<MemorySegment> newSegments = allocator.allocate(SEGMENT_SIZE);
            if (newSegments.isEmpty()) {
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
        } catch (MemoryAllocationException e) {
            return false;
        }
    }

    private long allocateEntry(int size) {
        long reused = allocateFromFreeListBounded(size);
        if (reused != 0) { return reused; }

        long addr = allocateFromCurrentSegment(size);
        if (addr != 0) { return addr; }

        return linearAllocate(size);
    }

    private long linearAllocate(int size) {
        long addr = allocateFromCurrentSegment(size);
        if (addr != 0) { return addr; }

        if (size > SEGMENT_SIZE) { return 0; }
        if (!allocateNewSegment()) { return 0; }

        return allocateFromCurrentSegment(size);
    }

    /**
     * Attempts to allocate from the current segment if there's enough space.
     * Returns the allocated address or 0 if allocation failed.
     */
    private long allocateFromCurrentSegment(int size) {
        if (currentSegment != null && currentOffset + size <= currentSegment.size()) {
            int slabIndex = segments.size() - 1;
            long addr = ((long)(slabIndex + 1) << 32) | (currentOffset + 1);
            currentOffset += size;
            totalAllocated += size;
            return addr;
        }
        return 0;
    }

    private long allocateFromFreeListBounded(int size) {
        SizeClass sc = SizeClass.getSizeClass(size);
        long addr = scanFreeList(sc, size, EntryArena.MAX_FREE_LIST_SCAN);
        if (addr != 0) { return addr; }
        int remainProbe = Math.max(1, EntryArena.MAX_FREE_LIST_SCAN / 4);
        for (SizeClass larger : SizeClass.values()) {
            if (larger.ordinal() > sc.ordinal()) {
                addr = scanFreeList(larger, size, remainProbe);
                if (addr != 0) { return addr; }
            }
        }
        return 0;
    }

    private long scanFreeList(SizeClass sc, int size, int maxScan) {
        FreeBlock prev = null, cur = freeListHeads[sc.ordinal()];
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
        AddressInfo info = resolveAddress(address);

        int k = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        int n = info.segment.getInt(info.offset + NAMESPACE_LEN_OFFSET);
        int v = info.segment.getInt(info.offset + VALUE_LEN_OFFSET);
        return calculateEntrySize(k, n, v);
    }

    public void removeEntry(long address) {
        int sz = getEntrySize(address);
        if (sz > 0) {
            addToFreeList(address, sz);
        }
        activeEntries--;
    }

    /**
     * Returns a Slice of the value for zero-copy deserialization.
     */
    public Slice getValueSlice(long address) {
        AddressInfo info = resolveAddress(address);
        int keyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        int nsLen = info.segment.getInt(info.offset + NAMESPACE_LEN_OFFSET);
        int valLen = info.segment.getInt(info.offset + VALUE_LEN_OFFSET);
        if (valLen < 0 || valLen > 256 * 1024) {
            return null;
        }
        int off = info.offset + ENTRY_HEADER_SIZE + keyLen + nsLen;
        return new Slice(info.segment, off, valLen);
    }

    /**
     * Returns a Slice of the key (optional helper for iterations).
     */
    public Slice getKeySlice(long address) {
        AddressInfo info = resolveAddress(address);
        int keyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        if (keyLen < 0 || keyLen > 16 * 1024) {
            return null;
        }
        int off = info.offset + ENTRY_HEADER_SIZE;
        return new Slice(info.segment, off, keyLen);
    }

    /**
     * Returns a Slice of the namespace (optional helper for iterations).
     */
    public Slice getNamespaceSlice(long address) {
        AddressInfo info = resolveAddress(address);
        int keyLen = info.segment.getInt(info.offset + KEY_LEN_OFFSET);
        int nsLen = info.segment.getInt(info.offset + NAMESPACE_LEN_OFFSET);
        if (nsLen < 0 || nsLen > 16 * 1024) {
            return null;
        }
        int off = info.offset + ENTRY_HEADER_SIZE + keyLen;
        return new Slice(info.segment, off, nsLen);
    }

    /**
     * Address resolution result containing segment and offset information.
     */
    private static final class AddressInfo {
        final MemorySegment segment;
        final int offset;

        AddressInfo(MemorySegment segment, int offset) {
            this.segment = segment;
            this.offset = offset;
        }
    }

    /**
     * Resolves an entry address to segment and offset.
     */
    private AddressInfo resolveAddress(long address) {
        int slabIndex = (int)(address >>> 32) - 1;
        int offset = (int)(address & 0xFFFFFFFFL) - 1;

        return new AddressInfo(segments.get(slabIndex), offset);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;

        // Clear free lists
        Arrays.fill(freeListHeads, null);
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
