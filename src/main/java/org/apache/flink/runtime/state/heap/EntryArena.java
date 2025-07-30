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

    private final MemoryManagerAllocator allocator;
    private final List<MemorySegment> segments;
    private final List<List<MemorySegment>> originalAllocations; // Track original allocations for proper cleanup

    // Current allocation segment
    private MemorySegment currentSegment;
    private int currentOffset;

    // Statistics - simplified
    private long totalAllocated;
    private int activeEntries;
    private boolean closed;

    /**
     * Creates an Entry Arena with the given memory allocator.
     */
    public EntryArena(MemoryManagerAllocator allocator) {
        this.allocator = allocator;
        this.segments = new ArrayList<>();
        this.originalAllocations = new ArrayList<>();
        this.totalAllocated = 0;
        this.activeEntries = 0;
        this.closed = false;

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
     * Updates an existing entry's value.
     * Simplified: always allocates new entry, no in-place updates.
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

            // Always allocate new entry (simplified approach)
            long newAddress = putEntry(keyBytes, namespaceBytes, valueBytes);

            if (newAddress != 0) {
                // Mark old entry as inactive (we don't reclaim memory)
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
     * Removes an entry - simplified to just mark as inactive.
     */
    public void removeEntry(long address) {
        if (closed || address == 0) {
            return;
        }

        try {
            // Just decrement active entries count - don't actually free memory
            activeEntries--;
        } catch (Exception e) {
            // Ignore errors in simplified implementation
        }
    }

    /**
     * Gets entry size for given address.
     */
    public int getEntrySize(long address) {
        if (closed || address == 0) {
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
            int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
            int namespaceLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
            int valueLen = segment.getInt(offset + VALUE_LEN_OFFSET);

            return calculateEntrySize(keyLen, namespaceLen, valueLen);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Calculates total entry size including alignment.
     */
    private int calculateEntrySize(int keyLen, int namespaceLen, int valueLen) {
        int dataSize = ENTRY_HEADER_SIZE + keyLen + namespaceLen + valueLen;
        return alignSize(Math.max(dataSize, MIN_ENTRY_SIZE));
    }

    private boolean allocateNewSegment() {
        if (closed) {
            return false;
        }

        try {
            // Allocate memory segments
            List<MemorySegment> newSegments = allocator.allocate(SEGMENT_SIZE);

            if (newSegments.isEmpty()) {
                return false;
            }

            // Store original allocation for proper cleanup
            originalAllocations.add(new ArrayList<>(newSegments));

            // Calculate total available memory from all segments
            long totalSize = 0;
            for (MemorySegment segment : newSegments) {
                totalSize += segment.size();
            }

            // Check if we have enough total memory
            if (totalSize < SEGMENT_SIZE) {
                allocator.release(newSegments);
                originalAllocations.remove(originalAllocations.size() - 1); // Remove the allocation we just added
                return false;
            }

            // Use the largest available segment
            MemorySegment largestSegment = newSegments.get(0);
            for (MemorySegment seg : newSegments) {
                if (seg.size() > largestSegment.size()) {
                    largestSegment = seg;
                }
            }

            segments.add(largestSegment);
            currentSegment = largestSegment;
            currentOffset = 0;

            // Note: We keep all segments in originalAllocations for proper cleanup
            // even though we only use the largest one

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Allocates memory for an entry of the specified size.
     */
    private long allocateEntry(int size) {
        if (closed || size <= 0) {
            return 0;
        }

        // Try to allocate from current segment
        if (currentSegment != null && currentOffset + size <= currentSegment.size()) {
            int slabIndex = segments.size() - 1; // Current segment is always the last one

            // Address encoding: ((slabIndex + 1) << 32) | (currentOffset + 1)
            // This ensures address is never 0, which we use to indicate allocation failure
            long address = ((long)(slabIndex + 1) << 32) | (currentOffset + 1);
            currentOffset += size;
            totalAllocated += size;
            return address;
        }

        // Need new segment - but first check if entry can fit in a new segment
        if (size > SEGMENT_SIZE) {
            return 0; // Entry too large for any segment
        }

        // Try to allocate new segment
        if (!allocateNewSegment()) {
            return 0;
        }

        // Try again with new segment
        if (currentSegment != null && currentOffset + size <= currentSegment.size()) {
            int slabIndex = segments.size() - 1; // Current segment is always the last one
            long address = ((long)(slabIndex + 1) << 32) | (currentOffset + 1);
            currentOffset += size;
            totalAllocated += size;
            return address;
        }

        return 0;
    }

    private static int alignSize(int size) {
        return (size + ALIGNMENT - 1) & (-ALIGNMENT);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;

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
        return new ArenaStats(
            segments.size() * SEGMENT_SIZE,  // Total allocated from system
            totalAllocated,            // Total allocated to users
            0,                         // No freed memory tracking in simplified version
            activeEntries,             // Current active entries
            0                          // No free blocks in simplified version
        );
    }

    /**
     * Compacts the arena - no-op in simplified version.
     */
    public void compact() {
        // No-op in simplified version - we don't manage free memory
    }


    /**
     * Statistics class for arena memory usage.
     */
    public static class ArenaStats {
        public final long totalSystemMemory;
        public final long usedMemory;
        public final long freedMemory;
        public final int activeAllocations;
        public final double fragmentation;

        public ArenaStats(long totalSystemMemory, long usedMemory, long freedMemory,
                         int activeAllocations, int freeBlocks) {
            this.totalSystemMemory = totalSystemMemory;
            this.usedMemory = usedMemory;
            this.freedMemory = freedMemory;
            this.activeAllocations = activeAllocations;
            this.fragmentation = freeBlocks; // Simplified: just report free blocks count
        }
    }
}
