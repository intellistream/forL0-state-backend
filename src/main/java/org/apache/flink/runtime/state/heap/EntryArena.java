package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Entry Arena manages the actual storage of key-value entries.
 * Stores serialized key/namespace/value byte arrays directly in off-heap memory.
 * Entry format: [keyLen(4B)][namespaceLen(4B)][valueLen(4B)][key][namespace][value]
 *
 * Supports two allocation strategies:
 * 1. LINEAR: Simple linear allocation (original implementation)
 * 2. FREE_LIST: Free list with size classes for better memory reuse
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

    // Free list constants for FREE_LIST strategy
    private static final int FREE_BLOCK_HEADER_SIZE = 8;  // next_pointer(8B)
    private static final int MIN_FREE_BLOCK_SIZE = FREE_BLOCK_HEADER_SIZE + ALIGNMENT;

    /**
     * Memory allocation strategy.
     */
    public enum AllocationStrategy {
        /**
         * Linear allocation strategy - simple write cursor advancement.
         * No memory reuse, but very fast allocation.
         */
        LINEAR,

        /**
         * Free list allocation strategy - maintains free blocks by size classes.
         * Better memory utilization with slight allocation overhead.
         */
        FREE_LIST
    }

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
    private final AllocationStrategy strategy;
    private final List<MemorySegment> segments;
    private final List<List<MemorySegment>> originalAllocations;

    // Current allocation segment (used by both strategies)
    private MemorySegment currentSegment;
    private int currentOffset;

    // Free list data structures (only used by FREE_LIST strategy)
    private final Map<SizeClass, FreeBlock> freeListHeads;
    private long totalFreedMemory;
    private int totalFreeBlocks;

    // Statistics
    private long totalAllocated;
    private int activeEntries;
    private boolean closed;

    /**
     * Creates an Entry Arena with linear allocation strategy (backward compatibility).
     */
    public EntryArena(MemoryManagerAllocator allocator) {
        this(allocator, AllocationStrategy.LINEAR);
    }

    /**
     * Creates an Entry Arena with the specified allocation strategy.
     */
    public EntryArena(MemoryManagerAllocator allocator, AllocationStrategy strategy) {
        this.allocator = allocator;
        this.strategy = strategy;
        this.segments = new ArrayList<>();
        this.originalAllocations = new ArrayList<>();
        this.totalAllocated = 0;
        this.activeEntries = 0;
        this.closed = false;

        // Initialize free list structures only if needed
        if (strategy == AllocationStrategy.FREE_LIST) {
            this.freeListHeads = new HashMap<>();
            for (SizeClass sizeClass : SizeClass.values()) {
                freeListHeads.put(sizeClass, null);
            }
            this.totalFreedMemory = 0;
            this.totalFreeBlocks = 0;
        } else {
            this.freeListHeads = null;
            this.totalFreedMemory = 0;
            this.totalFreeBlocks = 0;
        }

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
     * For FREE_LIST strategy, this properly frees the old entry.
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
                // For FREE_LIST strategy, properly free the old entry
                if (strategy == AllocationStrategy.FREE_LIST) {
                    int oldSize = getEntrySize(address);
                    if (oldSize > 0) {
                        addToFreeList(address, oldSize);
                    }
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
     * Removes an entry and potentially adds it to free list.
     */
    public void removeEntry(long address) {
        if (closed || address == 0) {
            return;
        }

        try {
            if (strategy == AllocationStrategy.FREE_LIST) {
                // Get the size of the entry to be removed
                int entrySize = getEntrySize(address);
                if (entrySize > 0) {
                    // Add to free list for reuse
                    addToFreeList(address, entrySize);
                }
            }

            // Decrement active entries count
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

        // Try free list allocation first if using FREE_LIST strategy
        if (strategy == AllocationStrategy.FREE_LIST) {
            long address = allocateFromFreeList(size);
            if (address != 0) {
                return address;
            }
        }

        // Fall back to linear allocation
        return linearAllocate(size);
    }

    /**
     * Linear allocation from current segment.
     */
    private long linearAllocate(int size) {
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

    /**
     * Attempts to allocate from free list.
     */
    private long allocateFromFreeList(int size) {
        SizeClass sizeClass = SizeClass.getSizeClass(size);
        FreeBlock head = freeListHeads.get(sizeClass);

        // Look for a suitable block in this size class
        FreeBlock prev = null;
        FreeBlock current = head;

        while (current != null) {
            if (current.size >= size) {
                // Found a suitable block
                if (prev == null) {
                    freeListHeads.put(sizeClass, current.next);
                } else {
                    prev.next = current.next;
                }

                totalFreeBlocks--;
                totalFreedMemory -= current.size;

                // If the block is much larger than needed, split it
                int remaining = current.size - size;
                if (remaining >= MIN_FREE_BLOCK_SIZE) {
                    long remainingAddress = current.address + size;
                    addToFreeList(remainingAddress, remaining);
                }

                return current.address;
            }
            prev = current;
            current = current.next;
        }

        // No suitable block found in this size class, try larger classes
        for (SizeClass largerClass : SizeClass.values()) {
            if (largerClass.ordinal() > sizeClass.ordinal()) {
                head = freeListHeads.get(largerClass);
                current = head;

                if (current != null && current.size >= size) {
                    // Use the first block from a larger class
                    freeListHeads.put(largerClass, current.next);
                    totalFreeBlocks--;
                    totalFreedMemory -= current.size;

                    // Split the block if it's much larger
                    int remaining = current.size - size;
                    if (remaining >= MIN_FREE_BLOCK_SIZE) {
                        long remainingAddress = current.address + size;
                        addToFreeList(remainingAddress, remaining);
                    }

                    return current.address;
                }
            }
        }

        return 0; // No suitable free block found
    }

    /**
     * Adds a block to the appropriate free list.
     */
    private void addToFreeList(long address, int size) {
        if (size < MIN_FREE_BLOCK_SIZE) {
            return; // Block too small to be useful
        }

        SizeClass sizeClass = SizeClass.getSizeClass(size);
        FreeBlock newBlock = new FreeBlock(address, size);

        // Insert at head of free list
        newBlock.next = freeListHeads.get(sizeClass);
        freeListHeads.put(sizeClass, newBlock);

        totalFreeBlocks++;
        totalFreedMemory += size;
    }

    private static int alignSize(int size) {
        return (size + ALIGNMENT - 1) & (-ALIGNMENT);
    }

    /**
     * Gets the current allocation strategy.
     */
    public AllocationStrategy getAllocationStrategy() {
        return strategy;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;

        // Clear free lists if using FREE_LIST strategy
        if (strategy == AllocationStrategy.FREE_LIST && freeListHeads != null) {
            for (SizeClass sizeClass : SizeClass.values()) {
                freeListHeads.put(sizeClass, null);
            }
            totalFreedMemory = 0;
            totalFreeBlocks = 0;
        }

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
            strategy,
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

        if (strategy == AllocationStrategy.FREE_LIST) {
            // For FREE_LIST, efficiency should consider active memory usage
            // We use totalAllocated as it represents the total amount of memory that has been used
            // The freed memory is available for reuse, so it's not "wasted"
            return (double) totalAllocated / systemMemory * 100.0;
        } else {
            // For LINEAR, efficiency = allocated / system
            return (double) totalAllocated / systemMemory * 100.0;
        }
    }

    /**
     * Compacts the arena by attempting to coalesce adjacent free blocks.
     * Only effective for FREE_LIST strategy.
     */
    public void compact() {
        if (strategy != AllocationStrategy.FREE_LIST || freeListHeads == null) {
            return; // No-op for LINEAR strategy
        }

        // Basic compaction: this is a placeholder for more sophisticated
        // compaction algorithms that could be implemented in the future
        // For now, we just ensure free lists are clean
        int coalescedBlocks = 0;

        // TODO: Implement more sophisticated compaction logic
        // This could include:
        // 1. Sorting free blocks by address
        // 2. Merging adjacent blocks
        // 3. Moving active entries to reduce fragmentation

        // For now, just report if we have fragmentation
        if (totalFreeBlocks > 0) {
            // Simple defragmentation opportunity exists
        }
    }

    /**
     * Statistics class for arena memory usage.
     */
    public static class ArenaStats {
        public final AllocationStrategy strategy;
        public final long totalSystemMemory;
        public final long totalAllocated;
        public final long totalFreed;
        public final int activeAllocations;
        public final int freeBlocks;
        public final double memoryEfficiency;
        public final double fragmentation;

        public ArenaStats(AllocationStrategy strategy, long totalSystemMemory, long totalAllocated,
                         long totalFreed, int activeAllocations, int freeBlocks, double memoryEfficiency) {
            this.strategy = strategy;
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
                "ArenaStats{strategy=%s, systemMemory=%d, allocated=%d, freed=%d, " +
                "activeEntries=%d, freeBlocks=%d, efficiency=%.2f%%, fragmentation=%.2f%%}",
                strategy, totalSystemMemory, totalAllocated, totalFreed,
                activeAllocations, freeBlocks, memoryEfficiency, fragmentation
            );
        }
    }
}
