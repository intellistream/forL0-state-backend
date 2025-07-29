package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.List;

/**
 * Entry Arena manages memory allocation for KVNode objects.
 * Uses slab allocation and free list management for efficient memory utilization.
 * Single-threaded implementation as Flink Task state access is single-threaded.
 */
public class EntryArena implements AutoCloseable {

    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();

    // Memory alignment (8 bytes for better performance)
    private static final int ALIGNMENT = 8;
    private static final int MIN_ALLOCATION_SIZE = 32;  // Minimum KVNode size

    // Slab management
    private static final int SLAB_SIZE = 64 * 1024;  // 64KB per slab

    private final MemoryManagerAllocator allocator;
    private final List<Slab> slabs;
    private final FreeList freeList;

    // Current allocation slab
    private Slab currentSlab;
    private long currentOffset;

    // Statistics
    private long totalAllocated;
    private long totalFreed;
    private int activeAllocations;

    /**
     * Creates an Entry Arena with the given memory allocator.
     */
    public EntryArena(MemoryManagerAllocator allocator) {
        this.allocator = allocator;
        this.slabs = new ArrayList<>();
        this.freeList = new FreeList();
        this.totalAllocated = 0;
        this.totalFreed = 0;
        this.activeAllocations = 0;

        // Allocate initial slab
        allocateNewSlab();
    }

    /**
     * Allocates memory for a KVNode of the specified size.
     *
     * @param size Required size in bytes
     * @return Memory address of allocated block, or 0 if allocation failed
     */
    public long allocate(int size) {
        if (size <= 0) {
            return 0;
        }

        // Align size to 8-byte boundary
        int alignedSize = alignSize(Math.max(size, MIN_ALLOCATION_SIZE));

        // Try to allocate from free list first
        long address = freeList.allocate(alignedSize);
        if (address != 0) {
            totalAllocated += alignedSize;
            activeAllocations++;
            return address;
        }

        // Allocate from current slab
        if (currentSlab != null && currentOffset + alignedSize <= currentSlab.endAddress) {
            address = currentOffset;
            currentOffset += alignedSize;
            totalAllocated += alignedSize;
            activeAllocations++;
            return address;
        }

        // Need new slab
        if (!allocateNewSlab()) {
            return 0; // Failed to allocate new slab
        }

        // Try again with new slab
        if (currentOffset + alignedSize <= currentSlab.endAddress) {
            address = currentOffset;
            currentOffset += alignedSize;
            totalAllocated += alignedSize;
            activeAllocations++;
            return address;
        }

        return 0; // Size too large for a single slab
    }

    /**
     * Frees a previously allocated KVNode.
     *
     * @param address Memory address to free
     * @param size Size of the block to free
     */
    public void deallocate(long address, int size) {
        if (address == 0 || size <= 0) {
            return;
        }

        int alignedSize = alignSize(Math.max(size, MIN_ALLOCATION_SIZE));

        // Add to free list
        freeList.deallocate(address, alignedSize);

        totalFreed += alignedSize;
        activeAllocations--;
    }

    /**
     * Gets memory usage statistics.
     */
    public ArenaStats getStats() {
        return new ArenaStats(
            slabs.size() * SLAB_SIZE,  // Total allocated from system
            totalAllocated,            // Total allocated to users
            totalFreed,                // Total freed by users
            activeAllocations,         // Current active allocations
            freeList.getFreeBlocks()   // Available free blocks
        );
    }

    /**
     * Compacts the arena by moving active allocations to reduce fragmentation.
     * This is an expensive operation and should be used sparingly.
     */
    public void compact() {
        // For now, just rebuild free list to merge adjacent blocks
        freeList.compact();
    }

    private boolean allocateNewSlab() {
        try {
            long slabAddress = allocator.allocateAligned(SLAB_SIZE, ALIGNMENT);
            Slab slab = new Slab(slabAddress, slabAddress + SLAB_SIZE);

            slabs.add(slab);
            currentSlab = slab;
            currentOffset = slabAddress;

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static int alignSize(int size) {
        return (size + ALIGNMENT - 1) & (-ALIGNMENT);
    }

    @Override
    public void close() throws Exception {
        // Free all slabs
        for (Slab slab : slabs) {
            try {
                allocator.deallocate(slab.baseAddress, SLAB_SIZE);
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
        slabs.clear();
        freeList.clear();
    }

    /**
     * Represents a memory slab.
     */
    private static class Slab {
        final long baseAddress;
        final long endAddress;

        Slab(long baseAddress, long endAddress) {
            this.baseAddress = baseAddress;
            this.endAddress = endAddress;
        }
    }

    /**
     * Free list management for deallocated blocks.
     * Single-threaded implementation.
     */
    private static class FreeList {
        private final List<FreeBlock> freeBlocks;

        FreeList() {
            this.freeBlocks = new ArrayList<>();
        }

        /**
         * Allocates a block from free list.
         */
        long allocate(int size) {
            // Find best fit block
            int bestFitIndex = -1;
            int bestFitSize = Integer.MAX_VALUE;

            for (int i = 0; i < freeBlocks.size(); i++) {
                FreeBlock block = freeBlocks.get(i);
                if (block.size >= size && block.size < bestFitSize) {
                    bestFitIndex = i;
                    bestFitSize = block.size;

                    // Perfect fit, use immediately
                    if (block.size == size) {
                        break;
                    }
                }
            }

            if (bestFitIndex == -1) {
                return 0; // No suitable block found
            }

            FreeBlock block = freeBlocks.get(bestFitIndex);
            long address = block.address;

            if (block.size == size) {
                // Use entire block
                freeBlocks.remove(bestFitIndex);
            } else {
                // Split block
                block.address += size;
                block.size -= size;
            }

            return address;
        }

        /**
         * Adds a block to free list.
         */
        void deallocate(long address, int size) {
            FreeBlock newBlock = new FreeBlock(address, size);

            // Try to merge with adjacent blocks
            boolean merged = false;

            for (int i = 0; i < freeBlocks.size(); i++) {
                FreeBlock existing = freeBlocks.get(i);

                // Check if new block is adjacent to existing block
                if (existing.address + existing.size == address) {
                    // Merge with previous block
                    existing.size += size;
                    merged = true;

                    // Check if we can merge with next block too
                    for (int j = 0; j < freeBlocks.size(); j++) {
                        if (j != i) {
                            FreeBlock next = freeBlocks.get(j);
                            if (existing.address + existing.size == next.address) {
                                existing.size += next.size;
                                freeBlocks.remove(j);
                                break;
                            }
                        }
                    }
                    break;
                } else if (address + size == existing.address) {
                    // Merge with next block
                    existing.address = address;
                    existing.size += size;
                    merged = true;
                    break;
                }
            }

            if (!merged) {
                freeBlocks.add(newBlock);
            }
        }

        /**
         * Compacts free list by merging adjacent blocks.
         */
        void compact() {
            if (freeBlocks.size() <= 1) {
                return;
            }

            // Sort by address
            freeBlocks.sort((a, b) -> Long.compare(a.address, b.address));

            // Merge adjacent blocks
            List<FreeBlock> compacted = new ArrayList<>();
            FreeBlock current = freeBlocks.get(0);

            for (int i = 1; i < freeBlocks.size(); i++) {
                FreeBlock next = freeBlocks.get(i);

                if (current.address + current.size == next.address) {
                    // Merge blocks
                    current.size += next.size;
                } else {
                    // Add current block and move to next
                    compacted.add(current);
                    current = next;
                }
            }

            compacted.add(current);

            freeBlocks.clear();
            freeBlocks.addAll(compacted);
        }

        int getFreeBlocks() {
            return freeBlocks.size();
        }

        void clear() {
            freeBlocks.clear();
        }
    }

    /**
     * Represents a free memory block.
     */
    private static class FreeBlock {
        long address;
        int size;

        FreeBlock(long address, int size) {
            this.address = address;
            this.size = size;
        }
    }

    /**
     * Statistics about arena usage.
     */
    public static class ArenaStats {
        public final long totalSystemMemory;
        public final long totalAllocated;
        public final long totalFreed;
        public final int activeAllocations;
        public final int freeBlocks;
        public final long usedMemory;
        public final double fragmentation;

        public ArenaStats(long totalSystemMemory, long totalAllocated, long totalFreed,
                         int activeAllocations, int freeBlocks) {
            this.totalSystemMemory = totalSystemMemory;
            this.totalAllocated = totalAllocated;
            this.totalFreed = totalFreed;
            this.activeAllocations = activeAllocations;
            this.freeBlocks = freeBlocks;
            this.usedMemory = totalAllocated - totalFreed;
            this.fragmentation = totalSystemMemory > 0 ?
                (double) freeBlocks / (totalSystemMemory / 1024) : 0.0;
        }

        @Override
        public String toString() {
            return String.format("Arena[system=%dKB, used=%dKB, active=%d, fragments=%d, frag=%.2f%%]",
                totalSystemMemory / 1024, usedMemory / 1024, activeAllocations,
                freeBlocks, fragmentation * 100);
        }
    }
}
