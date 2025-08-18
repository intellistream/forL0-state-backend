package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A memory allocator that uses Flink's MemoryManager to allocate and manage off-heap memory segments.
 * This allocator is single-threaded as Flink Task state access is single-threaded.
 * Supports both regular segment allocation and aligned memory allocation.
 */
public final class MemoryManagerAllocator implements HybridMemoryAllocator {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManagerAllocator.class);

    private final MemoryManager memoryManager;
    private final int pageSize;
    private final Object owner;
    private long usedBytes = 0;
    private boolean closed = false;

    // Track allocated segments and aligned memory blocks
    private final Map<List<MemorySegment>, Long> allocatedSegments = new HashMap<>();
    private final Map<Long, AlignedAllocation> alignedAllocations = new HashMap<>();

    /**
     * Creates a new MemoryManagerAllocator.
     *
     * @param memoryManager the Task's MemoryManager (obtained from RuntimeContext → Environment)
     * @param owner an arbitrary token that identifies this allocator instance
     */
    public MemoryManagerAllocator(MemoryManager memoryManager, Object owner) {
        if (memoryManager == null) {
            throw new IllegalArgumentException(
                "MemoryManager cannot be null. This usually indicates that the Flink environment " +
                "is not properly configured or the state backend is being used in an unsupported context. " +
                "Please ensure that the task is running in a proper Flink TaskManager environment.");
        }

        this.memoryManager = memoryManager;
        this.pageSize = memoryManager.getPageSize();
        this.owner = owner;

        LOG.debug("Created MemoryManagerAllocator with page size: {} bytes", pageSize);
    }

    @Override
    public List<MemorySegment> allocate(int bytes) throws MemoryAllocationException {
        ensureOpen();

        if (bytes <= 0) {
            throw new IllegalArgumentException("Requested bytes must be positive, but was: " + bytes);
        }

        // Calculate number of pages needed
        int numPages = MathUtils.divideRoundUp(bytes, pageSize);

        try {
            List<MemorySegment> segments = memoryManager.allocatePages(owner, numPages);

            if (segments.isEmpty()) {
                throw new MemoryAllocationException("Failed to allocate " + numPages + " pages");
            }

            // Track allocated segments with the actual allocated size
            long allocatedBytes = (long) segments.size() * pageSize;
            allocatedSegments.put(segments, allocatedBytes);
            usedBytes += allocatedBytes;

            LOG.debug("Allocated {} pages ({} bytes) for owner {}", segments.size(), allocatedBytes, owner);
            return segments;

        } catch (Exception e) {
            throw new MemoryAllocationException("Failed to allocate memory", e);
        }
    }

    @Override
    public long allocateAligned(long size, int alignment) throws MemoryAllocationException {
        ensureOpen();

        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }

        if (!isPowerOfTwo(alignment)) {
            throw new IllegalArgumentException("Alignment must be power of 2");
        }

        // Allocate extra space to ensure we can align the result
        int extraSpace = alignment - 1;
        int totalSize = (int) (size + extraSpace);

        // Calculate number of pages needed
        int numPages = MathUtils.divideRoundUp(totalSize, pageSize);

        try {
            List<MemorySegment> segments = memoryManager.allocatePages(owner, numPages);

            if (segments.isEmpty()) {
                throw new MemoryAllocationException("Failed to allocate memory segments for aligned allocation");
            }

            // For now, we'll use the first segment and assume it's off-heap
            MemorySegment segment = segments.get(0);

            if (segment.isOffHeap()) {
                long baseAddress = segment.getAddress();
                long alignedAddress = (baseAddress + alignment - 1) & ~((long) alignment - 1);

                // Store allocation info for cleanup
                long allocatedBytes = (long) segments.size() * pageSize;
                AlignedAllocation allocation = new AlignedAllocation(segments, baseAddress, alignedAddress, (int) size, allocatedBytes);
                alignedAllocations.put(alignedAddress, allocation);

                // Track memory usage
                usedBytes += allocatedBytes;

                LOG.debug("Allocated aligned memory: base=0x{}, aligned=0x{}, size={}, alignment={}, pages={}",
                         Long.toHexString(baseAddress), Long.toHexString(alignedAddress), size, alignment, segments.size());

                return alignedAddress;
            } else {
                // Release the segments since we can't use heap memory for aligned allocation
                memoryManager.release(segments);
                throw new MemoryAllocationException("Cannot perform aligned allocation on heap memory segments");
            }
        } catch (Exception e) {
            throw new MemoryAllocationException("Failed to allocate aligned memory", e);
        }
    }

    @Override
    public List<MemorySegment> allocateL0(int bytes) throws MemoryAllocationException {
        ensureOpen();

        // Currently, this is a reserved interface for future L0 integration
        // For now, we simply delegate to the regular allocate method
        LOG.debug("Allocating L0 memory using regular allocation (reserved interface), bytes: {}", bytes);
        return allocate(bytes);
    }

    @Override
    public void release(List<MemorySegment> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }

        try {
            // First, remove from tracking to get the allocated size
            Long allocatedBytes = allocatedSegments.remove(segments);

            // Release the memory
            memoryManager.release(segments);

            // Update used bytes only if this was actually tracked
            if (allocatedBytes != null) {
                usedBytes -= allocatedBytes;
                LOG.debug("Released {} memory segments ({} bytes) for owner {}", segments.size(), allocatedBytes, owner);
            } else {
                LOG.debug("Released {} untracked memory segments for owner {}", segments.size(), owner);
            }

        } catch (Exception e) {
            LOG.warn("Error releasing memory segments for owner {}: {}", owner, e.getMessage());
        }
    }

    @Override
    public void deallocate(long address, long size) {
        // 忽略无效地址的释放请求（常见于测试用例），避免产生误导性告警
        if (address == 0L) {
            LOG.debug("Ignore deallocate for null/zero aligned address");
            return;
        }
        AlignedAllocation allocation = alignedAllocations.remove(address);
        if (allocation != null) {
            // Update used bytes first
            usedBytes -= allocation.allocatedBytes;

            // Then release the underlying segments
            try {
                memoryManager.release(allocation.segments);
                LOG.debug("Deallocated aligned memory at address 0x{} ({} bytes)", Long.toHexString(address), allocation.allocatedBytes);
            } catch (Exception e) {
                LOG.warn("Error releasing aligned memory segments: {}", e.getMessage());
            }
        } else {
            // 将告警降级为调试日志：未知地址通常来自边界/生命周期测试
            LOG.debug("Attempted to deallocate unknown aligned address: 0x{}", Long.toHexString(address));
        }
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Gets the current memory usage in bytes.
     */
    public long getUsedBytes() {
        return usedBytes;
    }

    /**
     * Gets the number of currently allocated segment lists.
     */
    public int getAllocatedSegmentLists() {
        return allocatedSegments.size();
    }

    /**
     * Gets the number of currently allocated aligned memory blocks.
     */
    public int getAllocatedAlignedBlocks() {
        return alignedAllocations.size();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            LOG.debug("Closing MemoryManagerAllocator for owner {}", owner);

            // Release all aligned allocations first
            for (AlignedAllocation allocation : alignedAllocations.values()) {
                try {
                    usedBytes -= allocation.allocatedBytes;
                    memoryManager.release(allocation.segments);
                } catch (Exception e) {
                    LOG.warn("Error releasing aligned allocation during close: {}", e.getMessage());
                }
            }
            alignedAllocations.clear();

            // Release all remaining segment allocations
            for (Map.Entry<List<MemorySegment>, Long> entry : allocatedSegments.entrySet()) {
                try {
                    usedBytes -= entry.getValue();
                    memoryManager.release(entry.getKey());
                } catch (Exception e) {
                    LOG.warn("Error releasing segments during close: {}", e.getMessage());
                }
            }
            allocatedSegments.clear();

            LOG.debug("MemoryManagerAllocator closed for owner {}, final used bytes: {}", owner, usedBytes);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("MemoryManagerAllocator is closed");
        }
    }

    private static boolean isPowerOfTwo(int n) {
        return n > 0 && (n & (n - 1)) == 0;
    }

    /**
     * Information about an aligned memory allocation.
     */
    private static class AlignedAllocation {
        final List<MemorySegment> segments;
        final long baseAddress;
        final long alignedAddress;
        final int size;
        final long allocatedBytes;

        AlignedAllocation(List<MemorySegment> segments, long baseAddress, long alignedAddress, int size, long allocatedBytes) {
            this.segments = segments;
            this.baseAddress = baseAddress;
            this.alignedAddress = alignedAddress;
            this.size = size;
            this.allocatedBytes = allocatedBytes;
        }
    }
}
