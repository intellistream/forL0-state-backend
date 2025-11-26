package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * A temporary L0 memory allocator implementation using heap memory.
 * This is used for development and testing purposes until the native JNI implementation is ready.
 *
 * <p>This implementation allocates memory from the JVM heap using {@link MemorySegmentFactory#allocateUnpooledSegment}.
 * It simulates the L0 memory allocation interface but does not provide the performance characteristics
 * of real L0 memory (CXL/PMEM).
 *
 * <p>In production, this should be replaced by {@code NativeL0MemoryAllocator} which uses JNI
 * to allocate memory from specialized hardware (CXL memory, PMEM, etc.).
 */
public class HeapL0MemoryAllocator implements L0MemoryAllocator {

    private static final Logger LOG = LoggerFactory.getLogger(HeapL0MemoryAllocator.class);

    /** Default segment size for heap allocation (32KB) */
    private static final int DEFAULT_SEGMENT_SIZE = 32 * 1024;

    private final int segmentSize;
    private final long maxCapacity;
    private long usedBytes = 0;
    private boolean closed = false;

    // Track allocated segments for proper cleanup
    private final Map<List<MemorySegment>, Long> allocatedSegments = new IdentityHashMap<>();

    /**
     * Creates a HeapL0MemoryAllocator with default settings.
     * Uses default segment size (32KB) and unlimited capacity.
     */
    public HeapL0MemoryAllocator() {
        this(DEFAULT_SEGMENT_SIZE, -1);
    }

    /**
     * Creates a HeapL0MemoryAllocator with specified segment size.
     *
     * @param segmentSize Size of each memory segment in bytes
     */
    public HeapL0MemoryAllocator(int segmentSize) {
        this(segmentSize, -1);
    }

    /**
     * Creates a HeapL0MemoryAllocator with specified segment size and capacity limit.
     *
     * @param segmentSize Size of each memory segment in bytes
     * @param maxCapacity Maximum total capacity in bytes, or -1 for unlimited
     */
    public HeapL0MemoryAllocator(int segmentSize, long maxCapacity) {
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("Segment size must be positive: " + segmentSize);
        }
        this.segmentSize = segmentSize;
        this.maxCapacity = maxCapacity;
        LOG.debug("Created HeapL0MemoryAllocator with segmentSize={}, maxCapacity={}",
                  segmentSize, maxCapacity == -1 ? "unlimited" : maxCapacity);
    }

    @Override
    public List<MemorySegment> allocate(int bytes) throws L0MemoryAllocationException {
        ensureOpen();

        if (bytes <= 0) {
            throw new IllegalArgumentException("Requested bytes must be positive: " + bytes);
        }

        // Check capacity limit
        if (maxCapacity > 0 && usedBytes + bytes > maxCapacity) {
            throw new L0MemoryAllocationException(
                String.format("L0 memory allocation failed: requested %d bytes, but only %d bytes available (used: %d, capacity: %d)",
                              bytes, maxCapacity - usedBytes, usedBytes, maxCapacity));
        }

        try {
            // Calculate number of segments needed
            int numSegments = (bytes + segmentSize - 1) / segmentSize;
            List<MemorySegment> segments = new ArrayList<>(numSegments);

            long totalAllocated = 0;
            for (int i = 0; i < numSegments; i++) {
                // For the last segment, only allocate what's needed
                int sizeToAllocate = (i == numSegments - 1) 
                    ? bytes - (i * segmentSize) 
                    : segmentSize;
                
                // Use unpooled segment (not managed by Flink's MemoryManager)
                MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(sizeToAllocate);
                segments.add(segment);
                totalAllocated += sizeToAllocate;
            }

            // Track allocation
            allocatedSegments.put(segments, totalAllocated);
            usedBytes += totalAllocated;

            LOG.debug("Allocated {} L0 memory segments ({} bytes total)", numSegments, totalAllocated);
            return segments;

        } catch (OutOfMemoryError e) {
            throw new L0MemoryAllocationException("L0 memory allocation failed due to OutOfMemoryError", e);
        } catch (Exception e) {
            throw new L0MemoryAllocationException("L0 memory allocation failed", e);
        }
    }

    @Override
    public void release(List<MemorySegment> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }

        Long allocatedBytes = allocatedSegments.remove(segments);
        
        // Free each segment
        for (MemorySegment segment : segments) {
            if (segment != null) {
                segment.free();
            }
        }

        if (allocatedBytes != null) {
            usedBytes -= allocatedBytes;
            LOG.debug("Released {} L0 memory segments ({} bytes)", segments.size(), allocatedBytes);
        } else {
            LOG.debug("Released {} untracked L0 memory segments", segments.size());
        }
    }

    @Override
    public long getUsedBytes() {
        return usedBytes;
    }

    @Override
    public long getTotalCapacity() {
        return maxCapacity;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            LOG.debug("Closing HeapL0MemoryAllocator");

            // Release all remaining allocations
            for (Map.Entry<List<MemorySegment>, Long> entry : allocatedSegments.entrySet()) {
                for (MemorySegment segment : entry.getKey()) {
                    if (segment != null) {
                        segment.free();
                    }
                }
            }
            allocatedSegments.clear();
            usedBytes = 0;

            LOG.debug("HeapL0MemoryAllocator closed");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("HeapL0MemoryAllocator is closed");
        }
    }

    /**
     * Gets the segment size used by this allocator.
     *
     * @return Segment size in bytes
     */
    public int getSegmentSize() {
        return segmentSize;
    }

    /**
     * Gets the number of currently allocated segment lists.
     *
     * @return Number of allocations
     */
    public int getAllocationCount() {
        return allocatedSegments.size();
    }
}
