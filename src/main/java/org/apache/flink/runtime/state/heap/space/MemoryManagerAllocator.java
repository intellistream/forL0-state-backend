package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * A memory allocator that uses Flink's MemoryManager to allocate and manage off-heap memory segments.
 * This allocator is thread-safe and tracks memory usage for proper cleanup.
 */
public final class MemoryManagerAllocator implements HybridMemoryAllocator {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryManagerAllocator.class);

    private final MemoryManager memoryManager;
    private final int pageSize;
    private final Object owner;
    private final LongAdder usedBytes = new LongAdder();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new MemoryManagerAllocator.
     *
     * @param memoryManager the Task's MemoryManager (obtained from RuntimeContext → Environment)
     * @param owner an arbitrary token that identifies this allocator instance
     */
    public MemoryManagerAllocator(MemoryManager memoryManager, Object owner) {
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

        final int numPages = MathUtils.divideRoundUp(bytes, pageSize);

        try {
            // Allocate pages from MemoryManager
            final List<MemorySegment> segments = memoryManager.allocatePages(owner, numPages);

            if (segments == null || segments.isEmpty()) {
                throw new MemoryAllocationException("Failed to allocate " + numPages + " pages from MemoryManager");
            }

            final long allocatedBytes = numPages * (long) pageSize;
            usedBytes.add(allocatedBytes);

            LOG.debug("Allocated {} pages ({} bytes) for {} bytes request. Total outstanding: {} bytes",
                    numPages, allocatedBytes, bytes, usedBytes.sum());

            return segments;

        } catch (Exception e) {
            throw new MemoryAllocationException("Failed to allocate " + bytes + " bytes (" + numPages + " pages)", e);
        }
    }

    @Override
    public void free(List<MemorySegment> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }

        // Check if segments are already freed to prevent double-free
        for (MemorySegment segment : segments) {
            if (segment.isFreed()) {
                LOG.warn("Attempting to free already freed segment, skipping");
                return;
            }
        }

        final long releasedBytes = segments.size() * (long) pageSize;

        try {
            // Create a mutable copy of the segments list to avoid UnsupportedOperationException
            // since MemoryManager.allocatePages() might return an immutable list
            List<MemorySegment> mutableSegments = new ArrayList<>(segments);

            // Release pages back to MemoryManager
            memoryManager.release(mutableSegments);

            // Only update counter if release was successful
            usedBytes.add(-releasedBytes);

            LOG.debug("Released {} pages ({} bytes). Remaining outstanding: {} bytes",
                    segments.size(), releasedBytes, usedBytes.sum());

        } catch (Exception e) {
            LOG.error("Error releasing memory segments, counter not updated", e);
            // Don't update counter if release failed
            throw new RuntimeException("Failed to release memory segments", e);
        }
    }

    /**
     * Convenience method to free a single memory segment.
     *
     * @param segment the memory segment to free
     */
    public void free(MemorySegment segment) {
        if (segment != null) {
            free(Collections.singletonList(segment));
        }
    }

    @Override
    public long outstandingBytes() {
        return Math.max(0, usedBytes.sum()); // Ensure non-negative value
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                // Release all outstanding memory for this owner
                memoryManager.releaseAll(owner);

                final long outstandingBeforeClose = usedBytes.sum();
                usedBytes.reset(); // Reset counter after releasing all memory

                LOG.debug("Closed MemoryManagerAllocator, released {} bytes", outstandingBeforeClose);

            } catch (Exception e) {
                LOG.warn("Error during MemoryManagerAllocator cleanup", e);
            }
        }
    }

    /**
     * Returns the page size used by this allocator.
     *
     * @return the page size in bytes
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Returns the owner object associated with this allocator.
     *
     * @return the owner object
     */
    public Object getOwner() {
        return owner;
    }

    /**
     * Checks if this allocator has been closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("MemoryManagerAllocator has already been closed");
        }
    }
}
