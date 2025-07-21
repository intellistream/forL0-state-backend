package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

public final class MemoryManagerAllocator implements HybridMemoryAllocator {

    private final MemoryManager memoryManager;
    private final int pageSize;
    private final Object owner;
    private final LongAdder usedBytes = new LongAdder();
    private volatile boolean closed;

    /**
     * @param mm    the Task's MemoryManager (obtained from RuntimeContext → Environment)
     * @param owner an arbitrary token that identifies <i>this</i> allocator
     */
    public MemoryManagerAllocator(MemoryManager mm, Object owner) {
        this.memoryManager = mm;
        this.pageSize = mm.getPageSize();
        this.owner = owner;
    }

    @Override
    public List<MemorySegment> allocate(int bytes) throws MemoryAllocationException {
        ensureOpen();

        final int numPages = MathUtils.divideRoundUp(bytes, pageSize);
        final List<MemorySegment> segments =
                new ArrayList<>(memoryManager.allocatePages(owner, numPages));

        usedBytes.add(numPages * (long) pageSize);
        return segments;
    }

    @Override
    public void free(List<MemorySegment> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }

        for (MemorySegment seg : segments) {
            // return the page to the MemoryManager (= to the global pool)
            seg.free(); // internally triggers MemoryManager#releaseMemory(...)
            usedBytes.add(-pageSize);
        }
    }

    public void free(MemorySegment segment) {
        if (segment != null) {
            segment.free(); // internally triggers MemoryManager#releaseMemory(...)
            usedBytes.add(-pageSize);
        }
    }

    @Override
    public long outstandingBytes() {
        return usedBytes.sum();
    }

    @Override
    public void close() {
        if (!closed) {
            // one call is enough; MemoryManager tracks all pages by owner
            memoryManager.releaseAll(owner);
            closed = true;
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("allocator has already been closed");
        }
    }

    public long getPageSize() {
        return  pageSize;
    }
}
