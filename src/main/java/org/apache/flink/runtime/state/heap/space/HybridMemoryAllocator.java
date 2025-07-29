package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;

import java.util.List;

/**
 * This is for Flink MemoryManager-based allocators that can allocate and free memory segments.
 */
public interface HybridMemoryAllocator extends AutoCloseable {
    /** Allocates at least {@code bytes} bytes and returns the backing segments. */
    List<MemorySegment> allocate(int bytes) throws MemoryAllocationException;

    /** Releases the segments obtained from {@link #allocate}. */
    void free(List<MemorySegment> segments);

    /** Current amount of memory (bytes) that is still outstanding. */
    long outstandingBytes();

    /** Releases <b>all</b> outstanding memory that belongs to this allocator. */
    @Override
    void close();
}
