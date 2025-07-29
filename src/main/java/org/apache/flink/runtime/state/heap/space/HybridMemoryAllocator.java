package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;

import java.util.List;

/**
 * Interface for hybrid memory allocation supporting both regular and aligned allocations.
 */
public interface HybridMemoryAllocator extends AutoCloseable {

    /**
     * Allocates memory segments for the specified number of bytes.
     *
     * @param bytes Number of bytes to allocate
     * @return List of memory segments
     * @throws MemoryAllocationException if allocation fails
     */
    List<MemorySegment> allocate(int bytes) throws MemoryAllocationException;

    /**
     * Allocates aligned memory and returns the raw address.
     *
     * @param size Size in bytes
     * @param alignment Alignment requirement in bytes (must be power of 2)
     * @return Aligned memory address
     * @throws MemoryAllocationException if allocation fails
     */
    long allocateAligned(long size, int alignment) throws MemoryAllocationException;

    /**
     * Releases previously allocated memory segments.
     *
     * @param segments Memory segments to release
     */
    void release(List<MemorySegment> segments);

    /**
     * Deallocates previously allocated aligned memory.
     *
     * @param address The aligned address returned by allocateAligned
     * @param size The size that was originally allocated
     */
    void deallocate(long address, long size);

    /**
     * Gets the page size of the underlying memory manager.
     *
     * @return Page size in bytes
     */
    int getPageSize();

    /**
     * Checks if the allocator is closed.
     *
     * @return true if closed, false otherwise
     */
    boolean isClosed();
}
