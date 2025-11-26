package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;

import java.util.List;

/**
 * Interface for L0 memory allocation.
 * L0 memory is specialized memory (e.g., CXL memory, PMEM) allocated via JNI native methods.
 * This is separate from the regular Flink MemoryManager-managed memory used by MainTable and EntryArena.
 *
 * <p>The L0 memory is used exclusively for the L0Table (hot key cache) and has different
 * characteristics from regular off-heap memory:
 * <ul>
 *     <li>May be backed by CXL (Compute Express Link) memory</li>
 *     <li>May be backed by persistent memory (PMEM)</li>
 *     <li>Allocated and managed by native code via JNI</li>
 * </ul>
 *
 * <p>Implementations:
 * <ul>
 *     <li>{@link HeapL0MemoryAllocator} - Temporary implementation using heap memory for development/testing</li>
 *     <li>NativeL0MemoryAllocator - Future implementation using JNI native methods (TODO)</li>
 * </ul>
 */
public interface L0MemoryAllocator extends AutoCloseable {

    /**
     * Allocates L0 memory segments for the specified number of bytes.
     *
     * @param bytes Number of bytes to allocate
     * @return List of memory segments backed by L0 memory
     * @throws L0MemoryAllocationException if allocation fails
     */
    List<MemorySegment> allocate(int bytes) throws L0MemoryAllocationException;

    /**
     * Releases previously allocated L0 memory segments.
     *
     * @param segments Memory segments to release
     */
    void release(List<MemorySegment> segments);

    /**
     * Gets the current L0 memory usage in bytes.
     *
     * @return Used bytes
     */
    long getUsedBytes();

    /**
     * Gets the total L0 memory capacity in bytes.
     * Returns -1 if the capacity is unlimited or unknown.
     *
     * @return Total capacity in bytes, or -1 if unknown
     */
    long getTotalCapacity();

    /**
     * Checks if the allocator is closed.
     *
     * @return true if closed, false otherwise
     */
    boolean isClosed();

    /**
     * Exception thrown when L0 memory allocation fails.
     */
    class L0MemoryAllocationException extends Exception {
        public L0MemoryAllocationException(String message) {
            super(message);
        }

        public L0MemoryAllocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
