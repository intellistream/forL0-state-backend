package org.apache.flink.runtime.state.heap.space;

public interface OffHeapMemoryAllocator extends AutoCloseable {

    /**
     * Allocate at least a slice of native memory of {@code bytes}.
     *
     * @param bytes size of allocation, &gt; 0
     * @return memory slice
     */
    MemorySlice allocate(int bytes);

    /**
     * Return the memory slice obtained by {@link #allocate(int)}
     * Implementation: call {@code Unsafe.freeMemory} or put in a buffer pool
     * @param slice the memory slice to be freed
     */
    void free(MemorySlice slice);

    /**
     * Get allocated yet not returned bytes num
     * @return the number of bytes allocated
     */
    long outstandingBytes();

    /**
     * Release all memory slices
     */
    @Override
    void close();
}
