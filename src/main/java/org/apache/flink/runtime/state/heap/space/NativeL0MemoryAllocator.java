package org.apache.flink.runtime.state.heap.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * L0 memory allocator implementation using JNI native methods.
 * This allocator uses C malloc/free for memory allocation, providing
 * direct access to native memory outside the JVM heap.
 *
 * <p>This implementation provides raw native memory addresses without any
 * wrapper overhead. Memory is accessed directly via UNSAFE for maximum performance.
 *
 * <p>The native library must be available for this allocator to work.
 * If the native library is not available, an {@link IllegalStateException} will be thrown.
 *
 * @see NativeL0Memory
 */
public class NativeL0MemoryAllocator implements L0MemoryAllocator {

    private static final Logger LOG = LoggerFactory.getLogger(NativeL0MemoryAllocator.class);

    /** Default alignment for memory allocations (64 bytes for cache line alignment) */
    private static final int DEFAULT_ALIGNMENT = 64;

    private final long maxCapacity;
    private long usedBytes = 0;
    private boolean closed = false;

    /** Track allocated memory: allocation -> address/size info */
    private final Map<L0Allocation, Long> allocations = new IdentityHashMap<>();

    /**
     * Creates a NativeL0MemoryAllocator with unlimited capacity.
     *
     * @throws IllegalStateException if native library is not available
     */
    public NativeL0MemoryAllocator() {
        this(-1);
    }

    /**
     * Creates a NativeL0MemoryAllocator with specified capacity limit.
     *
     * @param maxCapacity Maximum total capacity in bytes, or -1 for unlimited
     * @throws IllegalStateException if native library is not available
     */
    public NativeL0MemoryAllocator(long maxCapacity) {
        if (!NativeL0Memory.isAvailable()) {
            throw new IllegalStateException(
                "Native L0 memory library is not available: " + NativeL0Memory.getLoadError() +
                ". Please ensure the native library is in java.library.path.");
        }
        this.maxCapacity = maxCapacity;
        
        // Pass capacity to native layer BEFORE any allocation
        if (maxCapacity > 0) {
            NativeL0Memory.setMaxCapacity(maxCapacity);
        }
        
        LOG.info("Created NativeL0MemoryAllocator with maxCapacity={}",
                 maxCapacity == -1 ? "unlimited" : maxCapacity);
    }

    @Override
    public L0Allocation allocate(int bytes) throws L0MemoryAllocationException {
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

        // Try to use raw pool if in L0 mode (more efficient for fixed-size allocations)
        if (NativeL0Memory.isL0Mode()) {
            return allocateRawPool(bytes);
        }

        // Fall back to dynamic allocation (simulation mode or fallback)
        return allocateWithMalloc(bytes);
    }

    /**
     * Allocates memory using raw memory pool (more efficient in L0 mode).
     * Each allocation gets a dedicated raw pool from L0 library.
     *
     * @param bytes Number of bytes to allocate
     * @return L0Allocation handle
     * @throws L0MemoryAllocationException if allocation fails
     */
    private L0Allocation allocateRawPool(int bytes) throws L0MemoryAllocationException {
        // Generate unique pool name
        String poolName = String.format("l0table_%d_%d", System.nanoTime(), bytes);

        // Create raw pool (returns address of the memory block)
        long address = NativeL0Memory.createRawPool(poolName, bytes);
        if (address == 0) {
            String mode = NativeL0Memory.isL0Mode() ? "L0 mode" : "Simulation mode";
            throw new L0MemoryAllocationException(
                String.format("Failed to create raw pool for %d bytes (%s). Memory may be exhausted. Check native logs for details.",
                    bytes, mode));
        }

        // Zero-initialize the memory
        NativeL0Memory.memset(address, (byte) 0, bytes);

        // Create allocation handle
        L0Allocation allocation = new L0Allocation(address, bytes);

        // Track allocation (mark as raw pool with negative address)
        allocations.put(allocation, -address); // negative indicates raw pool
        usedBytes += bytes;

        LOG.debug("Allocated {} bytes raw pool '{}' at address 0x{}", bytes, poolName, Long.toHexString(address));
        return allocation;
    }

    /**
     * Allocates memory using malloc (for simulation mode or fallback).
     *
     * @param bytes Number of bytes to allocate
     * @return L0Allocation handle
     * @throws L0MemoryAllocationException if allocation fails
     */
    private L0Allocation allocateWithMalloc(int bytes) throws L0MemoryAllocationException {
        // Allocate aligned native memory
        long address = NativeL0Memory.mallocAligned(bytes, DEFAULT_ALIGNMENT);
        if (address == 0) {
            String mode = NativeL0Memory.isL0Mode() ? "L0 mode" : "Simulation mode";
            throw new L0MemoryAllocationException(
                String.format("Native malloc failed for %d bytes (%s). System may be out of memory. Check native logs for details.",
                    bytes, mode));
        }

        // Zero-initialize the memory
        NativeL0Memory.memset(address, (byte) 0, bytes);

        // Create allocation handle
        L0Allocation allocation = new L0Allocation(address, bytes);

        // Track allocation
        allocations.put(allocation, address);
        usedBytes += bytes;

        LOG.debug("Allocated {} bytes of native L0 memory (malloc) at address 0x{}", bytes, Long.toHexString(address));
        return allocation;
    }



    @Override
    public void release(L0Allocation allocation) {
        if (allocation == null) {
            return;
        }

        Long trackedAddress = allocations.remove(allocation);
        if (trackedAddress != null) {
            if (trackedAddress < 0) {
                // Negative address indicates raw pool
                long poolAddress = -trackedAddress;
                NativeL0Memory.releaseRawPool(poolAddress);
                LOG.debug("Released {} bytes raw pool at address 0x{}", 
                         allocation.totalSize, Long.toHexString(poolAddress));
            } else {
                // Positive address indicates malloc'd memory
                NativeL0Memory.free(trackedAddress);
                LOG.debug("Released {} bytes of native L0 memory (malloc) at address 0x{}", 
                         allocation.totalSize, Long.toHexString(trackedAddress));
            }
            usedBytes -= allocation.totalSize;
        } else {
            LOG.warn("Attempted to release untracked memory allocation");
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
            LOG.debug("Closing NativeL0MemoryAllocator");

            // Release all remaining allocations
            for (Map.Entry<L0Allocation, Long> entry : allocations.entrySet()) {
                Long trackedAddress = entry.getValue();
                L0Allocation allocation = entry.getKey();
                
                if (trackedAddress < 0) {
                    // Negative address indicates raw pool
                    long poolAddress = -trackedAddress;
                    NativeL0Memory.releaseRawPool(poolAddress);
                    LOG.debug("Released unreleased raw pool at 0x{} ({} bytes)",
                             Long.toHexString(poolAddress), allocation.totalSize);
                } else {
                    // Positive address indicates malloc'd memory
                    NativeL0Memory.free(trackedAddress);
                    LOG.debug("Released unreleased native memory (malloc) at 0x{} ({} bytes)",
                             Long.toHexString(trackedAddress), allocation.totalSize);
                }
            }
            allocations.clear();
            usedBytes = 0;

            LOG.info("NativeL0MemoryAllocator closed");
        }
    }

    /**
     * Gets the alignment used for allocations.
     *
     * @return Alignment in bytes
     */
    public int getAlignment() {
        return DEFAULT_ALIGNMENT;
    }

    /**
     * Gets the number of active allocations.
     *
     * @return Number of allocations
     */
    public int getAllocationCount() {
        return allocations.size();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("NativeL0MemoryAllocator is closed");
        }
    }

}
