package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * L0 memory allocator implementation using JNI native methods.
 * This allocator uses C malloc/free for memory allocation, providing
 * direct access to native memory outside the JVM heap.
 *
 * <p>This implementation wraps native memory addresses in Flink's MemorySegment
 * for compatibility with existing code that uses MemorySegment operations.
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

    /** Track allocated memory: segment list -> (address, size) pairs */
    private final Map<List<MemorySegment>, AllocationInfo> allocations = new IdentityHashMap<>();

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
        LOG.info("Created NativeL0MemoryAllocator with maxCapacity={}",
                 maxCapacity == -1 ? "unlimited" : maxCapacity);
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

        // Allocate aligned native memory
        long address = NativeL0Memory.mallocAligned(bytes, DEFAULT_ALIGNMENT);
        if (address == 0) {
            throw new L0MemoryAllocationException(
                "Native malloc failed for " + bytes + " bytes. System may be out of memory.");
        }

        // Zero-initialize the memory
        NativeL0Memory.memset(address, (byte) 0, bytes);

        // Create a DirectByteBuffer pointing to the native memory
        ByteBuffer directBuffer = createDirectByteBuffer(address, bytes);
        
        // Wrap in MemorySegment using Flink's factory
        MemorySegment segment = MemorySegmentFactory.wrapOffHeapMemory(directBuffer);

        List<MemorySegment> segments = new ArrayList<>(1);
        segments.add(segment);

        // Track allocation
        allocations.put(segments, new AllocationInfo(address, bytes));
        usedBytes += bytes;

        LOG.debug("Allocated {} bytes of native L0 memory at address 0x{}", bytes, Long.toHexString(address));
        return segments;
    }

    /**
     * Creates a DirectByteBuffer that wraps the given native memory address.
     * Uses reflection to create a DirectByteBuffer pointing to existing memory.
     */
    private static ByteBuffer createDirectByteBuffer(long address, int capacity) throws L0MemoryAllocationException {
        try {
            // Get the DirectByteBuffer class and its constructor
            Class<?> directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
            Constructor<?> constructor = directByteBufferClass.getDeclaredConstructor(long.class, int.class);
            constructor.setAccessible(true);
            
            return (ByteBuffer) constructor.newInstance(address, capacity);
        } catch (Exception e) {
            throw new L0MemoryAllocationException(
                "Failed to create DirectByteBuffer for native memory at 0x" + Long.toHexString(address), e);
        }
    }

    @Override
    public void release(List<MemorySegment> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }

        AllocationInfo info = allocations.remove(segments);
        if (info != null) {
            // Free native memory
            NativeL0Memory.free(info.address);
            usedBytes -= info.size;
            LOG.debug("Released {} bytes of native L0 memory at address 0x{}", 
                     info.size, Long.toHexString(info.address));
        } else {
            LOG.warn("Attempted to release untracked memory segments");
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
            for (Map.Entry<List<MemorySegment>, AllocationInfo> entry : allocations.entrySet()) {
                AllocationInfo info = entry.getValue();
                NativeL0Memory.free(info.address);
                LOG.debug("Released unreleased native memory at 0x{} ({} bytes)",
                         Long.toHexString(info.address), info.size);
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

    /**
     * Information about a native memory allocation.
     */
    private static class AllocationInfo {
        final long address;
        final int size;

        AllocationInfo(long address, int size) {
            this.address = address;
            this.size = size;
        }
    }

}
