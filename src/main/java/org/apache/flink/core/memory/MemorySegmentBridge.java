package org.apache.flink.core.memory;

import java.nio.ByteBuffer;

/**
 * Bridge class to access package-private MemorySegment/MemoryUtils APIs
 * for zero-copy native memory wrapping.
 *
 * <p>This class MUST be in the {@code org.apache.flink.core.memory} package
 * to access the package-private {@link MemoryUtils#wrapUnsafeMemoryWithByteBuffer}
 * and {@link MemorySegmentFactory#wrapOffHeapMemory}.
 *
 * <p>Used by ForL0 state backend to create off-heap {@link MemorySegment}s
 * that point directly to C++ SwissTable memory, enabling zero-copy reads
 * for BinaryRowData values.
 */
public class MemorySegmentBridge {

    /**
     * Create an off-heap MemorySegment wrapping the given native memory address.
     *
     * <p>The returned segment does NOT own the memory — the caller must ensure
     * the native memory remains valid while the segment is in use. No cleaner
     * or deallocator is registered; GC of the segment will NOT free native memory.
     *
     * <p>This is used for zero-copy reads: C++ returns a pointer to
     * {@code std::string::data()} within the SwissTable, and Java wraps it
     * as a MemorySegment for BinaryRowData to read from directly.
     *
     * @param address native memory address (e.g., from JNI)
     * @param size    number of bytes accessible at that address
     * @return a new off-heap MemorySegment wrapping the given native memory
     */
    public static MemorySegment wrapNativeAddress(long address, int size) {
        ByteBuffer buf = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, size);
        return MemorySegmentFactory.wrapOffHeapMemory(buf);
    }
}
