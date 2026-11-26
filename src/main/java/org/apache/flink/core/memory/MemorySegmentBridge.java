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

    /** Release-publish a count in a bounded direct-native descriptor. */
    public static void publishBufferCount(ByteBuffer buffer, int offset, long count) {
        MemoryUtils.UNSAFE.storeFence();
        buffer.putLong(offset, count);
    }

    /** Base of an aligned, native-order descriptor owned by the calling state. */
    public static long nativeCounterBase(ByteBuffer buffer) {
        if (buffer == null) return 0;
        if (!buffer.isDirect() || buffer.isReadOnly() || buffer.order() != MemoryUtils.NATIVE_BYTE_ORDER) {
            throw new IllegalArgumentException("Expected a writable native-order direct descriptor");
        }
        long address = MemoryUtils.getByteBufferAddress(buffer);
        if ((address & 7) != 0 || address == 0) throw new IllegalArgumentException("Unaligned native descriptor");
        return address;
    }

    /** A single Flink task writes the counter. Native metrics readers acquire-load this aligned word. */
    public static void incrementNativeCounter(long address) {
        MemoryUtils.UNSAFE.putOrderedLong(null, address, MemoryUtils.UNSAFE.getLong(address) + 1);
    }

    private static final long BYTE_ARRAY_BASE = MemoryUtils.UNSAFE.arrayBaseOffset(byte[].class);

    /** Copy a synchronous JNI result before exposing mutable BinaryRowData to Flink. */
    public static byte[] copyNativeBytes(long address, int length) {
        byte[] result = new byte[length];
        if (length > 0) MemoryUtils.UNSAFE.copyMemory(null, address, result, BYTE_ARRAY_BASE, length);
        return result;
    }

    /** Read a word from a live native state handle's synchronous result area. */
    public static long readNativeLong(long address) {
        return MemoryUtils.UNSAFE.getLong(address);
    }

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
