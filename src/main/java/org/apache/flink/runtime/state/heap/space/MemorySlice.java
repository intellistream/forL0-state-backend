package org.apache.flink.runtime.state.heap.space;


import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;

/**
 *  A slice of continuous native memory
 */
public final class MemorySlice implements AutoCloseable {
    private final long address;
    private final int size;
    private final OffHeapMemoryAllocator owner;

    MemorySlice(long address, int size, OffHeapMemoryAllocator owner) {
        this.address = address;
        this.size = size;
        this.owner = owner;
    }

    // Getters

    public long address() { return address; }
    public int size() { return size; }

    // Read/Write

    public byte getByte(int offset) {
        rangeCheck(offset, Byte.BYTES);
        return UnsafeUtils.unsafe().getByte(address + offset);
    }
    public void putByte(int offset, byte value) {
        rangeCheck(offset, Byte.BYTES);
        UnsafeUtils.unsafe().putByte(address + offset, value);
    }

    public int getInt(int offset) {
        rangeCheck(offset, Integer.BYTES);
        return UnsafeUtils.unsafe().getInt(address + offset);
    }
    public void putInt(int offset, int value) {
        rangeCheck(offset, Integer.BYTES);
        UnsafeUtils.unsafe().putInt(address + offset, value);
    }

    public long getLong(int offset) {
        rangeCheck(offset, Long.BYTES);
        return UnsafeUtils.unsafe().getLong(address + offset);
    }
    public void putLong(int offset, long value) {
        rangeCheck(offset, Long.BYTES);
        UnsafeUtils.unsafe().putLong(address + offset, value);
    }

    // Release

    /**
     *  Return this slice to its owner
     */
    public void release() {
        owner.free(this);
    }

    // Utils

    private void rangeCheck(int offset, int length) {
        if (offset < 0 || offset + length > size) {
            throw new IndexOutOfBoundsException(
                    String.format("Offset %d is out of range [0, %d]", offset, length)
            );
        }
    }

    @Override
    public void close() {
        release();
    }
}
