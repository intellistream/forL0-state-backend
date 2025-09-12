package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

/**
 * A DataOutputView that writes directly to a MemorySegment for zero-copy serialization.
 * Supports dynamic space allocation when the target segment is too small.
 */
public class MemorySegmentDataOutputView implements DataOutputView {

    private MemorySegment segment;
    private int position;
    private int limit;

    // Callback for requesting more space when current segment is full
    private SpaceAllocator spaceAllocator;

    @FunctionalInterface
    public interface SpaceAllocator {
        /**
         * Allocates a new segment with at least the requested size.
         * @param minSize minimum required size
         * @return new MemorySegment, or null if allocation failed
         */
        MemorySegment allocate(int minSize);
    }

    public MemorySegmentDataOutputView() {
        // Default constructor for reuse
    }

    public MemorySegmentDataOutputView(MemorySegment segment, int offset, int length) {
        reset(segment, offset, length, null);
    }

    public MemorySegmentDataOutputView(MemorySegment segment, int offset, int length, SpaceAllocator spaceAllocator) {
        reset(segment, offset, length, spaceAllocator);
    }

    /**
     * Resets the output view to write to a new segment region.
     */
    public void reset(MemorySegment segment, int offset, int length, SpaceAllocator spaceAllocator) {
        if (segment == null) {
            throw new IllegalArgumentException("MemorySegment cannot be null");
        }
        if (offset < 0 || length < 0 || offset + length > segment.size()) {
            throw new IllegalArgumentException("Invalid offset/length for segment");
        }

        this.segment = segment;
        this.position = offset;
        this.limit = offset + length;
        this.spaceAllocator = spaceAllocator;
    }

    /**
     * Returns the current position in the segment.
     */
    public int getPosition() {
        return position;
    }

    /**
     * Returns the number of bytes written since last reset.
     */
    public int getBytesWritten() {
        return position - (limit - (limit - position));
    }

    /**
     * Ensures there's enough space for the specified number of bytes.
     */
    private void ensureSpace(int bytes) throws IOException {
        if (position + bytes <= limit) {
            return; // Enough space available
        }

        if (spaceAllocator == null) {
            throw new IOException("Not enough space in MemorySegment and no SpaceAllocator available");
        }

        // Calculate required size
        int currentDataSize = position - (limit - (limit - position));
        int requiredSize = currentDataSize + bytes + Math.max(bytes, 1024); // Add buffer

        MemorySegment newSegment = spaceAllocator.allocate(requiredSize);
        if (newSegment == null) {
            throw new IOException("SpaceAllocator failed to provide more space");
        }

        // Copy existing data to new segment
        if (currentDataSize > 0) {
            int originalStart = limit - (limit - position) - currentDataSize;
            // 使用正确的MemorySegment API进行数据拷贝
            for (int i = 0; i < currentDataSize; i++) {
                newSegment.put(i, segment.get(originalStart + i));
            }
        }

        // Update to use new segment
        this.segment = newSegment;
        this.position = currentDataSize;
        this.limit = newSegment.size();
    }

    @Override
    public void write(int b) throws IOException {
        ensureSpace(1);
        segment.put(position, (byte) b);
        position++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len == 0) return;
        ensureSpace(len);
        segment.put(position, b, off, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        write(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        ensureSpace(2);
        segment.putShortBigEndian(position, (short) v);
        position += 2;
    }

    @Override
    public void writeChar(int v) throws IOException {
        ensureSpace(2);
        segment.putCharBigEndian(position, (char) v);
        position += 2;
    }

    @Override
    public void writeInt(int v) throws IOException {
        ensureSpace(4);
        segment.putIntBigEndian(position, v);
        position += 4;
    }

    @Override
    public void writeLong(long v) throws IOException {
        ensureSpace(8);
        segment.putLongBigEndian(position, v);
        position += 8;
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            write((byte) s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (bytes.length > 65535) {
            throw new IOException("UTF string too long: " + bytes.length);
        }
        writeShort(bytes.length);
        write(bytes);
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IllegalArgumentException("numBytes cannot be negative");
        }
        ensureSpace(numBytes);
        position += numBytes;
    }

    @Override
    public void write(org.apache.flink.core.memory.DataInputView source, int numBytes) throws IOException {
        throw new UnsupportedOperationException("write(DataInputView, int) not supported");
    }
}
