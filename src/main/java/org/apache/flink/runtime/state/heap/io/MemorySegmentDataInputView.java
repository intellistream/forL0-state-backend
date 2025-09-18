package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A DataInputView backed by a single MemorySegment window (offset, length).
 * Used for zero-copy deserialization directly from EntryArena slices.
 */
public class MemorySegmentDataInputView implements DataInputView {

    private MemorySegment segment;
    private int position;
    private int limit;

    public MemorySegmentDataInputView() {
        this.segment = null;
        this.position = 0;
        this.limit = 0;
    }

    public void reset(MemorySegment segment, int offset, int length) {
        this.segment = segment;
        this.position = offset;
        this.limit = offset + length;
    }

    private void ensureAvailable(int numBytes) throws IOException {
        if (segment == null || position + numBytes > limit) {
            throw new EOFException("Not enough bytes available: pos=" + position + ", need=" + numBytes + ", limit=" + limit);
        }
    }

    // InputStream-like methods required by DataInputView
    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) { throw new NullPointerException("buffer"); }
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) { throw new NullPointerException("buffer"); }
        if (len < 0) { throw new IndexOutOfBoundsException("len < 0"); }
        if (len == 0) { return 0; }
        // DataInput 风格：要么读取恰好 len 字节，要么抛 EOF
        ensureAvailable(len);
        segment.get(position, b, off, len);
        position += len;
        return len;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        if (len == 0) return;
        ensureAvailable(len);
        segment.get(position, b, off, len);
        position += len;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int k = Math.min(n, available());
        position += k;
        return k;
    }

    @Override
    public boolean readBoolean() throws IOException {
        ensureAvailable(1);
        return (segment.get(position++) & 0xFF) != 0;
    }

    @Override
    public byte readByte() throws IOException {
        ensureAvailable(1);
        return segment.get(position++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        ensureAvailable(1);
        return segment.get(position++) & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        ensureAvailable(2);
        short value = segment.getShortBigEndian(position);
        position += 2;
        return value;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        ensureAvailable(2);
        int value = segment.getShortBigEndian(position) & 0xFFFF;
        position += 2;
        return value;
    }

    @Override
    public char readChar() throws IOException {
        ensureAvailable(2);
        char value = segment.getCharBigEndian(position);
        position += 2;
        return value;
    }

    @Override
    public int readInt() throws IOException {
        ensureAvailable(4);
        int value = segment.getIntBigEndian(position);
        position += 4;
        return value;
    }

    @Override
    public long readLong() throws IOException {
        ensureAvailable(8);
        long value = segment.getLongBigEndian(position);
        position += 8;
        return value;
    }

    @Override
    public float readFloat() throws IOException {
        ensureAvailable(4);
        float value = segment.getFloatBigEndian(position);
        position += 4;
        return value;
    }

    @Override
    public double readDouble() throws IOException {
        ensureAvailable(8);
        double value = segment.getDoubleBigEndian(position);
        position += 8;
        return value;
    }

    @Override
    public String readLine() throws IOException {
        // Deprecated; provide a minimal implementation that reads until newline or end
        StringBuilder sb = new StringBuilder();
        while (available() > 0) {
            byte b = readByte();
            if (b == '\n') break;
            if (b != '\r') sb.append((char) b);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    @Override
    @Nonnull
    public String readUTF() throws IOException {
        int utfLen = readUnsignedShort();
        byte[] buf = new byte[utfLen];
        readFully(buf, 0, utfLen);
        return new String(buf, StandardCharsets.UTF_8);
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        ensureAvailable(numBytes);
        position += numBytes;
    }

    private int available() {
        return Math.max(0, limit - position);
    }
}
