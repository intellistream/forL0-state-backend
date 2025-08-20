package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

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
        int ch1 = segment.get(position++) & 0xFF;
        int ch2 = segment.get(position++) & 0xFF;
        return (short) ((ch1 << 8) | ch2);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        ensureAvailable(2);
        int ch1 = segment.get(position++) & 0xFF;
        int ch2 = segment.get(position++) & 0xFF;
        return (ch1 << 8) | ch2;
    }

    @Override
    public char readChar() throws IOException {
        return (char) readUnsignedShort();
    }

    @Override
    public int readInt() throws IOException {
        ensureAvailable(4);
        int ch1 = segment.get(position++) & 0xFF;
        int ch2 = segment.get(position++) & 0xFF;
        int ch3 = segment.get(position++) & 0xFF;
        int ch4 = segment.get(position++) & 0xFF;
        return (ch1 << 24) | (ch2 << 16) | (ch3 << 8) | ch4;
    }

    @Override
    public long readLong() throws IOException {
        ensureAvailable(8);
        return ((long)(segment.get(position++) & 0xFF) << 56) |
               ((long)(segment.get(position++) & 0xFF) << 48) |
               ((long)(segment.get(position++) & 0xFF) << 40) |
               ((long)(segment.get(position++) & 0xFF) << 32) |
               ((long)(segment.get(position++) & 0xFF) << 24) |
               ((long)(segment.get(position++) & 0xFF) << 16) |
               ((long)(segment.get(position++) & 0xFF) << 8)  |
               ((long)(segment.get(position++) & 0xFF));
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
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
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        byte[] buf = new byte[utflen];
        readFully(buf, 0, utflen);
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
