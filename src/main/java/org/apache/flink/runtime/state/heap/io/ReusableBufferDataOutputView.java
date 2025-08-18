package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A reusable, growable DataOutputView that exposes its internal buffer and length
 * without extra copying. Intended to be used on hot write paths to avoid
 * DataOutputSerializer#getCopyOfBuffer allocations.
 */
public class ReusableBufferDataOutputView implements DataOutputView {

    private byte[] buffer;
    private int position;

    public ReusableBufferDataOutputView(int initialCapacity) {
        this.buffer = new byte[Math.max(32, initialCapacity)];
        this.position = 0;
    }

    public void clear() {
        this.position = 0;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int getLength() {
        return position;
    }

    private void ensureCapacity(int add) {
        int required = position + add;
        if (required <= buffer.length) {
            return;
        }
        int newCap = Math.max(buffer.length << 1, required);
        byte[] nb = new byte[newCap];
        System.arraycopy(buffer, 0, nb, 0, position);
        buffer = nb;
    }

    @Override
    public void write(int b) throws IOException {
        ensureCapacity(1);
        buffer[position++] = (byte) b;
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b == null) return;
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len <= 0) return;
        ensureCapacity(len);
        System.arraycopy(b, off, buffer, position, len);
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
        ensureCapacity(2);
        buffer[position++] = (byte) (v >>> 8);
        buffer[position++] = (byte) (v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        writeShort(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        ensureCapacity(4);
        buffer[position++] = (byte) (v >>> 24);
        buffer[position++] = (byte) (v >>> 16);
        buffer[position++] = (byte) (v >>> 8);
        buffer[position++] = (byte) (v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        ensureCapacity(8);
        buffer[position++] = (byte) (v >>> 56);
        buffer[position++] = (byte) (v >>> 48);
        buffer[position++] = (byte) (v >>> 40);
        buffer[position++] = (byte) (v >>> 32);
        buffer[position++] = (byte) (v >>> 24);
        buffer[position++] = (byte) (v >>> 16);
        buffer[position++] = (byte) (v >>> 8);
        buffer[position++] = (byte) (v);
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
        if (s == null) return;
        byte[] b = s.getBytes(StandardCharsets.ISO_8859_1);
        write(b);
    }

    @Override
    public void writeChars(String s) throws IOException {
        if (s == null) return;
        int len = s.length();
        ensureCapacity(len * 2);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            buffer[position++] = (byte) (v >>> 8);
            buffer[position++] = (byte) (v);
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        if (s == null) {
            writeShort(0);
            return;
        }
        // Use standard modified UTF-8 encoding with 2-byte length prefix
        int strlen = s.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++) {
            int c = s.charAt(i);
            if (c >= 0x0001 && c <= 0x007F) utflen++;
            else if (c > 0x07FF) utflen += 3;
            else utflen += 2;
        }
        if (utflen > 65535) {
            throw new IOException("encoded string too long: " + utflen + " bytes");
        }
        ensureCapacity(2 + utflen);
        writeShort(utflen);
        for (int i = 0; i < strlen; i++) {
            int c = s.charAt(i);
            if (c >= 0x0001 && c <= 0x007F) {
                writeByte(c);
            } else if (c > 0x07FF) {
                writeByte(0xE0 | ((c >> 12) & 0x0F));
                writeByte(0x80 | ((c >> 6) & 0x3F));
                writeByte(0x80 | (c & 0x3F));
            } else {
                writeByte(0xC0 | ((c >> 6) & 0x1F));
                writeByte(0x80 | (c & 0x3F));
            }
        }
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        ensureCapacity(numBytes);
        position += numBytes;
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        if (numBytes <= 0) { return; }
        ensureCapacity(numBytes);
        int remaining = numBytes;
        int off = position;
        while (remaining > 0) {
            int read = source.read(buffer, off, remaining);
            if (read < 0) {
                throw new IOException("Unexpected end of input while writing " + numBytes + " bytes");
            }
            off += read;
            remaining -= read;
        }
        position += numBytes;
    }
}
