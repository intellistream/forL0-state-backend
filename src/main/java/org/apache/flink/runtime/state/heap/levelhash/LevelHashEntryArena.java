package org.apache.flink.runtime.state.heap.levelhash;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.List;

final class LevelHashEntryArena implements AutoCloseable {
    private static final Unsafe U = UnsafeUtils.unsafe();
    private static final int HEADER_SIZE = 12; // keyHash (4) + lenK (4) + lenV (4)

    private final MemoryManagerAllocator allocator;
    private final List<MemorySegment> pages = new ArrayList<>();
    private long writeCursor = 0;            // absolute address for next alloc

    LevelHashEntryArena(MemoryManagerAllocator allocator) {
        this.allocator = allocator;
    }

    /** Serialize key & value into arena; return address pointer (uint64). */
    long put(byte[] keySer, byte[] valSer) throws MemoryAllocationException {
        final int bytesNeeded = HEADER_SIZE + keySer.length + valSer.length;

        ensure(bytesNeeded);
        long addr = writeCursor;
        // header
        int keyHash = (int) HashFunctions.mix64(hashBytes(keySer));
        U.putInt(addr, keyHash);
        U.putInt(addr + 4, keySer.length);
        U.putInt(addr + 8, valSer.length);
        // body
        long pos = addr + HEADER_SIZE;
        copyBytes(keySer, pos);
        copyBytes(valSer, pos + keySer.length);

        writeCursor += bytesNeeded;
        return addr;
    }

    /** Read value bytes given entry address; returns a fresh byte[]. */
    byte[] getValue(long addr) {
        int lenK = UnsafeUtils.unsafe().getInt(addr + 4);
        int lenV = UnsafeUtils.unsafe().getInt(addr + 8);
        byte[] value = new byte[lenV];
        copyBytes(addr + HEADER_SIZE + lenK, value);
        return value;
    }

    void clear() {
        pages.forEach(MemorySegment::free);
        pages.clear();
        writeCursor = 0;
    }

    // ---------------------------------------------------------------------
    // internal helpers
    // ---------------------------------------------------------------------
    private void ensure(int bytes) throws MemoryAllocationException {
        if (pages.isEmpty()) { allocateNewPage(bytes); return; }
        long lastBase = pages.get(pages.size() - 1).getAddress();
        long usedInPage = writeCursor - lastBase;
        if (usedInPage + bytes > allocator.getPageSize()) {
            allocateNewPage(bytes);
        }
    }

    private void allocateNewPage(int minBytes) throws MemoryAllocationException {
        int pagesNeeded = 1;
        List<MemorySegment> segs = allocator.allocate(minBytes);
        pages.addAll(segs);
        writeCursor = pages.get(pages.size() - 1).getAddress();
    }

    private static long hashBytes(byte[] src) {
        long h = 0;
        for (byte b : src) {
            h = h * 31 + (b & 0xFF);
        }
        return h;
    }

    private static void copyBytes(byte[] src, long dstAddr) {
        Unsafe U = UnsafeUtils.unsafe();
        for (int i = 0; i < src.length; i++) {
            U.putByte(dstAddr + i, src[i]);
        }
    }

    private static void copyBytes(long srcAddr, byte[] dst) {
        Unsafe U = UnsafeUtils.unsafe();
        for (int i = 0; i < dst.length; i++) {
            dst[i] = U.getByte(srcAddr + i);
        }
    }

    @Override
    public void close() {
        allocator.free(pages);
        allocator.close();
    }
}
