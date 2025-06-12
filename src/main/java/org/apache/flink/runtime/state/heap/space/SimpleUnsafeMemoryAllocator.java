package org.apache.flink.runtime.heap.space;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * The single-thread version of off-heap memory allocator
 */
public final class SimpleUnsafeMemoryAllocator implements OffHeapMemoryAllocator {

    // free address lists, (size, List)
    private final Map<Integer, ArrayDeque<Long>> freeLists = new HashMap<>();

    private long outstandingBytes = 0L;

    // Allocate/Free

    @Override
    public MemorySlice allocate(int bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be positive");
        }

        ArrayDeque<Long> freeList = freeLists.get(bytes);
        Long addr = (freeList == null || freeList.isEmpty()) ? null : freeList.pollFirst();

        if (addr == null) {
            // no reusable block, allocate one
            addr = UnsafeUtils.unsafe().allocateMemory(bytes);
            UnsafeUtils.unsafe().setMemory(addr, bytes, (byte) 0);
            outstandingBytes += bytes;
        }
        return new MemorySlice(addr, bytes, this);
    }

    @Override
    public void free(MemorySlice slice) {
        freeLists.computeIfAbsent(slice.size(), k -> new ArrayDeque<>())
                .addFirst(slice.address());
    }

    // Housekeeping

    @Override
    public void close() {
        for (Map.Entry<Integer, ArrayDeque<Long>> e : freeLists.entrySet()) {
            int size = e.getKey();
            for (Long addr : e.getValue()) {
                UnsafeUtils.unsafe().freeMemory(addr);
                outstandingBytes -= size;
            }
        }
        freeLists.clear();
    }

    @Override
    public long outstandingBytes() {
        return outstandingBytes;
    }
}
