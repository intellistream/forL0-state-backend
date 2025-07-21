package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class MemoryManagerAllocatorTest {
    private MemoryManager memoryManager;
    private Object owner;
    private MemoryManagerAllocator allocator;
    private int pageSize = 4096;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setPageSize(pageSize)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
    }

    @Test
    void testAllocateAndOutstandingBytes() throws MemoryAllocationException {
        List<MemorySegment> allocated = allocator.allocate(pageSize * 2);
        assertEquals(2 * pageSize, allocator.outstandingBytes());
        assertEquals(2, allocated.size());
    }

    @Test
    void testFree() throws MemoryAllocationException {
        List<MemorySegment> segments = allocator.allocate(pageSize * 2);
        allocator.free(segments);
        assertEquals(0, allocator.outstandingBytes());
        for (MemorySegment seg : segments) {
            assertTrue(seg.isFreed());
        }
    }

    @Test
    void testClose() {
        allocator.close();
        assertThrows(IllegalStateException.class, () -> allocator.allocate(pageSize));
    }

    @Test
    void testFreeNullOrEmpty() {
        allocator.free((MemorySegment) null);
        allocator.free(new ArrayList<>());
        // no exception
    }
}

