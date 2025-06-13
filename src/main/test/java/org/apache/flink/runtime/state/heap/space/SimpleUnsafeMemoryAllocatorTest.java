package org.apache.flink.runtime.state.heap.space;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SimpleUnsafeMemoryAllocator}.
 */
class SimpleUnsafeMemoryAllocatorTest {

    private SimpleUnsafeMemoryAllocator alloc;

    @BeforeEach
    void setUp() {
        alloc = new SimpleUnsafeMemoryAllocator();
    }

    @AfterEach
    void tearDown() {
        alloc.close();
        assertEquals(0L, alloc.outstandingBytes(),
                "All native memory should be released after close()");
    }

    /** 基本的申请-写-读流程。 */
    @Test
    void testAllocateWriteRead() {
        try (MemorySlice slice = alloc.allocate(64)) {
            slice.putInt(0, 42);
            slice.putLong(8, 123456789L);

            assertEquals(42, slice.getInt(0));
            assertEquals(123456789L, slice.getLong(8));
        }
    }

    /** 同尺寸块应该被复用（返回地址相同）。 */
    @Test
    void testReuseSameSizeBlock() {
        long addr1, addr2;

        try (MemorySlice slice1 = alloc.allocate(32)) {
            addr1 = slice1.address();
        } // slice1.release() 自动放回空闲池

        try (MemorySlice slice2 = alloc.allocate(32)) {
            addr2 = slice2.address();
        }

        assertEquals(addr1, addr2,
                "Allocator should reuse the same native block for identical size");
    }

    /** outstandingBytes 计数逻辑。*/
    @Test
    void testOutstandingBytes() {
        assertEquals(0L, alloc.outstandingBytes());

        MemorySlice s1 = alloc.allocate(16);
        assertEquals(16L, alloc.outstandingBytes());

        MemorySlice s2 = alloc.allocate(48);
        assertEquals(64L, alloc.outstandingBytes());

        s1.release();               // 放回空闲池，仍算已分配
        assertEquals(64L, alloc.outstandingBytes());

        s2.release();
        assertEquals(64L, alloc.outstandingBytes());
    }

    /** 非法参数应抛异常。 */
    @Test
    void testIllegalSize() {
        assertThrows(IllegalArgumentException.class, () -> alloc.allocate(0));
        assertThrows(IllegalArgumentException.class, () -> alloc.allocate(-10));
    }

    /** 越界读写应抛 IndexOutOfBoundsException。 */
    @Test
    void testRangeCheck() {
        try (MemorySlice slice = alloc.allocate(8)) {
            // 读／写 4 字节 int，偏移 5 会溢出
            assertThrows(IndexOutOfBoundsException.class, () -> slice.putInt(5, 7));
            assertThrows(IndexOutOfBoundsException.class, () -> slice.getInt(5));
        }
    }
}
