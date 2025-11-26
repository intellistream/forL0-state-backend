package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NativeL0MemoryAllocator.
 * These tests are only run when the native library is available.
 */
class NativeL0MemoryAllocatorTest {

    private static boolean nativeAvailable;
    private NativeL0MemoryAllocator allocator;

    @BeforeAll
    static void checkNativeAvailable() {
        nativeAvailable = NativeL0Memory.isAvailable();
        if (!nativeAvailable) {
            System.out.println("Native library not available: " + NativeL0Memory.getLoadError());
            System.out.println("Skipping native allocator tests.");
        }
    }

    static boolean isNativeAvailable() {
        return nativeAvailable;
    }

    @BeforeEach
    void setUp() {
        if (nativeAvailable) {
            // Create allocator with 1MB capacity
            allocator = new NativeL0MemoryAllocator(1024 * 1024);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testBasicAllocation() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<MemorySegment> segments = allocator.allocate(4096);
        assertNotNull(segments);
        assertFalse(segments.isEmpty());
        
        int totalSize = segments.stream().mapToInt(MemorySegment::size).sum();
        assertEquals(4096, totalSize);
        assertEquals(4096, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testMultipleAllocations() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<MemorySegment> seg1 = allocator.allocate(1024);
        List<MemorySegment> seg2 = allocator.allocate(2048);
        List<MemorySegment> seg3 = allocator.allocate(4096);

        assertNotNull(seg1);
        assertNotNull(seg2);
        assertNotNull(seg3);

        assertEquals(1024 + 2048 + 4096, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testWriteAndRead() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<MemorySegment> segments = allocator.allocate(1024);
        MemorySegment segment = segments.get(0);
        
        // Test put/get byte
        segment.put(0, (byte) 0x42);
        assertEquals((byte) 0x42, segment.get(0));
        
        // Test put/get int
        segment.putInt(4, 0x12345678);
        assertEquals(0x12345678, segment.getInt(4));
        
        // Test put/get long
        segment.putLong(8, 0x123456789ABCDEF0L);
        assertEquals(0x123456789ABCDEF0L, segment.getLong(8));
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testBulkCopy() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<MemorySegment> segments = allocator.allocate(1024);
        MemorySegment segment = segments.get(0);
        
        // Write array to segment
        byte[] src = new byte[256];
        for (int i = 0; i < 256; i++) {
            src[i] = (byte) i;
        }
        segment.put(0, src);
        
        // Read back
        byte[] dest = new byte[256];
        segment.get(0, dest);
        
        assertArrayEquals(src, dest);
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testRelease() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<MemorySegment> segments = allocator.allocate(4096);
        assertEquals(4096, allocator.getUsedBytes());
        
        allocator.release(segments);
        assertEquals(0, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testReleaseMultiple() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<List<MemorySegment>> allSegments = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allSegments.add(allocator.allocate(1024));
        }
        assertEquals(10 * 1024, allocator.getUsedBytes());
        
        // Release half
        for (int i = 0; i < 5; i++) {
            allocator.release(allSegments.get(i));
        }
        assertEquals(5 * 1024, allocator.getUsedBytes());
        
        // Release rest
        for (int i = 5; i < 10; i++) {
            allocator.release(allSegments.get(i));
        }
        assertEquals(0, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testCapacityLimit() {
        // Try to allocate more than capacity
        assertThrows(L0MemoryAllocator.L0MemoryAllocationException.class, () -> {
            allocator.allocate(2 * 1024 * 1024); // 2MB > 1MB capacity
        });
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testCapacityAccumulation() throws L0MemoryAllocator.L0MemoryAllocationException {
        // Allocate until we hit capacity
        List<List<MemorySegment>> allSegments = new ArrayList<>();
        long remaining = allocator.getTotalCapacity();
        
        while (remaining >= 64 * 1024) {
            allSegments.add(allocator.allocate(64 * 1024));
            remaining -= 64 * 1024;
        }
        
        // Should be near capacity now
        assertTrue(allocator.getUsedBytes() >= allocator.getTotalCapacity() - 64 * 1024);
        
        // Next allocation should fail
        assertThrows(L0MemoryAllocator.L0MemoryAllocationException.class, () -> {
            allocator.allocate(64 * 1024);
        });
        
        // Release one, then allocation should succeed
        allocator.release(allSegments.get(0));
        List<MemorySegment> newSeg = allocator.allocate(64 * 1024);
        assertNotNull(newSeg);
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testClose() throws Exception {
        // Allocate some memory to ensure allocator is in use
        allocator.allocate(4096);
        assertFalse(allocator.isClosed());
        
        allocator.close();
        assertTrue(allocator.isClosed());
        
        // Allocation should fail after close with IllegalStateException
        assertThrows(IllegalStateException.class, () -> {
            allocator.allocate(1024);
        });
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testCloseReleasesAllMemory() throws Exception {
        for (int i = 0; i < 100; i++) {
            allocator.allocate(1024);
        }
        assertEquals(100 * 1024, allocator.getUsedBytes());
        
        // Close should release all
        allocator.close();
        assertEquals(0, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testZeroSizeAllocation() {
        assertThrows(IllegalArgumentException.class, () -> {
            allocator.allocate(0);
        });
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testNegativeSizeAllocation() {
        assertThrows(IllegalArgumentException.class, () -> {
            allocator.allocate(-1);
        });
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testTotalCapacity() {
        assertEquals(1024 * 1024, allocator.getTotalCapacity());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testSegmentAddressIsOffHeap() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<MemorySegment> segments = allocator.allocate(4096);
        assertTrue(segments.get(0).isOffHeap(), "Segment should be off-heap");
    }
}
