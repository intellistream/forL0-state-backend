package org.apache.flink.runtime.state.heap.space;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NativeL0MemoryAllocator.
 * These tests are only run when the native library is available.
 */
@SuppressWarnings("restriction")
class NativeL0MemoryAllocatorTest {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Unsafe instance", e);
        }
    }

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
        L0MemoryAllocator.L0Allocation allocation = allocator.allocate(4096);
        assertNotNull(allocation);
        assertNotNull(allocation.addresses);
        assertTrue(allocation.addresses.length > 0);
        assertEquals(4096, allocation.totalSize);
        assertEquals(4096, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testMultipleAllocations() throws L0MemoryAllocator.L0MemoryAllocationException {
        L0MemoryAllocator.L0Allocation alloc1 = allocator.allocate(1024);
        L0MemoryAllocator.L0Allocation alloc2 = allocator.allocate(2048);
        L0MemoryAllocator.L0Allocation alloc3 = allocator.allocate(4096);

        assertNotNull(alloc1);
        assertNotNull(alloc2);
        assertNotNull(alloc3);

        assertEquals(1024 + 2048 + 4096, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testWriteAndRead() throws L0MemoryAllocator.L0MemoryAllocationException {
        L0MemoryAllocator.L0Allocation allocation = allocator.allocate(1024);
        long address = allocation.addresses[0];
        
        // Test put/get byte
        UNSAFE.putByte(address, (byte) 0x42);
        assertEquals((byte) 0x42, UNSAFE.getByte(address));
        
        // Test put/get int
        UNSAFE.putInt(address + 4, 0x12345678);
        assertEquals(0x12345678, UNSAFE.getInt(address + 4));
        
        // Test put/get long
        UNSAFE.putLong(address + 8, 0x123456789ABCDEF0L);
        assertEquals(0x123456789ABCDEF0L, UNSAFE.getLong(address + 8));
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testBulkCopy() throws L0MemoryAllocator.L0MemoryAllocationException {
        L0MemoryAllocator.L0Allocation allocation = allocator.allocate(1024);
        long address = allocation.addresses[0];
        
        // Write array to memory
        byte[] src = new byte[256];
        for (int i = 0; i < 256; i++) {
            src[i] = (byte) i;
        }
        UNSAFE.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address, 256);
        
        // Read back
        byte[] dest = new byte[256];
        UNSAFE.copyMemory(null, address, dest, Unsafe.ARRAY_BYTE_BASE_OFFSET, 256);
        
        assertArrayEquals(src, dest);
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testRelease() throws L0MemoryAllocator.L0MemoryAllocationException {
        L0MemoryAllocator.L0Allocation allocation = allocator.allocate(4096);
        assertEquals(4096, allocator.getUsedBytes());
        
        allocator.release(allocation);
        assertEquals(0, allocator.getUsedBytes());
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testReleaseMultiple() throws L0MemoryAllocator.L0MemoryAllocationException {
        List<L0MemoryAllocator.L0Allocation> allAllocations = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allAllocations.add(allocator.allocate(1024));
        }
        assertEquals(10 * 1024, allocator.getUsedBytes());
        
        // Release half
        for (int i = 0; i < 5; i++) {
            allocator.release(allAllocations.get(i));
        }
        assertEquals(5 * 1024, allocator.getUsedBytes());
        
        // Release rest
        for (int i = 5; i < 10; i++) {
            allocator.release(allAllocations.get(i));
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
        List<L0MemoryAllocator.L0Allocation> allAllocations = new ArrayList<>();
        long remaining = allocator.getTotalCapacity();
        
        while (remaining >= 64 * 1024) {
            allAllocations.add(allocator.allocate(64 * 1024));
            remaining -= 64 * 1024;
        }
        
        // Should be near capacity now
        assertTrue(allocator.getUsedBytes() >= allocator.getTotalCapacity() - 64 * 1024);
        
        // Next allocation should fail
        assertThrows(L0MemoryAllocator.L0MemoryAllocationException.class, () -> {
            allocator.allocate(64 * 1024);
        });
        
        // Release one, then allocation should succeed
        allocator.release(allAllocations.get(0));
        L0MemoryAllocator.L0Allocation newAlloc = allocator.allocate(64 * 1024);
        assertNotNull(newAlloc);
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
    void testAllocationAddressValid() throws L0MemoryAllocator.L0MemoryAllocationException {
        L0MemoryAllocator.L0Allocation allocation = allocator.allocate(4096);
        assertTrue(allocation.addresses[0] != 0, "Address should be non-zero (valid native pointer)");
        
        // Verify we can write and read from the address
        long addr = allocation.addresses[0];
        UNSAFE.putLong(addr, 0xDEADBEEFCAFEBABEL);
        assertEquals(0xDEADBEEFCAFEBABEL, UNSAFE.getLong(addr));
    }
}
