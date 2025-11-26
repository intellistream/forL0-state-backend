package org.apache.flink.runtime.state.heap.space;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NativeL0Memory JNI implementation.
 * These tests are only run when the native library is available.
 */
class NativeL0MemoryTest {

    private static boolean nativeAvailable;

    @BeforeAll
    static void checkNativeAvailable() {
        nativeAvailable = NativeL0Memory.isAvailable();
        if (!nativeAvailable) {
            System.out.println("Native library not available: " + NativeL0Memory.getLoadError());
            System.out.println("Skipping native memory tests.");
        }
    }

    static boolean isNativeAvailable() {
        return nativeAvailable;
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testMallocAndFree() {
        // Allocate 1KB
        long address = NativeL0Memory.malloc(1024);
        assertTrue(address != 0, "malloc should return non-zero address");

        // Free
        NativeL0Memory.free(address);
        // No assertion needed - just verify no crash
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testMallocAligned() {
        // Allocate 4KB aligned to 64 bytes
        long address = NativeL0Memory.mallocAligned(4096, 64);
        assertTrue(address != 0, "mallocAligned should return non-zero address");
        assertEquals(0, address % 64, "Address should be 64-byte aligned");

        NativeL0Memory.free(address);
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testByteOperations() {
        long address = NativeL0Memory.malloc(16);
        assertTrue(address != 0);

        try {
            // Write and read bytes
            NativeL0Memory.putByte(address, (byte) 0x42);
            assertEquals((byte) 0x42, NativeL0Memory.getByte(address));

            NativeL0Memory.putByte(address + 1, (byte) 0xFF);
            assertEquals((byte) 0xFF, NativeL0Memory.getByte(address + 1));
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testShortOperations() {
        long address = NativeL0Memory.malloc(16);
        assertTrue(address != 0);

        try {
            NativeL0Memory.putShort(address, (short) 0x1234);
            assertEquals((short) 0x1234, NativeL0Memory.getShort(address));

            NativeL0Memory.putShort(address + 2, (short) -1);
            assertEquals((short) -1, NativeL0Memory.getShort(address + 2));
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testIntOperations() {
        long address = NativeL0Memory.malloc(16);
        assertTrue(address != 0);

        try {
            NativeL0Memory.putInt(address, 0x12345678);
            assertEquals(0x12345678, NativeL0Memory.getInt(address));

            NativeL0Memory.putInt(address + 4, Integer.MIN_VALUE);
            assertEquals(Integer.MIN_VALUE, NativeL0Memory.getInt(address + 4));
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testLongOperations() {
        long address = NativeL0Memory.malloc(16);
        assertTrue(address != 0);

        try {
            NativeL0Memory.putLong(address, 0x123456789ABCDEF0L);
            assertEquals(0x123456789ABCDEF0L, NativeL0Memory.getLong(address));

            NativeL0Memory.putLong(address + 8, Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, NativeL0Memory.getLong(address + 8));
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testMemset() {
        long address = NativeL0Memory.malloc(64);
        assertTrue(address != 0);

        try {
            // Fill with 0xAA
            NativeL0Memory.memset(address, (byte) 0xAA, 64);

            // Verify all bytes are 0xAA
            for (int i = 0; i < 64; i++) {
                assertEquals((byte) 0xAA, NativeL0Memory.getByte(address + i),
                           "Byte at offset " + i + " should be 0xAA");
            }
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testMemcpy() {
        long src = NativeL0Memory.malloc(32);
        long dest = NativeL0Memory.malloc(32);
        assertTrue(src != 0 && dest != 0);

        try {
            // Write pattern to source
            for (int i = 0; i < 32; i++) {
                NativeL0Memory.putByte(src + i, (byte) i);
            }

            // Copy
            NativeL0Memory.memcpy(dest, src, 32);

            // Verify
            for (int i = 0; i < 32; i++) {
                assertEquals((byte) i, NativeL0Memory.getByte(dest + i),
                           "Byte at offset " + i + " should match");
            }
        } finally {
            NativeL0Memory.free(src);
            NativeL0Memory.free(dest);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testCopyFromArray() {
        long address = NativeL0Memory.malloc(32);
        assertTrue(address != 0);

        try {
            byte[] src = new byte[16];
            for (int i = 0; i < 16; i++) {
                src[i] = (byte) (i + 100);
            }

            NativeL0Memory.copyFromArray(address, src, 0, 16);

            for (int i = 0; i < 16; i++) {
                assertEquals((byte) (i + 100), NativeL0Memory.getByte(address + i));
            }
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testCopyToArray() {
        long address = NativeL0Memory.malloc(32);
        assertTrue(address != 0);

        try {
            // Write pattern to native memory
            for (int i = 0; i < 16; i++) {
                NativeL0Memory.putByte(address + i, (byte) (i + 50));
            }

            byte[] dest = new byte[16];
            NativeL0Memory.copyToArray(address, dest, 0, 16);

            for (int i = 0; i < 16; i++) {
                assertEquals((byte) (i + 50), dest[i]);
            }
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testCopyWithOffset() {
        long address = NativeL0Memory.malloc(32);
        assertTrue(address != 0);

        try {
            byte[] arr = new byte[32];
            for (int i = 0; i < 32; i++) {
                arr[i] = (byte) i;
            }

            // Copy with offset
            NativeL0Memory.copyFromArray(address, arr, 8, 16);

            for (int i = 0; i < 16; i++) {
                assertEquals((byte) (i + 8), NativeL0Memory.getByte(address + i));
            }

            // Copy back with offset
            byte[] result = new byte[32];
            NativeL0Memory.copyToArray(address, result, 4, 16);

            for (int i = 0; i < 16; i++) {
                assertEquals((byte) (i + 8), result[i + 4]);
            }
        } finally {
            NativeL0Memory.free(address);
        }
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testZeroSizeAllocation() {
        // malloc(0) should return 0 or valid pointer depending on implementation
        // Our implementation returns 0 for size <= 0
        long address = NativeL0Memory.malloc(0);
        assertEquals(0, address, "malloc(0) should return 0");

        address = NativeL0Memory.malloc(-1);
        assertEquals(0, address, "malloc(-1) should return 0");
    }

    @Test
    @EnabledIf("isNativeAvailable")
    void testFreeNull() {
        // free(0) should be safe (no-op)
        NativeL0Memory.free(0);
        // No assertion - just verify no crash
    }
}
