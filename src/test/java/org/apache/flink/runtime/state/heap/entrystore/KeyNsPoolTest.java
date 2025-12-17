package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.state.heap.entrystore.EntryStoreConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KeyNsPool.
 */
class KeyNsPoolTest {
    
    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 128L * DEFAULT_PAGE_SIZE; // 4MB
    
    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private KeyNsPool pool;
    
    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        Object owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        pool = new KeyNsPool(allocator);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (pool != null && !pool.isClosed()) {
            pool.close();
        }
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }
    
    @Nested
    class BasicAllocationTests {
        
        @Test
        void testPoolInitialization() {
            assertEquals(1, pool.getActiveSegmentCount());
            assertEquals(0, pool.getActiveEntries());
            assertFalse(pool.isClosed());
        }
        
        @Test
        void testSimpleAllocateAndRead() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNamespace".getBytes();
            int hash = 12345;
            long valueHandle = 99999L;
            
            long address = pool.allocate(hash, key, key.length, ns, ns.length, valueHandle);
            
            assertTrue(address != NULL_HANDLE);
            assertEquals(hash, pool.getHash(address));
            assertArrayEquals(key, pool.getKeyBytes(address));
            assertArrayEquals(ns, pool.getNamespaceBytes(address));
            assertEquals(valueHandle, pool.getValueHandle(address));
            assertEquals(1, pool.getActiveEntries());
        }
        
        @Test
        void testMultipleAllocations() {
            int count = 100;
            long[] addresses = new long[count];
            
            for (int i = 0; i < count; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] ns = ("ns" + i).getBytes();
                addresses[i] = pool.allocate(i, key, key.length, ns, ns.length, i * 1000L);
                assertTrue(addresses[i] != NULL_HANDLE, "Allocation " + i + " failed");
            }
            
            assertEquals(count, pool.getActiveEntries());
            
            // Verify all entries
            for (int i = 0; i < count; i++) {
                byte[] expectedKey = ("key" + i).getBytes();
                byte[] expectedNs = ("ns" + i).getBytes();
                
                assertEquals(i, pool.getHash(addresses[i]));
                assertArrayEquals(expectedKey, pool.getKeyBytes(addresses[i]));
                assertArrayEquals(expectedNs, pool.getNamespaceBytes(addresses[i]));
                assertEquals(i * 1000L, pool.getValueHandle(addresses[i]));
            }
        }
        
        @Test
        void testEmptyKeyAndNamespace() {
            byte[] key = new byte[0];
            byte[] ns = new byte[0];
            int hash = 0;
            long valueHandle = 42L;
            
            long address = pool.allocate(hash, key, 0, ns, 0, valueHandle);
            
            assertTrue(address != NULL_HANDLE);
            assertEquals(hash, pool.getHash(address));
            assertArrayEquals(key, pool.getKeyBytes(address));
            assertArrayEquals(ns, pool.getNamespaceBytes(address));
            assertEquals(valueHandle, pool.getValueHandle(address));
        }
        
        @Test
        void testLargeKeyAndNamespace() {
            byte[] key = new byte[1024];  // 1KB key
            byte[] ns = new byte[512];    // 512B namespace
            for (int i = 0; i < key.length; i++) key[i] = (byte) (i & 0xFF);
            for (int i = 0; i < ns.length; i++) ns[i] = (byte) ((i * 2) & 0xFF);
            
            int hash = 0x12345678;
            long valueHandle = Long.MAX_VALUE;
            
            long address = pool.allocate(hash, key, key.length, ns, ns.length, valueHandle);
            
            assertTrue(address != NULL_HANDLE);
            assertArrayEquals(key, pool.getKeyBytes(address));
            assertArrayEquals(ns, pool.getNamespaceBytes(address));
        }
    }
    
    @Nested
    class FreeAndLiveCountTests {
        
        @Test
        void testFreeDecreasesLiveCount() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            long addr1 = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            long addr2 = pool.allocate(2, key, key.length, ns, ns.length, 200L);
            
            assertEquals(2, pool.getActiveEntries());
            
            pool.free(addr1);
            assertEquals(1, pool.getActiveEntries());
            
            pool.free(addr2);
            assertEquals(0, pool.getActiveEntries());
        }
        
        @Test
        void testFreeNullAddressIsNoOp() {
            assertEquals(0, pool.getActiveEntries());
            pool.free(NULL_HANDLE);
            assertEquals(0, pool.getActiveEntries());
        }
        
        @Test
        void testFreeInvalidAddressIsNoOp() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            assertEquals(1, pool.getActiveEntries());
            
            // Free with invalid segment index
            pool.free(encodeKeyNsAddress(999, 0));
            assertEquals(1, pool.getActiveEntries());
        }
    }
    
    @Nested
    class UpdateValueHandleTests {
        
        @Test
        void testUpdateValueHandle() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            long initialHandle = 100L;
            long newHandle = 200L;
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, initialHandle);
            assertEquals(initialHandle, pool.getValueHandle(address));
            
            pool.updateValueHandle(address, newHandle);
            assertEquals(newHandle, pool.getValueHandle(address));
            
            // Key and namespace should be unchanged
            assertArrayEquals(key, pool.getKeyBytes(address));
            assertArrayEquals(ns, pool.getNamespaceBytes(address));
        }
        
        @Test
        void testUpdateNullAddressIsNoOp() {
            // Should not throw
            pool.updateValueHandle(NULL_HANDLE, 999L);
        }
    }
    
    @Nested
    class MatchesKeyTests {
        
        @Test
        void testMatchesKeyExactMatch() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            assertTrue(pool.matchesKey(address, key, key.length, ns, ns.length));
        }
        
        @Test
        void testMatchesKeyDifferentKey() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            byte[] differentKey = "differentKey".getBytes();
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            assertFalse(pool.matchesKey(address, differentKey, differentKey.length, ns, ns.length));
        }
        
        @Test
        void testMatchesKeyDifferentNamespace() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            byte[] differentNs = "differentNs".getBytes();
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            assertFalse(pool.matchesKey(address, key, key.length, differentNs, differentNs.length));
        }
        
        @Test
        void testMatchesKeyDifferentLength() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            byte[] shorterKey = "test".getBytes();
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            assertFalse(pool.matchesKey(address, shorterKey, shorterKey.length, ns, ns.length));
        }
        
        @Test
        void testMatchesKeyNullAddress() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            
            assertFalse(pool.matchesKey(NULL_HANDLE, key, key.length, ns, ns.length));
        }
        
        @Test
        void testMatchesKeyNullInputs() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            assertFalse(pool.matchesKey(address, null, 0, ns, ns.length));
            assertFalse(pool.matchesKey(address, key, key.length, null, 0));
        }
    }
    
    @Nested
    class ZeroCopySliceTests {
        
        @Test
        void testGetKeySlice() {
            byte[] key = "sliceKey".getBytes();
            byte[] ns = "sliceNs".getBytes();
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            MemorySegmentSlice slice = pool.getKeySlice(address);
            assertNotNull(slice);
            assertEquals(key.length, slice.length);
            
            // Read bytes from slice
            byte[] readKey = new byte[slice.length];
            slice.segment.get(slice.offset, readKey);
            assertArrayEquals(key, readKey);
        }
        
        @Test
        void testGetNamespaceSlice() {
            byte[] key = "sliceKey".getBytes();
            byte[] ns = "sliceNamespace".getBytes();
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, 100L);
            
            MemorySegmentSlice slice = pool.getNamespaceSlice(address);
            assertNotNull(slice);
            assertEquals(ns.length, slice.length);
            
            // Read bytes from slice
            byte[] readNs = new byte[slice.length];
            slice.segment.get(slice.offset, readNs);
            assertArrayEquals(ns, readNs);
        }
    }
    
    @Nested
    class SegmentManagementTests {
        
        @Test
        void testSegmentSwitchOnFull() {
            // Fill up the first segment
            byte[] key = new byte[1024];  // 1KB key
            byte[] ns = new byte[512];    // 512B namespace
            int entrySize = align8(KEY_ENTRY_HEADER_SIZE + key.length + ns.length);
            int entriesPerSegment = DEFAULT_PAGE_SIZE / entrySize;
            
            int initialSegments = pool.getActiveSegmentCount();
            
            // Allocate enough entries to fill multiple segments
            for (int i = 0; i < entriesPerSegment * 2; i++) {
                long addr = pool.allocate(i, key, key.length, ns, ns.length, i * 1000L);
                assertTrue(addr != NULL_HANDLE, "Allocation " + i + " failed");
            }
            
            // Should have allocated more segments
            assertTrue(pool.getActiveSegmentCount() > initialSegments);
        }
        
        @Test
        void testEmptySegmentRelease() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            int entrySize = align8(KEY_ENTRY_HEADER_SIZE + key.length + ns.length);
            int entriesPerSegment = DEFAULT_PAGE_SIZE / entrySize;
            
            // Allocate entries across multiple segments
            long[] addresses = new long[entriesPerSegment * 3];
            for (int i = 0; i < addresses.length; i++) {
                addresses[i] = pool.allocate(i, key, key.length, ns, ns.length, i * 1000L);
                if (addresses[i] == NULL_HANDLE) {
                    // Stop if allocation fails
                    addresses = java.util.Arrays.copyOf(addresses, i);
                    break;
                }
            }
            
            int segmentsBefore = pool.getActiveSegmentCount();
            
            // Free all entries
            for (long addr : addresses) {
                pool.free(addr);
            }
            
            // Segments should be released (or reused)
            assertEquals(0, pool.getActiveEntries());
            // At least one segment should remain (current write segment)
            assertTrue(pool.getActiveSegmentCount() >= 1);
        }
    }
    
    @Nested
    class EdgeCaseTests {
        
        @Test
        void testInvalidInputs() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            // Null key
            assertEquals(NULL_HANDLE, pool.allocate(1, null, 0, ns, ns.length, 100L));
            
            // Null namespace
            assertEquals(NULL_HANDLE, pool.allocate(1, key, key.length, null, 0, 100L));
            
            // Negative key length
            assertEquals(NULL_HANDLE, pool.allocate(1, key, -1, ns, ns.length, 100L));
            
            // Negative namespace length
            assertEquals(NULL_HANDLE, pool.allocate(1, key, key.length, ns, -1, 100L));
            
            // Key length exceeds buffer
            assertEquals(NULL_HANDLE, pool.allocate(1, key, key.length + 100, ns, ns.length, 100L));
        }
        
        @Test
        void testAddressEncoding() {
            // Test address encoding/decoding
            int segIdx = 5;
            int offset = 12345;
            
            long address = encodeKeyNsAddress(segIdx, offset);
            
            assertEquals(segIdx, decodeSegmentIndex(address));
            assertEquals(offset, decodeOffset(address));
        }
        
        @Test
        void testPreAllocation() {
            // Create pool with pre-allocation
            KeyNsPool preAllocPool = new KeyNsPool(allocator, DEFAULT_PAGE_SIZE * 4);
            
            try {
                // Should have multiple segments pre-allocated
                assertTrue(preAllocPool.getSegmentCount() > 1);
            } finally {
                preAllocPool.close();
            }
        }
    }
    
    @Nested
    class LifecycleTests {
        
        @Test
        void testCloseReleasesAllMemory() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            // Allocate some entries
            for (int i = 0; i < 100; i++) {
                pool.allocate(i, key, key.length, ns, ns.length, i * 1000L);
            }
            
            pool.close();
            
            assertTrue(pool.isClosed());
            assertEquals(0, pool.getActiveEntries());
            assertEquals(0, pool.getSegmentCount());
        }
        
        @Test
        void testAllocateAfterCloseReturnsNull() {
            pool.close();
            
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            assertEquals(NULL_HANDLE, pool.allocate(1, key, key.length, ns, ns.length, 100L));
        }
        
        @Test
        void testDoubleCloseIsNoOp() {
            pool.close();
            pool.close();  // Should not throw
            assertTrue(pool.isClosed());
        }
    }
    
    @Nested
    class StressTests {
        
        @Test
        void testHighVolumeAllocations() {
            byte[] key = "stressKey".getBytes();
            byte[] ns = "stressNs".getBytes();
            
            int count = 10000;
            long[] addresses = new long[count];
            
            // Allocate many entries
            for (int i = 0; i < count; i++) {
                addresses[i] = pool.allocate(i, key, key.length, ns, ns.length, i * 1000L);
                assertTrue(addresses[i] != NULL_HANDLE, "Allocation " + i + " failed");
            }
            
            assertEquals(count, pool.getActiveEntries());
            
            // Verify random samples
            for (int i = 0; i < 100; i++) {
                int idx = (i * 97) % count;  // Pseudo-random access pattern
                assertEquals(idx, pool.getHash(addresses[idx]));
                assertEquals(idx * 1000L, pool.getValueHandle(addresses[idx]));
            }
            
            // Free all
            for (long addr : addresses) {
                pool.free(addr);
            }
            
            assertEquals(0, pool.getActiveEntries());
        }
        
        @Test
        void testMixedAllocateAndFree() {
            byte[] key = "mixedKey".getBytes();
            byte[] ns = "mixedNs".getBytes();
            
            java.util.List<Long> activeAddresses = new java.util.ArrayList<>();
            java.util.Random random = new java.util.Random(42);
            
            for (int i = 0; i < 5000; i++) {
                if (random.nextDouble() < 0.7 || activeAddresses.isEmpty()) {
                    // 70% allocate
                    long addr = pool.allocate(i, key, key.length, ns, ns.length, i * 1000L);
                    if (addr != NULL_HANDLE) {
                        activeAddresses.add(addr);
                    }
                } else {
                    // 30% free
                    int idx = random.nextInt(activeAddresses.size());
                    pool.free(activeAddresses.remove(idx));
                }
            }
            
            assertEquals(activeAddresses.size(), pool.getActiveEntries());
            
            // Cleanup
            for (long addr : activeAddresses) {
                pool.free(addr);
            }
            
            assertEquals(0, pool.getActiveEntries());
        }
    }
    
    /**
     * Tests for Phase 2.5 inline value support in KeyNsPool.
     */
    @Nested
    class InlineValueTests {
        
        /** Helper: pack byte[] into long (little-endian) */
        private long packToLong(byte[] bytes) {
            long result = 0;
            for (int i = 0; i < bytes.length && i < 8; i++) {
                result |= ((long) (bytes[i] & 0xFF)) << (i * 8);
            }
            return result;
        }
        
        @Test
        void testAllocateInline() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "inline".getBytes();  // 6 bytes
            
            long address = pool.allocateInline(1, key, key.length, ns, ns.length, packToLong(value), value.length);
            assertNotEquals(NULL_HANDLE, address);
            
            // Verify inline mode
            assertTrue(pool.isInlineMode(address));
            assertEquals(value.length, pool.getInlineValueLen(address));
        }
        
        @Test
        void testAllocateInlineMaxSize() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = new byte[EntryStoreConstants.INLINE_THRESHOLD];  // 8 bytes max
            for (int i = 0; i < value.length; i++) {
                value[i] = (byte) (i + 1);
            }
            
            long address = pool.allocateInline(1, key, key.length, ns, ns.length, packToLong(value), value.length);
            assertNotEquals(NULL_HANDLE, address);
            
            assertTrue(pool.isInlineMode(address));
            assertEquals(INLINE_THRESHOLD, pool.getInlineValueLen(address));
            
            // Read back value
            byte[] retrieved = pool.getInlineValueBytes(address);
            assertArrayEquals(value, retrieved);
        }
        
        @Test
        void testAllocateInlineEmpty() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            long address = pool.allocateInline(1, key, key.length, ns, ns.length, 0L, 0);
            assertNotEquals(NULL_HANDLE, address);
            
            assertTrue(pool.isInlineMode(address));
            assertEquals(0, pool.getInlineValueLen(address));
        }
        
        @Test
        void testGetInlineValue() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = {1, 2, 3, 4, 5};  // 5 bytes
            
            long address = pool.allocateInline(1, key, key.length, ns, ns.length, packToLong(value), value.length);
            
            // Get as long
            long inlineValue = pool.getInlineValue(address);
            
            // Verify bytes
            byte[] retrieved = pool.getInlineValueBytes(address);
            assertArrayEquals(value, retrieved);
        }
        
        @Test
        void testUpdateInlineValue() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] original = {1, 2, 3};
            
            long address = pool.allocateInline(1, key, key.length, ns, ns.length, packToLong(original), original.length);
            
            // Update to different inline value
            byte[] updated = {9, 8, 7, 6, 5};
            pool.updateInlineValue(address, packToLong(updated), updated.length);
            
            assertEquals(updated.length, pool.getInlineValueLen(address));
            byte[] retrieved = pool.getInlineValueBytes(address);
            assertArrayEquals(updated, retrieved);
        }
        
        @Test
        void testSwitchToPointerMode() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = {1, 2, 3};
            
            long address = pool.allocateInline(1, key, key.length, ns, ns.length, packToLong(value), value.length);
            assertTrue(pool.isInlineMode(address));
            
            // Switch to pointer mode
            long valueHandle = 0x123456789ABCL;
            pool.switchToPointerMode(address, valueHandle);
            
            assertFalse(pool.isInlineMode(address));
            assertEquals(valueHandle, pool.getValueHandle(address));
        }
        
        @Test
        void testSwitchToInlineMode() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            long valueHandle = 0x123456789ABCL;
            
            // Start with pointer mode (using regular allocate)
            long address = pool.allocate(1, key, key.length, ns, ns.length, valueHandle);
            assertFalse(pool.isInlineMode(address));
            
            // Switch to inline mode
            byte[] newValue = {7, 8, 9};
            pool.switchToInlineMode(address, packToLong(newValue), newValue.length);
            
            assertTrue(pool.isInlineMode(address));
            assertEquals(newValue.length, pool.getInlineValueLen(address));
            assertArrayEquals(newValue, pool.getInlineValueBytes(address));
        }
        
        @Test
        void testGetInlineValueSlice() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = {0x11, 0x22, 0x33, 0x44};  // 4 bytes
            
            long address = pool.allocateInline(1, key, key.length, ns, ns.length, packToLong(value), value.length);
            
            MemorySegmentSlice slice = pool.getInlineValueSlice(address);
            assertNotNull(slice);
            assertEquals(value.length, slice.length);
            
            // Read from slice
            byte[] fromSlice = new byte[slice.length];
            slice.segment.get(slice.offset, fromSlice);
            assertArrayEquals(value, fromSlice);
        }
        
        @Test
        void testPointerModeNotInline() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            long valueHandle = 0x123456789L;
            
            long address = pool.allocate(1, key, key.length, ns, ns.length, valueHandle);
            assertFalse(pool.isInlineMode(address));
        }
        
        @Test
        void testMultipleInlineEntries() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            long[] addresses = new long[100];
            for (int i = 0; i < addresses.length; i++) {
                byte[] value = new byte[i % (INLINE_THRESHOLD + 1)];  // 0-8 bytes
                for (int j = 0; j < value.length; j++) {
                    value[j] = (byte) (i + j);
                }
                addresses[i] = pool.allocateInline(i, key, key.length, ns, ns.length, packToLong(value), value.length);
                assertTrue(pool.isInlineMode(addresses[i]));
            }
            
            // Verify all entries
            for (int i = 0; i < addresses.length; i++) {
                int expectedLen = i % (INLINE_THRESHOLD + 1);
                assertEquals(expectedLen, pool.getInlineValueLen(addresses[i]));
            }
        }
    }
}
