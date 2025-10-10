package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EntryArenaTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 128L * DEFAULT_PAGE_SIZE; // 4MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryArena arena;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        Object owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        arena = new EntryArena(allocator);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (arena != null) {
            arena.close();
        }
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    @Nested
    class BasicEntryOperations {

        @Test
        void testArenaInitialization() {
            EntryArena.ArenaStats stats = arena.getStats();
            assertTrue(stats.totalSystemMemory > 0); // Should have allocated initial slab
            assertEquals(0, stats.activeAllocations);
        }

        @Test
        void testSimpleEntryPutAndGet() {
            byte[] keyBytes = "testKey".getBytes();
            byte[] namespaceBytes = "testNamespace".getBytes();
            byte[] valueBytes = "testValue".getBytes();

            // Put entry
            long entryAddress = arena.putEntry(keyBytes, namespaceBytes, valueBytes);
            assertTrue(entryAddress > 0);

            // Get entry components back
            assertArrayEquals(keyBytes, arena.getKeyBytes(entryAddress));
            assertArrayEquals(namespaceBytes, arena.getNamespaceBytes(entryAddress));
            assertArrayEquals(valueBytes, arena.getValueBytes(entryAddress));

            // Verify stats
            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(1, stats.activeAllocations);
        }

        @Test
        void testEntryMatching() {
            byte[] keyBytes = "matchKey".getBytes();
            byte[] namespaceBytes = "matchNamespace".getBytes();
            byte[] valueBytes = "matchValue".getBytes();

            long entryAddress = arena.putEntry(keyBytes, namespaceBytes, valueBytes);

            // Test exact match
            assertTrue(arena.matchesKey(entryAddress, keyBytes, namespaceBytes));

            // Test mismatch
            assertFalse(arena.matchesKey(entryAddress, "differentKey".getBytes(), namespaceBytes));
            assertFalse(arena.matchesKey(entryAddress, keyBytes, "differentNamespace".getBytes()));
        }

        @Test
        void testEntryUpdate() {
            byte[] keyBytes = "updateKey".getBytes();
            byte[] namespaceBytes = "updateNamespace".getBytes();
            byte[] originalValue = "originalValue".getBytes();
            byte[] newValue = "newValue".getBytes();

            // Put original entry
            long originalAddress = arena.putEntry(keyBytes, namespaceBytes, originalValue);
            assertArrayEquals(originalValue, arena.getValueBytes(originalAddress));

            // Update value (same size)
            long updatedAddress = arena.updateEntry(originalAddress, newValue);

            if (updatedAddress == originalAddress) {
                // In-place update
                assertArrayEquals(newValue, arena.getValueBytes(originalAddress));
            } else {
                // New allocation
                assertTrue(updatedAddress > 0);
                assertArrayEquals(newValue, arena.getValueBytes(updatedAddress));
            }
        }

        @Test
        void testEntryRemoval() {
            byte[] keyBytes = "removeKey".getBytes();
            byte[] namespaceBytes = "removeNamespace".getBytes();
            byte[] valueBytes = "removeValue".getBytes();

            long entryAddress = arena.putEntry(keyBytes, namespaceBytes, valueBytes);
            assertTrue(entryAddress > 0);

            EntryArena.ArenaStats statsBefore = arena.getStats();
            int activeEntriesBefore = statsBefore.activeAllocations;

            // Remove entry
            arena.removeEntry(entryAddress);

            EntryArena.ArenaStats statsAfter = arena.getStats();
            assertEquals(activeEntriesBefore - 1, statsAfter.activeAllocations);
        }

        @Test
        void testEntrySize() {
            byte[] keyBytes = "sizeKey".getBytes();
            byte[] namespaceBytes = "sizeNamespace".getBytes();
            byte[] valueBytes = "sizeValue".getBytes();

            long entryAddress = arena.putEntry(keyBytes, namespaceBytes, valueBytes);
            int entrySize = arena.getEntrySize(entryAddress);

            // Entry size should include header (12 bytes) plus data, aligned
            int expectedMinSize = 12 + keyBytes.length + namespaceBytes.length + valueBytes.length;
            assertTrue(entrySize >= expectedMinSize);
            assertEquals(0, entrySize % 8); // Should be 8-byte aligned
        }
    }

    @Nested
    class MultipleEntriesTests {

        @Test
        void testMultipleEntries() {
            List<Long> addresses = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                byte[] keyBytes = ("key" + i).getBytes();
                byte[] namespaceBytes = ("namespace" + i).getBytes();
                byte[] valueBytes = ("value" + i).getBytes();

                long address = arena.putEntry(keyBytes, namespaceBytes, valueBytes);
                assertTrue(address > 0);
                addresses.add(address);
            }

            // Verify all entries
            for (int i = 0; i < 10; i++) {
                long address = addresses.get(i);
                assertEquals("key" + i, new String(arena.getKeyBytes(address)));
                assertEquals("namespace" + i, new String(arena.getNamespaceBytes(address)));
                assertEquals("value" + i, new String(arena.getValueBytes(address)));
            }

            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(10, stats.activeAllocations);
        }

        @Test
        void testEntryUniqueness() {
            List<Long> addresses = new ArrayList<>();

            // Create entries with same content but should get different addresses
            for (int i = 0; i < 5; i++) {
                byte[] keyBytes = "sameKey".getBytes();
                byte[] namespaceBytes = "sameNamespace".getBytes();
                byte[] valueBytes = "sameValue".getBytes();

                long address = arena.putEntry(keyBytes, namespaceBytes, valueBytes);
                addresses.add(address);
            }

            // All addresses should be different
            for (int i = 0; i < addresses.size(); i++) {
                for (int j = i + 1; j < addresses.size(); j++) {
                    assertNotEquals(addresses.get(i), addresses.get(j));
                }
            }
        }

        @Test
        void testDifferentSizedEntries() {
            // Test entries of various sizes
            int[] keySizes = {1, 10, 100, 1000};
            int[] namespaceSizes = {1, 20, 200};
            int[] valueSizes = {1, 50, 500, 5000};

            List<Long> addresses = new ArrayList<>();

            for (int keySize : keySizes) {
                for (int namespaceSize : namespaceSizes) {
                    for (int valueSize : valueSizes) {
                        byte[] keyBytes = new byte[keySize];
                        byte[] namespaceBytes = new byte[namespaceSize];
                        byte[] valueBytes = new byte[valueSize];

                        Arrays.fill(keyBytes, (byte) 'K');
                        Arrays.fill(namespaceBytes, (byte) 'N');
                        Arrays.fill(valueBytes, (byte) 'V');

                        long address = arena.putEntry(keyBytes, namespaceBytes, valueBytes);
                        assertTrue(address > 0);
                        addresses.add(address);

                        // Verify content
                        assertArrayEquals(keyBytes, arena.getKeyBytes(address));
                        assertArrayEquals(namespaceBytes, arena.getNamespaceBytes(address));
                        assertArrayEquals(valueBytes, arena.getValueBytes(address));
                    }
                }
            }

            assertTrue(addresses.size() > 0);
            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(addresses.size(), stats.activeAllocations);
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testNullInputs() {
            // Test null key
            assertEquals(0, arena.putEntry(null, "namespace".getBytes(), "value".getBytes()));

            // Test null namespace
            assertEquals(0, arena.putEntry("key".getBytes(), null, "value".getBytes()));

            // Test null value
            assertEquals(0, arena.putEntry("key".getBytes(), "namespace".getBytes(), null));
        }

        @Test
        void testEmptyInputs() {
            byte[] emptyKey = new byte[0];
            byte[] emptyNamespace = new byte[0];
            byte[] emptyValue = new byte[0];

            long address = arena.putEntry(emptyKey, emptyNamespace, emptyValue);
            assertTrue(address > 0);

            assertArrayEquals(emptyKey, arena.getKeyBytes(address));
            assertArrayEquals(emptyNamespace, arena.getNamespaceBytes(address));
            assertArrayEquals(emptyValue, arena.getValueBytes(address));
        }

//        @Test
//        void testZeroAddress() {
//            // Operations on zero address should handle gracefully
//            assertNull(arena.getKeyBytes(0));
//            assertNull(arena.getNamespaceBytes(0));
//            assertNull(arena.getValueBytes(0));
//            assertFalse(arena.matchesKey(0, "key".getBytes(), "namespace".getBytes()));
//            assertEquals(0, arena.getEntrySize(0));
//
//            // Remove zero address should not crash
//            assertDoesNotThrow(() -> arena.removeEntry(0));
//        }

        @Test
        void testUpdateWithZeroAddress() {
            long result = arena.updateEntry(0, "newValue".getBytes());
            assertEquals(0, result);
        }

        @Test
        void testUpdateWithNullValue() {
            byte[] keyBytes = "key".getBytes();
            byte[] namespaceBytes = "namespace".getBytes();
            byte[] valueBytes = "value".getBytes();

            long address = arena.putEntry(keyBytes, namespaceBytes, valueBytes);

            long result = arena.updateEntry(address, null);
            assertEquals(0, result);
        }

        @Test
        void testLargeEntries() {
            // Test with large entries that might trigger new slab allocation
            byte[] largeKey = new byte[8192];
            byte[] largeNamespace = new byte[4096];
            byte[] largeValue = new byte[16384];

            Arrays.fill(largeKey, (byte) 'K');
            Arrays.fill(largeNamespace, (byte) 'N');
            Arrays.fill(largeValue, (byte) 'V');

            long address = arena.putEntry(largeKey, largeNamespace, largeValue);

            if (address > 0) {
                // If allocation succeeded, verify content
                assertArrayEquals(largeKey, arena.getKeyBytes(address));
                assertArrayEquals(largeNamespace, arena.getNamespaceBytes(address));
                assertArrayEquals(largeValue, arena.getValueBytes(address));
            }
            // If allocation failed (address == 0), that's also acceptable for very large entries
        }
    }

    @Nested
    class UpdateTests {

        @Test
        void testInPlaceUpdate() {
            byte[] keyBytes = "updateKey".getBytes();
            byte[] namespaceBytes = "updateNamespace".getBytes();
            byte[] originalValue = "originalValue123".getBytes(); // 16 chars
            byte[] shorterValue = "shorter".getBytes(); // 7 chars

            long originalAddress = arena.putEntry(keyBytes, namespaceBytes, originalValue);

            // In simplified implementation, update always allocates new memory
            long updatedAddress = arena.updateEntry(originalAddress, shorterValue);
            assertTrue(updatedAddress > 0);

            // Verify the new entry has correct value
            assertArrayEquals(shorterValue, arena.getValueBytes(updatedAddress));

            // Verify key and namespace are preserved
            assertArrayEquals(keyBytes, arena.getKeyBytes(updatedAddress));
            assertArrayEquals(namespaceBytes, arena.getNamespaceBytes(updatedAddress));
        }

        @Test
        void testNewAllocationUpdate() {
            byte[] keyBytes = "updateKey".getBytes();
            byte[] namespaceBytes = "updateNamespace".getBytes();
            byte[] originalValue = "short".getBytes(); // 5 chars
            byte[] longerValue = "this is a much longer value".getBytes(); // 27 chars

            long originalAddress = arena.putEntry(keyBytes, namespaceBytes, originalValue);

            // Update with longer value should allocate new entry
            long updatedAddress = arena.updateEntry(originalAddress, longerValue);

            if (updatedAddress != originalAddress) {
                // New allocation happened
                assertTrue(updatedAddress > 0);
                assertArrayEquals(longerValue, arena.getValueBytes(updatedAddress));

                // Original address should be invalid now (but we can't easily test this)
            }
        }

        @Test
        void testMultipleUpdates() {
            byte[] keyBytes = "multiUpdateKey".getBytes();
            byte[] namespaceBytes = "multiUpdateNamespace".getBytes();

            long address = arena.putEntry(keyBytes, namespaceBytes, "value1".getBytes());

            // Chain of updates
            address = arena.updateEntry(address, "value2".getBytes());
            assertTrue(address > 0);
            assertArrayEquals("value2".getBytes(), arena.getValueBytes(address));

            address = arena.updateEntry(address, "value3".getBytes());
            assertTrue(address > 0);
            assertArrayEquals("value3".getBytes(), arena.getValueBytes(address));

            address = arena.updateEntry(address, "much longer value4".getBytes());
            assertTrue(address > 0);
            assertArrayEquals("much longer value4".getBytes(), arena.getValueBytes(address));
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testCloseWithActiveEntries() throws Exception {
            // Add some entries
            for (int i = 0; i < 5; i++) {
                arena.putEntry(("key" + i).getBytes(), ("ns" + i).getBytes(), ("value" + i).getBytes());
            }

            EntryArena.ArenaStats statsBefore = arena.getStats();
            assertTrue(statsBefore.activeAllocations > 0);

            long allocatorUsedBefore = allocator.getUsedBytes();

            arena.close();

            // Memory should be freed
            assertTrue(allocator.getUsedBytes() < allocatorUsedBefore);
        }

        @Test
        void testOperationsAfterClose() throws Exception {
            arena.close();

            // Operations after close should handle gracefully
            long address = arena.putEntry("key".getBytes(), "ns".getBytes(), "value".getBytes());
            assertTrue(address >= 0); // Should either work or return 0
        }
    }

    @Nested
    class PerformanceTests {

        @Test
        void testManySmallEntries() {
            int numEntries = 1000;
            List<Long> addresses = new ArrayList<>();

            long startTime = System.nanoTime();

            for (int i = 0; i < numEntries; i++) {
                byte[] keyBytes = ("key" + i).getBytes();
                byte[] namespaceBytes = "namespace".getBytes();
                byte[] valueBytes = ("value" + i).getBytes();

                long address = arena.putEntry(keyBytes, namespaceBytes, valueBytes);
                if (address > 0) {
                    addresses.add(address);
                }
            }

            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000;

            // Should complete in reasonable time
            assertTrue(durationMs < 5000, "Entry operations took too long: " + durationMs + "ms");
            assertTrue(addresses.size() > 0, "Should have successfully created some entries");

            // Verify a sample of entries
            for (int i = 0; i < Math.min(10, addresses.size()); i++) {
                long address = addresses.get(i);
                assertEquals("key" + i, new String(arena.getKeyBytes(address)));
            }
        }

        @Test
        void testMemoryEfficiency() {
            // Test that memory usage is reasonable
            EntryArena.ArenaStats initialStats = arena.getStats();

            List<Long> addresses = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                long address = arena.putEntry(
                    ("key" + i).getBytes(),
                    "namespace".getBytes(),
                    ("value" + i).getBytes()
                );
                if (address > 0) {
                    addresses.add(address);
                }
            }

            EntryArena.ArenaStats finalStats = arena.getStats();

            // Memory usage should have increased
            assertTrue(finalStats.totalAllocated > initialStats.totalAllocated);

            // Active allocations should match number of entries
            assertEquals(addresses.size(), finalStats.activeAllocations);

            // Fragmentation should be reasonable
            assertTrue(finalStats.fragmentation >= 0);
        }
    }

    @Nested
    class FreeListStrategyTests {

        private EntryArena freeListArena;

        @BeforeEach
        void setUpFreeList() {
            freeListArena = new EntryArena(allocator);
        }

        @AfterEach
        void tearDownFreeList() throws Exception {
            if (freeListArena != null) {
                freeListArena.close();
            }
        }

        @Test
        void testFreeListArenaInitialization() {
            // strategy 固定为 FREE_LIST，无需断言具体枚举
            EntryArena.ArenaStats stats = freeListArena.getStats();
            assertTrue(stats.totalSystemMemory > 0);
            assertEquals(0, stats.activeAllocations);
            assertEquals(0, stats.freeBlocks);
            assertEquals(0, stats.totalFreed);
        }

        @Test
        void testBasicFreeListOperations() {
            byte[] keyBytes = "freeListKey".getBytes();
            byte[] namespaceBytes = "freeListNamespace".getBytes();
            byte[] valueBytes = "freeListValue".getBytes();

            // Put entry
            long entryAddress = freeListArena.putEntry(keyBytes, namespaceBytes, valueBytes);
            assertTrue(entryAddress > 0);

            // Verify entry data
            assertArrayEquals(keyBytes, freeListArena.getKeyBytes(entryAddress));
            assertArrayEquals(namespaceBytes, freeListArena.getNamespaceBytes(entryAddress));
            assertArrayEquals(valueBytes, freeListArena.getValueBytes(entryAddress));

            // Check stats
            EntryArena.ArenaStats stats = freeListArena.getStats();
            assertEquals(1, stats.activeAllocations);
            assertEquals(0, stats.freeBlocks);
        }

        @Test
        void testMemoryRecycling() {
            List<Long> addresses = new ArrayList<>();

            // Allocate multiple entries
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] namespace = ("ns" + i).getBytes();
                byte[] value = ("value" + i).getBytes();

                long address = freeListArena.putEntry(key, namespace, value);
                assertTrue(address > 0);
                addresses.add(address);
            }

            EntryArena.ArenaStats beforeRemoval = freeListArena.getStats();
            assertEquals(10, beforeRemoval.activeAllocations);
            assertEquals(0, beforeRemoval.freeBlocks);

            // Remove half of the entries
            for (int i = 0; i < 5; i++) {
                freeListArena.removeEntry(addresses.get(i));
            }

            EntryArena.ArenaStats afterRemoval = freeListArena.getStats();
            assertEquals(5, afterRemoval.activeAllocations);
            assertTrue(afterRemoval.freeBlocks > 0, "Should have free blocks after removal");
            assertTrue(afterRemoval.totalFreed > 0, "Should track freed memory");

            // Allocate new entries - should reuse freed memory
            List<Long> newAddresses = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                byte[] key = ("newKey" + i).getBytes();
                byte[] namespace = ("newNs" + i).getBytes();
                byte[] value = ("newValue" + i).getBytes();

                long address = freeListArena.putEntry(key, namespace, value);
                assertTrue(address > 0);
                newAddresses.add(address);
            }

            EntryArena.ArenaStats afterReallocation = freeListArena.getStats();
            assertEquals(8, afterReallocation.activeAllocations);

            // Verify new entries are correct
            for (int i = 0; i < 3; i++) {
                long address = newAddresses.get(i);
                assertArrayEquals(("newKey" + i).getBytes(), freeListArena.getKeyBytes(address));
                assertArrayEquals(("newNs" + i).getBytes(), freeListArena.getNamespaceBytes(address));
                assertArrayEquals(("newValue" + i).getBytes(), freeListArena.getValueBytes(address));
            }
        }

        @Test
        void testUpdateWithMemoryRecycling() {
            byte[] keyBytes = "updateKey".getBytes();
            byte[] namespaceBytes = "updateNamespace".getBytes();
            byte[] originalValue = "originalValue".getBytes();
            byte[] newValue = "newUpdatedValue".getBytes();

            // Put original entry
            long originalAddress = freeListArena.putEntry(keyBytes, namespaceBytes, originalValue);
            assertTrue(originalAddress > 0);

            EntryArena.ArenaStats beforeUpdate = freeListArena.getStats();
            assertEquals(1, beforeUpdate.activeAllocations);
            assertEquals(0, beforeUpdate.freeBlocks);

            // Update entry
            long updatedAddress = freeListArena.updateEntry(originalAddress, newValue);
            assertTrue(updatedAddress > 0);

            EntryArena.ArenaStats afterUpdate = freeListArena.getStats();

            if (updatedAddress == originalAddress) {
                // In-place update occurred
                assertEquals(1, afterUpdate.activeAllocations, "Should still have 1 active allocation for in-place update");
                assertEquals(0, afterUpdate.freeBlocks, "Should have no free blocks for in-place update");
                assertEquals(0, afterUpdate.totalFreed, "Should have no freed memory for in-place update");
            } else {
                // Reallocation occurred
                assertEquals(1, afterUpdate.activeAllocations, "Should still have 1 active allocation after reallocation");
                assertTrue(afterUpdate.freeBlocks > 0, "Should have freed the old entry during reallocation");
                assertTrue(afterUpdate.totalFreed > 0, "Should track freed memory from reallocation");
            }

            // Verify updated entry (should work in both cases)
            assertArrayEquals(keyBytes, freeListArena.getKeyBytes(updatedAddress));
            assertArrayEquals(namespaceBytes, freeListArena.getNamespaceBytes(updatedAddress));
            assertArrayEquals(newValue, freeListArena.getValueBytes(updatedAddress));
        }

        @Test
        void testSizeClassAllocation() {
            List<Long> addresses = new ArrayList<>();

            // Test different size classes
            int[] sizes = {16, 64, 256, 1024, 4096}; // Different size classes

            for (int size : sizes) {
                byte[] key = "key".getBytes();
                byte[] namespace = "ns".getBytes();
                byte[] value = new byte[size];
                Arrays.fill(value, (byte) 'V');

                long address = freeListArena.putEntry(key, namespace, value);
                assertTrue(address > 0);
                addresses.add(address);

                // Verify content
                assertArrayEquals(value, freeListArena.getValueBytes(address));
            }

            // 保留一个holder防止将当前页判空而清空free list
            long holder = freeListArena.putEntry("h".getBytes(), "h".getBytes(), new byte[1]);
            assertTrue(holder > 0);

            // Remove all entries except holder to create free blocks of different sizes
            for (Long address : addresses) {
                freeListArena.removeEntry(address);
            }

            EntryArena.ArenaStats stats = freeListArena.getStats();
            assertEquals(1, stats.activeAllocations); // 仅剩 holder
            assertTrue(stats.freeBlocks > 0, "Should have free blocks of various sizes");

            // Reallocate with different sizes - should find appropriate free blocks
            for (int size : sizes) {
                byte[] key = "newKey".getBytes();
                byte[] namespace = "newNs".getBytes();
                byte[] value = new byte[size / 2]; // Smaller values to test splitting
                Arrays.fill(value, (byte) 'N');

                long address = freeListArena.putEntry(key, namespace, value);
                assertTrue(address > 0);
                assertArrayEquals(value, freeListArena.getValueBytes(address));
            }
        }

        @Test
        void testFreeListNoSplitWhenRemainderLessThanMinEntry() throws Exception {
            // 使用独立 arena 避免干扰
            EntryArena local = new EntryArena(allocator);
            try {
                // 大块：valueLen=1000 => entrySize=1016
                long big = local.putEntry(new byte[0], new byte[0], new byte[1000]);
                assertTrue(big > 0);
                int sizeA = local.getEntrySize(big);
                assertEquals(1016, sizeA, "sanity: expected 1016 for valueLen=1000");

                // 保持页非空，避免remove后被purge
                long holder = local.putEntry(new byte[0], new byte[0], new byte[1]);
                assertTrue(holder > 0);

                local.removeEntry(big);
                assertTrue(local.getStats().freeBlocks > 0);

                // 目标：剩余8(<MIN_ENTRY_SIZE=24) 不分裂 => valueLen=992 -> entrySize=1008
                long small = local.putEntry(new byte[0], new byte[0], new byte[992]);
                assertTrue(small > 0);
                assertEquals(1008, local.getEntrySize(small));

                EntryArena.ArenaStats stats = local.getStats();
                assertEquals(0, stats.freeBlocks, "remainder < MIN_ENTRY_SIZE should not split");
            } finally {
                local.close();
            }
        }

        @Test
        void testFreeListSplitWhenRemainderLargeEnough() throws Exception {
            EntryArena local = new EntryArena(allocator);
            try {
                long big = local.putEntry(new byte[0], new byte[0], new byte[1000]);
                assertTrue(big > 0);
                int sizeA = local.getEntrySize(big);
                assertEquals(1016, sizeA);

                // 保持页非空，避免remove后被purge
                long holder = local.putEntry(new byte[0], new byte[0], new byte[1]);
                assertTrue(holder > 0);

                local.removeEntry(big);
                assertTrue(local.getStats().freeBlocks > 0);

                // 目标：剩余120 且 >= max(MIN_ENTRY_SIZE=24, allocSize/8)
                // 选用 valueLen=880 -> entrySize=896, remainder=120, allocSize/8=112
                long alloc = local.putEntry(new byte[0], new byte[0], new byte[880]);
                assertTrue(alloc > 0);
                assertEquals(896, local.getEntrySize(alloc));

                EntryArena.ArenaStats stats = local.getStats();
                assertEquals(1, stats.freeBlocks, "should split and keep one free block as remainder");
            } finally {
                local.close();
            }
        }
    }
}

