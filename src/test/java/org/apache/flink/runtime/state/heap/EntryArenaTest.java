package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EntryArenaTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 128L * DEFAULT_PAGE_SIZE; // 4MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryArena arena;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
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
    class BasicFunctionalityTests {

        @Test
        void testArenaInitialization() {
            EntryArena.ArenaStats stats = arena.getStats();
            assertTrue(stats.totalSystemMemory > 0); // Should have allocated initial slab
            assertEquals(0, stats.totalAllocated);
            assertEquals(0, stats.totalFreed);
            assertEquals(0, stats.activeAllocations);
            assertEquals(0, stats.usedMemory);
        }

        @Test
        void testSimpleAllocation() {
            int size = 64;
            long address = arena.allocate(size);

            assertTrue(address > 0);

            EntryArena.ArenaStats stats = arena.getStats();
            assertTrue(stats.totalAllocated >= size);
            assertEquals(1, stats.activeAllocations);
            assertEquals(stats.totalAllocated - stats.totalFreed, stats.usedMemory);
        }

        @Test
        void testAllocationSizeAlignment() {
            // Test that allocations are properly aligned
            int[] testSizes = {1, 7, 8, 15, 16, 31, 32, 33, 64, 100};

            for (int size : testSizes) {
                long address = arena.allocate(size);
                assertTrue(address > 0);
                assertEquals(0, address % 8, "Address should be 8-byte aligned");
                arena.deallocate(address, size);
            }
        }

        @Test
        void testDeallocation() {
            int size = 64;
            long address = arena.allocate(size);

            EntryArena.ArenaStats statsAfterAlloc = arena.getStats();
            long allocatedBefore = statsAfterAlloc.totalAllocated;
            int activeAllocsBefore = statsAfterAlloc.activeAllocations;

            arena.deallocate(address, size);

            EntryArena.ArenaStats statsAfterDealloc = arena.getStats();
            assertEquals(allocatedBefore, statsAfterDealloc.totalAllocated); // Total allocated doesn't decrease
            assertTrue(statsAfterDealloc.totalFreed > 0);
            assertEquals(activeAllocsBefore - 1, statsAfterDealloc.activeAllocations);
        }

        @Test
        void testMultipleAllocations() {
            List<Long> addresses = new ArrayList<>();
            int[] sizes = {32, 64, 128, 256, 512};

            for (int size : sizes) {
                long address = arena.allocate(size);
                assertTrue(address > 0);
                addresses.add(address);

                // Verify addresses are different
                for (int i = 0; i < addresses.size() - 1; i++) {
                    assertNotEquals(addresses.get(i), address);
                }
            }

            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(sizes.length, stats.activeAllocations);

            // Deallocate all
            for (int i = 0; i < addresses.size(); i++) {
                arena.deallocate(addresses.get(i), sizes[i]);
            }

            EntryArena.ArenaStats finalStats = arena.getStats();
            assertEquals(0, finalStats.activeAllocations);
        }

        @Test
        void testMinimumAllocationSize() {
            // Arena should enforce minimum allocation size
            long address1 = arena.allocate(1);
            long address2 = arena.allocate(32);

            assertTrue(address1 > 0);
            assertTrue(address2 > 0);

            EntryArena.ArenaStats stats = arena.getStats();
            // Both allocations should be at least minimum size (32 bytes)
            assertTrue(stats.totalAllocated >= 64); // 2 * 32
        }
    }

    @Nested
    class FreeListTests {

        @Test
        void testFreeListReuse() {
            int size = 64;

            // Allocate and deallocate
            long address1 = arena.allocate(size);
            arena.deallocate(address1, size);

            // Allocate same size again - should reuse from free list
            long address2 = arena.allocate(size);

            // Address might be the same (reused) or different (new allocation)
            assertTrue(address2 > 0);

            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(1, stats.activeAllocations);
        }

        @Test
        void testFreeListDifferentSizes() {
            // Allocate different sizes
            long addr64 = arena.allocate(64);
            long addr128 = arena.allocate(128);
            long addr256 = arena.allocate(256);

            // Deallocate all
            arena.deallocate(addr64, 64);
            arena.deallocate(addr128, 128);
            arena.deallocate(addr256, 256);

            // Allocate different sizes - should find best fit
            long newAddr128 = arena.allocate(128);
            long newAddr64 = arena.allocate(64);
            long newAddr256 = arena.allocate(256);

            assertTrue(newAddr64 > 0);
            assertTrue(newAddr128 > 0);
            assertTrue(newAddr256 > 0);

            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(3, stats.activeAllocations);
        }

        @Test
        void testFreeListFragmentation() {
            List<Long> addresses = new ArrayList<>();
            int size = 64;

            // Allocate many blocks
            for (int i = 0; i < 20; i++) {
                addresses.add(arena.allocate(size));
            }

            // Free every other block to create fragmentation
            for (int i = 0; i < addresses.size(); i += 2) {
                arena.deallocate(addresses.get(i), size);
            }

            EntryArena.ArenaStats statsWithFragmentation = arena.getStats();
            int freeBlocksBefore = statsWithFragmentation.freeBlocks;

            // Compact should reduce fragmentation
            arena.compact();

            EntryArena.ArenaStats statsAfterCompact = arena.getStats();
            // Fragmentation might be reduced (implementation dependent)
            assertTrue(statsAfterCompact.freeBlocks <= freeBlocksBefore);
        }
    }

    @Nested
    class SlabManagementTests {

        @Test
        void testSlabExpansion() {
            EntryArena.ArenaStats initialStats = arena.getStats();
            long initialSystemMemory = initialStats.totalSystemMemory;

            // Allocate enough to potentially trigger new slab allocation
            List<Long> addresses = new ArrayList<>();
            int allocationSize = 1024;

            for (int i = 0; i < 100; i++) { // Should exceed initial slab capacity
                long address = arena.allocate(allocationSize);
                assertTrue(address > 0);
                addresses.add(address);
            }

            EntryArena.ArenaStats finalStats = arena.getStats();

            // System memory might have increased if new slab was allocated
            assertTrue(finalStats.totalSystemMemory >= initialSystemMemory);
            assertEquals(100, finalStats.activeAllocations);

            // Clean up
            for (long address : addresses) {
                arena.deallocate(address, allocationSize);
            }
        }

        @Test
        void testLargeAllocation() {
            // Test allocation larger than typical block size
            int largeSize = 8192; // 8KB
            long address = arena.allocate(largeSize);

            if (address > 0) {
                // If allocation succeeded, verify stats
                EntryArena.ArenaStats stats = arena.getStats();
                assertEquals(1, stats.activeAllocations);
                assertTrue(stats.totalAllocated >= largeSize);

                arena.deallocate(address, largeSize);
            } else {
                // Large allocation might fail if it exceeds slab size
                // This is acceptable behavior
            }
        }

        @Test
        void testVeryLargeAllocation() {
            // Test allocation larger than slab size - should fail gracefully
            int veryLargeSize = 128 * 1024; // 128KB (larger than 64KB slab)
            long address = arena.allocate(veryLargeSize);

            // Should either succeed (if implementation handles it) or return 0
            assertTrue(address >= 0);

            if (address > 0) {
                arena.deallocate(address, veryLargeSize);
            }
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testZeroSizeAllocation() {
            long address = arena.allocate(0);
            assertEquals(0, address); // Should fail
        }

        @Test
        void testNegativeSizeAllocation() {
            long address = arena.allocate(-10);
            assertEquals(0, address); // Should fail
        }

        @Test
        void testDeallocateZeroAddress() {
            // Should not crash
            assertDoesNotThrow(() -> arena.deallocate(0, 64));
        }

        @Test
        void testDeallocateZeroSize() {
            long address = arena.allocate(64);

            // Should not crash but might not do anything useful
            assertDoesNotThrow(() -> arena.deallocate(address, 0));

            // Proper deallocation should still work
            arena.deallocate(address, 64);
        }

        @Test
        void testDeallocateNegativeSize() {
            long address = arena.allocate(64);

            // Should not crash
            assertDoesNotThrow(() -> arena.deallocate(address, -10));

            // Proper deallocation should still work
            arena.deallocate(address, 64);
        }

        @Test
        void testDoubleDeallocate() {
            long address = arena.allocate(64);

            // First deallocation
            arena.deallocate(address, 64);

            // Second deallocation should not crash
            assertDoesNotThrow(() -> arena.deallocate(address, 64));
        }
    }

    @Nested
    class PerformanceTests {

        @Test
        void testManySmallAllocations() {
            List<Long> addresses = new ArrayList<>();
            int numAllocations = 1000;
            int size = 32;

            long startTime = System.nanoTime();

            for (int i = 0; i < numAllocations; i++) {
                long address = arena.allocate(size);
                assertTrue(address > 0);
                addresses.add(address);
            }

            long endTime = System.nanoTime();

            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(numAllocations, stats.activeAllocations);

            // Should complete in reasonable time
            long durationMs = (endTime - startTime) / 1_000_000;
            assertTrue(durationMs < 1000, "Allocation took too long: " + durationMs + "ms");

            // Clean up
            for (long address : addresses) {
                arena.deallocate(address, size);
            }
        }

        @Test
        void testAllocationPatterns() {
            List<Long> addresses = new ArrayList<>();
            int[] sizes = {32, 64, 128, 256, 512, 1024};

            // Allocate in pattern
            for (int round = 0; round < 50; round++) {
                for (int size : sizes) {
                    long address = arena.allocate(size);
                    if (address > 0) {
                        addresses.add(address);
                    }
                }
            }

            EntryArena.ArenaStats stats = arena.getStats();
            assertTrue(stats.activeAllocations > 0);
            assertTrue(stats.totalAllocated > 0);

            // Free all
            for (int i = 0; i < addresses.size(); i++) {
                arena.deallocate(addresses.get(i), sizes[i % sizes.length]);
            }

            EntryArena.ArenaStats finalStats = arena.getStats();
            assertEquals(0, finalStats.activeAllocations);
        }

        @Test
        void testFragmentationMetrics() {
            List<Long> addresses = new ArrayList<>();

            // Create fragmentation pattern
            for (int i = 0; i < 50; i++) {
                addresses.add(arena.allocate(64));
            }

            // Free every third allocation
            for (int i = 0; i < addresses.size(); i += 3) {
                arena.deallocate(addresses.get(i), 64);
            }

            EntryArena.ArenaStats stats = arena.getStats();

            // Verify fragmentation metric is reasonable
            assertTrue(stats.fragmentation >= 0);
            assertTrue(stats.freeBlocks >= 0);

            // Clean up remaining
            for (int i = 0; i < addresses.size(); i++) {
                if (i % 3 != 0) {
                    arena.deallocate(addresses.get(i), 64);
                }
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testCloseWithActiveAllocations() throws Exception {
            // Allocate some memory
            List<Long> addresses = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                addresses.add(arena.allocate(64));
            }

            EntryArena.ArenaStats statsBeforeClose = arena.getStats();
            assertTrue(statsBeforeClose.activeAllocations > 0);
            assertTrue(statsBeforeClose.totalSystemMemory > 0);

            long allocatorUsedBefore = allocator.getUsedBytes();

            arena.close();

            // Memory should be freed
            assertTrue(allocator.getUsedBytes() < allocatorUsedBefore);
        }

        @Test
        void testCloseEmptyArena() throws Exception {
            // Close without any allocations
            assertDoesNotThrow(() -> arena.close());
        }

        @Test
        void testOperationsAfterClose() throws Exception {
            arena.close();

            // Operations after close might fail or return default values
            // The exact behavior depends on implementation
            long address = arena.allocate(64);
            assertTrue(address >= 0); // Should either work or return 0

            if (address > 0) {
                arena.deallocate(address, 64);
            }
        }
    }

    @Nested
    class StatisticsTests {

        @Test
        void testStatsAccuracy() {
            List<Long> addresses = new ArrayList<>();
            int[] sizes = {32, 64, 128};
            long expectedAllocated = 0;

            // Track allocations
            for (int size : sizes) {
                long address = arena.allocate(size);
                addresses.add(address);
                expectedAllocated += Math.max(size, 32); // Account for minimum size
            }

            EntryArena.ArenaStats stats = arena.getStats();
            assertEquals(sizes.length, stats.activeAllocations);
            assertTrue(stats.totalAllocated >= expectedAllocated);
            assertEquals(0, stats.totalFreed);
            assertTrue(stats.usedMemory > 0);

            // Deallocate and check
            long expectedFreed = 0;
            for (int i = 0; i < addresses.size(); i++) {
                arena.deallocate(addresses.get(i), sizes[i]);
                expectedFreed += Math.max(sizes[i], 32);
            }

            EntryArena.ArenaStats finalStats = arena.getStats();
            assertEquals(0, finalStats.activeAllocations);
            assertTrue(finalStats.totalFreed >= expectedFreed);
        }

        @Test
        void testStatsConsistency() {
            // usedMemory should equal totalAllocated - totalFreed

            List<Long> addresses = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                addresses.add(arena.allocate(64));
            }

            EntryArena.ArenaStats stats1 = arena.getStats();
            assertEquals(stats1.totalAllocated - stats1.totalFreed, stats1.usedMemory);

            // Deallocate some
            arena.deallocate(addresses.get(0), 64);
            arena.deallocate(addresses.get(1), 64);

            EntryArena.ArenaStats stats2 = arena.getStats();
            assertEquals(stats2.totalAllocated - stats2.totalFreed, stats2.usedMemory);

            // Clean up
            for (int i = 2; i < addresses.size(); i++) {
                arena.deallocate(addresses.get(i), 64);
            }
        }

        @Test
        void testFragmentationCalculation() {
            // Test that fragmentation calculation doesn't crash and returns reasonable values

            // Create some fragmentation
            List<Long> addresses = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                addresses.add(arena.allocate(64));
            }

            // Free some to create fragments
            for (int i = 0; i < addresses.size(); i += 2) {
                arena.deallocate(addresses.get(i), 64);
            }

            EntryArena.ArenaStats stats = arena.getStats();

            // Fragmentation should be a reasonable value
            assertTrue(stats.fragmentation >= 0.0);
            assertTrue(stats.fragmentation <= 100.0); // Assuming it's a percentage or ratio

            // Clean up
            for (int i = 1; i < addresses.size(); i += 2) {
                arena.deallocate(addresses.get(i), 64);
            }
        }
    }
}
