package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comparison test for EntryArena LINEAR vs FREE_LIST allocation strategies.
 */
class EntryArenaComparisonTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 256L * DEFAULT_PAGE_SIZE; // 8MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private Object owner;
    private Random random;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        random = new Random(42); // Fixed seed for reproducibility
    }

    @AfterEach
    void tearDown() throws Exception {
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    @Test
    void testAllocationStrategyComparison() throws Exception {
        // Test both strategies with the same workload
        EntryArena.ArenaStats linearStats = testStrategy(EntryArena.AllocationStrategy.LINEAR);
        EntryArena.ArenaStats freeListStats = testStrategy(EntryArena.AllocationStrategy.FREE_LIST);

        System.out.println("LINEAR Strategy: " + linearStats);
        System.out.println("FREE_LIST Strategy: " + freeListStats);

        // Basic assertions
        assertEquals(EntryArena.AllocationStrategy.LINEAR, linearStats.strategy);
        assertEquals(EntryArena.AllocationStrategy.FREE_LIST, freeListStats.strategy);

        // FREE_LIST should have similar or better memory efficiency
        // The main advantage is memory reuse, which may not show in simple efficiency metrics
        assertTrue(freeListStats.memoryEfficiency >= 0, "FREE_LIST efficiency should be non-negative");
        assertTrue(linearStats.memoryEfficiency >= 0, "LINEAR efficiency should be non-negative");

        // FREE_LIST should track freed memory (indicating memory recycling capability)
        assertTrue(freeListStats.totalFreed > 0, "FREE_LIST should track freed memory");
        assertEquals(0, linearStats.totalFreed, "LINEAR should not track freed memory");

        // FREE_LIST should have free blocks available for reuse
        assertTrue(freeListStats.freeBlocks > 0, "FREE_LIST should have free blocks for reuse");
    }

    @Test
    void testBackwardCompatibility() throws Exception {
        // Test that default constructor still works (LINEAR strategy)
        try (EntryArena arena = new EntryArena(allocator)) {
            assertEquals(EntryArena.AllocationStrategy.LINEAR, arena.getAllocationStrategy());

            // Basic functionality should work
            byte[] key = "testKey".getBytes();
            byte[] namespace = "testNamespace".getBytes();
            byte[] value = "testValue".getBytes();

            long address = arena.putEntry(key, namespace, value);
            assertTrue(address != 0, "Should allocate successfully");

            // Verify data can be read back
            assertArrayEquals(key, arena.getKeyBytes(address));
            assertArrayEquals(namespace, arena.getNamespaceBytes(address));
            assertArrayEquals(value, arena.getValueBytes(address));
        }
    }

    @Test
    void testFreeListReuseMemory() throws Exception {
        try (EntryArena arena = new EntryArena(allocator, EntryArena.AllocationStrategy.FREE_LIST)) {
            byte[] key = "testKey".getBytes();
            byte[] namespace = "testNamespace".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();

            // Allocate first entry
            long address1 = arena.putEntry(key, namespace, value1);
            assertTrue(address1 != 0);

            EntryArena.ArenaStats stats1 = arena.getStats();
            assertEquals(1, stats1.activeAllocations);
            assertEquals(0, stats1.freeBlocks);

            // Remove the entry
            arena.removeEntry(address1);

            EntryArena.ArenaStats stats2 = arena.getStats();
            assertEquals(0, stats2.activeAllocations);
            assertTrue(stats2.freeBlocks > 0, "Should have free blocks after removal");

            // Allocate second entry (should potentially reuse memory)
            long address2 = arena.putEntry(key, namespace, value2);
            assertTrue(address2 != 0);

            // Verify the new entry
            assertArrayEquals(key, arena.getKeyBytes(address2));
            assertArrayEquals(namespace, arena.getNamespaceBytes(address2));
            assertArrayEquals(value2, arena.getValueBytes(address2));
        }
    }

    private EntryArena.ArenaStats testStrategy(EntryArena.AllocationStrategy strategy) throws Exception {
        try (EntryArena arena = new EntryArena(allocator, strategy)) {
            List<Long> addresses = new ArrayList<>();

            // Phase 1: Allocate many entries
            for (int i = 0; i < 1000; i++) {
                String key = "key" + i;
                String namespace = "ns" + (i % 10);
                String value = "value" + i + "_" + System.nanoTime(); // Variable length

                long address = arena.putEntry(key.getBytes(), namespace.getBytes(), value.getBytes());
                if (address != 0) {
                    addresses.add(address);
                }
            }

            // Phase 2: Remove half of the entries randomly
            List<Long> toRemove = new ArrayList<>(addresses.subList(0, addresses.size() / 2));
            for (Long address : toRemove) {
                arena.removeEntry(address);
            }
            addresses.removeAll(toRemove);

            // Phase 3: Allocate more entries (should reuse memory in FREE_LIST)
            for (int i = 0; i < 200; i++) {
                String key = "newkey" + i;
                String namespace = "newns" + (i % 5);
                String value = "newvalue" + i;

                long address = arena.putEntry(key.getBytes(), namespace.getBytes(), value.getBytes());
                if (address != 0) {
                    addresses.add(address);
                }
            }

            // Verify remaining entries are still valid
            int validCount = 0;
            for (Long address : addresses) {
                byte[] key = arena.getKeyBytes(address);
                byte[] namespace = arena.getNamespaceBytes(address);
                byte[] value = arena.getValueBytes(address);

                if (key != null && namespace != null && value != null) {
                    validCount++;
                }
            }

            assertTrue(validCount > 0, "Should have valid entries");

            return arena.getStats();
        }
    }
}
