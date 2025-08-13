package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for EntryArena to verify stability and catch potential JVM crashes.
 */
class EntryArenaStressTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 512L * DEFAULT_PAGE_SIZE; // 16MB for stress tests - increased from 8MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryArena arena;
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
        arena = new EntryArena(allocator);
        random = new Random(42); // Fixed seed for reproducibility
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

    @RepeatedTest(10)
    void testMassiveAllocation() {
        List<Long> addresses = new ArrayList<>();

        // Allocate many entries of various sizes
        for (int i = 0; i < 10000; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] namespace = ("ns" + (i % 100)).getBytes();
            byte[] value = generateRandomValue(random.nextInt(1000) + 1);

            long address = arena.putEntry(key, namespace, value);
            if (address > 0) {
                addresses.add(address);

                // Verify data integrity for some entries
                if (i % 1000 == 0) {
                    assertArrayEquals(key, arena.getKeyBytes(address));
                    assertArrayEquals(namespace, arena.getNamespaceBytes(address));
                    assertArrayEquals(value, arena.getValueBytes(address));
                }
            }
        }

        System.out.println("Successfully allocated " + addresses.size() + " entries");
        assertTrue(addresses.size() > 8000, "Should allocate most entries successfully");
    }

    @RepeatedTest(5)
    void testMixedOperations() {
        List<Long> addresses = new ArrayList<>();

        // Phase 1: Allocate entries
        for (int i = 0; i < 5000; i++) {
            byte[] key = ("mixedKey" + i).getBytes();
            byte[] namespace = "mixedNs".getBytes();
            byte[] value = generateRandomValue(random.nextInt(500) + 50);

            long address = arena.putEntry(key, namespace, value);
            if (address > 0) {
                addresses.add(address);
            }
        }

        // Phase 2: Update some entries
        for (int i = 0; i < Math.min(1000, addresses.size()); i++) {
            long oldAddress = addresses.get(i);
            byte[] newValue = generateRandomValue(random.nextInt(800) + 100);

            long newAddress = arena.updateEntry(oldAddress, newValue);
            if (newAddress > 0) {
                addresses.set(i, newAddress);
                assertArrayEquals(newValue, arena.getValueBytes(newAddress));
            }
        }

        // Phase 3: Remove some entries
        for (int i = 0; i < Math.min(500, addresses.size()); i++) {
            arena.removeEntry(addresses.get(i));
        }

        // Phase 4: Verify remaining entries
        for (int i = 500; i < Math.min(1500, addresses.size()); i++) {
            long address = addresses.get(i);
            byte[] key = arena.getKeyBytes(address);
            byte[] namespace = arena.getNamespaceBytes(address);
            byte[] value = arena.getValueBytes(address);

            assertNotNull(key);
            assertNotNull(namespace);
            assertNotNull(value);
            assertTrue(key.length > 0);
        }

        System.out.println("Mixed operations completed successfully");
    }

    @Test
    void testLargeEntryHandling() {
        // Test very large entries
        byte[] largeKey = generateRandomValue(8000);
        byte[] largeNamespace = generateRandomValue(4000);
        byte[] largeValue = generateRandomValue(16000);

        long address = arena.putEntry(largeKey, largeNamespace, largeValue);

        if (address > 0) {
            // Verify large entry integrity
            assertArrayEquals(largeKey, arena.getKeyBytes(address));
            assertArrayEquals(largeNamespace, arena.getNamespaceBytes(address));
            assertArrayEquals(largeValue, arena.getValueBytes(address));

            // Test update with even larger value
            byte[] evenLargerValue = generateRandomValue(32000);
            long newAddress = arena.updateEntry(address, evenLargerValue);

            if (newAddress > 0) {
                assertArrayEquals(evenLargerValue, arena.getValueBytes(newAddress));
            }
        }

        System.out.println("Large entry handling test completed");
    }

    @Test
    void testMemoryExhaustion() {
        List<Long> addresses = new ArrayList<>();

        // Keep allocating until we run out of memory
        for (int i = 0; i < 100000; i++) {
            byte[] key = ("exhaustKey" + i).getBytes();
            byte[] namespace = "exhaustNs".getBytes();
            byte[] value = generateRandomValue(1000); // 1KB each

            long address = arena.putEntry(key, namespace, value);
            if (address == 0) {
                // Expected when memory is exhausted
                break;
            }
            addresses.add(address);

            // Check every 1000 allocations
            if (i % 1000 == 0) {
                EntryArena.ArenaStats stats = arena.getStats();
                System.out.println("Allocated " + i + " entries, used memory: " +
                    stats.totalAllocated + "/" + stats.totalSystemMemory);
            }
        }

        System.out.println("Memory exhaustion test completed with " + addresses.size() + " entries");
        assertTrue(addresses.size() > 1000, "Should allocate substantial number of entries");
    }

    @Test
    void testErrorRecovery() {
        // Test operations with invalid inputs mixed with valid ones
        List<Long> validAddresses = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            // Mix valid and invalid operations
            if (i % 10 == 0) {
                // Invalid operations should not crash
                assertEquals(0, arena.putEntry(null, "ns".getBytes(), "value".getBytes()));
                assertEquals(0, arena.putEntry("key".getBytes(), null, "value".getBytes()));
                assertEquals(0, arena.putEntry("key".getBytes(), "ns".getBytes(), null));
                assertEquals(0, arena.updateEntry(0, "newValue".getBytes()));
                arena.removeEntry(0);
            } else {
                // Valid operations should work
                byte[] key = ("errorKey" + i).getBytes();
                byte[] namespace = "errorNs".getBytes();
                byte[] value = ("errorValue" + i).getBytes();

                long address = arena.putEntry(key, namespace, value);
                if (address > 0) {
                    validAddresses.add(address);

                    // Verify data integrity
                    assertArrayEquals(key, arena.getKeyBytes(address));
                    assertArrayEquals(namespace, arena.getNamespaceBytes(address));
                    assertArrayEquals(value, arena.getValueBytes(address));
                }
            }
        }

        System.out.println("Error recovery test completed with " + validAddresses.size() + " valid entries");
        assertTrue(validAddresses.size() > 800, "Most valid operations should succeed");
    }

    @Test
    void testCloseAndReopenCycle() throws Exception {
        // Test multiple close/reopen cycles to ensure no resource leaks
        for (int cycle = 0; cycle < 5; cycle++) {
            // Allocate some entries
            List<Long> addresses = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                byte[] key = ("cycleKey" + cycle + "_" + i).getBytes();
                byte[] namespace = "cycleNs".getBytes();
                byte[] value = ("cycleValue" + i).getBytes();

                long address = arena.putEntry(key, namespace, value);
                if (address > 0) {
                    addresses.add(address);
                }
            }

            assertTrue(addresses.size() > 90, "Should allocate most entries in cycle " + cycle);

            // Close arena
            arena.close();

            // Reopen arena
            arena = new EntryArena(allocator);
        }

        System.out.println("Close/reopen cycle test completed");
    }

    @Test
    void testLightStress() {
        List<Long> addresses = new ArrayList<>();

        // 轻量压力测试：减少数量和大小
        for (int i = 0; i < 1000; i++) {
            byte[] key = ("lightKey" + i).getBytes();
            byte[] namespace = "lightNs".getBytes();
            byte[] value = generateRandomValue(random.nextInt(100) + 10); // 小值

            long address = arena.putEntry(key, namespace, value);
            if (address > 0) {
                addresses.add(address);

                // 验证数据完整性
                assertArrayEquals(key, arena.getKeyBytes(address));
                assertArrayEquals(namespace, arena.getNamespaceBytes(address));
                assertArrayEquals(value, arena.getValueBytes(address));
            }
        }

        System.out.println("Light stress test completed with " + addresses.size() + " entries");
        assertTrue(addresses.size() > 950, "Should allocate most entries successfully");
    }

    @RepeatedTest(5)
    void testFreeListStressAllocation() {
        try (EntryArena freeListArena = new EntryArena(allocator, EntryArena.AllocationStrategy.FREE_LIST)) {
            List<Long> addresses = new ArrayList<>();

            // Allocate many entries of various sizes with FREE_LIST strategy
            for (int i = 0; i < 8000; i++) {
                byte[] key = ("freeListKey" + i).getBytes();
                byte[] namespace = ("freeListNs" + (i % 50)).getBytes();
                byte[] value = generateRandomValue(random.nextInt(800) + 50);

                long address = freeListArena.putEntry(key, namespace, value);
                if (address > 0) {
                    addresses.add(address);

                    // Verify data integrity for some entries
                    if (i % 100 == 0) {
                        assertArrayEquals(key, freeListArena.getKeyBytes(address));
                        assertArrayEquals(namespace, freeListArena.getNamespaceBytes(address));
                        assertArrayEquals(value, freeListArena.getValueBytes(address));
                    }
                }
            }

            System.out.println("FREE_LIST stress allocation completed with " + addresses.size() + " entries");
            assertTrue(addresses.size() > 6000, "Should allocate most entries successfully with FREE_LIST");

            EntryArena.ArenaStats stats = freeListArena.getStats();
            assertEquals(EntryArena.AllocationStrategy.FREE_LIST, stats.strategy);
            assertTrue(stats.memoryEfficiency > 0, "Memory efficiency should be positive");
        } catch (Exception e) {
            fail("FREE_LIST stress test failed with exception: " + e.getMessage());
        }
    }

    @RepeatedTest(3)
    void testFreeListMixedOperationsStress() {
        try (EntryArena freeListArena = new EntryArena(allocator, EntryArena.AllocationStrategy.FREE_LIST)) {
            List<Long> addresses = new ArrayList<>();

            // Phase 1: Initial allocation
            for (int i = 0; i < 5000; i++) {
                byte[] key = ("mixedFreeListKey" + i).getBytes();
                byte[] namespace = ("mixedFreeListNs" + (i % 20)).getBytes();
                byte[] value = generateRandomValue(random.nextInt(500) + 50);

                long address = freeListArena.putEntry(key, namespace, value);
                if (address > 0) {
                    addresses.add(address);
                }
            }

            EntryArena.ArenaStats afterAllocation = freeListArena.getStats();
            int initialAllocations = afterAllocation.activeAllocations;
            System.out.println("FREE_LIST mixed operations - Initial allocations: " + initialAllocations);

            // Phase 2: Remove random entries to create free blocks
            List<Long> toRemove = new ArrayList<>();
            for (int i = 0; i < Math.min(2000, addresses.size()); i += 2) {
                toRemove.add(addresses.get(i));
            }

            for (Long address : toRemove) {
                freeListArena.removeEntry(address);
            }
            addresses.removeAll(toRemove);

            EntryArena.ArenaStats afterRemoval = freeListArena.getStats();
            System.out.println("FREE_LIST mixed operations - After removal: active=" +
                afterRemoval.activeAllocations + ", freed=" + afterRemoval.totalFreed +
                ", freeBlocks=" + afterRemoval.freeBlocks);

            assertTrue(afterRemoval.freeBlocks > 0, "Should have free blocks after removal");
            assertTrue(afterRemoval.totalFreed > 0, "Should track freed memory");

            // Phase 3: Update some entries
            for (int i = 0; i < Math.min(500, addresses.size()); i++) {
                long oldAddress = addresses.get(i);
                byte[] newValue = generateRandomValue(random.nextInt(600) + 100);

                long newAddress = freeListArena.updateEntry(oldAddress, newValue);
                if (newAddress > 0) {
                    addresses.set(i, newAddress);
                    assertArrayEquals(newValue, freeListArena.getValueBytes(newAddress));
                }
            }

            // Phase 4: Allocate new entries (should reuse freed memory)
            for (int i = 0; i < 1000; i++) {
                byte[] key = ("newFreeListKey" + i).getBytes();
                byte[] namespace = "newFreeListNs".getBytes();
                byte[] value = generateRandomValue(random.nextInt(400) + 50);

                long address = freeListArena.putEntry(key, namespace, value);
                if (address > 0) {
                    addresses.add(address);
                }
            }

            // Phase 5: Verify remaining entries
            int validCount = 0;
            for (Long address : addresses) {
                byte[] key = freeListArena.getKeyBytes(address);
                byte[] namespace = freeListArena.getNamespaceBytes(address);
                byte[] value = freeListArena.getValueBytes(address);

                if (key != null && namespace != null && value != null) {
                    validCount++;
                }
            }

            EntryArena.ArenaStats finalStats = freeListArena.getStats();
            System.out.println("FREE_LIST mixed operations completed - Valid entries: " + validCount +
                ", Final active: " + finalStats.activeAllocations +
                ", Memory efficiency: " + String.format("%.2f%%", finalStats.memoryEfficiency) +
                ", Fragmentation: " + String.format("%.2f%%", finalStats.fragmentation));

            assertTrue(validCount > 0, "Should have valid entries");
            assertTrue(finalStats.memoryEfficiency > 0, "Memory efficiency should be positive");
        } catch (Exception e) {
            fail("FREE_LIST mixed operations stress test failed: " + e.getMessage());
        }
    }

    @Test
    void testFreeListMemoryExhaustion() {
        try (EntryArena freeListArena = new EntryArena(allocator, EntryArena.AllocationStrategy.FREE_LIST)) {
            List<Long> addresses = new ArrayList<>();

            // Keep allocating until we run out of memory, with periodic removals to test recycling
            for (int i = 0; i < 100000; i++) {
                byte[] key = ("exhaustFreeListKey" + i).getBytes();
                byte[] namespace = "exhaustFreeListNs".getBytes();
                byte[] value = generateRandomValue(800); // 800 bytes each

                long address = freeListArena.putEntry(key, namespace, value);
                if (address == 0) {
                    // Expected when memory is exhausted
                    break;
                }
                addresses.add(address);

                // Periodically remove some entries to test memory recycling
                if (i > 0 && i % 500 == 0 && addresses.size() > 100) {
                    // Remove oldest 50 entries
                    for (int j = 0; j < 50 && !addresses.isEmpty(); j++) {
                        freeListArena.removeEntry(addresses.remove(0));
                    }
                }

                // Check progress every 1000 allocations
                if (i % 1000 == 0) {
                    EntryArena.ArenaStats stats = freeListArena.getStats();
                    System.out.println("FREE_LIST exhaustion test - Allocated " + i + " entries, " +
                        "active: " + stats.activeAllocations +
                        ", freed: " + stats.totalFreed +
                        ", freeBlocks: " + stats.freeBlocks +
                        ", efficiency: " + String.format("%.2f%%", stats.memoryEfficiency));
                }
            }

            EntryArena.ArenaStats finalStats = freeListArena.getStats();
            System.out.println("FREE_LIST memory exhaustion test completed with " + addresses.size() +
                " final entries, freed: " + finalStats.totalFreed +
                " bytes, freeBlocks: " + finalStats.freeBlocks);

            assertTrue(addresses.size() > 1000, "Should allocate substantial number of entries");
            assertTrue(finalStats.totalFreed > 0, "Should have recycled memory during test");
        } catch (Exception e) {
            fail("FREE_LIST memory exhaustion test failed: " + e.getMessage());
        }
    }

    @Test
    void testFreeListLargeEntryHandling() {
        try (EntryArena freeListArena = new EntryArena(allocator, EntryArena.AllocationStrategy.FREE_LIST)) {
            List<Long> largeAddresses = new ArrayList<>();

            // Test very large entries with FREE_LIST
            for (int i = 0; i < 10; i++) {
                byte[] largeKey = generateRandomValue(4000 + i * 100);
                byte[] largeNamespace = generateRandomValue(2000 + i * 50);
                byte[] largeValue = generateRandomValue(8000 + i * 200);

                long address = freeListArena.putEntry(largeKey, largeNamespace, largeValue);

                if (address > 0) {
                    largeAddresses.add(address);
                    // Verify large entry integrity
                    assertArrayEquals(largeKey, freeListArena.getKeyBytes(address));
                    assertArrayEquals(largeNamespace, freeListArena.getNamespaceBytes(address));
                    assertArrayEquals(largeValue, freeListArena.getValueBytes(address));
                }
            }

            System.out.println("FREE_LIST large entries allocated: " + largeAddresses.size());

            // Remove some large entries to create large free blocks
            for (int i = 0; i < largeAddresses.size() / 2; i++) {
                freeListArena.removeEntry(largeAddresses.get(i));
            }

            EntryArena.ArenaStats afterRemoval = freeListArena.getStats();
            System.out.println("FREE_LIST after removing large entries - freeBlocks: " +
                afterRemoval.freeBlocks + ", totalFreed: " + afterRemoval.totalFreed);

            // Try to allocate medium-sized entries that should fit in the freed large blocks
            for (int i = 0; i < 5; i++) {
                byte[] mediumKey = generateRandomValue(1000);
                byte[] mediumNamespace = generateRandomValue(500);
                byte[] mediumValue = generateRandomValue(2000);

                long address = freeListArena.putEntry(mediumKey, mediumNamespace, mediumValue);
                if (address > 0) {
                    assertArrayEquals(mediumValue, freeListArena.getValueBytes(address));
                }
            }

            System.out.println("FREE_LIST large entry handling test completed successfully");
        } catch (Exception e) {
            fail("FREE_LIST large entry handling test failed: " + e.getMessage());
        }
    }

    private byte[] generateRandomValue(int size) {
        byte[] value = new byte[size];
        random.nextBytes(value);
        return value;
    }
}
