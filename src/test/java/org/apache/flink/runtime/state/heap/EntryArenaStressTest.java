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
                    stats.usedMemory + "/" + stats.totalSystemMemory);
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

    private byte[] generateRandomValue(int size) {
        byte[] value = new byte[size];
        random.nextBytes(value);
        return value;
    }
}
