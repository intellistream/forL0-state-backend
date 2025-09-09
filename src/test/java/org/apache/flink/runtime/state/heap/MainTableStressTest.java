package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for MainTable implementation.
 * Tests the stability and performance under high load conditions,
 * including massive operations, collision handling, and memory pressure.
 */
class MainTableStressTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 512L * DEFAULT_PAGE_SIZE; // 16MB for stress tests - increased from 8MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryArena entryArena;
    private MainTable mainTable;
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
        entryArena = new EntryArena(allocator);

        // Create MainTable with 32 buckets (2^5) and higher load factor threshold for stress testing
        mainTable = new MainTable(allocator, 5, 1.5); // 阈值调整为1.5（entries/base buckets）
        random = new Random(42); // Fixed seed for reproducibility
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mainTable != null) {
            mainTable.close();
        }
        if (entryArena != null) {
            entryArena.close();
        }
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    @RepeatedTest(5)
    void testMassiveInsertions() {
        Map<String, TestEntry> expectedEntries = new HashMap<>();
        List<TestEntry> insertedEntries = new ArrayList<>();

        // Insert large number of entries - full stress test
        for (int i = 0; i < 10000; i++) {
            TestEntry entry = generateRandomEntry("massiveKey" + i, random);

            if (insertEntry(entry)) {
                expectedEntries.put(entry.keyString, entry);
                insertedEntries.add(entry);
            }

            // Print progress every 1000 entries
            if (i > 0 && i % 1000 == 0) {
                MainTable.TableStats stats = mainTable.getStats();
                System.out.println("Inserted " + i + " entries, success: " + insertedEntries.size() +
                                 ", load: " + String.format("%.2f", stats.loadFactor) +
                                 ", extBuckets: " + stats.allocatedExtensionBuckets +
                                 ", needsResize: " + stats.needsResize);
            }
        }

        System.out.println("Successfully inserted " + insertedEntries.size() + " entries");

        // Verify all inserted entries can be retrieved
        int verifyCount = 0;
        for (TestEntry entry : insertedEntries) {
            long retrievedAddress = mainTable.get(entry.hash, entry.tag, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);

            if (retrievedAddress > 0) {
                assertArrayEquals(entry.key, entryArena.getKeyBytes(retrievedAddress));
                assertArrayEquals(entry.namespace, entryArena.getNamespaceBytes(retrievedAddress));
                assertArrayEquals(entry.value, entryArena.getValueBytes(retrievedAddress));
                verifyCount++;
            }
        }

        assertEquals(insertedEntries.size(), verifyCount, "All inserted entries should be retrievable");

        // Print final statistics
        MainTable.TableStats finalStats = mainTable.getStats();
        System.out.println("Final stats: " + finalStats);

        // Should insert a reasonable number of entries before table becomes full
        assertTrue(insertedEntries.size() > 100, "Should successfully insert at least 100 entries");

        // If table stopped accepting entries, it should have triggered resize
        if (insertedEntries.size() < 9000) {
            assertTrue(finalStats.needsResize, "Should trigger resize when table becomes full");
        }
    }

    @RepeatedTest(3)
    void testMixedOperationsStress() {
        Map<String, TestEntry> currentEntries = new HashMap<>();
        List<TestEntry> allEntries = new ArrayList<>();

        // Phase 1: Insert initial entries
        for (int i = 0; i < 5000; i++) {
            TestEntry entry = generateRandomEntry("mixedKey" + i, random);
            if (insertEntry(entry)) {
                currentEntries.put(entry.keyString, entry);
                allEntries.add(entry);
            }
        }

        System.out.println("Phase 1: Inserted " + currentEntries.size() + " initial entries");

        // Phase 2: Mixed operations (insert, update, delete)
        for (int round = 0; round < 10; round++) {
            int operations = 1000;
            int insertCount = 0, updateCount = 0, deleteCount = 0;

            for (int i = 0; i < operations; i++) {
                double operation = random.nextDouble();

                if (operation < 0.4) { // 40% insert
                    TestEntry entry = generateRandomEntry("mixedRound" + round + "_" + i, random);
                    if (insertEntry(entry)) {
                        currentEntries.put(entry.keyString, entry);
                        allEntries.add(entry);
                        insertCount++;
                    }

                } else if (operation < 0.7 && !currentEntries.isEmpty()) { // 30% update
                    List<TestEntry> entryList = new ArrayList<>(currentEntries.values());
                    TestEntry oldEntry = entryList.get(random.nextInt(entryList.size()));

                    // Create updated entry with new value
                    TestEntry newEntry = new TestEntry(oldEntry.keyString, oldEntry.key,
                                                     oldEntry.namespace, generateRandomValue(random));
                    newEntry.hash = oldEntry.hash;
                    newEntry.tag = oldEntry.tag;

                    if (insertEntry(newEntry)) {
                        currentEntries.put(newEntry.keyString, newEntry);
                        updateCount++;
                    }

                } else if (!currentEntries.isEmpty()) { // 30% delete
                    List<TestEntry> entryList = new ArrayList<>(currentEntries.values());
                    TestEntry entryToDelete = entryList.get(random.nextInt(entryList.size()));

                    long removedAddress = mainTable.remove(entryToDelete.hash, entryToDelete.tag, entryToDelete.key, entryToDelete.key.length, entryToDelete.namespace, entryToDelete.namespace.length, entryArena);

                    if (removedAddress > 0) {
                        currentEntries.remove(entryToDelete.keyString);
                        deleteCount++;
                    }
                }
            }

            System.out.println("Round " + round + ": inserted=" + insertCount +
                             ", updated=" + updateCount + ", deleted=" + deleteCount +
                             ", total=" + currentEntries.size());
        }

        // Phase 3: Verify remaining entries
        int verifyCount = 0;
        for (TestEntry entry : currentEntries.values()) {
            long retrievedAddress = mainTable.get(entry.hash, entry.tag, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);

            if (retrievedAddress > 0) {
                assertArrayEquals(entry.value, entryArena.getValueBytes(retrievedAddress));
                verifyCount++;
            }
        }

        assertEquals(currentEntries.size(), verifyCount, "All remaining entries should be verifiable");
        System.out.println("Phase 3: Verified " + verifyCount + " remaining entries");
    }


    @Test
    void testMemoryPressureStress() {
        // Use smaller table to create memory pressure faster
        try (MainTable smallTable = new MainTable(allocator, 2, 0.6)) { // 4 buckets, low threshold
            List<TestEntry> pressureEntries = new ArrayList<>();

            // Insert until memory pressure or resize trigger
            boolean needsResize = false;
            for (int i = 0; i < 20000; i++) {
                TestEntry entry = generateRandomEntry("pressure" + i, random);

                long entryAddress = entryArena.putEntry(entry.key, entry.namespace, entry.value);
                if (entryAddress > 0) {
                    entry.entryAddress = entryAddress;
                    int hash = HashFunctions.jenkinsHashCombined(entry.key, entry.key.length, entry.namespace, entry.namespace.length);
                    short tag = (short) ((hash >> 16) ^ (hash & 0xFFFF));
                    entry.hash = hash;
                    entry.tag = tag;

                    try {
                        long result = smallTable.put(hash, tag, entryAddress, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);
                        if (result >= 0) {
                            pressureEntries.add(entry);
                        }
                    } catch (RuntimeException e) {
                        if (e.getMessage().contains("resize needed")) {
                            needsResize = true;
                            break;
                        }
                        throw e;
                    }
                }

                // Check for resize trigger every 100 entries
                if (i % 100 == 0 && smallTable.needsResize()) {
                    needsResize = true;
                    break;
                }
            }

            System.out.println("Memory pressure test: inserted " + pressureEntries.size() + " entries, needsResize=" + needsResize);

            // Verify entries still accessible under pressure
            int verifyCount = 0;
            for (TestEntry entry : pressureEntries) {
                long retrievedAddress = smallTable.get(entry.hash, entry.tag, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);

                if (retrievedAddress > 0) {
                    assertArrayEquals(entry.value, entryArena.getValueBytes(retrievedAddress));
                    verifyCount++;
                }
            }

            assertEquals(pressureEntries.size(), verifyCount, "All entries should remain accessible under memory pressure");

            // Print final statistics
            MainTable.TableStats finalStats = smallTable.getStats();
            System.out.println("Memory pressure final stats: " + finalStats);
        } catch (Exception e) {
            fail("Memory pressure test failed: " + e.getMessage());
        }
    }

    @Test
    void testConcurrentAccessStress() {
        // Test concurrent read operations (MainTable is not thread-safe for writes, but should handle concurrent reads)
        List<TestEntry> baseEntries = new ArrayList<>();

        // Insert base entries
        for (int i = 0; i < 1000; i++) {
            TestEntry entry = generateRandomEntry("concurrent" + i, random);
            if (insertEntry(entry)) {
                baseEntries.add(entry);
            }
        }

        System.out.println("Concurrent test: inserted " + baseEntries.size() + " base entries");

        // Simulate concurrent reads
        int readCount = 0;
        for (int round = 0; round < 100; round++) {
            for (TestEntry entry : baseEntries) {
                long retrievedAddress = mainTable.get(entry.hash, entry.tag, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);

                if (retrievedAddress > 0) {
                    assertArrayEquals(entry.value, entryArena.getValueBytes(retrievedAddress));
                    readCount++;
                }
            }
        }

        assertEquals(baseEntries.size() * 100, readCount, "All concurrent reads should succeed");
        System.out.println("Completed " + readCount + " concurrent read operations");
    }

    @Test
    void testExtensionBucketExhaustion() {
        // Force allocation of all extension buckets
        List<TestEntry> exhaustionEntries = new ArrayList<>();
        int baseHash = 0x00000000; // Force everything to bucket 0

        // Insert entries until extension buckets are exhausted
        for (int i = 0; i < 300; i++) { // More than 255 extension buckets
            TestEntry entry = generateRandomEntry("exhaustion" + i, random);
            entry.hash = baseHash; // Same bucket
            entry.tag = (short) (i + 1); // Different tags

            long entryAddress = entryArena.putEntry(entry.key, entry.namespace, entry.value);
            if (entryAddress > 0) {
                entry.entryAddress = entryAddress;

                try {
                    long result = mainTable.put(entry.hash, entry.tag, entryAddress, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);
                    if (result >= 0) {
                        exhaustionEntries.add(entry);
                    }
                } catch (RuntimeException e) {
                    // Expected when extension buckets are exhausted or resize is needed
                    System.out.println("Extension bucket exhaustion at entry " + i + ": " + e.getMessage());
                    break;
                }
            }
        }

        System.out.println("Extension exhaustion test: inserted " + exhaustionEntries.size() + " entries");

        // Verify inserted entries are still accessible
        int verifyCount = 0;
        for (TestEntry entry : exhaustionEntries) {
            long retrievedAddress = mainTable.get(entry.hash, entry.tag, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);

            if (retrievedAddress > 0) {
                verifyCount++;
            }
        }

        assertEquals(exhaustionEntries.size(), verifyCount, "All inserted entries should remain accessible");

        MainTable.TableStats stats = mainTable.getStats();
        System.out.println("Extension exhaustion stats: " + stats);
        assertTrue(stats.allocatedExtensionBuckets > 200 || mainTable.needsResize(),
                  "Should have allocated many extension buckets or triggered resize");
    }

    // Helper methods
    private boolean insertEntry(TestEntry entry) {
        long entryAddress = entryArena.putEntry(entry.key, entry.namespace, entry.value);
        if (entryAddress <= 0) {
            return false;
        }
        entry.entryAddress = entryAddress;
        int hash = HashFunctions.jenkinsHashCombined(entry.key, entry.key.length, entry.namespace, entry.namespace.length);
        short tag = (short) ((hash >> 16) ^ (hash & 0xFFFF));
        entry.hash = hash;
        entry.tag = tag;
        try {
            long result = mainTable.put(hash, tag, entryAddress, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryArena);
            return result >= 0;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private TestEntry generateRandomEntry(String keyPrefix, Random rand) {
        String keyString = keyPrefix + "_" + rand.nextInt(1000000);
        byte[] key = keyString.getBytes();
        byte[] namespace = ("ns_" + rand.nextInt(100)).getBytes();
        byte[] value = generateRandomValue(rand);

        return new TestEntry(keyString, key, namespace, value);
    }

    private byte[] generateRandomValue(Random rand) {
        int valueSize = 50 + rand.nextInt(200); // 50-250 bytes
        byte[] value = new byte[valueSize];
        rand.nextBytes(value);
        return value;
    }

    // Test entry helper class
    private static class TestEntry {
        final String keyString;
        final byte[] key;
        final byte[] namespace;
        final byte[] value;
        long entryAddress = 0;
        int hash;
        short tag;

        TestEntry(String keyString, byte[] key, byte[] namespace, byte[] value) {
            this.keyString = keyString;
            this.key = key;
            this.namespace = namespace;
            this.value = value;
        }
    }
}
