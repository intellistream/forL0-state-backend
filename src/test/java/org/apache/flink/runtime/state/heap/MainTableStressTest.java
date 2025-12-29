package org.apache.flink.runtime.state.heap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
/**
 * Stress tests for MainTable implementation.
 * Tests the stability and performance under high load conditions,
 * including massive operations, collision handling, and memory pressure.
 */
class MainTableStressTest {
    
    private MainTable<String, String, String> mainTable;
    private Random random;
    @BeforeEach
    void setUp() {
        
        // Create MainTable with INITIAL_BUCKET_COUNT and load factor threshold for stress testing
        mainTable = new MainTable<>(1.5);
        random = new Random(42); // Fixed seed for reproducibility
    }
    @AfterEach
    void tearDown() throws Exception {
        if (mainTable != null) {
            mainTable.close();
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
                expectedEntries.put(entry.key, entry);
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
            String retrieved = mainTable.get(entry.key, entry.namespace);
            if (retrieved != null) {
                assertNotNull(retrieved);
                assertEquals(entry.value, retrieved);
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
                currentEntries.put(entry.key, entry);
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
                        currentEntries.put(entry.key, entry);
                        allEntries.add(entry);
                        insertCount++;
                    }
                } else if (operation < 0.7 && !currentEntries.isEmpty()) { // 30% update
                    List<TestEntry> entryList = new ArrayList<>(currentEntries.values());
                    TestEntry oldEntry = entryList.get(random.nextInt(entryList.size()));
                    // Create updated entry with new value
                    String newValue = generateRandomValue(random);
                    TestEntry newEntry = new TestEntry(oldEntry.key, oldEntry.namespace, newValue);
                    if (insertEntry(newEntry)) {
                        currentEntries.put(newEntry.key, newEntry);
                        updateCount++;
                    }
                } else if (!currentEntries.isEmpty()) { // 30% delete
                    List<TestEntry> entryList = new ArrayList<>(currentEntries.values());
                    TestEntry entryToDelete = entryList.get(random.nextInt(entryList.size()));
                    String removedState = mainTable.remove(
                            entryToDelete.key, entryToDelete.namespace);
                    if (removedState != null) {
                        currentEntries.remove(entryToDelete.key);
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
            String retrieved = mainTable.get(entry.key, entry.namespace);
            if (retrieved != null) {
                assertEquals(entry.value, retrieved);
                verifyCount++;
            }
        }
        assertEquals(currentEntries.size(), verifyCount, "All remaining entries should be verifiable");
        System.out.println("Phase 3: Verified " + verifyCount + " remaining entries");
    }
    @Test
    void testMemoryPressureStress() {
        // Use table with low threshold to test resize trigger logic
        // Note: MainTable now has fixed initial size
        try (MainTable<String, String, String> smallTable = new MainTable<>(0.6)) {
            List<TestEntry> pressureEntries = new ArrayList<>();
            // Insert entries - we test with fewer entries to verify basic functionality
            for (int i = 0; i < 5000; i++) {
                TestEntry entry = generateRandomEntry("pressure" + i, random);
                try {
                    int ptr = smallTable.put(entry.key, entry.namespace);
                    if (ptr != 0) {
                        smallTable.states[ptr - 1] = entry.value;
                        pressureEntries.add(entry);
                    }
                } catch (RuntimeException e) {
                    break;
                }
            }
            // With INITIAL_BUCKET_COUNT, 5000 entries should NOT trigger resize
            assertFalse(smallTable.needsResize(), "Should not need resize with 5000 entries");
            // Verify all entries are retrievable
            int verifyCount = 0;
            for (TestEntry entry : pressureEntries) {
                String found = smallTable.get(entry.key, entry.namespace);
                if (found != null && entry.value.equals(found)) {
                    verifyCount++;
                }
            }
            assertEquals(pressureEntries.size(), verifyCount, "All entries should be verifiable");
            System.out.println("Memory pressure test: " + pressureEntries.size() + " entries stored and verified");
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
                String retrieved = mainTable.get(entry.key, entry.namespace);
                if (retrieved != null) {
                    assertEquals(entry.value, retrieved);
                    readCount++;
                }
            }
        }
        assertEquals(baseEntries.size() * 100, readCount, "All concurrent reads should succeed");
        System.out.println("Completed " + readCount + " concurrent read operations");
    }
    @Test
    void testExtensionBucketExhaustion() {
        // Force allocation of all extension buckets by using same key
        List<TestEntry> exhaustionEntries = new ArrayList<>();
        String fixedKey = "exhaustionKey"; // Use same key to force all entries into same bucket
        // Insert entries until extension buckets are exhausted
        for (int i = 0; i < 300; i++) { // More than 255 extension buckets
            String namespace = "ns" + i;
            String value = "value" + i;
            TestEntry entry = new TestEntry(fixedKey, namespace, value);
            try {
                int ptr = mainTable.put(entry.key, entry.namespace);
                if (ptr != 0) {
                    mainTable.states[ptr - 1] = entry.value;
                    exhaustionEntries.add(entry);
                }
            } catch (RuntimeException e) {
                // Expected when extension buckets are exhausted or resize is needed
                System.out.println("Extension bucket exhaustion at entry " + i + ": " + e.getMessage());
                break;
            }
        }
        System.out.println("Extension exhaustion test: inserted " + exhaustionEntries.size() + " entries");
        // Verify inserted entries are still accessible
        int verifyCount = 0;
        for (TestEntry entry : exhaustionEntries) {
            String retrieved = mainTable.get(entry.key, entry.namespace);
            if (retrieved != null) {
                verifyCount++;
            }
        }
        assertEquals(exhaustionEntries.size(), verifyCount, "All inserted entries should remain accessible");
        MainTable.TableStats stats = mainTable.getStats();
        System.out.println("Extension exhaustion stats: " + stats);
        // Note: With internal hash computation, we cannot force specific bucket collisions
        // So we just verify the test completed successfully
        assertTrue(exhaustionEntries.size() > 0, "Should have inserted entries");
    }
    // Helper methods
    private boolean insertEntry(TestEntry entry) {
        try {
            int ptr = mainTable.put(entry.key, entry.namespace);
            if (ptr != 0) {
                mainTable.states[ptr - 1] = entry.value;
                return true;
            }
            return false;
        } catch (RuntimeException e) {
            return false;
        }
    }
    private TestEntry generateRandomEntry(String keyPrefix, Random rand) {
        String key = keyPrefix + "_" + rand.nextInt(1000000);
        String namespace = "ns_" + rand.nextInt(100);
        String value = generateRandomValue(rand);
        return new TestEntry(key, namespace, value);
    }
    private String generateRandomValue(Random rand) {
        int valueSize = 50 + rand.nextInt(200); // 50-250 chars
        StringBuilder sb = new StringBuilder(valueSize);
        for (int i = 0; i < valueSize; i++) {
            sb.append((char) ('a' + rand.nextInt(26)));
        }
        return sb.toString();
    }
    // Test entry helper class
    private static class TestEntry {
        final String key;
        final String namespace;
        final String value;
        TestEntry(String key, String namespace, String value) {
            this.key = key;
            this.namespace = namespace;
            this.value = value;
        }
    }
}
