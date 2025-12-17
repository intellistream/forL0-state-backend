package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.entrystore.EntryStore;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for MainTable implementation.
 * Tests the core functionality including hash-based operations,
 * extension buckets, and resize triggers.
 */
class MainTableTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB for tests

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryStore entryStore;
    private MainTable mainTable;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        entryStore = new EntryStore(allocator);

        // Create MainTable with 4 buckets (2^2) for testing
        mainTable = new MainTable(allocator, 2);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mainTable != null) {
            mainTable.close();
        }
        if (entryStore != null) {
            entryStore.close();
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
        void testPutAndGet() {
            // Prepare test data
            byte[] key = "testKey".getBytes();
            byte[] namespace = "testNamespace".getBytes();
            byte[] value = "testValue".getBytes();

            // Calculate hash and tag
            int hash = HashFunctions.compositeHash(key, namespace);
            short tag = (short) (hash & 0xFFFF);

            // Store entry in EntryStore
            long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);
            assertTrue(entryAddress > 0, "Entry should be stored successfully");

            // Put entry into MainTable using new inline method
            long result = mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(0, result, "Should return 0 for new insertion");

            // Get entry from MainTable using new inline method
            long retrievedAddress = mainTable.get(hash, tag, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(entryAddress, retrievedAddress, "Should retrieve the same entry address");

            // Verify entry data
            assertArrayEquals(key, entryStore.getKeyBytes(retrievedAddress));
            assertArrayEquals(namespace, entryStore.getNamespaceBytes(retrievedAddress));
            assertArrayEquals(value, entryStore.getValueBytes(retrievedAddress));
        }

        @Test
        void testUpdate() {
            // Prepare test data
            byte[] key = "updateKey".getBytes();
            byte[] namespace = "updateNamespace".getBytes();
            byte[] value1 = "initialValue".getBytes();
            byte[] value2 = "updatedValue".getBytes();

            // Calculate hash and tag
            int hash = HashFunctions.compositeHash(key, namespace);
            short tag = (short) (hash & 0xFFFF);

            // Store initial entry
            long entryAddress1 = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value1, value1.length);

            // Insert initial entry
            long result1 = mainTable.put(hash, tag, entryAddress1, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(0, result1, "Should return 0 for new insertion");

            // Store updated entry
            long entryAddress2 = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value2, value2.length);

            // Update entry in MainTable
            long result2 = mainTable.put(hash, tag, entryAddress2, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(entryAddress1, result2, "Should return previous entry address for update");

            // Verify updated entry
            long retrievedAddress = mainTable.get(hash, tag, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(entryAddress2, retrievedAddress, "Should retrieve updated entry address");
            assertArrayEquals(value2, entryStore.getValueBytes(retrievedAddress));
        }

        @Test
        void testRemove() {
            // Prepare test data
            byte[] key = "removeKey".getBytes();
            byte[] namespace = "removeNamespace".getBytes();
            byte[] value = "removeValue".getBytes();

            // Calculate hash and tag
            int hash = HashFunctions.compositeHash(key, namespace);
            short tag = (short) (hash & 0xFFFF);

            // Store and insert entry
            long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);

            mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);

            // Remove entry
            long removedAddress = mainTable.remove(hash, tag, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(entryAddress, removedAddress, "Should return removed entry address");

            // Verify entry is removed
            long retrievedAddress = mainTable.get(hash, tag, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(0, retrievedAddress, "Entry should not be found after removal");
        }

        @Test
        void testGetNonExistent() {
            byte[] key = "nonExistentKey".getBytes();
            byte[] namespace = "nonExistentNamespace".getBytes();
            int hash = HashFunctions.compositeHash(key, namespace);
            short tag = (short) (hash & 0xFFFF);

            long result = mainTable.get(hash, tag, key, key.length, namespace, namespace.length, entryStore);
            assertEquals(0, result, "Should return 0 for non-existent entry");
        }
    }

    @Nested
    class ExtensionBucketTests {

        @Test
        void testExtensionBucketAllocation() {
            // Fill main bucket slots (6 slots per bucket) and force extension
            for (int i = 0; i < 10; i++) { // Insert more than 6 to trigger extension
                byte[] key = ("extKey" + i).getBytes();
                byte[] namespace = "extNamespace".getBytes();
                byte[] value = ("extValue" + i).getBytes();

                // Force same bucket: use hash that always maps to bucket 0
                // MainTable has 4 buckets (2^2), so bucket index = hash & 3
                // Use hash values like 0x1000, 0x1004, 0x1008, etc. (all map to bucket 0)
                int hash = 0x12340000 | (i << 2); // All these hashes will map to bucket 0
                short tag = (short) ((0x5000 + i) & 0xFFFF); // Different tags to avoid conflicts

                long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);

                long result = mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);
                assertEquals(0, result, "Should insert successfully, using extension buckets if needed");
            }

            // Verify extension buckets were allocated
            MainTable.TableStats stats = mainTable.getStats();
            System.out.println("Stats after insertion: " + stats);
            assertTrue(stats.allocatedExtensionBuckets > 0, "Extension buckets should be allocated");
        }

        @Test
        void testExtensionBucketOperations() {
            // Insert entries that will use extension buckets
            byte[] key1 = "extKey1".getBytes();
            byte[] key2 = "extKey2".getBytes();
            byte[] namespace = "extNamespace".getBytes();
            byte[] value1 = "extValue1".getBytes();
            byte[] value2 = "extValue2".getBytes();

            // Force collision by using same bucket
            int baseHash = 0x12340000;
            int hash1 = baseHash | 0x0001;
            int hash2 = baseHash | 0x0002;
            short tag1 = (short) (hash1 & 0xFFFF);
            short tag2 = (short) (hash2 & 0xFFFF);

            long entryAddress1 = entryStore.allocateEntry(hash1, key1, key1.length, namespace, namespace.length, value1, value1.length);
            long entryAddress2 = entryStore.allocateEntry(hash2, key2, key2.length, namespace, namespace.length, value2, value2.length);

            // Insert entries
            mainTable.put(hash1, tag1, entryAddress1, key1, key1.length, namespace, namespace.length, entryStore);
            mainTable.put(hash2, tag2, entryAddress2, key2, key2.length, namespace, namespace.length, entryStore);

            // Verify both entries can be retrieved
            assertEquals(entryAddress1, mainTable.get(hash1, tag1, key1, key1.length, namespace, namespace.length, entryStore));
            assertEquals(entryAddress2, mainTable.get(hash2, tag2, key2, key2.length, namespace, namespace.length, entryStore));

            // Remove one entry
            assertEquals(entryAddress1, mainTable.remove(hash1, tag1, key1, key1.length, namespace, namespace.length, entryStore));

            // Verify removal
            assertEquals(0, mainTable.get(hash1, tag1, key1, key1.length, namespace, namespace.length, entryStore));
            assertEquals(entryAddress2, mainTable.get(hash2, tag2, key2, key2.length, namespace, namespace.length, entryStore)); // Other entry should remain
        }
    }

    @Nested
    class LoadFactorAndResizeTests {

        @Test
        void testLoadFactorCalculation() {
            MainTable.TableStats initialStats = mainTable.getStats();
            assertEquals(0.0, initialStats.loadFactor, 0.001, "Initial load factor should be 0");

            // Insert some entries
            for (int i = 0; i < 5; i++) {
                byte[] key = ("loadKey" + i).getBytes();
                byte[] namespace = "loadNamespace".getBytes();
                byte[] value = ("loadValue" + i).getBytes();

                int hash = HashFunctions.compositeHash(key, namespace);
                long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);
                short tag = (short) (hash & 0xFFFF);

                mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);
            }

            MainTable.TableStats stats = mainTable.getStats();
            assertTrue(stats.loadFactor > 0, "Load factor should increase after insertions");
            assertEquals(5, stats.totalEntries, "Should have 5 total entries");
        }

        @Test
        void testResizeTrigger() {
            // Test resize trigger with custom threshold
            try (MainTable customTable = new MainTable(allocator, 2, 0.5)) { // Lower threshold
                // Fill table beyond threshold
                for (int i = 0; i < 20; i++) {
                    byte[] key = ("resizeKey" + i).getBytes();
                    byte[] namespace = "resizeNamespace".getBytes();
                    byte[] value = ("resizeValue" + i).getBytes();

                    int hash = HashFunctions.compositeHash(key, namespace);
                    long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);
                    short tag = (short) (hash & 0xFFFF);

                    try {
                        customTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);
                    } catch (RuntimeException e) {
                        if (e.getMessage().contains("resize needed")) {
                            // Expected when table is full
                            break;
                        }
                        throw e;
                    }
                }

                // Check if resize is needed
                assertTrue(customTable.needsResize(), "Table should need resize after heavy load");
            } catch (Exception e) {
                fail("Should not throw exception during resize test: " + e.getMessage());
            }
        }

        @Test
        void testResizeNeeded() {
            // Insert entries until resize is needed
            int insertCount = 0;
            for (int i = 0; i < 1000; i++) {
                TestEntry entry = new TestEntry("resizeKey" + i,
                    ("resizeKey" + i).getBytes(),
                    ("ns" + (i % 10)).getBytes(),
                    ("value" + i).getBytes());

                if (insertTestEntry(entry)) {
                    insertCount++;
                }

                if (mainTable.needsResize()) {
                    break;
                }
            }

            System.out.println("Inserted " + insertCount + " entries before resize needed");
            MainTable.TableStats stats = mainTable.getStats();
            System.out.println("Stats when resize needed: " + stats);

            assertTrue(insertCount > 0, "Should insert at least some entries");
            assertTrue(mainTable.needsResize(), "Should need resize when load factor or extension buckets are high");
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testEmptyKeyAndNamespace() {
            byte[] emptyKey = new byte[0];
            byte[] emptyNamespace = new byte[0];
            byte[] value = "emptyKeyValue".getBytes();

            long entryAddress = entryStore.allocateEntry(0, emptyKey, emptyKey.length, emptyNamespace, emptyNamespace.length, value, value.length);
            short tag = 0;

            long result = mainTable.put(0, tag, entryAddress, emptyKey, emptyKey.length, emptyNamespace, emptyNamespace.length, entryStore);
            assertEquals(0, result, "Should handle empty key and namespace");

            long retrievedAddress = mainTable.get(0, tag, emptyKey, emptyKey.length, emptyNamespace, emptyNamespace.length, entryStore);
            assertEquals(entryAddress, retrievedAddress, "Should retrieve entry with empty key/namespace");
        }

        @Test
        void testLargeEntries() {
            byte[] largeKey = new byte[1000];
            byte[] largeNamespace = new byte[500];
            byte[] largeValue = new byte[2000];

            // Fill with test data
            for (int i = 0; i < largeKey.length; i++) {
                largeKey[i] = (byte) (i % 256);
            }
            for (int i = 0; i < largeNamespace.length; i++) {
                largeNamespace[i] = (byte) ((i + 100) % 256);
            }
            for (int i = 0; i < largeValue.length; i++) {
                largeValue[i] = (byte) ((i + 200) % 256);
            }

            int hash = HashFunctions.compositeHash(largeKey, largeNamespace);
            long entryAddress = entryStore.allocateEntry(hash, largeKey, largeKey.length, largeNamespace, largeNamespace.length, largeValue, largeValue.length);
            if (entryAddress > 0) { // Only test if EntryStore can handle large entries
                short tag = (short) (hash & 0xFFFF);

                long result = mainTable.put(hash, tag, entryAddress, largeKey, largeKey.length, largeNamespace, largeNamespace.length, entryStore);
                assertEquals(0, result, "Should handle large entries");

                long retrievedAddress = mainTable.get(hash, tag, largeKey, largeKey.length, largeNamespace, largeNamespace.length, entryStore);
                assertEquals(entryAddress, retrievedAddress, "Should retrieve large entry");
            }
        }

        @Test
        void testHashCollisions() {
            // Create entries with same hash but different content
            byte[] key1 = "collision1".getBytes();
            byte[] key2 = "collision2".getBytes();
            byte[] namespace = "collisionNamespace".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();

            // Force same hash by using fixed hash value
            int sameHash = 0x12345678;
            short tag1 = (short) 0x1234;
            short tag2 = (short) 0x5678; // Different tags

            long entryAddress1 = entryStore.allocateEntry(sameHash, key1, key1.length, namespace, namespace.length, value1, value1.length);
            long entryAddress2 = entryStore.allocateEntry(sameHash, key2, key2.length, namespace, namespace.length, value2, value2.length);

            // Insert both entries with same hash
            long result1 = mainTable.put(sameHash, tag1, entryAddress1, key1, key1.length, namespace, namespace.length, entryStore);
            long result2 = mainTable.put(sameHash, tag2, entryAddress2, key2, key2.length, namespace, namespace.length, entryStore);

            assertEquals(0, result1, "First entry should insert successfully");
            assertEquals(0, result2, "Second entry should insert successfully");

            // Verify both can be retrieved correctly
            assertEquals(entryAddress1, mainTable.get(sameHash, tag1, key1, key1.length, namespace, namespace.length, entryStore));
            assertEquals(entryAddress2, mainTable.get(sameHash, tag2, key2, key2.length, namespace, namespace.length, entryStore));
        }
    }

    @Nested
    class IterationTests {

        @Test
        void testIterationConcept() {
            // Insert test entries
            int numEntries = 10;
            for (int i = 0; i < numEntries; i++) {
                byte[] key = ("iterKey" + i).getBytes();
                byte[] namespace = "iterNamespace".getBytes();
                byte[] value = ("iterValue" + i).getBytes();

                int hash = HashFunctions.compositeHash(key, namespace);
                long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);
                short tag = (short) (hash & 0xFFFF);

                mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);
            }

            // Verify entries were inserted
            MainTable.TableStats stats = mainTable.getStats();
            assertEquals(numEntries, stats.totalEntries, "Should have inserted all entries");
        }

        @Test
        void testTableExpansionConcept() {
            // Insert entries into original table
            for (int i = 0; i < 5; i++) {
                byte[] key = ("expandKey" + i).getBytes();
                byte[] namespace = "expandNamespace".getBytes();
                byte[] value = ("expandValue" + i).getBytes();

                int hash = HashFunctions.compositeHash(key, namespace);
                long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);
                short tag = (short) (hash & 0xFFFF);

                mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);
            }

            // Verify entries can be retrieved
            for (int i = 0; i < 5; i++) {
                byte[] key = ("expandKey" + i).getBytes();
                byte[] namespace = "expandNamespace".getBytes();

                int hash = HashFunctions.compositeHash(key, namespace);
                short tag = (short) (hash & 0xFFFF);

                long retrievedAddress = mainTable.get(hash, tag, key, key.length, namespace, namespace.length, entryStore);
                assertTrue(retrievedAddress > 0, "Entry should be found");

                assertArrayEquals(key, entryStore.getKeyBytes(retrievedAddress));
                assertArrayEquals(namespace, entryStore.getNamespaceBytes(retrievedAddress));
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClose() throws Exception {
            // Insert some entries
            byte[] key = "closeKey".getBytes();
            byte[] namespace = "closeNamespace".getBytes();
            byte[] value = "closeValue".getBytes();

            int hash = HashFunctions.compositeHash(key, namespace);
            long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);
            short tag = (short) (hash & 0xFFFF);

            mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);

            // Close should not throw exception
            assertDoesNotThrow(() -> mainTable.close());
        }

        @Test
        void testMultipleClose() throws Exception {
            // Multiple close calls should be safe
            mainTable.close();
            assertDoesNotThrow(() -> mainTable.close());
        }
    }

    @Nested
    class StatisticsTests {

        @Test
        void testTableStats() {
            MainTable.TableStats stats = mainTable.getStats();

            assertNotNull(stats);
            assertEquals(4, stats.bucketCount, "Should have 4 buckets (2^2)");
            assertEquals(0, stats.totalEntries, "Should start with 0 entries");
            assertEquals(0.0, stats.loadFactor, 0.001, "Should start with 0 load factor");
            assertEquals(0, stats.maxExtensionBuckets, "Should start with 0 extension buckets");
            assertFalse(stats.needsResize, "Should not need resize initially");

            // Insert an entry and check stats update
            byte[] key = "statsKey".getBytes();
            byte[] namespace = "statsNamespace".getBytes();
            byte[] value = "statsValue".getBytes();

            int hash = HashFunctions.compositeHash(key, namespace);
            long entryAddress = entryStore.allocateEntry(hash, key, key.length, namespace, namespace.length, value, value.length);
            short tag = (short) (hash & 0xFFFF);

            mainTable.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryStore);

            MainTable.TableStats updatedStats = mainTable.getStats();
            assertEquals(1, updatedStats.totalEntries, "Should have 1 entry after insertion");
            assertTrue(updatedStats.loadFactor > 0, "Load factor should be positive after insertion");
        }

        @Test
        void testStatsToString() {
            MainTable.TableStats stats = mainTable.getStats();
            String statsString = stats.toString();

            assertNotNull(statsString);
            assertTrue(statsString.contains("MainTable"));
            assertTrue(statsString.contains("buckets=4"));
            assertTrue(statsString.contains("entries=0"));
        }
    }

    // Helper method for tests
    private boolean insertTestEntry(TestEntry entry) {
        int hash = HashFunctions.compositeHash(entry.key, entry.namespace);
        long entryAddress = entryStore.allocateEntry(hash, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entry.value, entry.value.length);
        if (entryAddress <= 0) {
            return false;
        }

        short tag = (short) (hash & 0xFFFF);

        try {
            long result = mainTable.put(hash, tag, entryAddress, entry.key, entry.key.length, entry.namespace, entry.namespace.length, entryStore);
            return result >= 0;
        } catch (RuntimeException e) {
            return false;
        }
    }

    // Test entry helper class
    private static class TestEntry {
        @SuppressWarnings("unused")
        final String keyString;  // Kept for debugging purposes
        final byte[] key;
        final byte[] namespace;
        final byte[] value;

        TestEntry(String keyString, byte[] key, byte[] namespace, byte[] value) {
            this.keyString = keyString;
            this.key = key;
            this.namespace = namespace;
            this.value = value;
        }
    }
}
