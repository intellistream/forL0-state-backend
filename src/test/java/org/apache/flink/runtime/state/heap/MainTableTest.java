package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.util.MathUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for MainTable implementation.
 * Tests the core functionality including hash-based operations,
 * extension buckets, and resize triggers using HeapEntryStore with object comparison.
 */
class MainTableTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB for tests

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private HeapEntryStore<String, String, String> entryStore;
    private MainTable<String, String, String> mainTable;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        entryStore = new HeapEntryStore<>();

        // Create MainTable with 4 buckets (2^2) for testing
        mainTable = new MainTable<>(allocator, 2);
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

    // ========== Helper Methods ==========

    private int compositeHash(String key, String namespace) {
        return MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());
    }

    private short extractTag(int hash) {
        return (short) (hash >>> 16);
    }

    @Nested
    class BasicFunctionalityTests {

        @Test
        void testPutAndGet() {
            String key = "testKey";
            String namespace = "testNamespace";
            String value = "testValue";

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // Store entry in HeapEntryStore
            long entryAddress = entryStore.allocate(key, namespace, value);
            assertTrue(entryAddress > 0, "Entry should be stored successfully");

            // Put entry into MainTable
            long result = mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);
            assertEquals(0, result, "Should return 0 for new insertion");

            // Get entry from MainTable
            long retrievedAddress = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(entryAddress, retrievedAddress, "Should retrieve the same entry address");

            // Verify entry data
            HeapStateEntry<String, String, String> entry = entryStore.get(retrievedAddress);
            assertNotNull(entry);
            assertEquals(key, entry.getKey());
            assertEquals(namespace, entry.getNamespace());
            assertEquals(value, entry.getState());
        }

        @Test
        void testUpdate() {
            String key = "updateKey";
            String namespace = "updateNamespace";
            String value1 = "initialValue";
            String value2 = "updatedValue";

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // Store initial entry
            long entryAddress1 = entryStore.allocate(key, namespace, value1);

            // Insert initial entry
            long result1 = mainTable.put(hash, tag, entryAddress1, key, namespace, entryStore);
            assertEquals(0, result1, "Should return 0 for new insertion");

            // Store updated entry
            long entryAddress2 = entryStore.allocate(key, namespace, value2);

            // Update entry in MainTable
            long result2 = mainTable.put(hash, tag, entryAddress2, key, namespace, entryStore);
            assertEquals(entryAddress1, result2, "Should return previous entry address for update");

            // Verify updated entry
            long retrievedAddress = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(entryAddress2, retrievedAddress, "Should retrieve updated entry address");
            assertEquals(value2, entryStore.get(retrievedAddress).getState());
        }

        @Test
        void testRemove() {
            String key = "removeKey";
            String namespace = "removeNamespace";
            String value = "removeValue";

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // Store and insert entry
            long entryAddress = entryStore.allocate(key, namespace, value);
            mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);

            // Remove entry
            long removedAddress = mainTable.remove(hash, tag, key, namespace, entryStore);
            assertEquals(entryAddress, removedAddress, "Should return removed entry address");

            // Verify entry is removed
            long retrievedAddress = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(0, retrievedAddress, "Entry should not be found after removal");
        }

        @Test
        void testGetNonExistent() {
            String key = "nonExistentKey";
            String namespace = "nonExistentNamespace";
            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            long result = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(0, result, "Should return 0 for non-existent entry");
        }
    }

    @Nested
    class ExtensionBucketTests {

        @Test
        void testExtensionBucketAllocation() {
            // Fill main bucket slots (6 slots per bucket) and force extension
            for (int i = 0; i < 10; i++) { // Insert more than 6 to trigger extension
                String key = "extKey" + i;
                String namespace = "extNamespace";
                String value = "extValue" + i;

                // Force same bucket: use fixed hash that maps to same bucket
                int hash = 0x12340000 | (i << 2); // All these hashes will map to bucket 0
                short tag = (short) ((0x5000 + i) & 0xFFFF); // Different tags to avoid conflicts

                long entryAddress = entryStore.allocate(key, namespace, value);

                long result = mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);
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
            String key1 = "extKey1";
            String key2 = "extKey2";
            String namespace = "extNamespace";
            String value1 = "extValue1";
            String value2 = "extValue2";

            // Force collision by using same bucket
            int baseHash = 0x12340000;
            int hash1 = baseHash | 0x0001;
            int hash2 = baseHash | 0x0002;
            short tag1 = (short) (hash1 & 0xFFFF);
            short tag2 = (short) (hash2 & 0xFFFF);

            long entryAddress1 = entryStore.allocate(key1, namespace, value1);
            long entryAddress2 = entryStore.allocate(key2, namespace, value2);

            // Insert entries
            mainTable.put(hash1, tag1, entryAddress1, key1, namespace, entryStore);
            mainTable.put(hash2, tag2, entryAddress2, key2, namespace, entryStore);

            // Verify both entries can be retrieved
            assertEquals(entryAddress1, mainTable.get(hash1, tag1, key1, namespace, entryStore));
            assertEquals(entryAddress2, mainTable.get(hash2, tag2, key2, namespace, entryStore));

            // Remove one entry
            assertEquals(entryAddress1, mainTable.remove(hash1, tag1, key1, namespace, entryStore));

            // Verify removal
            assertEquals(0, mainTable.get(hash1, tag1, key1, namespace, entryStore));
            assertEquals(entryAddress2, mainTable.get(hash2, tag2, key2, namespace, entryStore)); // Other entry should remain
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
                String key = "loadKey" + i;
                String namespace = "loadNamespace";
                String value = "loadValue" + i;

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                long entryAddress = entryStore.allocate(key, namespace, value);

                mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);
            }

            MainTable.TableStats stats = mainTable.getStats();
            assertTrue(stats.loadFactor > 0, "Load factor should increase after insertions");
            assertEquals(5, stats.totalEntries, "Should have 5 total entries");
        }

        @Test
        void testResizeTrigger() {
            // Test resize trigger with custom threshold
            try (MainTable<String, String, String> customTable = new MainTable<>(allocator, 2, 0.5)) { // Lower threshold
                // Fill table beyond threshold
                for (int i = 0; i < 20; i++) {
                    String key = "resizeKey" + i;
                    String namespace = "resizeNamespace";
                    String value = "resizeValue" + i;

                    int hash = compositeHash(key, namespace);
                    short tag = extractTag(hash);
                    long entryAddress = entryStore.allocate(key, namespace, value);

                    try {
                        customTable.put(hash, tag, entryAddress, key, namespace, entryStore);
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
                String key = "resizeKey" + i;
                String namespace = "ns" + (i % 10);
                String value = "value" + i;

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                long entryAddress = entryStore.allocate(key, namespace, value);

                try {
                    long result = mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);
                    if (result >= 0) {
                        insertCount++;
                    }
                } catch (RuntimeException e) {
                    break;
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
            String emptyKey = "";
            String emptyNamespace = "";
            String value = "emptyKeyValue";

            long entryAddress = entryStore.allocate(emptyKey, emptyNamespace, value);
            int hash = compositeHash(emptyKey, emptyNamespace);
            short tag = extractTag(hash);

            long result = mainTable.put(hash, tag, entryAddress, emptyKey, emptyNamespace, entryStore);
            assertEquals(0, result, "Should handle empty key and namespace");

            long retrievedAddress = mainTable.get(hash, tag, emptyKey, emptyNamespace, entryStore);
            assertEquals(entryAddress, retrievedAddress, "Should retrieve entry with empty key/namespace");
        }

        @Test
        void testLargeStringEntries() {
            // Create large strings
            StringBuilder largeKeyBuilder = new StringBuilder();
            StringBuilder largeNamespaceBuilder = new StringBuilder();
            StringBuilder largeValueBuilder = new StringBuilder();

            for (int i = 0; i < 1000; i++) {
                largeKeyBuilder.append("k");
            }
            for (int i = 0; i < 500; i++) {
                largeNamespaceBuilder.append("n");
            }
            for (int i = 0; i < 2000; i++) {
                largeValueBuilder.append("v");
            }

            String largeKey = largeKeyBuilder.toString();
            String largeNamespace = largeNamespaceBuilder.toString();
            String largeValue = largeValueBuilder.toString();

            int hash = compositeHash(largeKey, largeNamespace);
            short tag = extractTag(hash);
            long entryAddress = entryStore.allocate(largeKey, largeNamespace, largeValue);

            long result = mainTable.put(hash, tag, entryAddress, largeKey, largeNamespace, entryStore);
            assertEquals(0, result, "Should handle large entries");

            long retrievedAddress = mainTable.get(hash, tag, largeKey, largeNamespace, entryStore);
            assertEquals(entryAddress, retrievedAddress, "Should retrieve large entry");
        }

        @Test
        void testHashCollisions() {
            // Create entries with same hash but different content
            String key1 = "collision1";
            String key2 = "collision2";
            String namespace = "collisionNamespace";
            String value1 = "value1";
            String value2 = "value2";

            // Force same hash by using fixed hash value
            int sameHash = 0x12345678;
            short tag1 = (short) 0x1234;
            short tag2 = (short) 0x5678; // Different tags

            long entryAddress1 = entryStore.allocate(key1, namespace, value1);
            long entryAddress2 = entryStore.allocate(key2, namespace, value2);

            // Insert both entries with same hash
            long result1 = mainTable.put(sameHash, tag1, entryAddress1, key1, namespace, entryStore);
            long result2 = mainTable.put(sameHash, tag2, entryAddress2, key2, namespace, entryStore);

            assertEquals(0, result1, "First entry should insert successfully");
            assertEquals(0, result2, "Second entry should insert successfully");

            // Verify both can be retrieved correctly
            assertEquals(entryAddress1, mainTable.get(sameHash, tag1, key1, namespace, entryStore));
            assertEquals(entryAddress2, mainTable.get(sameHash, tag2, key2, namespace, entryStore));
        }
    }

    @Nested
    class IterationTests {

        @Test
        void testIterationConcept() {
            // Insert test entries
            int numEntries = 10;
            for (int i = 0; i < numEntries; i++) {
                String key = "iterKey" + i;
                String namespace = "iterNamespace";
                String value = "iterValue" + i;

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                long entryAddress = entryStore.allocate(key, namespace, value);

                mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);
            }

            // Verify entries were inserted
            MainTable.TableStats stats = mainTable.getStats();
            assertEquals(numEntries, stats.totalEntries, "Should have inserted all entries");
        }

        @Test
        void testTableExpansionConcept() {
            // Insert entries into original table
            for (int i = 0; i < 5; i++) {
                String key = "expandKey" + i;
                String namespace = "expandNamespace";
                String value = "expandValue" + i;

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                long entryAddress = entryStore.allocate(key, namespace, value);

                mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);
            }

            // Verify entries can be retrieved
            for (int i = 0; i < 5; i++) {
                String key = "expandKey" + i;
                String namespace = "expandNamespace";

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);

                long retrievedAddress = mainTable.get(hash, tag, key, namespace, entryStore);
                assertTrue(retrievedAddress > 0, "Entry should be found");

                HeapStateEntry<String, String, String> entry = entryStore.get(retrievedAddress);
                assertNotNull(entry);
                assertEquals(key, entry.getKey());
                assertEquals(namespace, entry.getNamespace());
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClose() throws Exception {
            // Insert some entries
            String key = "closeKey";
            String namespace = "closeNamespace";
            String value = "closeValue";

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);
            long entryAddress = entryStore.allocate(key, namespace, value);

            mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);

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
            String key = "statsKey";
            String namespace = "statsNamespace";
            String value = "statsValue";

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);
            long entryAddress = entryStore.allocate(key, namespace, value);

            mainTable.put(hash, tag, entryAddress, key, namespace, entryStore);

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
}
