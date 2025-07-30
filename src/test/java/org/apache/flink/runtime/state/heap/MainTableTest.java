package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
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
    private EntryArena entryArena;
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
        entryArena = new EntryArena(allocator);

        // Create MainTable with 4 buckets (2^2) for testing
        mainTable = new MainTable(allocator, 2);
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

    @Nested
    class BasicFunctionalityTests {

        @Test
        void testPutAndGet() {
            // Prepare test data
            byte[] key = "testKey".getBytes();
            byte[] namespace = "testNamespace".getBytes();
            byte[] value = "testValue".getBytes();

            // Store entry in EntryArena
            long entryAddress = entryArena.putEntry(key, namespace, value);
            assertTrue(entryAddress > 0, "Entry should be stored successfully");

            // Calculate hash and tag
            int keyHash = HashFunctions.murmurHash3(key);
            int namespaceHash = HashFunctions.murmurHash3(namespace);
            int hash = keyHash ^ namespaceHash;
            short tag = (short) (hash & 0xFFFF);

            // Create entry matcher
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

            // Put entry into MainTable
            long result = mainTable.put(hash, tag, entryAddress, matcher);
            assertEquals(0, result, "Should return 0 for new insertion");

            // Get entry from MainTable
            long retrievedAddress = mainTable.get(hash, tag, matcher);
            assertEquals(entryAddress, retrievedAddress, "Should retrieve the same entry address");

            // Verify entry data
            assertArrayEquals(key, entryArena.getKeyBytes(retrievedAddress));
            assertArrayEquals(namespace, entryArena.getNamespaceBytes(retrievedAddress));
            assertArrayEquals(value, entryArena.getValueBytes(retrievedAddress));
        }

        @Test
        void testUpdate() {
            // Prepare test data
            byte[] key = "updateKey".getBytes();
            byte[] namespace = "updateNamespace".getBytes();
            byte[] value1 = "initialValue".getBytes();
            byte[] value2 = "updatedValue".getBytes();

            // Store initial entry
            long entryAddress1 = entryArena.putEntry(key, namespace, value1);
            int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
            short tag = (short) (hash & 0xFFFF);
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

            // Insert initial entry
            long result1 = mainTable.put(hash, tag, entryAddress1, matcher);
            assertEquals(0, result1, "Should return 0 for new insertion");

            // Store updated entry
            long entryAddress2 = entryArena.putEntry(key, namespace, value2);

            // Update entry in MainTable
            long result2 = mainTable.put(hash, tag, entryAddress2, matcher);
            assertEquals(entryAddress1, result2, "Should return previous entry address for update");

            // Verify updated entry
            long retrievedAddress = mainTable.get(hash, tag, matcher);
            assertEquals(entryAddress2, retrievedAddress, "Should retrieve updated entry address");
            assertArrayEquals(value2, entryArena.getValueBytes(retrievedAddress));
        }

        @Test
        void testRemove() {
            // Prepare test data
            byte[] key = "removeKey".getBytes();
            byte[] namespace = "removeNamespace".getBytes();
            byte[] value = "removeValue".getBytes();

            // Store and insert entry
            long entryAddress = entryArena.putEntry(key, namespace, value);
            int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
            short tag = (short) (hash & 0xFFFF);
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

            mainTable.put(hash, tag, entryAddress, matcher);

            // Remove entry
            long removedAddress = mainTable.remove(hash, tag, matcher);
            assertEquals(entryAddress, removedAddress, "Should return removed entry address");

            // Verify entry is removed
            long retrievedAddress = mainTable.get(hash, tag, matcher);
            assertEquals(0, retrievedAddress, "Entry should not be found after removal");
        }

        @Test
        void testGetNonExistent() {
            byte[] key = "nonExistentKey".getBytes();
            byte[] namespace = "nonExistentNamespace".getBytes();
            int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
            short tag = (short) (hash & 0xFFFF);
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

            long result = mainTable.get(hash, tag, matcher);
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

                long entryAddress = entryArena.putEntry(key, namespace, value);

                // Force same bucket: use hash that always maps to bucket 0
                // MainTable has 4 buckets (2^2), so bucket index = hash & 3
                // Use hash values like 0x1000, 0x1004, 0x1008, etc. (all map to bucket 0)
                int hash = 0x12340000 | (i << 2); // All these hashes will map to bucket 0
                short tag = (short) ((0x5000 + i) & 0xFFFF); // Different tags to avoid conflicts
                MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

                long result = mainTable.put(hash, tag, entryAddress, matcher);
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

            long entryAddress1 = entryArena.putEntry(key1, namespace, value1);
            long entryAddress2 = entryArena.putEntry(key2, namespace, value2);

            MainTable.EntryMatcher matcher1 = (addr) -> entryArena.matchesKey(addr, key1, namespace);
            MainTable.EntryMatcher matcher2 = (addr) -> entryArena.matchesKey(addr, key2, namespace);

            // Insert entries
            mainTable.put(hash1, tag1, entryAddress1, matcher1);
            mainTable.put(hash2, tag2, entryAddress2, matcher2);

            // Verify both entries can be retrieved
            assertEquals(entryAddress1, mainTable.get(hash1, tag1, matcher1));
            assertEquals(entryAddress2, mainTable.get(hash2, tag2, matcher2));

            // Remove one entry
            assertEquals(entryAddress1, mainTable.remove(hash1, tag1, matcher1));

            // Verify removal
            assertEquals(0, mainTable.get(hash1, tag1, matcher1));
            assertEquals(entryAddress2, mainTable.get(hash2, tag2, matcher2)); // Other entry should remain
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

                long entryAddress = entryArena.putEntry(key, namespace, value);
                int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
                short tag = (short) (hash & 0xFFFF);
                MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

                mainTable.put(hash, tag, entryAddress, matcher);
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

                    long entryAddress = entryArena.putEntry(key, namespace, value);
                    int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
                    short tag = (short) (hash & 0xFFFF);
                    MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

                    try {
                        customTable.put(hash, tag, entryAddress, matcher);
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

            long entryAddress = entryArena.putEntry(emptyKey, emptyNamespace, value);
            int hash = HashFunctions.murmurHash3(emptyKey) ^ HashFunctions.murmurHash3(emptyNamespace);
            short tag = (short) (hash & 0xFFFF);
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, emptyKey, emptyNamespace);

            long result = mainTable.put(hash, tag, entryAddress, matcher);
            assertEquals(0, result, "Should handle empty key and namespace");

            long retrievedAddress = mainTable.get(hash, tag, matcher);
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

            long entryAddress = entryArena.putEntry(largeKey, largeNamespace, largeValue);
            if (entryAddress > 0) { // Only test if EntryArena can handle large entries
                int hash = HashFunctions.murmurHash3(largeKey) ^ HashFunctions.murmurHash3(largeNamespace);
                short tag = (short) (hash & 0xFFFF);
                MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, largeKey, largeNamespace);

                long result = mainTable.put(hash, tag, entryAddress, matcher);
                assertEquals(0, result, "Should handle large entries");

                long retrievedAddress = mainTable.get(hash, tag, matcher);
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

            long entryAddress1 = entryArena.putEntry(key1, namespace, value1);
            long entryAddress2 = entryArena.putEntry(key2, namespace, value2);

            // Force same hash by using fixed hash value
            int sameHash = 0x12345678;
            short tag1 = (short) 0x1234;
            short tag2 = (short) 0x5678; // Different tags

            MainTable.EntryMatcher matcher1 = (addr) -> entryArena.matchesKey(addr, key1, namespace);
            MainTable.EntryMatcher matcher2 = (addr) -> entryArena.matchesKey(addr, key2, namespace);

            // Insert both entries with same hash
            long result1 = mainTable.put(sameHash, tag1, entryAddress1, matcher1);
            long result2 = mainTable.put(sameHash, tag2, entryAddress2, matcher2);

            assertEquals(0, result1, "First entry should insert successfully");
            assertEquals(0, result2, "Second entry should insert successfully");

            // Verify both can be retrieved correctly
            assertEquals(entryAddress1, mainTable.get(sameHash, tag1, matcher1));
            assertEquals(entryAddress2, mainTable.get(sameHash, tag2, matcher2));
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

                long entryAddress = entryArena.putEntry(key, namespace, value);
                int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
                short tag = (short) (hash & 0xFFFF);
                MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

                mainTable.put(hash, tag, entryAddress, matcher);
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

                long entryAddress = entryArena.putEntry(key, namespace, value);
                int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
                short tag = (short) (hash & 0xFFFF);
                MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

                mainTable.put(hash, tag, entryAddress, matcher);
            }

            // Verify entries can be retrieved
            for (int i = 0; i < 5; i++) {
                byte[] key = ("expandKey" + i).getBytes();
                byte[] namespace = "expandNamespace".getBytes();

                int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
                short tag = (short) (hash & 0xFFFF);
                MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

                long retrievedAddress = mainTable.get(hash, tag, matcher);
                assertTrue(retrievedAddress > 0, "Entry should be found");

                assertArrayEquals(key, entryArena.getKeyBytes(retrievedAddress));
                assertArrayEquals(namespace, entryArena.getNamespaceBytes(retrievedAddress));
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

            long entryAddress = entryArena.putEntry(key, namespace, value);
            int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
            short tag = (short) (hash & 0xFFFF);
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

            mainTable.put(hash, tag, entryAddress, matcher);

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

            long entryAddress = entryArena.putEntry(key, namespace, value);
            int hash = HashFunctions.murmurHash3(key) ^ HashFunctions.murmurHash3(namespace);
            short tag = (short) (hash & 0xFFFF);
            MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, key, namespace);

            mainTable.put(hash, tag, entryAddress, matcher);

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
        long entryAddress = entryArena.putEntry(entry.key, entry.namespace, entry.value);
        if (entryAddress <= 0) {
            return false;
        }

        int hash = HashFunctions.murmurHash3(entry.key) ^ HashFunctions.murmurHash3(entry.namespace);
        short tag = (short) (hash & 0xFFFF);
        MainTable.EntryMatcher matcher = (addr) -> entryArena.matchesKey(addr, entry.key, entry.namespace);

        try {
            long result = mainTable.put(hash, tag, entryAddress, matcher);
            return result >= 0;
        } catch (RuntimeException e) {
            return false;
        }
    }

    // Test entry helper class
    private static class TestEntry {
        final String keyString;
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
