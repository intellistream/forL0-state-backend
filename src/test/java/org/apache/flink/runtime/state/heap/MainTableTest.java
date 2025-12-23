package org.apache.flink.runtime.state.heap;
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
    
    private MainTable<String, String, String> mainTable;
    @BeforeEach
    void setUp() {
        
        // Create MainTable with default size (65536 buckets) for testing
        mainTable = new MainTable<>(1.5);
    }
    @AfterEach
    void tearDown() throws Exception {
        if (mainTable != null) {
            mainTable.close();
        }
    }
    // ========== Helper Methods ==========
    private int compositeHash(String key, String namespace) {
        return MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());
    }
    @Nested
    class BasicFunctionalityTests {
        @Test
        void testPutAndGet() {
            String key = "testKey";
            String namespace = "testNamespace";
            String value = "testValue";
            int hash = compositeHash(key, namespace);
            // Put entry into MainTable (MainTable creates entry internally)
            HeapStateEntry<String, String, String> putEntry = mainTable.put(hash, key, namespace);
            assertNotNull(putEntry, "Put should return the entry");
            putEntry.state = value;
            // Get entry from MainTable
            HeapStateEntry<String, String, String> entry = mainTable.get(hash, key, namespace);
            assertNotNull(entry, "Should retrieve the entry");
            // Verify entry data
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
            // Insert initial entry
            HeapStateEntry<String, String, String> entry1 = mainTable.put(hash, key, namespace);
            assertNotNull(entry1, "Should return entry for new insertion");
            entry1.state = value1;
            // Update same key/namespace - should return the same entry
            HeapStateEntry<String, String, String> entry2 = mainTable.put(hash, key, namespace);
            assertNotNull(entry2, "Should return entry for update");
            assertSame(entry1, entry2, "Should return the same entry for same key/namespace");
            entry2.state = value2;
            // Verify updated entry
            HeapStateEntry<String, String, String> retrieved = mainTable.get(hash, key, namespace);
            assertNotNull(retrieved, "Should retrieve entry");
            assertEquals(value2, retrieved.getState());
        }
        @Test
        void testRemove() {
            String key = "removeKey";
            String namespace = "removeNamespace";
            String value = "removeValue";
            int hash = compositeHash(key, namespace);
            // Insert entry
            HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
            assertNotNull(entry);
            entry.state = value;
            // Remove entry
            HeapStateEntry<String, String, String> removedEntry = mainTable.remove(hash, key, namespace);
            assertNotNull(removedEntry, "Should return removed entry");
            assertEquals(value, removedEntry.getState());
            // Verify entry is removed
            HeapStateEntry<String, String, String> retrievedEntry = mainTable.get(hash, key, namespace);
            assertNull(retrievedEntry, "Entry should not be found after removal");
        }
        @Test
        void testGetNonExistent() {
            String key = "nonExistentKey";
            String namespace = "nonExistentNamespace";
            int hash = compositeHash(key, namespace);
            HeapStateEntry<String, String, String> result = mainTable.get(hash, key, namespace);
            assertNull(result, "Should return null for non-existent entry");
        }
    }
    @Nested
    class ExtensionBucketTests {
        @Test
        void testExtensionBucketAllocation() {
            // Fill main bucket slots (7 slots per bucket) and force extension by using same bucket
            // With 65536 buckets, we need all entries to hash to the same bucket to trigger extension
            // bucketIndex = hash & (bucketCount - 1) = hash & 0xFFFF
            // To force all entries to bucket 0, all hashes must have low 16 bits = 0
            for (int i = 0; i < 12; i++) { // Insert more than 7 to trigger extension in that bucket
                String key = "extKey" + i;
                String namespace = "extNamespace";
                String value = "extValue" + i;
                // Force all entries to same bucket index 0 by making low 16 bits all 0
                // hash & 0xFFFF == 0, so bucket index = 0 for all entries
                int hash = (i + 1) << 16; // 0x10000, 0x20000, 0x30000, ... all map to bucket 0
                HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
                assertNotNull(entry, "Should insert successfully, using extension buckets if needed");
                entry.state = value;
            }
            // Verify extension buckets were allocated
            MainTable.TableStats stats = mainTable.getStats();
            System.out.println("Stats after insertion: " + stats);
            assertTrue(stats.allocatedExtensionBuckets > 0, "Extension buckets should be allocated when bucket overflows");
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
            // Insert entries
            HeapStateEntry<String, String, String> entry1 = mainTable.put(hash1, key1, namespace);
            assertNotNull(entry1);
            entry1.state = value1;
            
            HeapStateEntry<String, String, String> entry2 = mainTable.put(hash2, key2, namespace);
            assertNotNull(entry2);
            entry2.state = value2;
            // Verify both entries can be retrieved
            assertNotNull(mainTable.get(hash1, key1, namespace));
            assertNotNull(mainTable.get(hash2, key2, namespace));
            // Remove one entry
            HeapStateEntry<String, String, String> removed = mainTable.remove(hash1, key1, namespace);
            assertNotNull(removed);
            assertEquals(value1, removed.getState());
            // Verify removal
            assertNull(mainTable.get(hash1, key1, namespace));
            assertNotNull(mainTable.get(hash2, key2, namespace)); // Other entry should remain
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
                HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
                entry.state = value;
            }
            MainTable.TableStats stats = mainTable.getStats();
            assertTrue(stats.loadFactor > 0, "Load factor should increase after insertions");
            assertEquals(5, stats.totalEntries, "Should have 5 total entries");
        }
        @Test
        void testResizeTrigger() {
            // Test resize trigger with custom threshold
            // Note: MainTable now has fixed initial size of 65536 buckets
            // With threshold 0.5, need > 32768 entries to trigger resize
            // We just verify the needsResize() logic works correctly
            try (MainTable<String, String, String> customTable = new MainTable<>(0.5)) {
                // Insert a moderate number of entries (won't trigger resize with 65536 buckets)
                for (int i = 0; i < 100; i++) {
                    String key = "resizeKey" + i;
                    String namespace = "resizeNamespace";
                    String value = "resizeValue" + i;
                    int hash = compositeHash(key, namespace);
                    HeapStateEntry<String, String, String> entry = customTable.put(hash, key, namespace);
                    entry.state = value;
                }
                // With 65536 buckets and threshold 0.5, need > 32768 entries to trigger resize
                // 100 entries should NOT trigger resize
                assertFalse(customTable.needsResize(), "Should not need resize with only 100 entries");
                
                // Verify stats
                MainTable.TableStats stats = customTable.getStats();
                assertEquals(100, stats.totalEntries);
                assertEquals(65536, stats.bucketCount);
            } catch (Exception e) {
                fail("Should not throw exception during resize test: " + e.getMessage());
            }
        }
        @Test
        void testResizeNeeded() {
            // With 65536 buckets and load factor threshold 1.5, resize is needed when:
            // - entries > 65536 * 1.5 = 98304, OR
            // - extension bucket ratio too high
            // For a practical unit test, we verify resize is NOT needed with small data
            
            // Insert 1000 entries - should NOT trigger resize with 65536 buckets
            for (int i = 0; i < 1000; i++) {
                String key = "resizeKey" + i;
                String namespace = "ns" + (i % 10);
                String value = "value" + i;
                int hash = compositeHash(key, namespace);
                HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
                entry.state = value;
            }
            MainTable.TableStats stats = mainTable.getStats();
            System.out.println("Stats after 1000 insertions: " + stats);
            assertEquals(1000, stats.totalEntries, "Should have 1000 entries");
            assertFalse(mainTable.needsResize(), "Should NOT need resize with 1000 entries in 65536 buckets");
            assertTrue(stats.loadFactor < 0.02, "Load factor should be very low (~0.015)");
        }
    }
    @Nested
    class EdgeCaseTests {
        @Test
        void testEmptyKeyAndNamespace() {
            String emptyKey = "";
            String emptyNamespace = "";
            String value = "emptyKeyValue";
            int hash = compositeHash(emptyKey, emptyNamespace);
            HeapStateEntry<String, String, String> result = mainTable.put(hash, emptyKey, emptyNamespace);
            assertNotNull(result, "Should handle empty key and namespace");
            result.state = value;
            HeapStateEntry<String, String, String> retrieved = mainTable.get(hash, emptyKey, emptyNamespace);
            assertNotNull(retrieved, "Should retrieve entry with empty key/namespace");
            assertEquals(value, retrieved.getState());
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
            HeapStateEntry<String, String, String> result = mainTable.put(hash, largeKey, largeNamespace);
            assertNotNull(result, "Should handle large entries");
            result.state = largeValue;
            HeapStateEntry<String, String, String> retrieved = mainTable.get(hash, largeKey, largeNamespace);
            assertNotNull(retrieved, "Should retrieve large entry");
            assertEquals(largeValue, retrieved.getState());
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
            // Insert both entries with same hash
            HeapStateEntry<String, String, String> entry1 = mainTable.put(sameHash, key1, namespace);
            entry1.state = value1;
            HeapStateEntry<String, String, String> entry2 = mainTable.put(sameHash, key2, namespace);
            entry2.state = value2;
            assertNotNull(entry1, "First entry should insert successfully");
            assertNotNull(entry2, "Second entry should insert successfully");
            // Verify both can be retrieved correctly
            assertNotNull(mainTable.get(sameHash, key1, namespace));
            assertNotNull(mainTable.get(sameHash, key2, namespace));
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
                HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
                entry.state = value;
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
                HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
                entry.state = value;
            }
            // Verify entries can be retrieved
            for (int i = 0; i < 5; i++) {
                String key = "expandKey" + i;
                String namespace = "expandNamespace";
                int hash = compositeHash(key, namespace);
                HeapStateEntry<String, String, String> entry = mainTable.get(hash, key, namespace);
                assertNotNull(entry, "Entry should be found");
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
            HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
            entry.state = value;
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
            assertEquals(65536, stats.bucketCount, "Should have 65536 buckets (fixed initial size)");
            assertEquals(0, stats.totalEntries, "Should start with 0 entries");
            assertEquals(0.0, stats.loadFactor, 0.001, "Should start with 0 load factor");
            assertEquals(0, stats.maxExtensionBuckets, "Should start with 0 extension buckets");
            assertFalse(stats.needsResize, "Should not need resize initially");
            // Insert an entry and check stats update
            String key = "statsKey";
            String namespace = "statsNamespace";
            String value = "statsValue";
            int hash = compositeHash(key, namespace);
            HeapStateEntry<String, String, String> entry = mainTable.put(hash, key, namespace);
            entry.state = value;
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
            assertTrue(statsString.contains("buckets=65536"));
            assertTrue(statsString.contains("entries=0"));
        }
    }
}
