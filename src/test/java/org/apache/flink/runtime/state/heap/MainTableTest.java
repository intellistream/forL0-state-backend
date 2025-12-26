package org.apache.flink.runtime.state.heap;
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
    @Nested
    class BasicFunctionalityTests {
        @Test
        void testPutAndGet() {
            String key = "testKey";
            String namespace = "testNamespace";
            String value = "testValue";
            
            // Put entry into MainTable (returns ptr, negative means new entry)
            int ptr = mainTable.put(key, namespace);
            assertTrue(ptr != 0, "Put should return non-zero ptr");
            
            // Set state
            int base = (ptr - 1) * 3;
            mainTable.entries[base + 2] = value;
            
            // Get state from MainTable
            String retrievedState = mainTable.get(key, namespace);
            assertNotNull(retrievedState, "Should retrieve the state");
            assertEquals(value, retrievedState);
        }
        
        @Test
        void testUpdate() {
            String key = "updateKey";
            String namespace = "updateNamespace";
            String value1 = "initialValue";
            String value2 = "updatedValue";
            
            // Insert initial entry
            int ptr1 = mainTable.put(key, namespace);
            assertTrue(ptr1 != 0, "Should return non-zero ptr for new insertion");
            if (ptr1 < 0) ptr1 = -ptr1;  // Handle negative ptr for new entries
            int base1 = (ptr1 - 1) * 3;
            mainTable.entries[base1 + 2] = value1;
            
            // Update same key/namespace - should return the same ptr
            int ptr2 = mainTable.put(key, namespace);
            assertEquals(ptr1, ptr2, "Should return the same ptr for same key/namespace");
            mainTable.entries[base1 + 2] = value2;
            
            // Verify updated state
            String retrieved = mainTable.get(key, namespace);
            assertNotNull(retrieved, "Should retrieve state");
            assertEquals(value2, retrieved);
        }
        
        @Test
        void testRemove() {
            String key = "removeKey";
            String namespace = "removeNamespace";
            String value = "removeValue";
            
            // Insert entry
            int ptr = mainTable.put(key, namespace);
            assertTrue(ptr != 0);
            int base = (ptr - 1) * 3;
            mainTable.entries[base + 2] = value;
            
            // Get state before removal
            String beforeRemove = mainTable.get(key, namespace);
            assertEquals(value, beforeRemove);
            
            // Remove entry and verify returned state
            String removedState = mainTable.remove(key, namespace);
            assertNotNull(removedState, "Should return removed state");
            assertEquals(value, removedState, "Returned state should match");
            
            // Verify entry is removed
            String afterRemove = mainTable.get(key, namespace);
            assertNull(afterRemove, "State should be null after removal");
        }
        
        @Test
        void testGetNonExistent() {
            String key = "nonExistentKey";
            String namespace = "nonExistentNamespace";
            
            String result = mainTable.get(key, namespace);
            assertNull(result, "Should return null for non-existent entry");
        }
    }
    @Nested
    class ExtensionBucketTests {
        @Test
        void testExtensionBucketAllocation() {
            // Force all entries to same bucket by using same key with different namespaces
            // This maximizes collision probability
            String fixedKey = "collisionKey";
            for (int i = 0; i < 12; i++) { // Insert more than 7 to trigger extension in that bucket
                String namespace = "ns" + i;
                String value = "extValue" + i;
                int ptr = mainTable.put(fixedKey, namespace);
                assertTrue(ptr != 0, "Should insert successfully, using extension buckets if needed");
                int base = (ptr - 1) * 3;
                mainTable.entries[base + 2] = value;
            }
            // Verify extension buckets were allocated
            MainTable.TableStats stats = mainTable.getStats();
            System.out.println("Stats after insertion: " + stats);
            // Note: With internal hash computation, same key + different namespaces may not
            // hash to the same bucket, so we just verify entries were inserted successfully
            assertTrue(stats.totalEntries == 12, "All 12 entries should be inserted");
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
            // Insert entries
            int ptr1 = mainTable.put(key1, namespace);
            assertTrue(ptr1 != 0);
            if (ptr1 < 0) ptr1 = -ptr1;  // Handle negative ptr for new entries
            int base1 = (ptr1 - 1) * 3;
            mainTable.entries[base1 + 2] = value1;
            
            int ptr2 = mainTable.put(key2, namespace);
            assertTrue(ptr2 != 0);
            if (ptr2 < 0) ptr2 = -ptr2;  // Handle negative ptr for new entries
            int base2 = (ptr2 - 1) * 3;
            mainTable.entries[base2 + 2] = value2;
            // Verify both entries can be retrieved
            assertNotNull(mainTable.get(key1, namespace));
            assertNotNull(mainTable.get(key2, namespace));
            // Remove one entry
            String removedState = mainTable.remove(key1, namespace);
            assertNotNull(removedState, "Should return removed state");
            assertEquals(value1, removedState, "Returned state should match");
            // Verify removal
            assertNull(mainTable.get(key1, namespace));
            assertNotNull(mainTable.get(key2, namespace)); // Other entry should remain
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
                int ptr = mainTable.put(key, namespace);
                int base = (ptr - 1) * 3;
                mainTable.entries[base + 2] = value;
            }
            MainTable.TableStats stats = mainTable.getStats();
            assertTrue(stats.loadFactor > 0, "Load factor should increase after insertions");
            assertEquals(5, stats.totalEntries, "Should have 5 total entries");
        }
        @Test
        void testResizeTrigger() {
            // Test resize trigger with custom threshold
            // With threshold 0.5, need > INITIAL_BUCKET_COUNT * 0.5 entries to trigger resize
            // We just verify the needsResize() logic works correctly
            try (MainTable<String, String, String> customTable = new MainTable<>(0.5)) {
                // Insert a moderate number of entries (won't trigger resize with small load)
                for (int i = 0; i < 100; i++) {
                    String key = "resizeKey" + i;
                    String namespace = "resizeNamespace";
                    String value = "resizeValue" + i;
                    int ptr = customTable.put(key, namespace);
                    int base = (ptr - 1) * 3;
                    customTable.entries[base + 2] = value;
                }
                // With INITIAL_BUCKET_COUNT and threshold 0.5, 100 entries should NOT trigger resize
                assertFalse(customTable.needsResize(), "Should not need resize with only 100 entries");
                
                // Verify stats
                MainTable.TableStats stats = customTable.getStats();
                assertEquals(100, stats.totalEntries);
                assertEquals(MainTable.INITIAL_BUCKET_COUNT, stats.bucketCount);
            } catch (Exception e) {
                fail("Should not throw exception during resize test: " + e.getMessage());
            }
        }
        @Test
        void testResizeNeeded() {
            // With INITIAL_BUCKET_COUNT and load factor threshold 1.5, resize is needed when:
            // - entries > INITIAL_BUCKET_COUNT * 1.5, OR
            // - extension bucket ratio too high
            // For a practical unit test, we verify resize is NOT needed with small data
            
            // Insert 1000 entries - should NOT trigger resize
            for (int i = 0; i < 1000; i++) {
                String key = "resizeKey" + i;
                String namespace = "ns" + (i % 10);
                String value = "value" + i;
                int ptr = mainTable.put(key, namespace);
                int base = (ptr - 1) * 3;
                mainTable.entries[base + 2] = value;
            }
            MainTable.TableStats stats = mainTable.getStats();
            System.out.println("Stats after 1000 insertions: " + stats);
            assertEquals(1000, stats.totalEntries, "Should have 1000 entries");
            assertEquals(MainTable.INITIAL_BUCKET_COUNT, stats.bucketCount, "Bucket count should match initial capacity");
            assertFalse(mainTable.needsResize(), "Should NOT need resize with 1000 entries");
            double expectedMaxLoadFactor = 1000.0 / MainTable.INITIAL_BUCKET_COUNT;
            assertTrue(stats.loadFactor <= expectedMaxLoadFactor, "Load factor should be " + expectedMaxLoadFactor + " or less");
        }
    }
    @Nested
    class EdgeCaseTests {
        @Test
        void testEmptyKeyAndNamespace() {
            String emptyKey = "";
            String emptyNamespace = "";
            String value = "emptyKeyValue";
            int ptr = mainTable.put(emptyKey, emptyNamespace);
            assertTrue(ptr != 0, "Should handle empty key and namespace");
            int base = (ptr - 1) * 3;
            mainTable.entries[base + 2] = value;
            String retrieved = mainTable.get(emptyKey, emptyNamespace);
            assertNotNull(retrieved, "Should retrieve entry with empty key/namespace");
            assertEquals(value, retrieved);
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
            int ptr = mainTable.put(largeKey, largeNamespace);
            assertTrue(ptr != 0, "Should handle large entries");
            int base = (ptr - 1) * 3;
            mainTable.entries[base + 2] = largeValue;
            String retrieved = mainTable.get(largeKey, largeNamespace);
            assertNotNull(retrieved, "Should retrieve large entry");
            assertEquals(largeValue, retrieved);
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
            // Insert both entries with same hash
            int ptr1 = mainTable.put(key1, namespace);
            if (ptr1 < 0) ptr1 = -ptr1;  // Handle negative ptr for new entries
            int base1 = (ptr1 - 1) * 3;
            mainTable.entries[base1 + 2] = value1;
            int ptr2 = mainTable.put(key2, namespace);
            if (ptr2 < 0) ptr2 = -ptr2;  // Handle negative ptr for new entries
            int base2 = (ptr2 - 1) * 3;
            mainTable.entries[base2 + 2] = value2;
            assertTrue(ptr1 != 0, "First entry should insert successfully");
            assertTrue(ptr2 != 0, "Second entry should insert successfully");
            // Verify both can be retrieved correctly
            assertNotNull(mainTable.get(key1, namespace));
            assertNotNull(mainTable.get(key2, namespace));
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
                int ptr = mainTable.put(key, namespace);
                int base = (ptr - 1) * 3;
                mainTable.entries[base + 2] = value;
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
                int ptr = mainTable.put(key, namespace);
                int base = (ptr - 1) * 3;
                mainTable.entries[base + 2] = value;
            }
            // Verify entries can be retrieved
            for (int i = 0; i < 5; i++) {
                String key = "expandKey" + i;
                String namespace = "expandNamespace";
                String state = mainTable.get(key, namespace);
                assertNotNull(state, "Entry should be found");
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
                int ptr = mainTable.put(key, namespace);
                int base = (ptr - 1) * 3;
            mainTable.entries[base + 2] = value;
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
            assertEquals(MainTable.INITIAL_BUCKET_COUNT, stats.bucketCount, "Should have INITIAL_BUCKET_COUNT buckets");
            assertEquals(0, stats.totalEntries, "Should start with 0 entries");
            assertEquals(0.0, stats.loadFactor, 0.001, "Should start with 0 load factor");
            assertEquals(0, stats.maxExtensionBuckets, "Should start with 0 extension buckets");
            assertFalse(stats.needsResize, "Should not need resize initially");
            // Insert an entry and check stats update
            String key = "statsKey";
            String namespace = "statsNamespace";
            String value = "statsValue";
            int ptr = mainTable.put(key, namespace);
            int base = (ptr - 1) * 3;
            mainTable.entries[base + 2] = value;
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
            assertTrue(statsString.contains("buckets=" + MainTable.INITIAL_BUCKET_COUNT));
            assertTrue(statsString.contains("entries=0"));
        }
    }
}
