package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.NativeL0MemoryAllocator;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.apache.flink.util.MathUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for L0Table implementation with HeapEntryStore.
 * Tests the cache functionality including get/put operations,
 * replacement policies, and cache statistics using object comparison.
 */
class L0TableTest {

    private L0MemoryAllocator l0Allocator;
    private HeapEntryStore<String, String, String> entryStore;
    private L0Table<String, String, String> l0Table;

    @BeforeEach
    void setUp() {
        l0Allocator = new NativeL0MemoryAllocator();
        entryStore = new HeapEntryStore<>();
        // Create L0Table with 4 buckets (2^2) and 4 slots per bucket = 16 total slots
        l0Table = new L0Table<>(l0Allocator, 2);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (l0Table != null) {
            l0Table.close();
        }
        if (entryStore != null) {
            entryStore.close();
        }
        if (l0Allocator != null && !l0Allocator.isClosed()) {
            l0Allocator.close();
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
            String state = "testValue";

            int hash = compositeHash(key, namespace);

            // Store entry in HeapEntryStore
            long entryAddress = entryStore.allocate(key, namespace, state, hash);
            assertTrue(entryAddress > 0, "Entry should be stored successfully");

            // Put entry into L0Table
            int result = l0Table.put(hash, (int) entryAddress);
            assertEquals(0, result, "Should return 0 for new insertion");

            // Get entry from L0Table
            HeapStateEntry<String, String, String> entry = l0Table.get(hash, key, namespace, entryStore);
            assertNotNull(entry, "Should retrieve entry");

            // Verify entry data
            assertEquals(key, entry.getKey());
            assertEquals(namespace, entry.getNamespace());
            assertEquals(state, entry.getState());
        }

        @Test
        void testUpdate() {
            String key = "updateKey";
            String namespace = "updateNamespace";
            String value1 = "initialValue";
            String value2 = "updatedValue";

            int hash = compositeHash(key, namespace);

            // Store initial entry
            long entryAddress1 = entryStore.allocate(key, namespace, value1, hash);

            // Insert initial entry
            int result1 = l0Table.put(hash, (int) entryAddress1);
            assertEquals(0, result1, "Should return 0 for new insertion");

            // Note: In production, put() is only called for entries NOT in L0.
            // This test verifies that put() finds empty slots correctly.
            // A second put with same key would create duplicate entries in L0
            // (which won't happen in production due to MainTable logic).

            // Verify entry is retrievable
            HeapStateEntry<String, String, String> entry = l0Table.get(hash, key, namespace, entryStore);
            assertNotNull(entry);
            assertEquals(value1, entry.getState());
        }

        @Test
        void testRemove() {
            String key = "removeKey";
            String namespace = "removeNamespace";
            String state = "removeValue";

            int hash = compositeHash(key, namespace);

            // Store and insert entry
            int entryAddress = (int) entryStore.allocate(key, namespace, state, hash);
            l0Table.put(hash, entryAddress);

            // Remove entry
            int removedAddress = l0Table.remove(hash, entryAddress);
            assertEquals(entryAddress, removedAddress, "Should return removed entry address");

            // Verify entry is removed
            HeapStateEntry<String, String, String> entry = l0Table.get(hash, key, namespace, entryStore);
            assertNull(entry, "Entry should not be found after removal");
        }

        @Test
        void testGetNonExistent() {
            String key = "nonExistentKey";
            String namespace = "nonExistentNamespace";
            int hash = compositeHash(key, namespace);

            HeapStateEntry<String, String, String> result = l0Table.get(hash, key, namespace, entryStore);
            assertNull(result, "Should return null for non-existent entry");
        }
    }

    @Nested
    class ReplacementPolicyTests {

        @Test
        void testLRUPolicy() {
            try (L0Table<String, String, Integer> lruTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.LRU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[8];

                for (int i = 0; i < 8; i++) {
                    String key = "lruKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    
                    long addr = store.allocate(key, ns, i, hash);
                    entries[i] = new TestEntry(key, ns, (int) addr, hash);
                }

                // Insert first 7 entries (fill bucket with 7 slots)
                for (int i = 0; i < 7; i++) {
                    long result = lruTable.put(entries[i].hash, entries[i].addr);
                    assertEquals(0, result, "Should insert successfully");
                }

                // Access entry 1 to make it most recently used
                lruTable.get(entries[1].hash, 
                        entries[1].key, entries[1].namespace, store);

                // Insert 8th entry - should evict LRU (entry 0)
                long evictedAddress = lruTable.put(entries[7].hash, entries[7].addr);
                assertEquals(entries[0].addr, evictedAddress, "Should evict least recently used entry");

                // Verify entry 0 is gone, but entry 1 is still there
                assertNull(lruTable.get(entries[0].hash, 
                        entries[0].key, entries[0].namespace, store));
                assertNotNull(lruTable.get(entries[1].hash, 
                        entries[1].key, entries[1].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testLFUPolicy() {
            try (L0Table<String, String, Integer> lfuTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.LFU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[8];

                for (int i = 0; i < 8; i++) {
                    String key = "lfuKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    
                    long addr = store.allocate(key, ns, i, hash);
                    entries[i] = new TestEntry(key, ns, (int) addr, hash);
                }

                // Insert first 7 entries (fill bucket with 7 slots)
                for (int i = 0; i < 7; i++) {
                    lfuTable.put(entries[i].hash, entries[i].addr);
                }

                // Access entry 1 multiple times to increase frequency
                for (int j = 0; j < 3; j++) {
                    lfuTable.get(entries[1].hash, 
                            entries[1].key, entries[1].namespace, store);
                }

                // Insert 8th entry - should evict LFU
                long evictedAddress = lfuTable.put(entries[7].hash, entries[7].addr);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Entry 1 should still be there (highest frequency)
                assertNotNull(lfuTable.get(entries[1].hash, 
                        entries[1].key, entries[1].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testClockPolicy() {
            try (L0Table<String, String, Integer> clockTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.CLOCK)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[8];

                for (int i = 0; i < 8; i++) {
                    String key = "clockKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    
                    long addr = store.allocate(key, ns, i, hash);
                    entries[i] = new TestEntry(key, ns, (int) addr, hash);
                }

                // Insert first 7 entries (fill bucket with 7 slots)
                for (int i = 0; i < 7; i++) {
                    clockTable.put(entries[i].hash, entries[i].addr);
                }

                // Access entries 1 and 2 to set their accessed bits
                clockTable.get(entries[1].hash, 
                        entries[1].key, entries[1].namespace, store);
                clockTable.get(entries[2].hash, 
                        entries[2].key, entries[2].namespace, store);

                // Insert 8th entry
                long evictedAddress = clockTable.put(entries[7].hash, entries[7].addr);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Verify 8th entry was inserted
                assertNotNull(clockTable.get(entries[7].hash, 
                        entries[7].key, entries[7].namespace, store));

                // At least one accessed entry should remain
                HeapStateEntry<String, String, Integer> e1 = clockTable.get(entries[1].hash, 
                        entries[1].key, entries[1].namespace, store);
                HeapStateEntry<String, String, Integer> e2 = clockTable.get(entries[2].hash, 
                        entries[2].key, entries[2].namespace, store);
                assertTrue(e1 != null || e2 != null, "At least one accessed entry should remain");
                
                store.close();
            }
        }

        @Test
        void testTinyLFUPolicy() {
            try (L0Table<String, String, Integer> tinyLfuTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.TINY_LFU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[8];

                for (int i = 0; i < 8; i++) {
                    String key = "tinylfuKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    
                    long addr = store.allocate(key, ns, i, hash);
                    entries[i] = new TestEntry(key, ns, (int) addr, hash);
                }

                // Insert first 7 entries (fill bucket with 7 slots)
                for (int i = 0; i < 7; i++) {
                    tinyLfuTable.put(entries[i].hash, entries[i].addr);
                }

                // Access entry 1 multiple times
                for (int j = 0; j < 5; j++) {
                    tinyLfuTable.get(entries[1].hash, 
                            entries[1].key, entries[1].namespace, store);
                }

                // Insert 8th entry
                long evictedAddress = tinyLfuTable.put(entries[7].hash, entries[7].addr);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // High-frequency entry 1 should still be there
                assertNotNull(tinyLfuTable.get(entries[1].hash, 
                        entries[1].key, entries[1].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testSampledLRUPolicy() {
            try (L0Table<String, String, Integer> sampledLruTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.SAMPLED_LRU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[8];

                for (int i = 0; i < 8; i++) {
                    String key = "sampledKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    
                    long addr = store.allocate(key, ns, i, hash);
                    entries[i] = new TestEntry(key, ns, (int) addr, hash);
                }

                // Insert first 7 entries (fill bucket with 7 slots)
                for (int i = 0; i < 7; i++) {
                    sampledLruTable.put(entries[i].hash, entries[i].addr);
                }

                // Access entry 6 to make it recently used
                sampledLruTable.get(entries[6].hash, 
                        entries[6].key, entries[6].namespace, store);

                // Insert 8th entry
                long evictedAddress = sampledLruTable.put(entries[7].hash, entries[7].addr);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Verify 8th entry was inserted
                assertNotNull(sampledLruTable.get(entries[7].hash, 
                        entries[7].key, entries[7].namespace, store));

                // Verify that we still have 7 valid entries
                int validCount = 0;
                for (int i = 0; i < 8; i++) {
                    HeapStateEntry<String, String, Integer> e = sampledLruTable.get(entries[i].hash, 
                            entries[i].key, entries[i].namespace, store);
                    if (e != null) validCount++;
                }
                assertEquals(7, validCount, "Should have exactly 7 entries after eviction");
                
                store.close();
            }
        }

        @Test
        void testLFUFrequencySaturation() {
            try (L0Table<String, String, Integer> lfuTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.LFU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[2];

                for (int i = 0; i < 2; i++) {
                    String key = "satKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    
                    long addr = store.allocate(key, ns, i, hash);
                    entries[i] = new TestEntry(key, ns, (int) addr, hash);
                    lfuTable.put(entries[i].hash, entries[i].addr);
                }

                // Access entry 0 many times to test frequency saturation
                for (int j = 0; j < 20; j++) {
                    lfuTable.get(entries[0].hash, 
                            entries[0].key, entries[0].namespace, store);
                }

                // Verify entry is still accessible
                assertNotNull(lfuTable.get(entries[0].hash, 
                        entries[0].key, entries[0].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testClockSecondChance() {
            try (L0Table<String, String, Integer> clockTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.CLOCK)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[8];

                for (int i = 0; i < 8; i++) {
                    String key = "secondKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    
                    long addr = store.allocate(key, ns, i, hash);
                    entries[i] = new TestEntry(key, ns, (int) addr, hash);
                }

                // Insert 7 entries (fill bucket with 7 slots)
                for (int i = 0; i < 7; i++) {
                    clockTable.put(entries[i].hash, entries[i].addr);
                }

                // Access all entries to set accessed bits
                for (int i = 0; i < 7; i++) {
                    clockTable.get(entries[i].hash, entries[i].key, entries[i].namespace, store);
                }

                // Insert 8th entry
                long evictedAddress = clockTable.put(entries[7].hash, entries[7].addr);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // New entry should be inserted
                assertNotNull(clockTable.get(entries[7].hash, 
                        entries[7].key, entries[7].namespace, store));
                
                store.close();
            }
        }
    }

    @Nested
    class CacheManagementTests {

        @Test
        void testClear() {
            String key1 = "clear1";
            String key2 = "clear2";
            String ns = "ns";
            
            int hash1 = compositeHash(key1, ns);
            int hash2 = compositeHash(key2, ns);
            
            long addr1 = entryStore.allocate(key1, ns, "value1", hash1);
            long addr2 = entryStore.allocate(key2, ns, "value2", hash2);

            l0Table.put(hash1, (int) addr1);
            l0Table.put(hash2, (int) addr2);

            // Clear all entries
            l0Table.clear();

            // Verify stats are reset
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(0, stats.validSlots);
            assertEquals(0, stats.accessCount);
            assertEquals(0, stats.hitCount);
            assertEquals(0, stats.missCount);
            assertEquals(0, stats.evictionCount);

            // Verify all entries are removed
            assertNull(l0Table.get(hash1, key1, ns, entryStore));
            assertNull(l0Table.get(hash2, key2, ns, entryStore));
        }
    }

    @Nested
    class StatisticsTests {

        @Test
        void testHitMissStatistics() {
            String key = "statsKey";
            String ns = "ns";
            int hash = compositeHash(key, ns);

            // Miss: get non-existent entry
            l0Table.get(hash, key, ns, entryStore);

            // Hit: insert then get
            long addr = entryStore.allocate(key, ns, "value", hash);
            l0Table.put(hash, (int) addr);
            l0Table.get(hash, key, ns, entryStore);

            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(2, stats.accessCount, "Should have 2 total accesses");
            assertEquals(1, stats.hitCount, "Should have 1 hit");
            assertEquals(1, stats.missCount, "Should have 1 miss");
            assertEquals(0.5, stats.hitRate, 0.001, "Hit rate should be 50%");
        }

        @Test
        void testLoadFactorCalculation() {
            L0Table.L0TableStats initialStats = l0Table.getStats();
            assertEquals(0.0, initialStats.loadFactor, 0.001, "Initial load factor should be 0");

            // Insert entries to increase load factor
            String key1 = "load1";
            String key2 = "load2";
            String ns = "ns";
            
            int hash1 = compositeHash(key1, ns);
            int hash2 = compositeHash(key2, ns);
            
            long addr1 = entryStore.allocate(key1, ns, "value1", hash1);
            long addr2 = entryStore.allocate(key2, ns, "value2", hash2);

            l0Table.put(hash1, (int) addr1);
            l0Table.put(hash2, (int) addr2);

            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(2, stats.validSlots, "Should have 2 valid slots");
            assertEquals(28, stats.totalSlots, "Should have 28 total slots (4 buckets × 7 slots)");
            assertEquals(2.0 / 28.0, stats.loadFactor, 0.001, "Load factor should be 2/28");
        }

        @Test
        void testEvictionCount() {
            HeapEntryStore<String, String, String> store = new HeapEntryStore<>();
            TestEntry[] entries = new TestEntry[8];

            for (int i = 0; i < 8; i++) {
                String key = "evict" + i;
                String ns = "ns";
                int hash = 0x12340000; // Force same bucket
                
                long addr = store.allocate(key, ns, "value" + i, hash);
                entries[i] = new TestEntry(key, ns, (int) addr, hash);
            }

            // Insert first 7 (no eviction) - fill bucket with 7 slots
            for (int i = 0; i < 7; i++) {
                l0Table.put(entries[i].hash, entries[i].addr);
            }

            // Insert 8th (should cause eviction)
            l0Table.put(entries[7].hash, entries[7].addr);

            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(1, stats.evictionCount, "Should have 1 eviction");
            
            store.close();
        }

        @Test
        void testStatsToString() {
            String key = "toStringKey";
            String ns = "ns";
            int hash = compositeHash(key, ns);
            long addr = entryStore.allocate(key, ns, "value", hash);
            
            l0Table.put(hash, (int) addr);

            L0Table.L0TableStats stats = l0Table.getStats();
            String statsString = stats.toString();

            assertNotNull(statsString);
            assertTrue(statsString.contains("L0TableStats"), "Should contain L0TableStats");
            assertTrue(statsString.contains("totalSlots=28"), "Should contain totalSlots=28 (4 buckets × 7 slots)");
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testEmptyTable() {
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(0, stats.validSlots);
            assertEquals(0.0, stats.loadFactor, 0.001);
            assertEquals(0.0, stats.hitRate, 0.001);
        }

        @Test
        void testTagCollisions() {
            // Create entries with same tag but different content
            String key1 = "tagCollision1";
            String key2 = "tagCollision2";
            String namespace = "collisionNamespace";
            String value1 = "value1";
            String value2 = "value2";

            // Force same hash
            int sameHash = 0x12345678;

            long addr1 = entryStore.allocate(key1, namespace, value1, sameHash);
            long addr2 = entryStore.allocate(key2, namespace, value2, sameHash);

            // Insert both entries
            int result1 = l0Table.put(sameHash, (int) addr1);
            int result2 = l0Table.put(sameHash, (int) addr2);

            assertEquals(0, result1, "First entry should insert successfully");
            assertEquals(0, result2, "Second entry should insert successfully");

            // Verify both can be retrieved correctly
            assertNotNull(l0Table.get(sameHash, key1, namespace, entryStore));
            assertNotNull(l0Table.get(sameHash, key2, namespace, entryStore));
        }

        @Test
        void testMultipleUpdates() {
            // Note: In the new design, L0Table.put() does NOT check for duplicates
            // because it's only called when the entry is guaranteed NOT in L0.
            // This test now verifies that insertion and eviction work correctly.
            
            String key = "updateKey";
            String ns = "ns";
            int hash = compositeHash(key, ns);

            // Insert initial entry
            long addr = entryStore.allocate(key, ns, "initialValue", hash);
            int result = l0Table.put(hash, (int) addr);
            assertEquals(0, result, "First insert should return 0");

            // Verify entry is retrievable
            HeapStateEntry<String, String, String> entry = l0Table.get(hash, key, ns, entryStore);
            assertNotNull(entry, "Should get entry after insert");
            assertEquals("initialValue", entry.getState());
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClose() throws Exception {
            // Insert some entries
            String key = "closeKey";
            String ns = "ns";
            int hash = compositeHash(key, ns);
            long addr = entryStore.allocate(key, ns, "closeValue", hash);
            
            l0Table.put(hash, (int) addr);

            // Close should not throw exception
            assertDoesNotThrow(() -> l0Table.close());
        }

        @Test
        void testMultipleClose() throws Exception {
            // Multiple close calls should be safe
            l0Table.close();
            assertDoesNotThrow(() -> l0Table.close());
        }
    }

    // Test entry helper class
    private static class TestEntry {
        final String key;
        final String namespace;
        final int addr;  // Changed from long to int
        final int hash;

        TestEntry(String key, String namespace, int addr, int hash) {
            this.key = key;
            this.namespace = namespace;
            this.addr = addr;
            this.hash = hash;
        }
    }
}
