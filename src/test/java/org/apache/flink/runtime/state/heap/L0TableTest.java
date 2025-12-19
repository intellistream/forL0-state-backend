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
        return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
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
            String state = "testValue";

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // Store entry in HeapEntryStore
            long entryAddress = entryStore.allocate(key, namespace, state);
            assertTrue(entryAddress > 0, "Entry should be stored successfully");

            // Put entry into L0Table
            long result = l0Table.put(hash, tag, entryAddress, key, namespace, entryStore);
            assertEquals(0, result, "Should return 0 for new insertion");

            // Get entry from L0Table
            long retrievedAddress = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(entryAddress, retrievedAddress, "Should retrieve the same entry address");

            // Verify entry data
            HeapStateEntry<String, String, String> entry = entryStore.get(retrievedAddress);
            assertNotNull(entry);
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
            short tag = extractTag(hash);

            // Store initial entry
            long entryAddress1 = entryStore.allocate(key, namespace, value1);

            // Insert initial entry
            long result1 = l0Table.put(hash, tag, entryAddress1, key, namespace, entryStore);
            assertEquals(0, result1, "Should return 0 for new insertion");

            // Store updated entry
            long entryAddress2 = entryStore.allocate(key, namespace, value2);

            // Update entry in L0Table
            long result2 = l0Table.put(hash, tag, entryAddress2, key, namespace, entryStore);
            assertEquals(entryAddress1, result2, "Should return previous entry address for update");

            // Verify updated entry
            long retrievedAddress = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(entryAddress2, retrievedAddress, "Should retrieve updated entry address");
            assertEquals(value2, entryStore.get(retrievedAddress).getState());
        }

        @Test
        void testRemove() {
            String key = "removeKey";
            String namespace = "removeNamespace";
            String state = "removeValue";

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // Store and insert entry
            long entryAddress = entryStore.allocate(key, namespace, state);
            l0Table.put(hash, tag, entryAddress, key, namespace, entryStore);

            // Remove entry
            long removedAddress = l0Table.remove(hash, tag, key, namespace, entryStore);
            assertEquals(entryAddress, removedAddress, "Should return removed entry address");

            // Verify entry is removed
            long retrievedAddress = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(0, retrievedAddress, "Entry should not be found after removal");
        }

        @Test
        void testGetNonExistent() {
            String key = "nonExistentKey";
            String namespace = "nonExistentNamespace";
            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            long result = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(0, result, "Should return 0 for non-existent entry");
        }
    }

    @Nested
    class ReplacementPolicyTests {

        @Test
        void testLRUPolicy() {
            try (L0Table<String, String, Integer> lruTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.LRU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    String key = "lruKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    long addr = store.allocate(key, ns, i);
                    entries[i] = new TestEntry(key, ns, addr, hash, tag);
                }

                // Insert first 4 entries (fill bucket)
                for (int i = 0; i < 4; i++) {
                    long result = lruTable.put(entries[i].hash, entries[i].tag,
                            entries[i].addr, entries[i].key, entries[i].namespace, store);
                    assertEquals(0, result, "Should insert successfully");
                }

                // Access entry 1 to make it most recently used
                lruTable.get(entries[1].hash, entries[1].tag, 
                        entries[1].key, entries[1].namespace, store);

                // Insert 5th entry - should evict LRU (entry 0)
                long evictedAddress = lruTable.put(entries[4].hash, entries[4].tag,
                        entries[4].addr, entries[4].key, entries[4].namespace, store);
                assertEquals(entries[0].addr, evictedAddress, "Should evict least recently used entry");

                // Verify entry 0 is gone, but entry 1 is still there
                assertEquals(0, lruTable.get(entries[0].hash, entries[0].tag, 
                        entries[0].key, entries[0].namespace, store));
                assertEquals(entries[1].addr, lruTable.get(entries[1].hash, entries[1].tag, 
                        entries[1].key, entries[1].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testLFUPolicy() {
            try (L0Table<String, String, Integer> lfuTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.LFU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    String key = "lfuKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    long addr = store.allocate(key, ns, i);
                    entries[i] = new TestEntry(key, ns, addr, hash, tag);
                }

                // Insert first 4 entries
                for (int i = 0; i < 4; i++) {
                    lfuTable.put(entries[i].hash, entries[i].tag,
                            entries[i].addr, entries[i].key, entries[i].namespace, store);
                }

                // Access entry 1 multiple times to increase frequency
                for (int j = 0; j < 3; j++) {
                    lfuTable.get(entries[1].hash, entries[1].tag, 
                            entries[1].key, entries[1].namespace, store);
                }

                // Insert 5th entry - should evict LFU
                long evictedAddress = lfuTable.put(entries[4].hash, entries[4].tag,
                        entries[4].addr, entries[4].key, entries[4].namespace, store);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Entry 1 should still be there (highest frequency)
                assertEquals(entries[1].addr, lfuTable.get(entries[1].hash, entries[1].tag, 
                        entries[1].key, entries[1].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testClockPolicy() {
            try (L0Table<String, String, Integer> clockTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.CLOCK)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    String key = "clockKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    long addr = store.allocate(key, ns, i);
                    entries[i] = new TestEntry(key, ns, addr, hash, tag);
                }

                // Insert first 4 entries
                for (int i = 0; i < 4; i++) {
                    clockTable.put(entries[i].hash, entries[i].tag,
                            entries[i].addr, entries[i].key, entries[i].namespace, store);
                }

                // Access entries 1 and 2 to set their accessed bits
                clockTable.get(entries[1].hash, entries[1].tag, 
                        entries[1].key, entries[1].namespace, store);
                clockTable.get(entries[2].hash, entries[2].tag, 
                        entries[2].key, entries[2].namespace, store);

                // Insert 5th entry
                long evictedAddress = clockTable.put(entries[4].hash, entries[4].tag,
                        entries[4].addr, entries[4].key, entries[4].namespace, store);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Verify 5th entry was inserted
                assertEquals(entries[4].addr, clockTable.get(entries[4].hash, entries[4].tag, 
                        entries[4].key, entries[4].namespace, store));

                // At least one accessed entry should remain
                long addr1 = clockTable.get(entries[1].hash, entries[1].tag, 
                        entries[1].key, entries[1].namespace, store);
                long addr2 = clockTable.get(entries[2].hash, entries[2].tag, 
                        entries[2].key, entries[2].namespace, store);
                assertTrue(addr1 > 0 || addr2 > 0, "At least one accessed entry should remain");
                
                store.close();
            }
        }

        @Test
        void testTinyLFUPolicy() {
            try (L0Table<String, String, Integer> tinyLfuTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.TINY_LFU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    String key = "tinylfuKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    long addr = store.allocate(key, ns, i);
                    entries[i] = new TestEntry(key, ns, addr, hash, tag);
                }

                // Insert first 4 entries
                for (int i = 0; i < 4; i++) {
                    tinyLfuTable.put(entries[i].hash, entries[i].tag,
                            entries[i].addr, entries[i].key, entries[i].namespace, store);
                }

                // Access entry 1 multiple times
                for (int j = 0; j < 5; j++) {
                    tinyLfuTable.get(entries[1].hash, entries[1].tag, 
                            entries[1].key, entries[1].namespace, store);
                }

                // Insert 5th entry
                long evictedAddress = tinyLfuTable.put(entries[4].hash, entries[4].tag,
                        entries[4].addr, entries[4].key, entries[4].namespace, store);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // High-frequency entry 1 should still be there
                assertEquals(entries[1].addr, tinyLfuTable.get(entries[1].hash, entries[1].tag, 
                        entries[1].key, entries[1].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testSampledLRUPolicy() {
            try (L0Table<String, String, Integer> sampledLruTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.SAMPLED_LRU)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    String key = "sampledKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    long addr = store.allocate(key, ns, i);
                    entries[i] = new TestEntry(key, ns, addr, hash, tag);
                }

                // Insert first 4 entries
                for (int i = 0; i < 4; i++) {
                    sampledLruTable.put(entries[i].hash, entries[i].tag,
                            entries[i].addr, entries[i].key, entries[i].namespace, store);
                }

                // Access entry 3 to make it recently used
                sampledLruTable.get(entries[3].hash, entries[3].tag, 
                        entries[3].key, entries[3].namespace, store);

                // Insert 5th entry
                long evictedAddress = sampledLruTable.put(entries[4].hash, entries[4].tag,
                        entries[4].addr, entries[4].key, entries[4].namespace, store);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Verify 5th entry was inserted
                assertEquals(entries[4].addr, sampledLruTable.get(entries[4].hash, entries[4].tag, 
                        entries[4].key, entries[4].namespace, store));

                // Verify that we still have 4 valid entries
                int validCount = 0;
                for (int i = 0; i < 5; i++) {
                    long addr = sampledLruTable.get(entries[i].hash, entries[i].tag, 
                            entries[i].key, entries[i].namespace, store);
                    if (addr > 0) validCount++;
                }
                assertEquals(4, validCount, "Should have exactly 4 entries after eviction");
                
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
                    short tag = (short) (0x1000 + i);
                    long addr = store.allocate(key, ns, i);
                    entries[i] = new TestEntry(key, ns, addr, hash, tag);
                    lfuTable.put(entries[i].hash, entries[i].tag,
                            entries[i].addr, entries[i].key, entries[i].namespace, store);
                }

                // Access entry 0 many times to test frequency saturation
                for (int j = 0; j < 20; j++) {
                    lfuTable.get(entries[0].hash, entries[0].tag, 
                            entries[0].key, entries[0].namespace, store);
                }

                // Verify entry is still accessible
                assertEquals(entries[0].addr, lfuTable.get(entries[0].hash, entries[0].tag, 
                        entries[0].key, entries[0].namespace, store));
                
                store.close();
            }
        }

        @Test
        void testClockSecondChance() {
            try (L0Table<String, String, Integer> clockTable = 
                    new L0Table<>(l0Allocator, 2, L0Table.ReplacementPolicy.CLOCK)) {
                
                HeapEntryStore<String, String, Integer> store = new HeapEntryStore<>();
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    String key = "secondKey" + i;
                    String ns = "ns";
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    long addr = store.allocate(key, ns, i);
                    entries[i] = new TestEntry(key, ns, addr, hash, tag);
                }

                // Insert 4 entries
                for (int i = 0; i < 4; i++) {
                    clockTable.put(entries[i].hash, entries[i].tag,
                            entries[i].addr, entries[i].key, entries[i].namespace, store);
                }

                // Access all entries to set accessed bits
                for (int i = 0; i < 4; i++) {
                    clockTable.get(entries[i].hash, entries[i].tag, 
                            entries[i].key, entries[i].namespace, store);
                }

                // Insert 5th entry
                long evictedAddress = clockTable.put(entries[4].hash, entries[4].tag,
                        entries[4].addr, entries[4].key, entries[4].namespace, store);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // New entry should be inserted
                assertEquals(entries[4].addr, clockTable.get(entries[4].hash, entries[4].tag, 
                        entries[4].key, entries[4].namespace, store));
                
                store.close();
            }
        }
    }

    @Nested
    class CacheManagementTests {

        @Test
        void testRemoveByAddress() {
            String key1 = "removeAddr1";
            String key2 = "removeAddr2";
            String ns = "ns";
            
            long addr1 = entryStore.allocate(key1, ns, "value1");
            long addr2 = entryStore.allocate(key2, ns, "value2");
            
            int hash1 = compositeHash(key1, ns);
            int hash2 = compositeHash(key2, ns);
            short tag1 = extractTag(hash1);
            short tag2 = extractTag(hash2);

            l0Table.put(hash1, tag1, addr1, key1, ns, entryStore);
            l0Table.put(hash2, tag2, addr2, key2, ns, entryStore);

            // Remove by address
            l0Table.removeByAddress(addr1);

            // Verify entry1 is removed, entry2 remains
            assertEquals(0, l0Table.get(hash1, tag1, key1, ns, entryStore));
            assertEquals(addr2, l0Table.get(hash2, tag2, key2, ns, entryStore));
        }

        @Test
        void testInvalidateRange() {
            String key1 = "range1";
            String key2 = "range2";
            String key3 = "range3";
            String ns = "ns";
            
            long addr1 = entryStore.allocate(key1, ns, "value1");
            long addr2 = entryStore.allocate(key2, ns, "value2");
            long addr3 = entryStore.allocate(key3, ns, "value3");
            
            int hash1 = compositeHash(key1, ns);
            int hash2 = compositeHash(key2, ns);
            int hash3 = compositeHash(key3, ns);
            short tag1 = extractTag(hash1);
            short tag2 = extractTag(hash2);
            short tag3 = extractTag(hash3);

            l0Table.put(hash1, tag1, addr1, key1, ns, entryStore);
            l0Table.put(hash2, tag2, addr2, key2, ns, entryStore);
            l0Table.put(hash3, tag3, addr3, key3, ns, entryStore);

            // Invalidate range that includes entry1 and entry2
            long minAddr = Math.min(addr1, addr2);
            long maxAddr = Math.max(addr1, addr2) + 1;

            l0Table.invalidateRange(minAddr, maxAddr);

            // Verify affected entries are invalidated
            assertEquals(0, l0Table.get(hash1, tag1, key1, ns, entryStore));
            assertEquals(0, l0Table.get(hash2, tag2, key2, ns, entryStore));

            // Entry3 might still be there if outside range
            if (addr3 < minAddr || addr3 >= maxAddr) {
                assertEquals(addr3, l0Table.get(hash3, tag3, key3, ns, entryStore));
            }
        }

        @Test
        void testClear() {
            String key1 = "clear1";
            String key2 = "clear2";
            String ns = "ns";
            
            long addr1 = entryStore.allocate(key1, ns, "value1");
            long addr2 = entryStore.allocate(key2, ns, "value2");
            
            int hash1 = compositeHash(key1, ns);
            int hash2 = compositeHash(key2, ns);
            short tag1 = extractTag(hash1);
            short tag2 = extractTag(hash2);

            l0Table.put(hash1, tag1, addr1, key1, ns, entryStore);
            l0Table.put(hash2, tag2, addr2, key2, ns, entryStore);

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
            assertEquals(0, l0Table.get(hash1, tag1, key1, ns, entryStore));
            assertEquals(0, l0Table.get(hash2, tag2, key2, ns, entryStore));
        }
    }

    @Nested
    class StatisticsTests {

        @Test
        void testHitMissStatistics() {
            String key = "statsKey";
            String ns = "ns";
            int hash = compositeHash(key, ns);
            short tag = extractTag(hash);

            // Miss: get non-existent entry
            l0Table.get(hash, tag, key, ns, entryStore);

            // Hit: insert then get
            long addr = entryStore.allocate(key, ns, "value");
            l0Table.put(hash, tag, addr, key, ns, entryStore);
            l0Table.get(hash, tag, key, ns, entryStore);

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
            
            long addr1 = entryStore.allocate(key1, ns, "value1");
            long addr2 = entryStore.allocate(key2, ns, "value2");
            
            int hash1 = compositeHash(key1, ns);
            int hash2 = compositeHash(key2, ns);
            short tag1 = extractTag(hash1);
            short tag2 = extractTag(hash2);

            l0Table.put(hash1, tag1, addr1, key1, ns, entryStore);
            l0Table.put(hash2, tag2, addr2, key2, ns, entryStore);

            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(2, stats.validSlots, "Should have 2 valid slots");
            assertEquals(16, stats.totalSlots, "Should have 16 total slots (4 buckets × 4 slots)");
            assertEquals(2.0 / 16.0, stats.loadFactor, 0.001, "Load factor should be 2/16");
        }

        @Test
        void testEvictionCount() {
            HeapEntryStore<String, String, String> store = new HeapEntryStore<>();
            TestEntry[] entries = new TestEntry[5];

            for (int i = 0; i < 5; i++) {
                String key = "evict" + i;
                String ns = "ns";
                int hash = 0x12340000; // Force same bucket
                short tag = (short) (0x1000 + i);
                long addr = store.allocate(key, ns, "value" + i);
                entries[i] = new TestEntry(key, ns, addr, hash, tag);
            }

            // Insert first 4 (no eviction)
            for (int i = 0; i < 4; i++) {
                l0Table.put(entries[i].hash, entries[i].tag, entries[i].addr,
                        entries[i].key, entries[i].namespace, store);
            }

            // Insert 5th (should cause eviction)
            l0Table.put(entries[4].hash, entries[4].tag, entries[4].addr,
                    entries[4].key, entries[4].namespace, store);

            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(1, stats.evictionCount, "Should have 1 eviction");
            
            store.close();
        }

        @Test
        void testStatsToString() {
            String key = "toStringKey";
            String ns = "ns";
            long addr = entryStore.allocate(key, ns, "value");
            int hash = compositeHash(key, ns);
            short tag = extractTag(hash);
            
            l0Table.put(hash, tag, addr, key, ns, entryStore);

            L0Table.L0TableStats stats = l0Table.getStats();
            String statsString = stats.toString();

            assertNotNull(statsString);
            assertTrue(statsString.contains("L0Table"));
            assertTrue(statsString.contains("buckets=4"));
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

            // Force same hash and tag
            int sameHash = 0x12345678;
            short sameTag = (short) 0x1234;

            long addr1 = entryStore.allocate(key1, namespace, value1);
            long addr2 = entryStore.allocate(key2, namespace, value2);

            // Insert both entries
            long result1 = l0Table.put(sameHash, sameTag, addr1, key1, namespace, entryStore);
            long result2 = l0Table.put(sameHash, sameTag, addr2, key2, namespace, entryStore);

            assertEquals(0, result1, "First entry should insert successfully");
            assertEquals(0, result2, "Second entry should insert successfully");

            // Verify both can be retrieved correctly
            assertEquals(addr1, l0Table.get(sameHash, sameTag, key1, namespace, entryStore));
            assertEquals(addr2, l0Table.get(sameHash, sameTag, key2, namespace, entryStore));
        }

        @Test
        void testMultipleUpdates() {
            String key = "updateKey";
            String ns = "ns";
            int hash = compositeHash(key, ns);
            short tag = extractTag(hash);

            // Insert initial entry
            long currentAddr = entryStore.allocate(key, ns, "initialValue");
            l0Table.put(hash, tag, currentAddr, key, ns, entryStore);

            // Update multiple times
            for (int i = 1; i <= 3; i++) {
                String newValue = "updatedValue" + i;
                long newAddr = entryStore.allocate(key, ns, newValue);

                long oldAddress = l0Table.put(hash, tag, newAddr, key, ns, entryStore);
                assertEquals(currentAddr, oldAddress, "Should return previous address for update");

                // Verify updated value
                long retrievedAddress = l0Table.get(hash, tag, key, ns, entryStore);
                assertEquals(newAddr, retrievedAddress, "Should get updated entry");
                HeapStateEntry<String, String, String> entry = entryStore.get(retrievedAddress);
                assertNotNull(entry);
                assertEquals(newValue, entry.getState());

                currentAddr = newAddr; // Update for next iteration
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClose() throws Exception {
            // Insert some entries
            String key = "closeKey";
            String ns = "ns";
            long addr = entryStore.allocate(key, ns, "closeValue");
            int hash = compositeHash(key, ns);
            short tag = extractTag(hash);
            
            l0Table.put(hash, tag, addr, key, ns, entryStore);

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
        final long addr;
        final int hash;
        final short tag;

        TestEntry(String key, String namespace, long addr, int hash, short tag) {
            this.key = key;
            this.namespace = namespace;
            this.addr = addr;
            this.hash = hash;
            this.tag = tag;
        }
    }
}
