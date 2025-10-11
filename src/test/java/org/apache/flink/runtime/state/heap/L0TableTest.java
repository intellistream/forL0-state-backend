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
 * Test class for L0Table implementation.
 * Tests the cache functionality including get/put operations,
 * replacement policies, and cache statistics.
 */
class L0TableTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB for tests

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryArena entryArena;
    private L0Table l0Table;
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

        // Create L0Table with 4 buckets (2^2) and 4 slots per bucket = 16 total slots
        l0Table = new L0Table(allocator, 2);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (l0Table != null) {
            l0Table.close();
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

            // Calculate hash and tag
            int hash = HashFunctions.compositeHash(key, namespace);
            short tag = (short) (hash & 0xFFFF);
            
            // Store entry in EntryArena
            long entryAddress = entryArena.putEntry(hash, key, namespace, value);
            assertTrue(entryAddress > 0, "Entry should be stored successfully");

            // Put entry into L0Table using new inline method
            long result = l0Table.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryArena);
            assertEquals(0, result, "Should return 0 for new insertion");

            // Get entry from L0Table using new inline method
            long retrievedAddress = l0Table.get(hash, tag, key, key.length, namespace, namespace.length, entryArena);
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

            // Calculate hash and tag
            int hash = HashFunctions.compositeHash(key, namespace);
            short tag = (short) (hash & 0xFFFF);

            // Store initial entry
            long entryAddress1 = entryArena.putEntry(hash, key, namespace, value1);

            // Insert initial entry
            long result1 = l0Table.put(hash, tag, entryAddress1, key, key.length, namespace, namespace.length, entryArena);
            assertEquals(0, result1, "Should return 0 for new insertion");

            // Store updated entry
            long entryAddress2 = entryArena.putEntry(hash, key, namespace, value2);

            // Update entry in L0Table
            long result2 = l0Table.put(hash, tag, entryAddress2, key, key.length, namespace, namespace.length, entryArena);
            assertEquals(entryAddress1, result2, "Should return previous entry address for update");

            // Verify updated entry
            long retrievedAddress = l0Table.get(hash, tag, key, key.length, namespace, namespace.length, entryArena);
            assertEquals(entryAddress2, retrievedAddress, "Should retrieve updated entry address");
            assertArrayEquals(value2, entryArena.getValueBytes(retrievedAddress));
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
            long entryAddress = entryArena.putEntry(hash, key, namespace, value);

            l0Table.put(hash, tag, entryAddress, key, key.length, namespace, namespace.length, entryArena);

            // Remove entry
            long removedAddress = l0Table.remove(hash, tag, key, key.length, namespace, namespace.length, entryArena);
            assertEquals(entryAddress, removedAddress, "Should return removed entry address");

            // Verify entry is removed
            long retrievedAddress = l0Table.get(hash, tag, key, key.length, namespace, namespace.length, entryArena);
            assertEquals(0, retrievedAddress, "Entry should not be found after removal");
        }

        @Test
        void testGetNonExistent() {
            byte[] key = "nonExistentKey".getBytes();
            byte[] namespace = "nonExistentNamespace".getBytes();
            int hash = HashFunctions.compositeHash(key, namespace);
            short tag = (short) (hash & 0xFFFF);

            long result = l0Table.get(hash, tag, key, key.length, namespace, namespace.length, entryArena);
            assertEquals(0, result, "Should return 0 for non-existent entry");
        }
    }

    @Nested
    class ReplacementPolicyTests {

        @Test
        void testLRUPolicy() {
            try (L0Table lruTable = new L0Table(allocator, 2, L0Table.ReplacementPolicy.LRU)) {
                // Fill all 4 slots in bucket 0 (force same bucket using hash & 3 = 0)
                TestEntry[] entries = new TestEntry[5];  // 5 entries, 4 slots

                for (int i = 0; i < 5; i++) {
                    entries[i] = createTestEntry("lruKey" + i, "lruValue" + i);
                    int hash = 0x12340000 | (i << 2); // All map to bucket 0
                    short tag = (short) (0x1000 + i);
                    entries[i].hash = hash;
                    entries[i].tag = tag;
                }

                // Insert first 4 entries (fill bucket)
                for (int i = 0; i < 4; i++) {
                    long result = lruTable.put(entries[i].hash, entries[i].tag,
                                             entries[i].entryAddress, entries[i].key, entries[i].key.length,
                                             entries[i].namespace, entries[i].namespace.length, entryArena);
                    assertEquals(0, result, "Should insert successfully");
                }

                // Access entry 1 to make it most recently used
                lruTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                           entries[1].namespace, entries[1].namespace.length, entryArena);

                // Insert 5th entry - should evict LRU (entry 0, since entry 1 was accessed)
                long evictedAddress = lruTable.put(entries[4].hash, entries[4].tag,
                                                 entries[4].entryAddress, entries[4].key, entries[4].key.length,
                                                 entries[4].namespace, entries[4].namespace.length, entryArena);
                assertEquals(entries[0].entryAddress, evictedAddress, "Should evict least recently used entry");

                // Verify entry 0 is gone, but entry 1 is still there
                assertEquals(0, lruTable.get(entries[0].hash, entries[0].tag, entries[0].key, entries[0].key.length,
                                           entries[0].namespace, entries[0].namespace.length, entryArena));
                assertEquals(entries[1].entryAddress,
                           lruTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                                      entries[1].namespace, entries[1].namespace.length, entryArena));
            }
        }

        @Test
        void testLFUPolicy() {
            try (L0Table lfuTable = new L0Table(allocator, 2, L0Table.ReplacementPolicy.LFU)) {
                // Create test entries
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    entries[i] = createTestEntry("lfuKey" + i, "lfuValue" + i);
                    int hash = 0x12340000 | (i << 2); // All map to bucket 0
                    short tag = (short) (0x1000 + i);
                    entries[i].hash = hash;
                    entries[i].tag = tag;
                }

                // Insert first 4 entries
                for (int i = 0; i < 4; i++) {
                    lfuTable.put(entries[i].hash, entries[i].tag,
                               entries[i].entryAddress, entries[i].key, entries[i].key.length,
                               entries[i].namespace, entries[i].namespace.length, entryArena);
                }

                // Access entry 1 multiple times to increase frequency
                for (int j = 0; j < 3; j++) {
                    lfuTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                               entries[1].namespace, entries[1].namespace.length, entryArena);
                }

                // Insert 5th entry - should evict LFU (one of the entries accessed only once)
                long evictedAddress = lfuTable.put(entries[4].hash, entries[4].tag,
                                                 entries[4].entryAddress, entries[4].key, entries[4].key.length,
                                                 entries[4].namespace, entries[4].namespace.length, entryArena);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Entry 1 should still be there (highest frequency)
                assertEquals(entries[1].entryAddress,
                           lfuTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                                      entries[1].namespace, entries[1].namespace.length, entryArena));
            }
        }

        @Test
        void testClockPolicy() {
            try (L0Table clockTable = new L0Table(allocator, 2, L0Table.ReplacementPolicy.CLOCK)) {
                // Create test entries
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    entries[i] = createTestEntry("clockKey" + i, "clockValue" + i);
                    int hash = 0x12340000 | (i << 2); // All map to bucket 0
                    short tag = (short) (0x1000 + i);
                    entries[i].hash = hash;
                    entries[i].tag = tag;
                }

                // Insert first 4 entries
                for (int i = 0; i < 4; i++) {
                    clockTable.put(entries[i].hash, entries[i].tag,
                                 entries[i].entryAddress, entries[i].key, entries[i].key.length,
                                 entries[i].namespace, entries[i].namespace.length, entryArena);
                }

                // Access entries 1 and 2 to set their accessed bits
                clockTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                             entries[1].namespace, entries[1].namespace.length, entryArena);
                clockTable.get(entries[2].hash, entries[2].tag, entries[2].key, entries[2].key.length,
                             entries[2].namespace, entries[2].namespace.length, entryArena);

                // Insert 5th entry - CLOCK should evict an entry without accessed bit
                // (likely entry 0 or 3, depending on clock hand position)
                long evictedAddress = clockTable.put(entries[4].hash, entries[4].tag,
                                                   entries[4].entryAddress, entries[4].key, entries[4].key.length,
                                                   entries[4].namespace, entries[4].namespace.length, entryArena);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Verify 5th entry was inserted
                assertEquals(entries[4].entryAddress,
                           clockTable.get(entries[4].hash, entries[4].tag, entries[4].key, entries[4].key.length,
                                        entries[4].namespace, entries[4].namespace.length, entryArena));

                // Accessed entries should still be present (1 and 2)
                long addr1 = clockTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                                          entries[1].namespace, entries[1].namespace.length, entryArena);
                long addr2 = clockTable.get(entries[2].hash, entries[2].tag, entries[2].key, entries[2].key.length,
                                          entries[2].namespace, entries[2].namespace.length, entryArena);
                assertTrue(addr1 > 0 || addr2 > 0, "At least one accessed entry should remain");
            }
        }

        @Test
        void testTinyLFUPolicy() {
            try (L0Table tinyLfuTable = new L0Table(allocator, 2, L0Table.ReplacementPolicy.TINY_LFU)) {
                // Create test entries
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    entries[i] = createTestEntry("tinylfuKey" + i, "tinylfuValue" + i);
                    int hash = 0x12340000 | (i << 2); // All map to bucket 0
                    short tag = (short) (0x1000 + i);
                    entries[i].hash = hash;
                    entries[i].tag = tag;
                }

                // Insert first 4 entries
                for (int i = 0; i < 4; i++) {
                    tinyLfuTable.put(entries[i].hash, entries[i].tag,
                                   entries[i].entryAddress, entries[i].key, entries[i].key.length,
                                   entries[i].namespace, entries[i].namespace.length, entryArena);
                }

                // Access entry 1 multiple times to build up frequency
                for (int j = 0; j < 5; j++) {
                    tinyLfuTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                                   entries[1].namespace, entries[1].namespace.length, entryArena);
                }

                // Insert 5th entry - TinyLFU should evict lowest frequency entry
                long evictedAddress = tinyLfuTable.put(entries[4].hash, entries[4].tag,
                                                     entries[4].entryAddress, entries[4].key, entries[4].key.length,
                                                     entries[4].namespace, entries[4].namespace.length, entryArena);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // High-frequency entry 1 should still be there
                assertEquals(entries[1].entryAddress,
                           tinyLfuTable.get(entries[1].hash, entries[1].tag, entries[1].key, entries[1].key.length,
                                          entries[1].namespace, entries[1].namespace.length, entryArena));

                // Verify 5th entry was inserted
                assertEquals(entries[4].entryAddress,
                           tinyLfuTable.get(entries[4].hash, entries[4].tag, entries[4].key, entries[4].key.length,
                                          entries[4].namespace, entries[4].namespace.length, entryArena));
            }
        }

        @Test
        void testSampledLRUPolicy() {
            try (L0Table sampledLruTable = new L0Table(allocator, 2, L0Table.ReplacementPolicy.SAMPLED_LRU)) {
                // Create test entries
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    entries[i] = createTestEntry("sampledKey" + i, "sampledValue" + i);
                    int hash = 0x12340000 | (i << 2); // All map to bucket 0
                    short tag = (short) (0x1000 + i);
                    entries[i].hash = hash;
                    entries[i].tag = tag;
                }

                // Insert first 4 entries with delays to ensure different timestamps
                for (int i = 0; i < 4; i++) {
                    sampledLruTable.put(entries[i].hash, entries[i].tag,
                                      entries[i].entryAddress, entries[i].key, entries[i].key.length,
                                      entries[i].namespace, entries[i].namespace.length, entryArena);
                    try {
                        Thread.sleep(1); // Ensure different timestamps
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                // Access entry 3 to make it recently used
                sampledLruTable.get(entries[3].hash, entries[3].tag, entries[3].key, entries[3].key.length,
                                  entries[3].namespace, entries[3].namespace.length, entryArena);

                // Insert 5th entry - Sampled LRU should evict based on random 2-sample
                long evictedAddress = sampledLruTable.put(entries[4].hash, entries[4].tag,
                                                        entries[4].entryAddress, entries[4].key, entries[4].key.length,
                                                        entries[4].namespace, entries[4].namespace.length, entryArena);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // Verify 5th entry was inserted
                assertEquals(entries[4].entryAddress,
                           sampledLruTable.get(entries[4].hash, entries[4].tag, entries[4].key, entries[4].key.length,
                                             entries[4].namespace, entries[4].namespace.length, entryArena));

                // Verify that we still have 4 valid entries in the cache
                int validCount = 0;
                for (int i = 0; i < 5; i++) {
                    long addr = sampledLruTable.get(entries[i].hash, entries[i].tag, 
                                                   entries[i].key, entries[i].key.length,
                                                   entries[i].namespace, entries[i].namespace.length, entryArena);
                    if (addr > 0) validCount++;
                }
                assertEquals(4, validCount, "Should have exactly 4 entries after eviction");
            }
        }

        @Test
        void testLFUFrequencySaturation() {
            try (L0Table lfuTable = new L0Table(allocator, 2, L0Table.ReplacementPolicy.LFU)) {
                TestEntry[] entries = new TestEntry[2];

                for (int i = 0; i < 2; i++) {
                    entries[i] = createTestEntry("satKey" + i, "satValue" + i);
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    entries[i].hash = hash;
                    entries[i].tag = tag;
                    lfuTable.put(entries[i].hash, entries[i].tag,
                               entries[i].entryAddress, entries[i].key, entries[i].key.length,
                               entries[i].namespace, entries[i].namespace.length, entryArena);
                }

                // Access entry 0 many times to test frequency saturation at 15
                for (int j = 0; j < 20; j++) {
                    lfuTable.get(entries[0].hash, entries[0].tag, entries[0].key, entries[0].key.length,
                               entries[0].namespace, entries[0].namespace.length, entryArena);
                }

                // Verify entry is still accessible (frequency should saturate, not overflow)
                assertEquals(entries[0].entryAddress,
                           lfuTable.get(entries[0].hash, entries[0].tag, entries[0].key, entries[0].key.length,
                                      entries[0].namespace, entries[0].namespace.length, entryArena));
            }
        }

        @Test
        void testClockSecondChance() {
            try (L0Table clockTable = new L0Table(allocator, 2, L0Table.ReplacementPolicy.CLOCK)) {
                TestEntry[] entries = new TestEntry[5];

                for (int i = 0; i < 5; i++) {
                    entries[i] = createTestEntry("secondKey" + i, "secondValue" + i);
                    int hash = 0x12340000 | (i << 2);
                    short tag = (short) (0x1000 + i);
                    entries[i].hash = hash;
                    entries[i].tag = tag;
                }

                // Insert 4 entries
                for (int i = 0; i < 4; i++) {
                    clockTable.put(entries[i].hash, entries[i].tag,
                                 entries[i].entryAddress, entries[i].key, entries[i].key.length,
                                 entries[i].namespace, entries[i].namespace.length, entryArena);
                }

                // Access all entries to set accessed bits
                for (int i = 0; i < 4; i++) {
                    clockTable.get(entries[i].hash, entries[i].tag, entries[i].key, entries[i].key.length,
                                 entries[i].namespace, entries[i].namespace.length, entryArena);
                }

                // Insert 5th entry - CLOCK should give second chance to accessed entries
                // by clearing their accessed bits and moving to next victim
                long evictedAddress = clockTable.put(entries[4].hash, entries[4].tag,
                                                   entries[4].entryAddress, entries[4].key, entries[4].key.length,
                                                   entries[4].namespace, entries[4].namespace.length, entryArena);
                assertTrue(evictedAddress > 0, "Should evict an entry");

                // New entry should be inserted
                assertEquals(entries[4].entryAddress,
                           clockTable.get(entries[4].hash, entries[4].tag, entries[4].key, entries[4].key.length,
                                        entries[4].namespace, entries[4].namespace.length, entryArena));
            }
        }

    }

    @Nested
    class CacheManagementTests {

        @Test
        void testRemoveByAddress() {
            // Insert test entries
            TestEntry entry1 = createTestEntry("removeAddr1", "value1");
            TestEntry entry2 = createTestEntry("removeAddr2", "value2");

            l0Table.put(entry1.hash, entry1.tag, entry1.entryAddress, entry1.key, entry1.key.length,
                       entry1.namespace, entry1.namespace.length, entryArena);
            l0Table.put(entry2.hash, entry2.tag, entry2.entryAddress, entry2.key, entry2.key.length,
                       entry2.namespace, entry2.namespace.length, entryArena);

            // Remove by address
            l0Table.removeByAddress(entry1.entryAddress);

            // Verify entry1 is removed, entry2 remains
            assertEquals(0, l0Table.get(entry1.hash, entry1.tag, entry1.key, entry1.key.length,
                                      entry1.namespace, entry1.namespace.length, entryArena));
            assertEquals(entry2.entryAddress, l0Table.get(entry2.hash, entry2.tag, entry2.key, entry2.key.length,
                                                        entry2.namespace, entry2.namespace.length, entryArena));
        }

        @Test
        void testInvalidateRange() {
            // Insert test entries with known addresses
            TestEntry entry1 = createTestEntry("range1", "value1");
            TestEntry entry2 = createTestEntry("range2", "value2");
            TestEntry entry3 = createTestEntry("range3", "value3");

            l0Table.put(entry1.hash, entry1.tag, entry1.entryAddress, entry1.key, entry1.key.length,
                       entry1.namespace, entry1.namespace.length, entryArena);
            l0Table.put(entry2.hash, entry2.tag, entry2.entryAddress, entry2.key, entry2.key.length,
                       entry2.namespace, entry2.namespace.length, entryArena);
            l0Table.put(entry3.hash, entry3.tag, entry3.entryAddress, entry3.key, entry3.key.length,
                       entry3.namespace, entry3.namespace.length, entryArena);

            // Invalidate range that includes entry1 and entry2
            long minAddr = Math.min(entry1.entryAddress, entry2.entryAddress);
            long maxAddr = Math.max(entry1.entryAddress, entry2.entryAddress) + 1;

            l0Table.invalidateRange(minAddr, maxAddr);

            // Verify affected entries are invalidated
            assertEquals(0, l0Table.get(entry1.hash, entry1.tag, entry1.key, entry1.key.length,
                                      entry1.namespace, entry1.namespace.length, entryArena));
            assertEquals(0, l0Table.get(entry2.hash, entry2.tag, entry2.key, entry2.key.length,
                                      entry2.namespace, entry2.namespace.length, entryArena));

            // Entry3 might still be there if outside range
            if (entry3.entryAddress < minAddr || entry3.entryAddress >= maxAddr) {
                assertEquals(entry3.entryAddress, l0Table.get(entry3.hash, entry3.tag, entry3.key, entry3.key.length,
                                                            entry3.namespace, entry3.namespace.length, entryArena));
            }
        }

        @Test
        void testClear() {
            // Insert test entries
            TestEntry entry1 = createTestEntry("clear1", "value1");
            TestEntry entry2 = createTestEntry("clear2", "value2");

            l0Table.put(entry1.hash, entry1.tag, entry1.entryAddress, entry1.key, entry1.key.length,
                       entry1.namespace, entry1.namespace.length, entryArena);
            l0Table.put(entry2.hash, entry2.tag, entry2.entryAddress, entry2.key, entry2.key.length,
                       entry2.namespace, entry2.namespace.length, entryArena);

            // Clear all entries
            l0Table.clear();

            // Verify stats are reset first (before calling get which would increment accessCount)
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(0, stats.validSlots);
            assertEquals(0, stats.accessCount);
            assertEquals(0, stats.hitCount);
            assertEquals(0, stats.missCount);
            assertEquals(0, stats.evictionCount);

            // Then verify all entries are removed
            assertEquals(0, l0Table.get(entry1.hash, entry1.tag, entry1.key, entry1.key.length,
                                      entry1.namespace, entry1.namespace.length, entryArena));
            assertEquals(0, l0Table.get(entry2.hash, entry2.tag, entry2.key, entry2.key.length,
                                      entry2.namespace, entry2.namespace.length, entryArena));
        }
    }

    @Nested
    class StatisticsTests {

        @Test
        void testHitMissStatistics() {
            TestEntry entry = createTestEntry("statsKey", "statsValue");

            // Miss: get non-existent entry
            l0Table.get(entry.hash, entry.tag, entry.key, entry.key.length,
                      entry.namespace, entry.namespace.length, entryArena);

            // Hit: insert then get
            l0Table.put(entry.hash, entry.tag, entry.entryAddress, entry.key, entry.key.length,
                       entry.namespace, entry.namespace.length, entryArena);
            l0Table.get(entry.hash, entry.tag, entry.key, entry.key.length,
                      entry.namespace, entry.namespace.length, entryArena);

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
            TestEntry entry1 = createTestEntry("load1", "value1");
            TestEntry entry2 = createTestEntry("load2", "value2");

            l0Table.put(entry1.hash, entry1.tag, entry1.entryAddress, entry1.key, entry1.key.length,
                       entry1.namespace, entry1.namespace.length, entryArena);
            l0Table.put(entry2.hash, entry2.tag, entry2.entryAddress, entry2.key, entry2.key.length,
                       entry2.namespace, entry2.namespace.length, entryArena);

            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(2, stats.validSlots, "Should have 2 valid slots");
            assertEquals(16, stats.totalSlots, "Should have 16 total slots (4 buckets × 4 slots)");
            assertEquals(2.0 / 16.0, stats.loadFactor, 0.001, "Load factor should be 2/16");
        }

        @Test
        void testEvictionCount() {
            // Fill one bucket completely (4 slots)
            TestEntry[] entries = new TestEntry[5]; // 5 entries for 4 slots

            for (int i = 0; i < 5; i++) {
                entries[i] = createTestEntry("evict" + i, "value" + i);
                int hash = 0x12340000; // Force same bucket
                short tag = (short) (0x1000 + i);
                entries[i].hash = hash;
                entries[i].tag = tag;
            }

            // Insert first 4 (no eviction)
            for (int i = 0; i < 4; i++) {
                l0Table.put(entries[i].hash, entries[i].tag, entries[i].entryAddress,
                          entries[i].key, entries[i].key.length,
                          entries[i].namespace, entries[i].namespace.length, entryArena);
            }

            // Insert 5th (should cause eviction)
            l0Table.put(entries[4].hash, entries[4].tag, entries[4].entryAddress,
                      entries[4].key, entries[4].key.length,
                      entries[4].namespace, entries[4].namespace.length, entryArena);

            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(1, stats.evictionCount, "Should have 1 eviction");
        }

        @Test
        void testStatsToString() {
            TestEntry entry = createTestEntry("toStringKey", "toStringValue");
            l0Table.put(entry.hash, entry.tag, entry.entryAddress, entry.key, entry.key.length,
                       entry.namespace, entry.namespace.length, entryArena);

            L0Table.L0TableStats stats = l0Table.getStats();
            String statsString = stats.toString();

            assertNotNull(statsString);
            assertTrue(statsString.contains("L0Table"));
            assertTrue(statsString.contains("buckets=4"));
            assertTrue(statsString.contains("LRU")); // Default policy
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
            byte[] key1 = "tagCollision1".getBytes();
            byte[] key2 = "tagCollision2".getBytes();
            byte[] namespace = "collisionNamespace".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();

            // Force same hash and tag
            int sameHash = 0x12345678;
            short sameTag = (short) 0x1234;

            long entryAddress1 = entryArena.putEntry(sameHash, key1, namespace, value1);
            long entryAddress2 = entryArena.putEntry(sameHash, key2, namespace, value2);

            // Insert both entries
            long result1 = l0Table.put(sameHash, sameTag, entryAddress1, key1, key1.length, namespace, namespace.length, entryArena);
            long result2 = l0Table.put(sameHash, sameTag, entryAddress2, key2, key2.length, namespace, namespace.length, entryArena);

            assertEquals(0, result1, "First entry should insert successfully");
            assertEquals(0, result2, "Second entry should insert successfully");

            // Verify both can be retrieved correctly
            assertEquals(entryAddress1, l0Table.get(sameHash, sameTag, key1, key1.length, namespace, namespace.length, entryArena));
            assertEquals(entryAddress2, l0Table.get(sameHash, sameTag, key2, key2.length, namespace, namespace.length, entryArena));
        }

        @Test
        void testMultipleUpdates() {
            TestEntry entry = createTestEntry("updateKey", "initialValue");

            // Insert initial entry
            l0Table.put(entry.hash, entry.tag, entry.entryAddress, entry.key, entry.key.length,
                       entry.namespace, entry.namespace.length, entryArena);

            // Keep track of current entry address
            long currentEntryAddress = entry.entryAddress;

            // Update multiple times
            for (int i = 1; i <= 3; i++) {
                byte[] newValue = ("updatedValue" + i).getBytes();
                long newEntryAddress = entryArena.putEntry(entry.hash, entry.key, entry.namespace, newValue);

                long oldAddress = l0Table.put(entry.hash, entry.tag, newEntryAddress, entry.key, entry.key.length,
                                            entry.namespace, entry.namespace.length, entryArena);
                assertEquals(currentEntryAddress, oldAddress, "Should return previous address for update");

                // Verify updated value
                long retrievedAddress = l0Table.get(entry.hash, entry.tag, entry.key, entry.key.length,
                                                  entry.namespace, entry.namespace.length, entryArena);
                assertEquals(newEntryAddress, retrievedAddress, "Should get updated entry");
                assertArrayEquals(newValue, entryArena.getValueBytes(retrievedAddress));

                currentEntryAddress = newEntryAddress; // Update for next iteration
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClose() throws Exception {
            // Insert some entries
            TestEntry entry = createTestEntry("closeKey", "closeValue");
            l0Table.put(entry.hash, entry.tag, entry.entryAddress, entry.key, entry.key.length,
                       entry.namespace, entry.namespace.length, entryArena);

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

    // Helper methods
    private TestEntry createTestEntry(String keyStr, String valueStr) {
        byte[] key = keyStr.getBytes();
        byte[] namespace = "testNamespace".getBytes();
        byte[] value = valueStr.getBytes();

        int hash = HashFunctions.compositeHash(key, namespace);
        long entryAddress = entryArena.putEntry(hash, key, namespace, value);
        short tag = (short) (hash & 0xFFFF);

        return new TestEntry(key, namespace, value, entryAddress, hash, tag);
    }

    // Test entry helper class
    private static class TestEntry {
        final byte[] key;
        final byte[] namespace;
        @SuppressWarnings("unused")
        final byte[] value;  // Kept for debugging purposes
        final long entryAddress;
        int hash;
        short tag;

        TestEntry(byte[] key, byte[] namespace, byte[] value, long entryAddress,
                  int hash, short tag) {
            this.key = key;
            this.namespace = namespace;
            this.value = value;
            this.entryAddress = entryAddress;
            this.hash = hash;
            this.tag = tag;
        }
    }
}
