/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.NativeL0MemoryAllocator;
import org.apache.flink.util.MathUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for L0Table and MainTable with HeapEntryStore.
 * 
 * <p>These tests verify that the heap object store architecture works correctly:
 * <ul>
 *   <li>L0Table + HeapEntryStore integration</li>
 *   <li>MainTable + HeapEntryStore integration</li>
 *   <li>Two-tier index (L0 + Main) integration</li>
 *   <li>MainTable resize with HeapEntryStore</li>
 *   <li>L0 cache hit rate improvement</li>
 * </ul>
 */
class IndexIntegrationTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024;
    private static final long DEFAULT_MEMORY_SIZE = 16L * 1024 * 1024; // 16MB

    private HeapEntryStore<String, String, Integer> entryStore;
    private L0Table<String, String, Integer> l0Table;
    private MainTable<String, String, Integer> mainTable;

    // Allocators for testing
    private NativeL0MemoryAllocator l0Allocator;
    private MemoryManagerAllocator mainAllocator;
    private MemoryManager memoryManager;

    @BeforeEach
    void setUp() {
        entryStore = new HeapEntryStore<>();
        l0Allocator = new NativeL0MemoryAllocator();
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        mainAllocator = new MemoryManagerAllocator(memoryManager, new Object());
    }

    @AfterEach
    void tearDown() {
        if (entryStore != null) {
            entryStore.close();
        }
        if (l0Table != null) {
            l0Table.close();
        }
        if (l0Allocator != null && !l0Allocator.isClosed()) {
            l0Allocator.close();
        }
        if (mainTable != null) {
            try {
                mainTable.close();
            } catch (Exception e) {
                // ignore
            }
        }
        if (mainAllocator != null && !mainAllocator.isClosed()) {
            mainAllocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    // ========== Helper Methods ==========

    /**
     * Computes the composite hash for key and namespace.
     */
    private static int compositeHash(Object key, Object namespace) {
        return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
    }

    /**
     * Extracts the tag from hash.
     */
    private static short extractTag(int hash) {
        return (short) (hash >>> 16);
    }

    // ========== L0Table + HeapEntryStore Integration Tests ==========

    @Nested
    @DisplayName("L0Table + HeapEntryStore Integration")
    class L0TableIntegrationTests {

        @BeforeEach
        void setUpL0Table() {
            l0Table = new L0Table<>(l0Allocator, 2);  // 4 buckets
        }

        @Test
        @DisplayName("Basic get/put/remove operations")
        void testBasicOperations() {
            String key = "testKey";
            String namespace = "testNs";
            Integer state = 42;

            // Allocate entry in store
            long addr = entryStore.allocate(key, namespace, state);
            assertTrue(addr > 0);

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // Put into L0 cache
            long oldAddr = l0Table.put(hash, tag, addr, key, namespace, entryStore);
            assertEquals(0, oldAddr);  // New entry

            // Get from L0 cache
            long foundAddr = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(addr, foundAddr);

            // Verify state
            HeapStateEntry<String, String, Integer> entry = entryStore.get(foundAddr);
            assertNotNull(entry);
            assertEquals(state, entry.getState());

            // Remove from L0 cache
            long removedAddr = l0Table.remove(hash, tag, key, namespace, entryStore);
            assertEquals(addr, removedAddr);

            // Should not find after removal
            long notFound = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(0, notFound);
        }

        @Test
        @DisplayName("Multiple entries with same hash bucket")
        void testMultipleEntriesSameBucket() {
            // Create entries that will hash to the same bucket
            List<Long> addresses = new ArrayList<>();
            for (int i = 0; i < 4; i++) {  // L0 has 4 slots per bucket
                String key = "key" + i;
                String namespace = "ns" + i;
                long addr = entryStore.allocate(key, namespace, i);
                addresses.add(addr);

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                l0Table.put(hash, tag, addr, key, namespace, entryStore);
            }

            // Verify all entries can be found
            for (int i = 0; i < 4; i++) {
                String key = "key" + i;
                String namespace = "ns" + i;
                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);

                long found = l0Table.get(hash, tag, key, namespace, entryStore);
                assertEquals(addresses.get(i), found);
            }
        }

        @Test
        @DisplayName("L0 cache eviction")
        void testEviction() {
            // Fill up more than 4 slots (L0 has 4 slots per bucket, 4 buckets = 16 total)
            // Add more entries to trigger eviction
            for (int i = 0; i < 20; i++) {
                String key = "key" + i;
                String namespace = "ns";
                long addr = entryStore.allocate(key, namespace, i);

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                l0Table.put(hash, tag, addr, key, namespace, entryStore);
            }

            // Some entries should have been evicted
            assertTrue(l0Table.getEvictionCount() > 0);
        }

        @Test
        @DisplayName("Hit/miss statistics")
        void testHitMissStats() {
            String key = "testKey";
            String namespace = "testNs";
            long addr = entryStore.allocate(key, namespace, 100);

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // First get should be a miss
            l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(1, l0Table.getMissCount());
            assertEquals(0, l0Table.getHitCount());

            // Put the entry
            l0Table.put(hash, tag, addr, key, namespace, entryStore);

            // Now get should be a hit
            l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(1, l0Table.getMissCount());
            assertEquals(1, l0Table.getHitCount());

            assertTrue(l0Table.getHitRate() > 0);
        }
    }

    // ========== MainTable + HeapEntryStore Integration Tests ==========

    @Nested
    @DisplayName("MainTable + HeapEntryStore Integration")
    class MainTableIntegrationTests {

        @BeforeEach
        void setUpMainTable() {
            mainTable = new MainTable<>(mainAllocator, 2);  // 4 buckets
        }

        @Test
        @DisplayName("Basic get/put/remove operations")
        void testBasicOperations() {
            String key = "testKey";
            String namespace = "testNs";
            Integer state = 42;

            // Allocate entry in store
            long addr = entryStore.allocate(key, namespace, state);

            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            // Put into main table
            long result = mainTable.put(hash, tag, addr, key, namespace, entryStore);
            assertEquals(0, result);  // New entry

            // Get from main table
            long found = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(addr, found);

            // Verify state through entry store
            HeapStateEntry<String, String, Integer> entry = entryStore.get(found);
            assertNotNull(entry);
            assertEquals(state, entry.getState());

            // Remove
            long removed = mainTable.remove(hash, tag, key, namespace, entryStore);
            assertEquals(addr, removed);

            // Should not find after removal
            long notFound = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(0, notFound);
        }

        @Test
        @DisplayName("Update existing entry")
        void testUpdateExistingEntry() {
            String key = "updateKey";
            String namespace = "updateNs";

            // Initial entry
            long addr1 = entryStore.allocate(key, namespace, 100);
            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            mainTable.put(hash, tag, addr1, key, namespace, entryStore);

            // Update state in-place (address doesn't change)
            entryStore.updateState(addr1, 200);

            // Verify updated state
            long found = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(addr1, found);

            HeapStateEntry<String, String, Integer> entry = entryStore.get(found);
            assertEquals(200, entry.getState());
        }

        @Test
        @DisplayName("Many entries with collision handling")
        void testManyEntriesWithCollisions() {
            int numEntries = 100;
            Map<String, Long> keyToAddr = new HashMap<>();

            // Insert many entries
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                long addr = entryStore.allocate(key, namespace, i);
                keyToAddr.put(key, addr);

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                mainTable.put(hash, tag, addr, key, namespace, entryStore);
            }

            // Verify all entries can be found
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);

                long found = mainTable.get(hash, tag, key, namespace, entryStore);
                assertEquals(keyToAddr.get(key), found);

                HeapStateEntry<String, String, Integer> entry = entryStore.get(found);
                assertEquals(i, entry.getState());
            }
        }

        @Test
        @DisplayName("Entry iteration with forEachEntry")
        void testForEachEntry() {
            // Insert some entries
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                String namespace = "ns";
                long addr = entryStore.allocate(key, namespace, i);
                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                mainTable.put(hash, tag, addr, key, namespace, entryStore);
            }

            // Count entries via iteration
            int[] count = {0};
            mainTable.forEachEntry((entryAddr, keyHash, tag) -> {
                assertNotNull(entryStore.get(entryAddr));
                count[0]++;
            });

            assertEquals(10, count[0]);
        }
    }

    // ========== MainTable Resize Tests ==========

    @Nested
    @DisplayName("MainTable Resize")
    class MainTableResizeTests {

        @Test
        @DisplayName("Resize preserves all entries")
        void testResizePreservesEntries() {
            // Use small table to trigger resize easily
            mainTable = new MainTable<>(mainAllocator, 1, 0.5);  // 2 buckets, low threshold

            int numEntries = 50;
            Map<String, Long> keyToAddr = new HashMap<>();
            Map<String, Integer> keyToState = new HashMap<>();

            // Insert entries until resize is needed
            for (int i = 0; i < numEntries; i++) {
                String key = "resizeKey" + i;
                String namespace = "ns";
                Integer state = i * 10;

                long addr = entryStore.allocate(key, namespace, state);
                keyToAddr.put(key, addr);
                keyToState.put(key, state);

                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);
                mainTable.put(hash, tag, addr, key, namespace, entryStore);
            }

            // Trigger resize if needed
            if (mainTable.needsResize()) {
                mainTable.resize(entryStore);
            }

            // Verify all entries are still accessible
            for (int i = 0; i < numEntries; i++) {
                String key = "resizeKey" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);

                long found = mainTable.get(hash, tag, key, namespace, entryStore);
                assertEquals(keyToAddr.get(key), found, "Entry not found after resize: " + key);

                HeapStateEntry<String, String, Integer> entry = entryStore.get(found);
                assertNotNull(entry);
                assertEquals(keyToState.get(key), entry.getState());
            }
        }

        @Test
        @DisplayName("Resize uses entry.hash from HeapEntryStore")
        void testResizeUsesEntryHash() {
            mainTable = new MainTable<>(mainAllocator, 1, 0.5);

            // Insert entries with known hashes
            String key1 = "key1";
            String ns1 = "ns1";
            int hash1 = compositeHash(key1, ns1);
            short tag1 = extractTag(hash1);

            String key2 = "key2";
            String ns2 = "ns2";
            int hash2 = compositeHash(key2, ns2);
            short tag2 = extractTag(hash2);

            long addr1 = entryStore.allocate(key1, ns1, 100);
            long addr2 = entryStore.allocate(key2, ns2, 200);

            mainTable.put(hash1, tag1, addr1, key1, ns1, entryStore);
            mainTable.put(hash2, tag2, addr2, key2, ns2, entryStore);

            // Verify hash is cached correctly in entries
            HeapStateEntry<String, String, Integer> entry1 = entryStore.get(addr1);
            HeapStateEntry<String, String, Integer> entry2 = entryStore.get(addr2);
            assertEquals(hash1, entry1.getHash());
            assertEquals(hash2, entry2.getHash());
            assertEquals(tag1, entry1.getTag());
            assertEquals(tag2, entry2.getTag());

            // Force resize
            for (int i = 0; i < 20; i++) {
                String k = "padding" + i;
                String n = "ns";
                long a = entryStore.allocate(k, n, i);
                int h = compositeHash(k, n);
                short t = extractTag(h);
                mainTable.put(h, t, a, k, n, entryStore);
            }

            if (mainTable.needsResize()) {
                mainTable.resize(entryStore);
            }

            // Original entries should still be findable
            assertEquals(addr1, mainTable.get(hash1, tag1, key1, ns1, entryStore));
            assertEquals(addr2, mainTable.get(hash2, tag2, key2, ns2, entryStore));
        }
    }

    // ========== Two-Tier Index Integration Tests ==========

    @Nested
    @DisplayName("L0 + MainTable Two-Tier Integration")
    class TwoTierIntegrationTests {

        @BeforeEach
        void setUpBothTables() {
            l0Table = new L0Table<>(l0Allocator, 2);  // 4 buckets
            mainTable = new MainTable<>(mainAllocator, 3);  // 8 buckets
        }

        @Test
        @DisplayName("L0 cache promotion after MainTable lookup")
        void testL0CachePromotion() {
            String key = "promotedKey";
            String namespace = "ns";
            Integer state = 999;

            // First, put entry in main table only
            long addr = entryStore.allocate(key, namespace, state);
            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            mainTable.put(hash, tag, addr, key, namespace, entryStore);

            // L0 should miss
            long l0Result = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(0, l0Result);
            assertEquals(1, l0Table.getMissCount());

            // Main table should hit
            long mainResult = mainTable.get(hash, tag, key, namespace, entryStore);
            assertEquals(addr, mainResult);

            // Promote to L0 cache
            l0Table.put(hash, tag, addr, key, namespace, entryStore);

            // Now L0 should hit
            l0Result = l0Table.get(hash, tag, key, namespace, entryStore);
            assertEquals(addr, l0Result);
            assertEquals(1, l0Table.getHitCount());
        }

        @Test
        @DisplayName("Consistent view across both indexes")
        void testConsistentView() {
            int numEntries = 30;

            // Insert entries to both tables
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                Integer state = i;

                long addr = entryStore.allocate(key, namespace, state);
                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);

                // Put in main table
                mainTable.put(hash, tag, addr, key, namespace, entryStore);

                // Promote to L0 for first 10 (simulating hot keys)
                if (i < 10) {
                    l0Table.put(hash, tag, addr, key, namespace, entryStore);
                }
            }

            // Verify consistency
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);
                short tag = extractTag(hash);

                // Main table should always have the entry
                long mainAddr = mainTable.get(hash, tag, key, namespace, entryStore);
                assertTrue(mainAddr > 0);

                HeapStateEntry<String, String, Integer> entry = entryStore.get(mainAddr);
                assertEquals(i, entry.getState());

                // L0 might or might not have it (depending on eviction)
                long l0Addr = l0Table.get(hash, tag, key, namespace, entryStore);
                if (l0Addr > 0) {
                    // If L0 has it, it should point to the same entry
                    assertEquals(mainAddr, l0Addr);
                }
            }
        }

        @Test
        @DisplayName("Update propagates correctly")
        void testUpdatePropagation() {
            String key = "updateKey";
            String namespace = "ns";

            // Initial entry
            long addr = entryStore.allocate(key, namespace, 1);
            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            mainTable.put(hash, tag, addr, key, namespace, entryStore);
            l0Table.put(hash, tag, addr, key, namespace, entryStore);

            // Update state in HeapEntryStore (in-place)
            entryStore.updateState(addr, 2);

            // Both L0 and Main should see the updated value
            long l0Addr = l0Table.get(hash, tag, key, namespace, entryStore);
            long mainAddr = mainTable.get(hash, tag, key, namespace, entryStore);

            assertEquals(addr, l0Addr);
            assertEquals(addr, mainAddr);

            // Both should return the updated state
            assertEquals(2, entryStore.get(l0Addr).getState());
            assertEquals(2, entryStore.get(mainAddr).getState());
        }

        @Test
        @DisplayName("Delete from both indexes")
        void testDeleteFromBothIndexes() {
            String key = "deleteKey";
            String namespace = "ns";

            long addr = entryStore.allocate(key, namespace, 100);
            int hash = compositeHash(key, namespace);
            short tag = extractTag(hash);

            mainTable.put(hash, tag, addr, key, namespace, entryStore);
            l0Table.put(hash, tag, addr, key, namespace, entryStore);

            // Delete from L0
            l0Table.remove(hash, tag, key, namespace, entryStore);
            assertEquals(0, l0Table.get(hash, tag, key, namespace, entryStore));

            // Main should still have it
            assertEquals(addr, mainTable.get(hash, tag, key, namespace, entryStore));

            // Delete from main
            mainTable.remove(hash, tag, key, namespace, entryStore);
            assertEquals(0, mainTable.get(hash, tag, key, namespace, entryStore));

            // Delete from entry store
            entryStore.remove(addr);
            assertNull(entryStore.get(addr));
        }
    }
}
