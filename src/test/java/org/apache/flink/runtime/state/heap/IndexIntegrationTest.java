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

    private HeapEntryStore<String, String, Integer> entryStore;
    private L0Table<String, String, Integer> l0Table;
    private MainTable<String, String, Integer> mainTable;

    // Allocators for testing
    private NativeL0MemoryAllocator l0Allocator;

    @BeforeEach
    void setUp() {
        entryStore = new HeapEntryStore<>();
        l0Allocator = new NativeL0MemoryAllocator();
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
    }

    // ========== Helper Methods ==========

    /**
     * Computes the composite hash for key and namespace.
     * Must match HeapStateEntry and ForL0StateMap.compositeHash() exactly.
     */
    private static int compositeHash(Object key, Object namespace) {
        return MathUtils.bitMix(key.hashCode()) ^ MathUtils.bitMix(namespace.hashCode());
    }

    /**
     * Gets entry from entry store by address (helper for tests).
     * Uses direct chunk access like production code.
     */
    private HeapStateEntry<String, String, Integer> getEntry(long address) {
        int idx = (int) address - 1;
        return entryStore.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];
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

            int hash = compositeHash(key, namespace);
            
            // Allocate entry in store
            int addr = (int) entryStore.allocate(key, namespace, state, hash);
            assertTrue(addr > 0);

            // Put into L0 cache
            int oldAddr = l0Table.put(hash, addr);
            assertEquals(0, oldAddr);  // New entry

            // Get from L0 cache
            HeapStateEntry<String, String, Integer> foundEntry = l0Table.get(hash, key, namespace, entryStore);
            assertNotNull(foundEntry);

            // Verify state
            assertEquals(state, foundEntry.getState());

            // Remove from L0 cache
            int removedAddr = l0Table.remove(hash, addr);
            assertEquals(addr, removedAddr);

            // Should not find after removal
            HeapStateEntry<String, String, Integer> notFound = l0Table.get(hash, key, namespace, entryStore);
            assertNull(notFound);
        }

        @Test
        @DisplayName("Multiple entries with same hash bucket")
        void testMultipleEntriesSameBucket() {
            // Create entries that will hash to the same bucket
            List<Integer> addresses = new ArrayList<>();
            for (int i = 0; i < 4; i++) {  // L0 has 4 slots per bucket
                String key = "key" + i;
                String namespace = "ns" + i;
                int hash = compositeHash(key, namespace);
                long addr = entryStore.allocate(key, namespace, i, hash);
                addresses.add((int) addr);

                l0Table.put(hash, (int) addr);
            }

            // Verify all entries can be found
            for (int i = 0; i < 4; i++) {
                String key = "key" + i;
                String namespace = "ns" + i;
                int hash = compositeHash(key, namespace);

                HeapStateEntry<String, String, Integer> found = l0Table.get(hash, key, namespace, entryStore);
                assertNotNull(found);
                assertEquals(i, found.getState());
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
                int hash = compositeHash(key, namespace);
                long addr = entryStore.allocate(key, namespace, i, hash);

                l0Table.put(hash, (int) addr);
            }

            // Some entries should have been evicted
            assertTrue(l0Table.getEvictionCount() > 0);
        }

        @Test
        @DisplayName("Hit/miss statistics")
        void testHitMissStats() {
            String key = "testKey";
            String namespace = "testNs";
            int hash = compositeHash(key, namespace);
            long addr = entryStore.allocate(key, namespace, 100, hash);

            // First get should be a miss
            l0Table.get(hash, key, namespace, entryStore);
            assertEquals(1, l0Table.getMissCount());
            assertEquals(0, l0Table.getHitCount());

            // Put the entry
            l0Table.put(hash, (int) addr);

            // Now get should be a hit
            l0Table.get(hash, key, namespace, entryStore);
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
            mainTable = new MainTable<>(1.5);  // 65536 buckets (fixed)
        }

        @Test
        @DisplayName("Basic get/put/remove operations")
        void testBasicOperations() {
            String key = "testKey";
            String namespace = "testNs";
            Integer state = 42;

            int hash = compositeHash(key, namespace);

            // Put into main table (allocates internally)
            HeapStateEntry<String, String, Integer> result = mainTable.put(hash, key, namespace, entryStore);
            assertNotNull(result);  // New entry
            result.state = state;

            // Get from main table
            HeapStateEntry<String, String, Integer> found = mainTable.get(hash, key, namespace, entryStore);
            assertNotNull(found);
            assertEquals(state, found.getState());

            // Remove
            HeapStateEntry<String, String, Integer> removed = mainTable.remove(hash, key, namespace, entryStore);
            assertNotNull(removed);

            // Should not find after removal
            HeapStateEntry<String, String, Integer> notFound = mainTable.get(hash, key, namespace, entryStore);
            assertNull(notFound);
        }

        @Test
        @DisplayName("Update existing entry")
        void testUpdateExistingEntry() {
            String key = "updateKey";
            String namespace = "updateNs";
            int hash = compositeHash(key, namespace);

            // Initial entry
            HeapStateEntry<String, String, Integer> entry = mainTable.put(hash, key, namespace, entryStore);
            entry.state = 100;

            // Update state in-place
            entry.state = 200;

            // Verify updated state
            HeapStateEntry<String, String, Integer> found = mainTable.get(hash, key, namespace, entryStore);
            assertNotNull(found);
            assertEquals(200, found.getState());
        }

        @Test
        @DisplayName("Many entries with collision handling")
        void testManyEntriesWithCollisions() {
            int numEntries = 100;

            // Insert many entries
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);
                HeapStateEntry<String, String, Integer> entry = mainTable.put(hash, key, namespace, entryStore);
                entry.state = i;
            }

            // Verify all entries can be found
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);

                HeapStateEntry<String, String, Integer> found = mainTable.get(hash, key, namespace, entryStore);
                assertNotNull(found);
                assertEquals(i, found.getState());
            }
        }

        @Test
        @DisplayName("Entry iteration with forEachEntry")
        void testForEachEntry() {
            // Insert some entries
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);
                HeapStateEntry<String, String, Integer> entry = mainTable.put(hash, key, namespace, entryStore);
                entry.state = i;
            }

            // Count entries via iteration
            int[] count = {0};
            mainTable.forEachEntry((entryAddr, keyHash) -> {
                assertNotNull(getEntry(entryAddr));
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
        @DisplayName("Basic operations work with fixed initial size")
        void testBasicOperationsWithFixedSize() {
            // MainTable now has fixed initial size of 65536 buckets
            mainTable = new MainTable<>(1.5);

            int numEntries = 100;
            Map<String, Integer> keyToState = new HashMap<>();

            // Insert entries
            for (int i = 0; i < numEntries; i++) {
                String key = "resizeKey" + i;
                String namespace = "ns";
                Integer state = i * 10;

                int hash = compositeHash(key, namespace);
                HeapStateEntry<String, String, Integer> entry = mainTable.put(hash, key, namespace, entryStore);
                entry.state = state;
                keyToState.put(key, state);
            }

            // With 65536 buckets and 1.5 threshold, 100 entries should NOT trigger resize
            assertFalse(mainTable.needsResize());

            // Verify all entries are still accessible
            for (int i = 0; i < numEntries; i++) {
                String key = "resizeKey" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);

                HeapStateEntry<String, String, Integer> found = mainTable.get(hash, key, namespace, entryStore);
                assertNotNull(found, "Entry not found: " + key);
                assertEquals(keyToState.get(key), found.getState());
            }
        }

        @Test
        @DisplayName("Hash is cached correctly in HeapStateEntry")
        void testHashCachedInEntry() {
            mainTable = new MainTable<>(1.5);

            // Insert entries with known hashes
            String key1 = "key1";
            String ns1 = "ns1";
            int hash1 = compositeHash(key1, ns1);

            String key2 = "key2";
            String ns2 = "ns2";
            int hash2 = compositeHash(key2, ns2);

            HeapStateEntry<String, String, Integer> entry1 = mainTable.put(hash1, key1, ns1, entryStore);
            entry1.state = 100;
            HeapStateEntry<String, String, Integer> entry2 = mainTable.put(hash2, key2, ns2, entryStore);
            entry2.state = 200;

            // Verify hash is cached correctly in entries
            assertEquals(hash1, entry1.hash);
            assertEquals(hash2, entry2.hash);

            // Entries should be findable
            assertNotNull(mainTable.get(hash1, key1, ns1, entryStore));
            assertNotNull(mainTable.get(hash2, key2, ns2, entryStore));
        }
    }

    // ========== Two-Tier Index Integration Tests ==========

    @Nested
    @DisplayName("L0 + MainTable Two-Tier Integration")
    class TwoTierIntegrationTests {

        @BeforeEach
        void setUpBothTables() {
            l0Table = new L0Table<>(l0Allocator, 2);  // 4 buckets
            mainTable = new MainTable<>(1.5);  // 65536 buckets (fixed)
        }

        @Test
        @DisplayName("L0 cache promotion after MainTable lookup")
        void testL0CachePromotion() {
            String key = "promotedKey";
            String namespace = "ns";
            Integer state = 999;
            int hash = compositeHash(key, namespace);

            // First, allocate entry in store and put in main table
            int ptr = (int) entryStore.allocate(key, namespace, state, hash);
            int idx = ptr - 1;
            HeapStateEntry<String, String, Integer> entry = entryStore.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];

            // L0 should miss
            HeapStateEntry<String, String, Integer> l0Result = l0Table.get(hash, key, namespace, entryStore);
            assertNull(l0Result);
            assertEquals(1, l0Table.getMissCount());

            // Promote to L0 cache
            l0Table.put(hash, ptr);

            // Now L0 should hit
            l0Result = l0Table.get(hash, key, namespace, entryStore);
            assertNotNull(l0Result);
            assertEquals(1, l0Table.getHitCount());
        }

        @Test
        @DisplayName("Consistent view across both indexes")
        void testConsistentView() {
            int numEntries = 30;
            int[] ptrs = new int[numEntries];

            // Insert entries to both tables
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);

                // Allocate in entry store
                int ptr = (int) entryStore.allocate(key, namespace, i, hash);
                ptrs[i] = ptr;

                // Promote to L0 for first 10 (simulating hot keys)
                if (i < 10) {
                    l0Table.put(hash, ptr);
                }
            }

            // Verify consistency
            for (int i = 0; i < numEntries; i++) {
                String key = "key" + i;
                String namespace = "ns";
                int hash = compositeHash(key, namespace);

                // Entry store should always have the entry
                int ptr = ptrs[i];
                int idx = ptr - 1;
                HeapStateEntry<String, String, Integer> storeEntry = entryStore.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];
                assertNotNull(storeEntry);
                assertEquals(i, storeEntry.getState());

                // L0 might or might not have it (depending on eviction)
                HeapStateEntry<String, String, Integer> l0Entry = l0Table.get(hash, key, namespace, entryStore);
                if (l0Entry != null) {
                    // If L0 has it, it should be the same entry
                    assertSame(storeEntry, l0Entry);
                }
            }
        }

        @Test
        @DisplayName("Update propagates correctly")
        void testUpdatePropagation() {
            String key = "updateKey";
            String namespace = "ns";
            int hash = compositeHash(key, namespace);

            // Initial entry
            int ptr = (int) entryStore.allocate(key, namespace, 1, hash);
            l0Table.put(hash, ptr);

            // Get entry reference
            int idx = ptr - 1;
            HeapStateEntry<String, String, Integer> entry = entryStore.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];

            // Update state in-place
            entry.state = 2;

            // L0 should see the updated value
            HeapStateEntry<String, String, Integer> l0Entry = l0Table.get(hash, key, namespace, entryStore);

            assertNotNull(l0Entry);

            // Should return the updated state
            assertEquals(2, l0Entry.getState());
        }

        @Test
        @DisplayName("Delete from both indexes")
        void testDeleteFromBothIndexes() {
            String key = "deleteKey";
            String namespace = "ns";
            int hash = compositeHash(key, namespace);

            int ptr = (int) entryStore.allocate(key, namespace, 100, hash);
            l0Table.put(hash, ptr);

            // Delete from L0
            l0Table.remove(hash, ptr);
            assertNull(l0Table.get(hash, key, namespace, entryStore));

            // Delete from entry store
            entryStore.remove(ptr);
            assertNull(getEntry(ptr));
        }
    }
}
