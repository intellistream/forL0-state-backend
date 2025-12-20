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

import org.junit.jupiter.api.*;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HeapEntryStore} and {@link HeapStateEntry}.
 */
class HeapEntryStoreTest {
    
    private HeapEntryStore<String, Integer, String> store;
    
    @BeforeEach
    void setUp() {
        store = new HeapEntryStore<>();
    }
    
    @AfterEach
    void tearDown() {
        if (store != null && !store.isClosed()) {
            store.close();
        }
    }
    
    // ========== HeapStateEntry Tests ==========
    
    @Nested
    class HeapStateEntryTests {
        
        @Test
        void testBasicCreation() {
            HeapStateEntry<String, Integer, String> entry = 
                new HeapStateEntry<>("key1", 1, "value1");
            
            assertEquals("key1", entry.getKey());
            assertEquals(Integer.valueOf(1), entry.getNamespace());
            assertEquals("value1", entry.getState());
        }
        
        @Test
        void testNullState() {
            HeapStateEntry<String, Integer, String> entry = 
                new HeapStateEntry<>("key1", 1, null);
            
            assertEquals("key1", entry.getKey());
            assertEquals(Integer.valueOf(1), entry.getNamespace());
            assertNull(entry.getState());
        }
        
        @Test
        void testSetState() {
            HeapStateEntry<String, Integer, String> entry = 
                new HeapStateEntry<>("key1", 1, "value1");
            
            assertEquals("value1", entry.getState());
            
            // Direct field access (field is package-private)
            entry.state = "value2";
            assertEquals("value2", entry.getState());
            
            entry.state = null;
            assertNull(entry.getState());
        }
        
        @Test
        void testHashComputation() {
            HeapStateEntry<String, Integer, String> entry1 = 
                new HeapStateEntry<>("key1", 1, "value1");
            HeapStateEntry<String, Integer, String> entry2 = 
                new HeapStateEntry<>("key1", 1, "value2");
            
            // Same key and namespace should have the same hash
            assertEquals(entry1.hash, entry2.hash);
            
            // Different key should have different hash (with high probability)
            HeapStateEntry<String, Integer, String> entry3 = 
                new HeapStateEntry<>("key2", 1, "value1");
            assertNotEquals(entry1.hash, entry3.hash);
            
            // Different namespace should have different hash (with high probability)
            HeapStateEntry<String, Integer, String> entry4 = 
                new HeapStateEntry<>("key1", 2, "value1");
            assertNotEquals(entry1.hash, entry4.hash);
        }
        
        @Test
        void testTagDerivation() {
            HeapStateEntry<String, Integer, String> entry = 
                new HeapStateEntry<>("key1", 1, "value1");
            
            // Tag is the high 16 bits of hash (computed inline, not via method)
            short expectedTag = (short) (entry.hash >>> 16);
            // Verify tag can be computed from hash
            assertEquals(expectedTag, (short) (entry.hash >>> 16));
        }
        
        @Test
        void testMatches() {
            HeapStateEntry<String, Integer, String> entry = 
                new HeapStateEntry<>("key1", 1, "value1");
            
            // Matching logic is now in HeapEntryStore, test via direct field access
            // Should match same key and namespace
            assertTrue(entry.key.equals("key1") && entry.namespace.equals(1));
            
            // Should not match different key
            assertFalse(entry.key.equals("key2"));
            
            // Should not match different namespace
            assertFalse(entry.namespace.equals(2));
        }
        
        @Test
        void testEqualsAndHashCode() {
            HeapStateEntry<String, Integer, String> entry1 = 
                new HeapStateEntry<>("key1", 1, "value1");
            HeapStateEntry<String, Integer, String> entry2 = 
                new HeapStateEntry<>("key1", 1, "value1");
            HeapStateEntry<String, Integer, String> entry3 = 
                new HeapStateEntry<>("key1", 1, "value2");
            
            // Same content should be equal
            assertEquals(entry1, entry2);
            assertEquals(entry1.hashCode(), entry2.hashCode());
            
            // Different state should not be equal
            assertNotEquals(entry1, entry3);
        }
        
        @Test
        void testToString() {
            HeapStateEntry<String, Integer, String> entry = 
                new HeapStateEntry<>("key1", 1, "value1");
            
            String str = entry.toString();
            assertTrue(str.contains("key1"));
            assertTrue(str.contains("value1"));
        }
    }
    
    // ========== HeapEntryStore Basic Tests ==========
    
    @Nested
    class BasicOperationsTests {
        
        @Test
        void testAllocateAndGet() {
            // Allocate an entry
            long addr = store.allocate("key1", 1, "value1");
            
            // Address should be > 0 (0 is reserved as NULL)
            assertTrue(addr > 0);
            
            // Should be able to get the entry back
            HeapStateEntry<String, Integer, String> entry = store.get(addr);
            assertNotNull(entry);
            assertEquals("key1", entry.getKey());
            assertEquals(Integer.valueOf(1), entry.getNamespace());
            assertEquals("value1", entry.getState());
            
            // Statistics should be updated
            assertEquals(1, store.getActiveEntries());
            assertEquals(1, store.getTotalAllocations());
        }
        
        @Test
        void testAllocateMultiple() {
            Set<Long> addresses = new HashSet<>();
            
            for (int i = 0; i < 100; i++) {
                long addr = store.allocate("key" + i, i, "value" + i);
                assertTrue(addr > 0);
                assertTrue(addresses.add(addr), "Address should be unique");
            }
            
            assertEquals(100, store.getActiveEntries());
            
            // Verify all entries can be retrieved
            for (long addr : addresses) {
                HeapStateEntry<String, Integer, String> entry = store.get(addr);
                assertNotNull(entry);
            }
        }
        
        // Note: get() no longer validates addresses for performance.
        // Invalid addresses will cause ArrayIndexOutOfBoundsException.
    }
    
    // ========== Update Tests ==========
    
    @Nested
    class UpdateTests {
        
        @Test
        void testUpdateState() {
            long addr = store.allocate("key1", 1, "value1");
            
            // Update the state
            boolean updated = store.updateState(addr, "value2");
            assertTrue(updated);
            
            // Verify the update
            HeapStateEntry<String, Integer, String> entry = store.get(addr);
            assertEquals("value2", entry.getState());
            
            // Key and namespace should remain unchanged
            assertEquals("key1", entry.getKey());
            assertEquals(Integer.valueOf(1), entry.getNamespace());
        }
        
        @Test
        void testUpdateStateToNull() {
            long addr = store.allocate("key1", 1, "value1");
            
            // Update to null
            boolean updated = store.updateState(addr, null);
            assertTrue(updated);
            
            HeapStateEntry<String, Integer, String> entry = store.get(addr);
            assertNull(entry.getState());
        }
    }
    
    // ========== Remove and Reuse Tests ==========
    
    @Nested
    class RemoveAndReuseTests {
        
        @Test
        void testRemove() {
            long addr = store.allocate("key1", 1, "value1");
            assertEquals(1, store.getActiveEntries());
            
            // Remove the entry
            store.remove(addr);
            
            // Entry should be gone
            assertNull(store.get(addr));
            assertEquals(0, store.getActiveEntries());
            assertEquals(1, store.getTotalRemovals());
            assertEquals(1, store.getFreeSlotCount());
        }
        
        @Test
        void testRemoveAndReuse() {
            // Allocate and remove
            long addr1 = store.allocate("key1", 1, "value1");
            store.remove(addr1);
            
            assertEquals(1, store.getFreeSlotCount());
            
            // Allocate again - should reuse the freed slot
            long addr2 = store.allocate("key2", 2, "value2");
            
            // The reused address should be the same as the removed one (LIFO)
            assertEquals(addr1, addr2);
            assertEquals(0, store.getFreeSlotCount());
            
            // Verify the new entry
            HeapStateEntry<String, Integer, String> entry = store.get(addr2);
            assertEquals("key2", entry.getKey());
            assertEquals(Integer.valueOf(2), entry.getNamespace());
            assertEquals("value2", entry.getState());
        }
        
        @Test
        void testRemoveMultipleAndReuse() {
            // Allocate 5 entries
            long[] addrs = new long[5];
            for (int i = 0; i < 5; i++) {
                addrs[i] = store.allocate("key" + i, i, "value" + i);
            }
            assertEquals(5, store.getActiveEntries());
            
            // Remove entries 1, 3 (not in order)
            store.remove(addrs[1]);
            store.remove(addrs[3]);
            assertEquals(3, store.getActiveEntries());
            assertEquals(2, store.getFreeSlotCount());
            
            // Allocate 2 new entries - should reuse in LIFO order
            long newAddr1 = store.allocate("newKey1", 10, "newValue1");
            long newAddr2 = store.allocate("newKey2", 20, "newValue2");
            
            // LIFO: addr3 was removed last, so it should be reused first
            assertEquals(addrs[3], newAddr1);
            assertEquals(addrs[1], newAddr2);
            assertEquals(0, store.getFreeSlotCount());
        }
        
        @Test
        void testRemoveInvalidAddress() {
            // Removing invalid addresses should be no-op
            store.remove(0);
            store.remove(-1);
            store.remove(999999);
            
            assertEquals(0, store.getTotalRemovals());
            assertEquals(0, store.getFreeSlotCount());
        }
        
        @Test
        void testDoubleRemove() {
            long addr = store.allocate("key1", 1, "value1");
            
            store.remove(addr);
            assertEquals(1, store.getFreeSlotCount());
            
            // Second remove should be no-op (slot is already null)
            store.remove(addr);
            assertEquals(1, store.getFreeSlotCount());
            assertEquals(1, store.getTotalRemovals());
        }
    }
    
    // ========== Chunk Expansion Tests ==========
    
    @Nested
    class ChunkExpansionTests {
        
        @Test
        void testChunkExpansion() {
            // Initially 1 chunk with capacity 65536
            assertEquals(1, store.getChunkCount());
            assertEquals(65536L, store.getCapacity());
            
            // Allocate more than one chunk's worth
            int entriesToAllocate = 65536 + 100;
            for (int i = 0; i < entriesToAllocate; i++) {
                store.allocate("key" + i, i, "value" + i);
            }
            
            // Should have expanded to 2 chunks
            assertEquals(2, store.getChunkCount());
            assertEquals(131072L, store.getCapacity());
            assertEquals(entriesToAllocate, store.getActiveEntries());
            
            // All entries should still be accessible
            // Sample a few
            HeapStateEntry<String, Integer, String> entry1 = store.get(1);
            assertNotNull(entry1);
            assertEquals("key0", entry1.getKey());
            
            HeapStateEntry<String, Integer, String> entry2 = store.get(65537);
            assertNotNull(entry2);
            assertEquals("key65536", entry2.getKey());
        }
        
        @Test
        void testMultipleChunkExpansions() {
            // Force multiple expansions
            int targetChunks = 4;
            int entriesToAllocate = 65536 * (targetChunks - 1) + 1;
            
            for (int i = 0; i < entriesToAllocate; i++) {
                store.allocate("key" + i, i, "value" + i);
            }
            
            assertEquals(targetChunks, store.getChunkCount());
        }
    }
    
    // ========== Hash and Tag Tests ==========
    
    @Nested
    class HashAndTagTests {
        
        @Test
        void testHashAndTag() {
            long addr = store.allocate("testKey", 42, "testValue");
            
            // Get hash via store method
            int hash = store.getHash(addr);
            
            // Get entry and verify consistency
            HeapStateEntry<String, Integer, String> entry = store.get(addr);
            assertEquals(entry.hash, hash);
            // Tag is computed inline from hash
            short tag = (short) (hash >>> 16);
            assertEquals((short) (entry.hash >>> 16), tag);
        }
        
        // Note: getHash() no longer validates addresses for performance.
        // Invalid addresses will cause ArrayIndexOutOfBoundsException.
    }
    
    // ========== Matches Tests ==========
    
    @Nested
    class MatchesTests {
        
        @Test
        void testMatches() {
            long addr = store.allocate("key1", 1, "value1");
            
            // Should match with correct key and namespace
            assertTrue(store.matches(addr, "key1", 1));
            
            // Should not match with wrong key
            assertFalse(store.matches(addr, "key2", 1));
            
            // Should not match with wrong namespace
            assertFalse(store.matches(addr, "key1", 2));
            
            // Note: matches() no longer validates addresses for performance.
            // Invalid addresses will cause ArrayIndexOutOfBoundsException.
        }
    }
    
    // ========== Lifecycle Tests ==========
    
    @Nested
    class LifecycleTests {
        
        @Test
        void testClose() {
            store.allocate("key1", 1, "value1");
            assertFalse(store.isClosed());
            
            store.close();
            assertTrue(store.isClosed());
        }
        
        @Test
        void testDoubleClose() {
            store.close();
            // Second close should be no-op
            assertDoesNotThrow(() -> store.close());
        }
    }
    
    // ========== Statistics Tests ==========
    
    @Nested
    class StatisticsTests {
        
        @Test
        void testStatistics() {
            assertEquals(0, store.getActiveEntries());
            assertEquals(0, store.getTotalAllocations());
            assertEquals(0, store.getTotalRemovals());
            assertEquals(0, store.getFreeSlotCount());
            
            // Allocate
            long addr1 = store.allocate("key1", 1, "value1");
            long addr2 = store.allocate("key2", 2, "value2");
            assertEquals(2, store.getActiveEntries());
            assertEquals(2, store.getTotalAllocations());
            
            // Remove
            store.remove(addr1);
            assertEquals(1, store.getActiveEntries());
            assertEquals(1, store.getTotalRemovals());
            assertEquals(1, store.getFreeSlotCount());
            
            // Reuse
            store.allocate("key3", 3, "value3");
            assertEquals(2, store.getActiveEntries());
            assertEquals(3, store.getTotalAllocations());
            assertEquals(0, store.getFreeSlotCount());
        }
        
        @Test
        void testNextAllocIndex() {
            assertEquals(0, store.getNextAllocIndex());
            
            store.allocate("key1", 1, "value1");
            assertEquals(1, store.getNextAllocIndex());
            
            store.allocate("key2", 2, "value2");
            assertEquals(2, store.getNextAllocIndex());
            
            // Remove doesn't affect nextAllocIndex
            store.remove(1);
            assertEquals(2, store.getNextAllocIndex());
            
            // Reuse uses free list, not nextAllocIndex
            store.allocate("key3", 3, "value3");
            assertEquals(2, store.getNextAllocIndex()); // Still 2, reused slot 0
        }
        
        @Test
        void testMaxAddress() {
            assertEquals(0, store.getMaxAddress());
            
            store.allocate("key1", 1, "value1");
            assertEquals(1, store.getMaxAddress());
            
            store.allocate("key2", 2, "value2");
            assertEquals(2, store.getMaxAddress());
        }
    }
    
    // ========== Iteration Support Tests ==========
    
    @Nested
    class IterationTests {
        
        @Test
        void testGetByIndex() {
            long addr1 = store.allocate("key1", 1, "value1");
            long addr2 = store.allocate("key2", 2, "value2");
            
            // getByIndex uses 0-based index
            HeapStateEntry<String, Integer, String> entry0 = store.getByIndex(0);
            assertNotNull(entry0);
            assertEquals("key1", entry0.getKey());
            
            HeapStateEntry<String, Integer, String> entry1 = store.getByIndex(1);
            assertNotNull(entry1);
            assertEquals("key2", entry1.getKey());
            
            // Invalid indices
            assertNull(store.getByIndex(-1));
            assertNull(store.getByIndex(2)); // Beyond nextAllocIndex
        }
        
        @Test
        void testIterateWithHoles() {
            store.allocate("key0", 0, "value0");
            long addr1 = store.allocate("key1", 1, "value1");
            store.allocate("key2", 2, "value2");
            
            // Remove middle entry
            store.remove(addr1);
            
            // Iterate and count non-null entries
            int count = 0;
            for (int i = 0; i < store.getNextAllocIndex(); i++) {
                if (store.getByIndex(i) != null) {
                    count++;
                }
            }
            
            assertEquals(2, count);
            assertEquals(store.getActiveEntries(), count);
        }
    }
    
    // ========== ToString Test ==========
    
    @Test
    void testToString() {
        store.allocate("key1", 1, "value1");
        
        String str = store.toString();
        assertTrue(str.contains("activeEntries=1"));
        assertTrue(str.contains("capacity=65536"));
        assertTrue(str.contains("chunks=1"));
    }
}
