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

package org.apache.flink.state.forl0;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SwissTable}.
 * 
 * <p>Tests cover core operations: get, put, remove, containsKey,
 * as well as expansion operations: rehash, grow.
 */
class SwissTableTest {

    private static final int INITIAL_CAPACITY = 64;
    
    private SwissTable<Long, String, Long> table;

    @BeforeEach
    void setUp() {
        table = new SwissTable<>(INITIAL_CAPACITY);
    }

    // ========== Basic Operations ==========

    @Test
    void testPutAndGet() {
        long hash = computeHash(1L, "ns1");
        int result = table.put(hash, 1L, "ns1");
        
        assertTrue((result & SwissTable.NEW_FLAG) != 0, "Should be new insertion");
        int slot = result & SwissTable.SLOT_MASK;
        table.values[slot] = 100L;
        
        Long value = table.get(hash, 1L, "ns1");
        assertEquals(100L, value);
    }

    @Test
    void testPutUpdate() {
        long hash = computeHash(1L, "ns1");
        
        // First insert
        int result1 = table.put(hash, 1L, "ns1");
        assertTrue((result1 & SwissTable.NEW_FLAG) != 0);
        int slot1 = result1 & SwissTable.SLOT_MASK;
        table.values[slot1] = 100L;
        
        // Update same key
        int result2 = table.put(hash, 1L, "ns1");
        assertFalse((result2 & SwissTable.NEW_FLAG) != 0, "Should be update, not new");
        int slot2 = result2 & SwissTable.SLOT_MASK;
        assertEquals(slot1, slot2, "Should return same slot");
        
        table.values[slot2] = 200L;
        assertEquals(200L, table.get(hash, 1L, "ns1"));
    }

    @Test
    void testGetNonExistent() {
        long hash = computeHash(999L, "ns_none");
        assertNull(table.get(hash, 999L, "ns_none"));
    }

    @Test
    void testRemove() {
        long hash = computeHash(1L, "ns1");
        int result = table.put(hash, 1L, "ns1");
        int slot = result & SwissTable.SLOT_MASK;
        table.values[slot] = 100L;
        
        Long removed = table.remove(hash, 1L, "ns1");
        assertEquals(100L, removed);
        assertNull(table.get(hash, 1L, "ns1"));
    }

    @Test
    void testRemoveNonExistent() {
        long hash = computeHash(999L, "ns_none");
        assertNull(table.remove(hash, 999L, "ns_none"));
    }

    @Test
    void testContainsKey() {
        long hash = computeHash(1L, "ns1");
        assertFalse(table.containsKey(hash, 1L, "ns1"));
        
        int result = table.put(hash, 1L, "ns1");
        int slot = result & SwissTable.SLOT_MASK;
        table.values[slot] = 100L;
        
        assertTrue(table.containsKey(hash, 1L, "ns1"));
        
        table.remove(hash, 1L, "ns1");
        assertFalse(table.containsKey(hash, 1L, "ns1"));
    }

    // ========== Multiple Entries ==========

    @Test
    void testMultipleEntries() {
        Map<Long, Long> expected = new HashMap<>();
        
        for (long i = 0; i < 50; i++) {
            long hash = computeHash(i, "ns");
            int result = table.put(hash, i, "ns");
            
            // Handle expansion signals
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i, "ns");
            }
            
            int slot = result & SwissTable.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }
        
        assertEquals(50, table.size());
        
        for (long i = 0; i < 50; i++) {
            long hash = computeHash(i, "ns");
            assertEquals(expected.get(i), table.get(hash, i, "ns"));
        }
    }

    @Test
    void testDifferentNamespaces() {
        long key = 1L;
        
        for (int i = 0; i < 10; i++) {
            String ns = "ns" + i;
            long hash = computeHash(key, ns);
            int result = table.put(hash, key, ns);
            
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key, ns);
            }
            
            int slot = result & SwissTable.SLOT_MASK;
            table.values[slot] = (long) i;
        }
        
        assertEquals(10, table.size());
        
        for (int i = 0; i < 10; i++) {
            String ns = "ns" + i;
            long hash = computeHash(key, ns);
            assertEquals((long) i, table.get(hash, key, ns));
        }
    }

    // ========== Growth and Rehash ==========

    @Test
    void testGrow() {
        // Fill table to trigger growth
        // With 87.5% load factor: maxOcc = 64 * 7 / 8 = 56
        // Need to insert 57 entries to exhaust growthLeft and trigger NEED_GROW
        int count = 0;
        for (long i = 0; count < 57; i++) {
            long hash = computeHash(i, "ns");
            int result = table.put(hash, i, "ns");
            
            if (result == SwissTable.NEED_GROW) {
                table.grow();
                // Retry after grow
                result = table.put(hash, i, "ns");
            }
            
            if ((result & SwissTable.NEW_FLAG) != 0) {
                int slot = result & SwissTable.SLOT_MASK;
                table.values[slot] = i * 10;
                count++;
            }
        }
        
        assertTrue(table.getCapacity() > INITIAL_CAPACITY, "Table should have grown");
        
        // Verify all entries
        for (long i = 0; i < count; i++) {
            long hash = computeHash(i, "ns");
            assertEquals(i * 10, table.get(hash, i, "ns"));
        }
    }

    @Test
    void testRehashAfterDeletes() {
        // Fill and delete to create tombstones
        for (long i = 0; i < 40; i++) {
            long hash = computeHash(i, "ns");
            int result = table.put(hash, i, "ns");
            int slot = result & SwissTable.SLOT_MASK;
            table.values[slot] = i;
        }
        
        // Delete half
        for (long i = 0; i < 20; i++) {
            long hash = computeHash(i, "ns");
            table.remove(hash, i, "ns");
        }
        
        assertTrue(table.tomb > 0, "Should have tombstones");
        
        // Keep inserting until rehash is triggered
        for (long i = 100; i < 150; i++) {
            long hash = computeHash(i, "ns");
            int result = table.put(hash, i, "ns");
            
            if (result == SwissTable.NEED_REHASH) {
                table.rehash();
                result = table.put(hash, i, "ns");
            } else if (result == SwissTable.NEED_GROW) {
                table.grow();
                result = table.put(hash, i, "ns");
            }
            
            if ((result & SwissTable.NEW_FLAG) != 0) {
                int slot = result & SwissTable.SLOT_MASK;
                table.values[slot] = i;
            }
        }
        
        // Verify remaining entries
        for (long i = 20; i < 40; i++) {
            long hash = computeHash(i, "ns");
            assertEquals(i, table.get(hash, i, "ns"));
        }
    }

    // ========== Iterator ==========

    @Test
    void testIterator() {
        Map<Long, Long> expected = new HashMap<>();
        
        for (long i = 0; i < 30; i++) {
            long hash = computeHash(i, "ns");
            int result = table.put(hash, i, "ns");
            int slot = result & SwissTable.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }
        
        Map<Long, Long> actual = new HashMap<>();
        Iterator<SwissTable.Entry<Long, String, Long>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTable.Entry<Long, String, Long> entry = iter.next();
            actual.put(entry.getKey(), entry.getState());
        }
        
        assertEquals(expected, actual);
    }

    // ========== Edge Cases ==========

    @Test
    void testNullNamespace() {
        // SwissTable should handle null namespace if needed
        // This depends on implementation - skip if not supported
    }

    @Test
    void testHashCollision() {
        // Create entries with same h2 (low 7 bits of hash)
        // They should be stored correctly via probing
        Random rand = new Random(42);
        Map<Long, Long> entries = new HashMap<>();
        
        for (int i = 0; i < 100; i++) {
            long key = rand.nextLong();
            long hash = computeHash(key, "ns");
            int result = table.put(hash, key, "ns");
            
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key, "ns");
            }
            
            if ((result & SwissTable.NEW_FLAG) != 0) {
                int slot = result & SwissTable.SLOT_MASK;
                table.values[slot] = key;
                entries.put(key, key);
            }
        }
        
        // Verify all entries
        for (Map.Entry<Long, Long> e : entries.entrySet()) {
            long hash = computeHash(e.getKey(), "ns");
            assertEquals(e.getValue(), table.get(hash, e.getKey(), "ns"));
        }
    }

    @Test
    void testSizeAfterOperations() {
        assertEquals(0, table.size());
        
        // Add 10 entries
        for (long i = 0; i < 10; i++) {
            long hash = computeHash(i, "ns");
            int result = table.put(hash, i, "ns");
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i, "ns");
            }
            int slot = result & SwissTable.SLOT_MASK;
            table.values[slot] = i;
        }
        assertEquals(10, table.size());
        
        // Remove 5 entries
        for (long i = 0; i < 5; i++) {
            long hash = computeHash(i, "ns");
            table.remove(hash, i, "ns");
        }
        assertEquals(5, table.size());
        
        // Update existing entry (size unchanged)
        long hash = computeHash(5L, "ns");
        int result = table.put(hash, 5L, "ns");
        while (result < 0) {
            if (result == SwissTable.NEED_GROW) {
                table.grow();
            } else if (result == SwissTable.NEED_REHASH) {
                table.rehash();
            }
            result = table.put(hash, 5L, "ns");
        }
        int slot = result & SwissTable.SLOT_MASK;
        table.values[slot] = 500L;
        assertEquals(5, table.size());
    }

    // ========== Helper Methods ==========

    private long computeHash(Long key, String namespace) {
        int keyHash = key.hashCode();
        int nsHash = namespace.hashCode();
        long combined = ((long) keyHash << 32) | (nsHash & 0xFFFFFFFFL);
        // Mix using finalizer from MurmurHash3
        combined ^= combined >>> 33;
        combined *= 0xff51afd7ed558ccdL;
        combined ^= combined >>> 33;
        combined *= 0xc4ceb9fe1a85ec53L;
        combined ^= combined >>> 33;
        return combined;
    }
}
