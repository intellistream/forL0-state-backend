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
    
    private SwissTable<Long, Long> table;

    @BeforeEach
    void setUp() {
        table = new SwissTable<>(INITIAL_CAPACITY);
    }

    // ========== Basic Operations ==========

    @Test
    void testPutAndGet() {
        int hash = computeHash(1L);
        int result = table.put(hash, 1L);
        
        assertTrue((result & SwissTable.NEW_FLAG) != 0, "Should be new insertion");
        int slot = result & SwissTable.SLOT_MASK;
        table.entries[(slot << 1) + 1] = 100L;
        
        Long value = table.get(hash, 1L);
        assertEquals(100L, value);
    }

    @Test
    void testPutUpdate() {
        int hash = computeHash(1L);
        
        // First insert
        int result1 = table.put(hash, 1L);
        assertTrue((result1 & SwissTable.NEW_FLAG) != 0);
        int slot1 = result1 & SwissTable.SLOT_MASK;
        table.entries[(slot1 << 1) + 1] = 100L;
        
        // Update same key
        int result2 = table.put(hash, 1L);
        assertFalse((result2 & SwissTable.NEW_FLAG) != 0, "Should be update, not new");
        int slot2 = result2 & SwissTable.SLOT_MASK;
        assertEquals(slot1, slot2, "Should return same slot");
        
        table.entries[(slot2 << 1) + 1] = 200L;
        assertEquals(200L, table.get(hash, 1L));
    }

    @Test
    void testGetNonExistent() {
        int hash = computeHash(999L);
        assertNull(table.get(hash, 999L));
    }

    @Test
    void testRemove() {
        int hash = computeHash(1L);
        int result = table.put(hash, 1L);
        int slot = result & SwissTable.SLOT_MASK;
        table.entries[(slot << 1) + 1] = 100L;
        
        Long removed = table.remove(hash, 1L);
        assertEquals(100L, removed);
        assertNull(table.get(hash, 1L));
    }

    @Test
    void testRemoveNonExistent() {
        int hash = computeHash(999L);
        assertNull(table.remove(hash, 999L));
    }

    @Test
    void testContainsKey() {
        int hash = computeHash(1L);
        assertFalse(table.containsKey(hash, 1L));
        
        int result = table.put(hash, 1L);
        int slot = result & SwissTable.SLOT_MASK;
        table.entries[(slot << 1) + 1] = 100L;
        
        assertTrue(table.containsKey(hash, 1L));
        
        table.remove(hash, 1L);
        assertFalse(table.containsKey(hash, 1L));
    }

    @Test
    void testIsEmpty() {
        assertTrue(table.isEmpty());
        
        int hash = computeHash(1L);
        int result = table.put(hash, 1L);
        int slot = result & SwissTable.SLOT_MASK;
        table.entries[(slot << 1) + 1] = 100L;
        
        assertFalse(table.isEmpty());
        
        table.remove(hash, 1L);
        assertTrue(table.isEmpty());
    }

    // ========== Multiple Entries ==========

    @Test
    void testMultipleEntries() {
        Map<Long, Long> expected = new HashMap<>();
        
        for (long i = 0; i < 50; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            // Handle expansion signals
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i);
            }
            
            int slot = result & SwissTable.SLOT_MASK;
            table.entries[(slot << 1) + 1] = i * 10;
            expected.put(i, i * 10);
        }
        
        assertEquals(50, table.size());
        
        for (long i = 0; i < 50; i++) {
            int hash = computeHash(i);
            assertEquals(expected.get(i), table.get(hash, i));
        }
    }

    @Test
    void testDifferentKeys() {
        // Test multiple keys (no namespaces to worry about now)
        for (int i = 0; i < 10; i++) {
            long key = i * 1000L;
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key);
            }
            
            int slot = result & SwissTable.SLOT_MASK;
            table.entries[(slot << 1) + 1] = (long) i;
        }
        
        assertEquals(10, table.size());
        
        for (int i = 0; i < 10; i++) {
            long key = i * 1000L;
            int hash = computeHash(key);
            assertEquals((long) i, table.get(hash, key));
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
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            if (result == SwissTable.NEED_GROW) {
                table.grow();
                // Retry after grow
                result = table.put(hash, i);
            }
            
            if ((result & SwissTable.NEW_FLAG) != 0) {
                int slot = result & SwissTable.SLOT_MASK;
                table.entries[(slot << 1) + 1] = i * 10;
                count++;
            }
        }
        
        assertTrue(table.getCapacity() > INITIAL_CAPACITY, "Table should have grown");
        
        // Verify all entries
        for (long i = 0; i < count; i++) {
            int hash = computeHash(i);
            assertEquals(i * 10, table.get(hash, i));
        }
    }

    @Test
    void testRehashAfterDeletes() {
        // Fill table more densely to ensure tombstones are created
        // With 87.5% load factor (56 max for capacity 64), fill it close to capacity
        for (long i = 0; i < 55; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTable.SLOT_MASK;
            table.entries[(slot << 1) + 1] = i;
        }
        
        // Delete half - at high load factor, deletions should create tombstones
        for (long i = 0; i < 27; i++) {
            int hash = computeHash(i);
            table.remove(hash, i);
        }
        
        // Table should have ~28 entries left and possibly some tombstones
        assertEquals(28, table.size(), "Should have 28 entries left");
        
        // Keep inserting until rehash or grow is triggered
        for (long i = 100; i < 150; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            if (result == SwissTable.NEED_REHASH) {
                int tombsBefore = table.tomb;
                table.rehash();
                assertEquals(0, table.tomb, "Rehash should clear tombstones");
                result = table.put(hash, i);
            } else if (result == SwissTable.NEED_GROW) {
                table.grow();
                result = table.put(hash, i);
            }
            
            if ((result & SwissTable.NEW_FLAG) != 0) {
                int slot = result & SwissTable.SLOT_MASK;
                table.entries[(slot << 1) + 1] = i;
            }
        }
        
        // Verify remaining entries from original insertions
        for (long i = 27; i < 55; i++) {
            int hash = computeHash(i);
            assertEquals(i, table.get(hash, i), "Entry " + i + " should be present");
        }
    }

    // ========== Iterator ==========

    @Test
    void testIterator() {
        Map<Long, Long> expected = new HashMap<>();
        
        for (long i = 0; i < 30; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTable.SLOT_MASK;
            table.entries[(slot << 1) + 1] = i * 10;
            expected.put(i, i * 10);
        }
        
        Map<Long, Long> actual = new HashMap<>();
        Iterator<SwissTable.Entry<Long, Long>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTable.Entry<Long, Long> entry = iter.next();
            actual.put(entry.getKey(), entry.getState());
        }
        
        assertEquals(expected, actual);
    }

    // ========== Edge Cases ==========

    @Test
    void testCollectKeys() {
        for (long i = 0; i < 10; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTable.SLOT_MASK;
            table.entries[(slot << 1) + 1] = i;
        }
        
        java.util.List<Long> keys = new java.util.ArrayList<>();
        table.collectKeys(keys);
        assertEquals(10, keys.size());
    }

    @Test
    void testHashCollision() {
        // Create entries with same h2 (low 7 bits of hash)
        // They should be stored correctly via probing
        Random rand = new Random(42);
        Map<Long, Long> entries = new HashMap<>();
        
        for (int i = 0; i < 100; i++) {
            long key = rand.nextLong();
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key);
            }
            
            if ((result & SwissTable.NEW_FLAG) != 0) {
                int slot = result & SwissTable.SLOT_MASK;
                table.entries[(slot << 1) + 1] = key;
                entries.put(key, key);
            }
        }
        
        // Verify all entries
        for (Map.Entry<Long, Long> e : entries.entrySet()) {
            int hash = computeHash(e.getKey());
            assertEquals(e.getValue(), table.get(hash, e.getKey()));
        }
    }

    @Test
    void testSizeAfterOperations() {
        assertEquals(0, table.size());
        assertTrue(table.isEmpty());
        
        // Add 10 entries
        for (long i = 0; i < 10; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            while (result < 0) {
                if (result == SwissTable.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTable.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i);
            }
            int slot = result & SwissTable.SLOT_MASK;
            table.entries[(slot << 1) + 1] = i;
        }
        assertEquals(10, table.size());
        assertFalse(table.isEmpty());
        
        // Remove 5 entries
        for (long i = 0; i < 5; i++) {
            int hash = computeHash(i);
            table.remove(hash, i);
        }
        assertEquals(5, table.size());
        
        // Update existing entry (size unchanged)
        int hash = computeHash(5L);
        int result = table.put(hash, 5L);
        while (result < 0) {
            if (result == SwissTable.NEED_GROW) {
                table.grow();
            } else if (result == SwissTable.NEED_REHASH) {
                table.rehash();
            }
            result = table.put(hash, 5L);
        }
        int slot = result & SwissTable.SLOT_MASK;
        table.entries[(slot << 1) + 1] = 500L;
        assertEquals(5, table.size());
    }

    // ========== Helper Methods ==========

    /**
     * Compute 32-bit hash for key using smear function (aligned with hash-smith).
     */
    private int computeHash(Long key) {
        int h = key.hashCode();
        return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
    }
}
