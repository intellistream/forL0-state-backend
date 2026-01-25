package org.apache.flink.state.forl0;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SwissTableLong}.
 * 
 * <p>Tests cover specialized Long key operations with Group-Interleaved layout:
 * primitive long key comparison, SWAR parallel matching, grow, and rehash.
 */
class SwissTableLongTest {

    private static final int INITIAL_CAPACITY = 64;
    
    private SwissTableLong<Long> table;

    @BeforeEach
    void setUp() {
        table = new SwissTableLong<>(INITIAL_CAPACITY);
    }

    // ========== Basic Operations ==========

    @Test
    void testPutAndGet() {
        long key = 12345L;
        int hash = computeHash(key);
        int result = table.put(hash, key);
        
        assertTrue((result & SwissTableLong.NEW_FLAG) != 0, "Should be new insertion");
        int slot = result & SwissTableLong.SLOT_MASK;
        table.values[slot] = 100L;
        
        Long value = table.get(hash, key);
        assertEquals(100L, value);
    }

    @Test
    void testPutUpdate() {
        long key = 12345L;
        int hash = computeHash(key);
        
        // First insert
        int result1 = table.put(hash, key);
        assertTrue((result1 & SwissTableLong.NEW_FLAG) != 0);
        int slot1 = result1 & SwissTableLong.SLOT_MASK;
        table.values[slot1] = 100L;
        
        // Update same key
        int result2 = table.put(hash, key);
        assertFalse((result2 & SwissTableLong.NEW_FLAG) != 0, "Should be update, not new");
        int slot2 = result2 & SwissTableLong.SLOT_MASK;
        assertEquals(slot1, slot2, "Should return same slot");
        
        table.values[slot2] = 200L;
        assertEquals(200L, table.get(hash, key));
    }

    @Test
    void testGetNonExistent() {
        long key = 999L;
        int hash = computeHash(key);
        assertNull(table.get(hash, key));
    }

    @Test
    void testRemove() {
        long key = 12345L;
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableLong.SLOT_MASK;
        table.values[slot] = 100L;
        
        Long removed = table.remove(hash, key);
        assertEquals(100L, removed);
        assertNull(table.get(hash, key));
    }

    @Test
    void testRemoveNonExistent() {
        long key = 999L;
        int hash = computeHash(key);
        assertNull(table.remove(hash, key));
    }

    @Test
    void testContainsKey() {
        long key = 12345L;
        int hash = computeHash(key);
        assertFalse(table.containsKey(hash, key));
        
        int result = table.put(hash, key);
        int slot = result & SwissTableLong.SLOT_MASK;
        table.values[slot] = 100L;
        
        assertTrue(table.containsKey(hash, key));
        
        table.remove(hash, key);
        assertFalse(table.containsKey(hash, key));
    }

    @Test
    void testIsEmpty() {
        assertTrue(table.isEmpty());
        
        long key = 12345L;
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableLong.SLOT_MASK;
        table.values[slot] = 100L;
        
        assertFalse(table.isEmpty());
        
        table.remove(hash, key);
        assertTrue(table.isEmpty());
    }

    // ========== Primitive Long Key Tests ==========

    @Test
    void testNegativeKeys() {
        // Test negative long keys
        for (long i = -100; i < 0; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            while (result < 0) {
                if (result == SwissTableLong.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableLong.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i);
            }
            
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = i * 10;
        }
        
        for (long i = -100; i < 0; i++) {
            int hash = computeHash(i);
            assertEquals(i * 10, table.get(hash, i));
        }
    }

    @Test
    void testLargeKeys() {
        // Test large long values (beyond int range)
        long[] largeKeys = {
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MAX_VALUE - 1,
            Long.MIN_VALUE + 1,
            1L << 40,
            -(1L << 50)
        };
        
        for (int i = 0; i < largeKeys.length; i++) {
            long key = largeKeys[i];
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = (long) i;
        }
        
        for (int i = 0; i < largeKeys.length; i++) {
            long key = largeKeys[i];
            int hash = computeHash(key);
            assertEquals((long) i, table.get(hash, key));
        }
    }

    // ========== Multiple Entries ==========

    @Test
    void testMultipleEntries() {
        Map<Long, Long> expected = new HashMap<>();
        
        for (long i = 0; i < 50; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            while (result < 0) {
                if (result == SwissTableLong.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableLong.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i);
            }
            
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }
        
        assertEquals(50, table.size());
        
        for (long i = 0; i < 50; i++) {
            int hash = computeHash(i);
            assertEquals(expected.get(i), table.get(hash, i));
        }
    }

    // ========== Growth and Rehash ==========

    @Test
    void testGrow() {
        // Fill table to trigger growth
        // With 87.5% load factor: maxOcc = 64 * 7 / 8 = 56
        int count = 0;
        for (long i = 0; count < 57; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            if (result == SwissTableLong.NEED_GROW) {
                table.grow();
                result = table.put(hash, i);
            }
            
            if ((result & SwissTableLong.NEW_FLAG) != 0) {
                int slot = result & SwissTableLong.SLOT_MASK;
                table.values[slot] = i * 10;
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
        // Fill table
        for (long i = 0; i < 55; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = i;
        }
        
        // Delete half
        for (long i = 0; i < 27; i++) {
            int hash = computeHash(i);
            table.remove(hash, i);
        }
        
        assertEquals(28, table.size());
        
        // Keep inserting until rehash or grow is triggered
        for (long i = 100; i < 150; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            if (result == SwissTableLong.NEED_REHASH) {
                table.rehash();
                assertEquals(0, table.tomb, "Rehash should clear tombstones");
                result = table.put(hash, i);
            } else if (result == SwissTableLong.NEED_GROW) {
                table.grow();
                result = table.put(hash, i);
            }
            
            if ((result & SwissTableLong.NEW_FLAG) != 0) {
                int slot = result & SwissTableLong.SLOT_MASK;
                table.values[slot] = i;
            }
        }
        
        // Verify remaining entries
        for (long i = 27; i < 55; i++) {
            int hash = computeHash(i);
            assertEquals(i, table.get(hash, i));
        }
    }

    // ========== Iterator ==========

    @Test
    void testIterator() {
        Map<Long, Long> expected = new HashMap<>();
        
        for (long i = 0; i < 30; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }
        
        Map<Long, Long> actual = new HashMap<>();
        Iterator<SwissTableLong.Entry<Long>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTableLong.Entry<Long> entry = iter.next();
            actual.put(entry.getKeyBoxed(), entry.getState());
        }
        
        assertEquals(expected, actual);
    }

    @Test
    void testIteratorPrimitiveKey() {
        // Test primitive key access via iterator
        long[] keys = {1L, 100L, 10000L, Long.MAX_VALUE};
        
        for (long key : keys) {
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = key;
        }
        
        int count = 0;
        Iterator<SwissTableLong.Entry<Long>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTableLong.Entry<Long> entry = iter.next();
            long key = entry.getKey();  // Primitive access
            assertEquals(key, table.get(computeHash(key), key));
            count++;
        }
        assertEquals(4, count);
    }

    // ========== Hash Collision ==========

    @Test
    void testHashCollision() {
        Random rand = new Random(42);
        Map<Long, Long> entries = new HashMap<>();
        
        for (int i = 0; i < 100; i++) {
            long key = rand.nextLong();
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            while (result < 0) {
                if (result == SwissTableLong.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableLong.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key);
            }
            
            if ((result & SwissTableLong.NEW_FLAG) != 0) {
                int slot = result & SwissTableLong.SLOT_MASK;
                table.values[slot] = key;
                entries.put(key, key);
            }
        }
        
        for (Map.Entry<Long, Long> e : entries.entrySet()) {
            int hash = computeHash(e.getKey());
            assertEquals(e.getValue(), table.get(hash, e.getKey()));
        }
    }

    // ========== Size Tracking ==========

    @Test
    void testSizeAfterOperations() {
        assertEquals(0, table.size());
        assertTrue(table.isEmpty());
        
        // Add entries
        for (long i = 0; i < 10; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = i;
        }
        assertEquals(10, table.size());
        assertFalse(table.isEmpty());
        
        // Remove some
        for (long i = 0; i < 5; i++) {
            int hash = computeHash(i);
            table.remove(hash, i);
        }
        assertEquals(5, table.size());
        
        // Update existing (size unchanged)
        int hash = computeHash(5L);
        int result = table.put(hash, 5L);
        int slot = result & SwissTableLong.SLOT_MASK;
        table.values[slot] = 500L;
        assertEquals(5, table.size());
    }

    // ========== Helper Methods ==========

    /**
     * Compute 32-bit hash for long key.
     * Uses same logic as SwissTableLong: (int)(key ^ (key >>> 32)) + smear
     */
    private int computeHash(long key) {
        int h = (int) (key ^ (key >>> 32));
        return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
    }
}
