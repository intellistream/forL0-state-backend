package org.apache.flink.state.forl0;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SwissTableInt}.
 * 
 * <p>Tests cover specialized Integer key operations with packed int storage:
 * 2 ints per long, primitive int comparison, SWAR parallel matching.
 */
class SwissTableIntTest {

    private static final int INITIAL_CAPACITY = 64;
    
    private SwissTableInt<Integer> table;

    @BeforeEach
    void setUp() {
        table = new SwissTableInt<>(INITIAL_CAPACITY);
    }

    // ========== Basic Operations ==========

    @Test
    void testPutAndGet() {
        int key = 12345;
        int hash = computeHash(key);
        int result = table.put(hash, key);
        
        assertTrue((result & SwissTableInt.NEW_FLAG) != 0, "Should be new insertion");
        int slot = result & SwissTableInt.SLOT_MASK;
        table.values[slot] = 100;
        
        Integer value = table.get(hash, key);
        assertEquals(100, value);
    }

    @Test
    void testPutUpdate() {
        int key = 12345;
        int hash = computeHash(key);
        
        // First insert
        int result1 = table.put(hash, key);
        assertTrue((result1 & SwissTableInt.NEW_FLAG) != 0);
        int slot1 = result1 & SwissTableInt.SLOT_MASK;
        table.values[slot1] = 100;
        
        // Update same key
        int result2 = table.put(hash, key);
        assertFalse((result2 & SwissTableInt.NEW_FLAG) != 0, "Should be update, not new");
        int slot2 = result2 & SwissTableInt.SLOT_MASK;
        assertEquals(slot1, slot2, "Should return same slot");
        
        table.values[slot2] = 200;
        assertEquals(200, table.get(hash, key));
    }

    @Test
    void testGetNonExistent() {
        int key = 999;
        int hash = computeHash(key);
        assertNull(table.get(hash, key));
    }

    @Test
    void testRemove() {
        int key = 12345;
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableInt.SLOT_MASK;
        table.values[slot] = 100;
        
        Integer removed = table.remove(hash, key);
        assertEquals(100, removed);
        assertNull(table.get(hash, key));
    }

    @Test
    void testRemoveNonExistent() {
        int key = 999;
        int hash = computeHash(key);
        assertNull(table.remove(hash, key));
    }

    @Test
    void testContainsKey() {
        int key = 12345;
        int hash = computeHash(key);
        assertFalse(table.containsKey(hash, key));
        
        int result = table.put(hash, key);
        int slot = result & SwissTableInt.SLOT_MASK;
        table.values[slot] = 100;
        
        assertTrue(table.containsKey(hash, key));
        
        table.remove(hash, key);
        assertFalse(table.containsKey(hash, key));
    }

    @Test
    void testIsEmpty() {
        assertTrue(table.isEmpty());
        
        int key = 12345;
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableInt.SLOT_MASK;
        table.values[slot] = 100;
        
        assertFalse(table.isEmpty());
        
        table.remove(hash, key);
        assertTrue(table.isEmpty());
    }

    // ========== Primitive Int Key Tests ==========

    @Test
    void testNegativeKeys() {
        // Test negative int keys
        for (int i = -100; i < 0; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            while (result < 0) {
                if (result == SwissTableInt.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableInt.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i);
            }
            
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i * 10;
        }
        
        for (int i = -100; i < 0; i++) {
            int hash = computeHash(i);
            assertEquals(i * 10, table.get(hash, i));
        }
    }

    @Test
    void testExtremeValues() {
        // Test boundary int values
        int[] extremeKeys = {
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE - 1,
            Integer.MIN_VALUE + 1,
            0,
            -1,
            1
        };
        
        for (int i = 0; i < extremeKeys.length; i++) {
            int key = extremeKeys[i];
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i;
        }
        
        for (int i = 0; i < extremeKeys.length; i++) {
            int key = extremeKeys[i];
            int hash = computeHash(key);
            assertEquals(i, table.get(hash, key));
        }
    }

    @Test
    void testPackedIntStorage() {
        // Test that 2 ints per long packing works correctly
        // Insert consecutive keys to fill different lane positions
        for (int i = 0; i < 16; i++) {  // 16 keys = 2 groups worth
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i * 100;
        }
        
        // Verify all packed values are retrieved correctly
        for (int i = 0; i < 16; i++) {
            int hash = computeHash(i);
            assertEquals(i * 100, table.get(hash, i));
        }
    }

    // ========== Multiple Entries ==========

    @Test
    void testMultipleEntries() {
        Map<Integer, Integer> expected = new HashMap<>();
        
        for (int i = 0; i < 50; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            while (result < 0) {
                if (result == SwissTableInt.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableInt.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i);
            }
            
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }
        
        assertEquals(50, table.size());
        
        for (int i = 0; i < 50; i++) {
            int hash = computeHash(i);
            assertEquals(expected.get(i), table.get(hash, i));
        }
    }

    // ========== Growth and Rehash ==========

    @Test
    void testGrow() {
        // Fill table to trigger growth
        int count = 0;
        for (int i = 0; count < 57; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            if (result == SwissTableInt.NEED_GROW) {
                table.grow();
                result = table.put(hash, i);
            }
            
            if ((result & SwissTableInt.NEW_FLAG) != 0) {
                int slot = result & SwissTableInt.SLOT_MASK;
                table.values[slot] = i * 10;
                count++;
            }
        }
        
        assertTrue(table.getCapacity() > INITIAL_CAPACITY, "Table should have grown");
        
        // Verify all entries
        for (int i = 0; i < count; i++) {
            int hash = computeHash(i);
            assertEquals(i * 10, table.get(hash, i));
        }
    }

    @Test
    void testRehashAfterDeletes() {
        // Fill table
        for (int i = 0; i < 55; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i;
        }
        
        // Delete half
        for (int i = 0; i < 27; i++) {
            int hash = computeHash(i);
            table.remove(hash, i);
        }
        
        assertEquals(28, table.size());
        
        // Keep inserting until rehash or grow is triggered
        for (int i = 100; i < 150; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            
            if (result == SwissTableInt.NEED_REHASH) {
                table.rehash();
                assertEquals(0, table.tomb, "Rehash should clear tombstones");
                result = table.put(hash, i);
            } else if (result == SwissTableInt.NEED_GROW) {
                table.grow();
                result = table.put(hash, i);
            }
            
            if ((result & SwissTableInt.NEW_FLAG) != 0) {
                int slot = result & SwissTableInt.SLOT_MASK;
                table.values[slot] = i;
            }
        }
        
        // Verify remaining entries
        for (int i = 27; i < 55; i++) {
            int hash = computeHash(i);
            assertEquals(i, table.get(hash, i));
        }
    }

    // ========== Iterator ==========

    @Test
    void testIterator() {
        Map<Integer, Integer> expected = new HashMap<>();
        
        for (int i = 0; i < 30; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }
        
        Map<Integer, Integer> actual = new HashMap<>();
        Iterator<SwissTableInt.Entry<Integer>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTableInt.Entry<Integer> entry = iter.next();
            actual.put(entry.getKeyBoxed(), entry.getState());
        }
        
        assertEquals(expected, actual);
    }

    @Test
    void testIteratorPrimitiveKey() {
        // Test primitive key access via iterator
        int[] keys = {1, 100, 10000, Integer.MAX_VALUE};
        
        for (int key : keys) {
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = key;
        }
        
        int count = 0;
        Iterator<SwissTableInt.Entry<Integer>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTableInt.Entry<Integer> entry = iter.next();
            int key = entry.getKey();  // Primitive access
            assertEquals(key, table.get(computeHash(key), key));
            count++;
        }
        assertEquals(4, count);
    }

    // ========== forEachEntry ==========

    @Test
    void testForEachEntry() {
        Map<Integer, Integer> expected = new HashMap<>();

        for (int i = 0; i < 30; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }

        Map<Integer, Integer> actual = new HashMap<>();
        table.forEachEntry((key, value) -> actual.put(key, (Integer) value));

        assertEquals(expected, actual);
    }

    @Test
    void testForEachEntryEmpty() {
        int[] count = {0};
        table.forEachEntry((key, value) -> count[0]++);
        assertEquals(0, count[0]);
    }

    @Test
    void testForEachEntryAfterDeletes() {
        for (int i = 0; i < 30; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i * 10;
        }

        for (int i = 0; i < 15; i++) {
            table.remove(computeHash(i), i);
        }

        Map<Integer, Integer> actual = new HashMap<>();
        table.forEachEntry((key, value) -> actual.put(key, (Integer) value));

        assertEquals(15, actual.size());
        for (int i = 15; i < 30; i++) {
            assertEquals(i * 10, actual.get(i));
        }
    }

    @Test
    void testForEachEntryConsistentWithIterator() {
        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            while (result < 0) {
                if (result == SwissTableInt.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableInt.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, i);
            }
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i * 10;
            expected.put(i, i * 10);
        }

        Map<Integer, Integer> iterResult = new HashMap<>();
        Iterator<SwissTableInt.Entry<Integer>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTableInt.Entry<Integer> entry = iter.next();
            iterResult.put(entry.getKeyBoxed(), entry.getState());
        }

        Map<Integer, Integer> forEachResult = new HashMap<>();
        table.forEachEntry((key, value) -> forEachResult.put(key, (Integer) value));

        assertEquals(expected, iterResult);
        assertEquals(expected, forEachResult);
    }

    @Test
    void testForEachEntryExceptionPropagation() {
        int hash = computeHash(1);
        int result = table.put(hash, 1);
        int slot = result & SwissTableInt.SLOT_MASK;
        table.values[slot] = 100;

        assertThrows(IOException.class, () ->
            table.<IOException>forEachEntry((key, value) -> {
                throw new IOException("test exception");
            })
        );
    }

    // ========== Hash Collision ==========

    @Test
    void testHashCollision() {
        Random rand = new Random(42);
        Map<Integer, Integer> entries = new HashMap<>();
        
        for (int i = 0; i < 100; i++) {
            int key = rand.nextInt();
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            while (result < 0) {
                if (result == SwissTableInt.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableInt.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key);
            }
            
            if ((result & SwissTableInt.NEW_FLAG) != 0) {
                int slot = result & SwissTableInt.SLOT_MASK;
                table.values[slot] = key;
                entries.put(key, key);
            }
        }
        
        for (Map.Entry<Integer, Integer> e : entries.entrySet()) {
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
        for (int i = 0; i < 10; i++) {
            int hash = computeHash(i);
            int result = table.put(hash, i);
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = i;
        }
        assertEquals(10, table.size());
        assertFalse(table.isEmpty());
        
        // Remove some
        for (int i = 0; i < 5; i++) {
            int hash = computeHash(i);
            table.remove(hash, i);
        }
        assertEquals(5, table.size());
        
        // Update existing (size unchanged)
        int hash = computeHash(5);
        int result = table.put(hash, 5);
        int slot = result & SwissTableInt.SLOT_MASK;
        table.values[slot] = 500;
        assertEquals(5, table.size());
    }

    // ========== Helper Methods ==========

    /**
     * Compute 32-bit hash for int key.
     * Uses same logic as SwissTableInt: key + smear
     */
    private int computeHash(int key) {
        return (int) (0x1b873593 * Integer.rotateLeft(key * 0xcc9e2d51, 15));
    }
}
