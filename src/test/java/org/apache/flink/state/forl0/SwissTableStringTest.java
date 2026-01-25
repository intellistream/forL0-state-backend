package org.apache.flink.state.forl0;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SwissTableString}.
 * 
 * <p>Tests cover specialized String key operations with strong-typed String[] array:
 * String.equals() comparison, SWAR parallel matching, grow, and rehash.
 */
class SwissTableStringTest {

    private static final int INITIAL_CAPACITY = 64;
    
    private SwissTableString<String> table;

    @BeforeEach
    void setUp() {
        table = new SwissTableString<>(INITIAL_CAPACITY);
    }

    // ========== Basic Operations ==========

    @Test
    void testPutAndGet() {
        String key = "hello";
        int hash = computeHash(key);
        int result = table.put(hash, key);
        
        assertTrue((result & SwissTableString.NEW_FLAG) != 0, "Should be new insertion");
        int slot = result & SwissTableString.SLOT_MASK;
        table.values[slot] = "world";
        
        String value = table.get(hash, key);
        assertEquals("world", value);
    }

    @Test
    void testPutUpdate() {
        String key = "hello";
        int hash = computeHash(key);
        
        // First insert
        int result1 = table.put(hash, key);
        assertTrue((result1 & SwissTableString.NEW_FLAG) != 0);
        int slot1 = result1 & SwissTableString.SLOT_MASK;
        table.values[slot1] = "world";
        
        // Update same key
        int result2 = table.put(hash, key);
        assertFalse((result2 & SwissTableString.NEW_FLAG) != 0, "Should be update, not new");
        int slot2 = result2 & SwissTableString.SLOT_MASK;
        assertEquals(slot1, slot2, "Should return same slot");
        
        table.values[slot2] = "universe";
        assertEquals("universe", table.get(hash, key));
    }

    @Test
    void testGetNonExistent() {
        String key = "nonexistent";
        int hash = computeHash(key);
        assertNull(table.get(hash, key));
    }

    @Test
    void testRemove() {
        String key = "hello";
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableString.SLOT_MASK;
        table.values[slot] = "world";
        
        String removed = table.remove(hash, key);
        assertEquals("world", removed);
        assertNull(table.get(hash, key));
    }

    @Test
    void testRemoveNonExistent() {
        String key = "nonexistent";
        int hash = computeHash(key);
        assertNull(table.remove(hash, key));
    }

    @Test
    void testContainsKey() {
        String key = "hello";
        int hash = computeHash(key);
        assertFalse(table.containsKey(hash, key));
        
        int result = table.put(hash, key);
        int slot = result & SwissTableString.SLOT_MASK;
        table.values[slot] = "world";
        
        assertTrue(table.containsKey(hash, key));
        
        table.remove(hash, key);
        assertFalse(table.containsKey(hash, key));
    }

    @Test
    void testIsEmpty() {
        assertTrue(table.isEmpty());
        
        String key = "hello";
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableString.SLOT_MASK;
        table.values[slot] = "world";
        
        assertFalse(table.isEmpty());
        
        table.remove(hash, key);
        assertTrue(table.isEmpty());
    }

    // ========== String-Specific Tests ==========

    @Test
    void testEmptyString() {
        String key = "";
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableString.SLOT_MASK;
        table.values[slot] = "empty key value";
        
        assertEquals("empty key value", table.get(hash, key));
    }

    @Test
    void testUnicodeStrings() {
        String[] unicodeKeys = {
            "你好世界",
            "こんにちは",
            "🚀🎉✨",
            "Ελληνικά",
            "العربية"
        };
        
        for (int i = 0; i < unicodeKeys.length; i++) {
            String key = unicodeKeys[i];
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableString.SLOT_MASK;
            table.values[slot] = "value_" + i;
        }
        
        for (int i = 0; i < unicodeKeys.length; i++) {
            String key = unicodeKeys[i];
            int hash = computeHash(key);
            assertEquals("value_" + i, table.get(hash, key));
        }
    }

    @Test
    void testLongStrings() {
        // Test with very long strings
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("abcdefghij");
        }
        String longKey = sb.toString();
        
        int hash = computeHash(longKey);
        int result = table.put(hash, longKey);
        int slot = result & SwissTableString.SLOT_MASK;
        table.values[slot] = "long key value";
        
        assertEquals("long key value", table.get(hash, longKey));
    }

    @Test
    void testSameHashCodeDifferentString() {
        // "FB" and "Ea" have same hashCode in Java
        String key1 = "FB";
        String key2 = "Ea";
        assertEquals(key1.hashCode(), key2.hashCode(), "These should have same hashCode");
        
        int hash1 = computeHash(key1);
        int hash2 = computeHash(key2);
        
        int result1 = table.put(hash1, key1);
        int slot1 = result1 & SwissTableString.SLOT_MASK;
        table.values[slot1] = "value1";
        
        int result2 = table.put(hash2, key2);
        int slot2 = result2 & SwissTableString.SLOT_MASK;
        table.values[slot2] = "value2";
        
        // Both should be retrievable
        assertEquals("value1", table.get(hash1, key1));
        assertEquals("value2", table.get(hash2, key2));
    }

    // ========== Multiple Entries ==========

    @Test
    void testMultipleEntries() {
        Map<String, String> expected = new HashMap<>();
        
        for (int i = 0; i < 50; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            while (result < 0) {
                if (result == SwissTableString.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableString.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key);
            }
            
            int slot = result & SwissTableString.SLOT_MASK;
            String value = "value_" + i;
            table.values[slot] = value;
            expected.put(key, value);
        }
        
        assertEquals(50, table.size());
        
        for (int i = 0; i < 50; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            assertEquals(expected.get(key), table.get(hash, key));
        }
    }

    // ========== Growth and Rehash ==========

    @Test
    void testGrow() {
        // Fill table to trigger growth
        int count = 0;
        for (int i = 0; count < 57; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            if (result == SwissTableString.NEED_GROW) {
                table.grow();
                result = table.put(hash, key);
            }
            
            if ((result & SwissTableString.NEW_FLAG) != 0) {
                int slot = result & SwissTableString.SLOT_MASK;
                table.values[slot] = "value_" + i;
                count++;
            }
        }
        
        assertTrue(table.getCapacity() > INITIAL_CAPACITY, "Table should have grown");
        
        // Verify all entries
        for (int i = 0; i < count; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            assertEquals("value_" + i, table.get(hash, key));
        }
    }

    @Test
    void testRehashAfterDeletes() {
        // Fill table
        for (int i = 0; i < 55; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableString.SLOT_MASK;
            table.values[slot] = "value_" + i;
        }
        
        // Delete half
        for (int i = 0; i < 27; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            table.remove(hash, key);
        }
        
        assertEquals(28, table.size());
        
        // Keep inserting until rehash or grow is triggered
        for (int i = 100; i < 150; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            if (result == SwissTableString.NEED_REHASH) {
                table.rehash();
                assertEquals(0, table.tomb, "Rehash should clear tombstones");
                result = table.put(hash, key);
            } else if (result == SwissTableString.NEED_GROW) {
                table.grow();
                result = table.put(hash, key);
            }
            
            if ((result & SwissTableString.NEW_FLAG) != 0) {
                int slot = result & SwissTableString.SLOT_MASK;
                table.values[slot] = "value_" + i;
            }
        }
        
        // Verify remaining entries
        for (int i = 27; i < 55; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            assertEquals("value_" + i, table.get(hash, key));
        }
    }

    // ========== Iterator ==========

    @Test
    void testIterator() {
        Map<String, String> expected = new HashMap<>();
        
        for (int i = 0; i < 30; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableString.SLOT_MASK;
            String value = "value_" + i;
            table.values[slot] = value;
            expected.put(key, value);
        }
        
        Map<String, String> actual = new HashMap<>();
        Iterator<SwissTableString.Entry<String>> iter = table.iterator();
        while (iter.hasNext()) {
            SwissTableString.Entry<String> entry = iter.next();
            actual.put(entry.getKey(), entry.getState());
        }
        
        assertEquals(expected, actual);
    }

    // ========== Hash Collision ==========

    @Test
    void testHashCollision() {
        Random rand = new Random(42);
        Map<String, String> entries = new HashMap<>();
        
        for (int i = 0; i < 100; i++) {
            String key = "random_" + rand.nextLong();
            int hash = computeHash(key);
            int result = table.put(hash, key);
            
            while (result < 0) {
                if (result == SwissTableString.NEED_GROW) {
                    table.grow();
                } else if (result == SwissTableString.NEED_REHASH) {
                    table.rehash();
                }
                result = table.put(hash, key);
            }
            
            if ((result & SwissTableString.NEW_FLAG) != 0) {
                int slot = result & SwissTableString.SLOT_MASK;
                table.values[slot] = key;
                entries.put(key, key);
            }
        }
        
        for (Map.Entry<String, String> e : entries.entrySet()) {
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
            String key = "key_" + i;
            int hash = computeHash(key);
            int result = table.put(hash, key);
            int slot = result & SwissTableString.SLOT_MASK;
            table.values[slot] = "value_" + i;
        }
        assertEquals(10, table.size());
        assertFalse(table.isEmpty());
        
        // Remove some
        for (int i = 0; i < 5; i++) {
            String key = "key_" + i;
            int hash = computeHash(key);
            table.remove(hash, key);
        }
        assertEquals(5, table.size());
        
        // Update existing (size unchanged)
        String key = "key_5";
        int hash = computeHash(key);
        int result = table.put(hash, key);
        int slot = result & SwissTableString.SLOT_MASK;
        table.values[slot] = "updated";
        assertEquals(5, table.size());
    }

    // ========== Null Key Handling ==========

    @Test
    void testNullKeyNotSupported() {
        // Null keys should throw or be rejected
        // String.hashCode() would throw NPE
        assertThrows(NullPointerException.class, () -> {
            String nullKey = null;
            int hash = computeHash(nullKey);
            table.put(hash, nullKey);
        });
    }

    // ========== Helper Methods ==========

    /**
     * Compute 32-bit hash for String key.
     * Uses same logic as SwissTableString: key.hashCode() + smear
     */
    private int computeHash(String key) {
        int h = key.hashCode();
        return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
    }
}
