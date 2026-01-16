package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.VoidNamespace;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SwissTableStringVoid specialized implementation.
 * 
 * <p>Tests the optimized storage with String keys and VoidNamespace.
 */
class SwissTableStringVoidTest {

    private ForL0StateMap<String, VoidNamespace, String> stateMap;

    @BeforeEach
    void setUp() {
        stateMap = new ForL0StateMap<>(String.class, VoidNamespace.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (stateMap != null) {
            stateMap.close();
        }
    }

    @Test
    void testBasicPutAndGet() {
        String key = "testKey";
        VoidNamespace ns = VoidNamespace.INSTANCE;
        String value = "testValue";

        assertNull(stateMap.get(key, ns));
        stateMap.put(key, ns, value);
        assertEquals(value, stateMap.get(key, ns));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testMultiplePutAndGet() {
        VoidNamespace ns = VoidNamespace.INSTANCE;

        for (int i = 0; i < 100; i++) {
            stateMap.put("key" + i, ns, "value" + i);
        }

        assertEquals(100, stateMap.size());

        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, stateMap.get("key" + i, ns));
        }
    }

    @Test
    void testUpdate() {
        String key = "updateKey";
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put(key, ns, "initial");
        assertEquals("initial", stateMap.get(key, ns));

        stateMap.put(key, ns, "updated");
        assertEquals("updated", stateMap.get(key, ns));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testRemove() {
        String key = "removeKey";
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put(key, ns, "toRemove");
        assertEquals(1, stateMap.size());

        stateMap.remove(key, ns);
        assertNull(stateMap.get(key, ns));
        assertEquals(0, stateMap.size());
    }

    @Test
    void testContainsKey() {
        String key = "existsKey";
        VoidNamespace ns = VoidNamespace.INSTANCE;

        assertFalse(stateMap.containsKey(key, ns));
        stateMap.put(key, ns, "exists");
        assertTrue(stateMap.containsKey(key, ns));
    }

    @Test
    void testLargeScale() {
        VoidNamespace ns = VoidNamespace.INSTANCE;
        int count = 10000;

        for (int i = 0; i < count; i++) {
            stateMap.put("key" + i, ns, "val" + i);
        }

        assertEquals(count, stateMap.size());

        // Verify all values
        for (int i = 0; i < count; i++) {
            assertEquals("val" + i, stateMap.get("key" + i, ns));
        }

        // Remove half
        for (int i = 0; i < count / 2; i++) {
            stateMap.remove("key" + i, ns);
        }

        assertEquals(count / 2, stateMap.size());
    }

    @Test
    void testEmptyString() {
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put("", ns, "empty");
        assertEquals("empty", stateMap.get("", ns));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testSpecialCharacters() {
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put("key with spaces", ns, "spaces");
        stateMap.put("key\twith\ttabs", ns, "tabs");
        stateMap.put("key\nwith\nnewlines", ns, "newlines");
        stateMap.put("中文键", ns, "chinese");
        stateMap.put("🔑emoji", ns, "emoji");

        assertEquals("spaces", stateMap.get("key with spaces", ns));
        assertEquals("tabs", stateMap.get("key\twith\ttabs", ns));
        assertEquals("newlines", stateMap.get("key\nwith\nnewlines", ns));
        assertEquals("chinese", stateMap.get("中文键", ns));
        assertEquals("emoji", stateMap.get("🔑emoji", ns));
    }

    @Test
    void testLongString() {
        VoidNamespace ns = VoidNamespace.INSTANCE;
        
        // Very long key
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("a");
        }
        String longKey = sb.toString();

        stateMap.put(longKey, ns, "longValue");
        assertEquals("longValue", stateMap.get(longKey, ns));
    }

    @Test
    void testIteration() {
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put("one", ns, "1");
        stateMap.put("two", ns, "2");
        stateMap.put("three", ns, "3");

        int count = 0;
        for (StateEntry<String, VoidNamespace, String> entry : stateMap) {
            count++;
            assertNotNull(entry.getKey());
            assertEquals(VoidNamespace.INSTANCE, entry.getNamespace());
            assertNotNull(entry.getState());
        }
        assertEquals(3, count);
    }

    @Test
    void testHashCollisions() {
        VoidNamespace ns = VoidNamespace.INSTANCE;
        
        // These strings may have hash collisions depending on hash function
        // The SwissTable should handle them correctly
        String[] keys = {"FB", "Ea", "abc", "bac", "cab"};
        
        for (int i = 0; i < keys.length; i++) {
            stateMap.put(keys[i], ns, "value" + i);
        }

        assertEquals(keys.length, stateMap.size());

        for (int i = 0; i < keys.length; i++) {
            assertEquals("value" + i, stateMap.get(keys[i], ns));
        }
    }
}
