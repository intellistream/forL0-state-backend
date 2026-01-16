package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SwissTableStringTimeWindow specialized implementation.
 * 
 * <p>Tests the optimized storage with String keys and TimeWindow namespace.
 */
class SwissTableStringTimeWindowTest {

    private ForL0StateMap<String, TimeWindow, String> stateMap;

    @BeforeEach
    void setUp() {
        stateMap = new ForL0StateMap<>(String.class, TimeWindow.class);
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
        TimeWindow window = new TimeWindow(0, 1000);
        String value = "testValue";

        assertNull(stateMap.get(key, window));
        stateMap.put(key, window, value);
        assertEquals(value, stateMap.get(key, window));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testSameKeyDifferentWindows() {
        String key = "sameKey";
        TimeWindow w1 = new TimeWindow(0, 1000);
        TimeWindow w2 = new TimeWindow(1000, 2000);
        TimeWindow w3 = new TimeWindow(2000, 3000);

        stateMap.put(key, w1, "window1");
        stateMap.put(key, w2, "window2");
        stateMap.put(key, w3, "window3");

        assertEquals(3, stateMap.size());
        assertEquals("window1", stateMap.get(key, w1));
        assertEquals("window2", stateMap.get(key, w2));
        assertEquals("window3", stateMap.get(key, w3));
    }

    @Test
    void testDifferentKeysSameWindow() {
        TimeWindow window = new TimeWindow(0, 60000);

        for (int i = 0; i < 100; i++) {
            stateMap.put("key" + i, window, "value" + i);
        }

        assertEquals(100, stateMap.size());

        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, stateMap.get("key" + i, window));
        }
    }

    @Test
    void testUpdate() {
        String key = "updateKey";
        TimeWindow window = new TimeWindow(5000, 6000);

        stateMap.put(key, window, "initial");
        assertEquals("initial", stateMap.get(key, window));

        stateMap.put(key, window, "updated");
        assertEquals("updated", stateMap.get(key, window));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testRemove() {
        String key = "removeKey";
        TimeWindow window = new TimeWindow(0, 1000);

        stateMap.put(key, window, "toRemove");
        assertEquals(1, stateMap.size());

        stateMap.remove(key, window);
        assertNull(stateMap.get(key, window));
        assertEquals(0, stateMap.size());
    }

    @Test
    void testContainsKey() {
        String key = "existsKey";
        TimeWindow window = new TimeWindow(10000, 20000);

        assertFalse(stateMap.containsKey(key, window));
        stateMap.put(key, window, "exists");
        assertTrue(stateMap.containsKey(key, window));
    }

    @Test
    void testLargeScale() {
        int count = 5000;

        for (int i = 0; i < count; i++) {
            TimeWindow window = new TimeWindow(i * 1000L, (i + 1) * 1000L);
            stateMap.put("key" + i, window, "val" + i);
        }

        assertEquals(count, stateMap.size());

        for (int i = 0; i < count; i++) {
            TimeWindow window = new TimeWindow(i * 1000L, (i + 1) * 1000L);
            assertEquals("val" + i, stateMap.get("key" + i, window));
        }
    }

    @Test
    void testSpecialCharacterKeys() {
        TimeWindow window = new TimeWindow(0, 1000);

        stateMap.put("key with spaces", window, "spaces");
        stateMap.put("中文键", window, "chinese");
        stateMap.put("🔑emoji", window, "emoji");
        stateMap.put("", window, "empty");

        assertEquals("spaces", stateMap.get("key with spaces", window));
        assertEquals("chinese", stateMap.get("中文键", window));
        assertEquals("emoji", stateMap.get("🔑emoji", window));
        assertEquals("empty", stateMap.get("", window));
    }

    @Test
    void testWindowEdgeCases() {
        String key = "edgeKey";

        TimeWindow zeroLength = new TimeWindow(1000, 1000);
        stateMap.put(key, zeroLength, "zeroLen");
        assertEquals("zeroLen", stateMap.get(key, zeroLength));

        TimeWindow largeWindow = new TimeWindow(0, Long.MAX_VALUE);
        stateMap.put(key, largeWindow, "large");
        assertEquals("large", stateMap.get(key, largeWindow));
    }

    @Test
    void testIteration() {
        stateMap.put("one", new TimeWindow(0, 1000), "1");
        stateMap.put("two", new TimeWindow(1000, 2000), "2");
        stateMap.put("three", new TimeWindow(2000, 3000), "3");

        int count = 0;
        for (StateEntry<String, TimeWindow, String> entry : stateMap) {
            count++;
            assertNotNull(entry.getKey());
            assertTrue(entry.getKey() instanceof String);
            assertNotNull(entry.getNamespace());
            assertTrue(entry.getNamespace() instanceof TimeWindow);
            assertNotNull(entry.getState());
        }
        assertEquals(3, count);
    }

    @Test
    void testMixedOperations() {
        TimeWindow w1 = new TimeWindow(0, 1000);
        TimeWindow w2 = new TimeWindow(1000, 2000);

        // Insert
        stateMap.put("a", w1, "a1");
        stateMap.put("a", w2, "a2");
        stateMap.put("b", w1, "b1");
        assertEquals(3, stateMap.size());

        // Update
        stateMap.put("a", w1, "a1-updated");
        assertEquals(3, stateMap.size());
        assertEquals("a1-updated", stateMap.get("a", w1));

        // Remove
        stateMap.remove("b", w1);
        assertEquals(2, stateMap.size());
        assertNull(stateMap.get("b", w1));

        // Reinsert
        stateMap.put("b", w1, "b1-new");
        assertEquals(3, stateMap.size());
        assertEquals("b1-new", stateMap.get("b", w1));
    }
}
