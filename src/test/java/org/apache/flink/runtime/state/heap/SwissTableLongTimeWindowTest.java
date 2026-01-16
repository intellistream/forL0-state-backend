package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SwissTableLongTimeWindow specialized implementation.
 * 
 * <p>Tests the optimized storage with primitive long keys and TimeWindow namespace.
 */
class SwissTableLongTimeWindowTest {

    private ForL0StateMap<Long, TimeWindow, String> stateMap;

    @BeforeEach
    void setUp() {
        stateMap = new ForL0StateMap<>(Long.class, TimeWindow.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (stateMap != null) {
            stateMap.close();
        }
    }

    @Test
    void testBasicPutAndGet() {
        Long key = 12345L;
        TimeWindow window = new TimeWindow(0, 1000);
        String value = "testValue";

        assertNull(stateMap.get(key, window));
        stateMap.put(key, window, value);
        assertEquals(value, stateMap.get(key, window));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testSameKeyDifferentWindows() {
        Long key = 42L;
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

        for (long i = 0; i < 100; i++) {
            stateMap.put(i, window, "value" + i);
        }

        assertEquals(100, stateMap.size());

        for (long i = 0; i < 100; i++) {
            assertEquals("value" + i, stateMap.get(i, window));
        }
    }

    @Test
    void testUpdate() {
        Long key = 999L;
        TimeWindow window = new TimeWindow(5000, 6000);

        stateMap.put(key, window, "initial");
        assertEquals("initial", stateMap.get(key, window));

        stateMap.put(key, window, "updated");
        assertEquals("updated", stateMap.get(key, window));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testRemove() {
        Long key = 42L;
        TimeWindow window = new TimeWindow(0, 1000);

        stateMap.put(key, window, "toRemove");
        assertEquals(1, stateMap.size());

        stateMap.remove(key, window);
        assertNull(stateMap.get(key, window));
        assertEquals(0, stateMap.size());
    }

    @Test
    void testContainsKey() {
        Long key = 777L;
        TimeWindow window = new TimeWindow(10000, 20000);

        assertFalse(stateMap.containsKey(key, window));
        stateMap.put(key, window, "exists");
        assertTrue(stateMap.containsKey(key, window));
    }

    @Test
    void testLargeScale() {
        int count = 5000;

        // Multiple keys with multiple windows
        for (long i = 0; i < count; i++) {
            TimeWindow window = new TimeWindow(i * 1000, (i + 1) * 1000);
            stateMap.put(i, window, "val" + i);
        }

        assertEquals(count, stateMap.size());

        // Verify all values
        for (long i = 0; i < count; i++) {
            TimeWindow window = new TimeWindow(i * 1000, (i + 1) * 1000);
            assertEquals("val" + i, stateMap.get(i, window));
        }
    }

    @Test
    void testWindowEdgeCases() {
        Long key = 1L;
        
        // Window with same start and end (zero-length)
        TimeWindow zeroLength = new TimeWindow(1000, 1000);
        stateMap.put(key, zeroLength, "zeroLen");
        assertEquals("zeroLen", stateMap.get(key, zeroLength));

        // Very large window
        TimeWindow largeWindow = new TimeWindow(0, Long.MAX_VALUE);
        stateMap.put(key, largeWindow, "large");
        assertEquals("large", stateMap.get(key, largeWindow));

        // Negative start time (should work)
        TimeWindow negativeStart = new TimeWindow(-1000, 0);
        stateMap.put(key, negativeStart, "negative");
        assertEquals("negative", stateMap.get(key, negativeStart));
    }

    @Test
    void testIteration() {
        stateMap.put(1L, new TimeWindow(0, 1000), "one");
        stateMap.put(2L, new TimeWindow(1000, 2000), "two");
        stateMap.put(3L, new TimeWindow(2000, 3000), "three");

        int count = 0;
        for (StateEntry<Long, TimeWindow, String> entry : stateMap) {
            count++;
            assertNotNull(entry.getKey());
            assertNotNull(entry.getNamespace());
            assertTrue(entry.getNamespace() instanceof TimeWindow);
            assertNotNull(entry.getState());
        }
        assertEquals(3, count);
    }

    @Test
    void testRemoveAndReinsert() {
        Long key = 100L;
        TimeWindow window = new TimeWindow(0, 5000);

        stateMap.put(key, window, "first");
        stateMap.remove(key, window);
        stateMap.put(key, window, "second");

        assertEquals("second", stateMap.get(key, window));
        assertEquals(1, stateMap.size());
    }
}
