package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.VoidNamespace;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SwissTableLongVoid specialized implementation.
 * 
 * <p>Tests the optimized storage with primitive long keys and VoidNamespace.
 */
class SwissTableLongVoidTest {

    private ForL0StateMap<Long, VoidNamespace, String> stateMap;

    @BeforeEach
    void setUp() {
        stateMap = new ForL0StateMap<>(Long.class, VoidNamespace.class);
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
        
        for (long i = 0; i < 100; i++) {
            stateMap.put(i, ns, "value" + i);
        }
        
        assertEquals(100, stateMap.size());
        
        for (long i = 0; i < 100; i++) {
            assertEquals("value" + i, stateMap.get(i, ns));
        }
    }

    @Test
    void testUpdate() {
        Long key = 999L;
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put(key, ns, "initial");
        assertEquals("initial", stateMap.get(key, ns));

        stateMap.put(key, ns, "updated");
        assertEquals("updated", stateMap.get(key, ns));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testRemove() {
        Long key = 42L;
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put(key, ns, "toRemove");
        assertEquals(1, stateMap.size());

        stateMap.remove(key, ns);
        assertNull(stateMap.get(key, ns));
        assertEquals(0, stateMap.size());
    }

    @Test
    void testContainsKey() {
        Long key = 777L;
        VoidNamespace ns = VoidNamespace.INSTANCE;

        assertFalse(stateMap.containsKey(key, ns));
        stateMap.put(key, ns, "exists");
        assertTrue(stateMap.containsKey(key, ns));
    }

    @Test
    void testLargeScale() {
        VoidNamespace ns = VoidNamespace.INSTANCE;
        int count = 10000;

        for (long i = 0; i < count; i++) {
            stateMap.put(i, ns, "val" + i);
        }

        assertEquals(count, stateMap.size());

        // Verify all values
        for (long i = 0; i < count; i++) {
            assertEquals("val" + i, stateMap.get(i, ns));
        }

        // Remove half
        for (long i = 0; i < count / 2; i++) {
            stateMap.remove(i, ns);
        }

        assertEquals(count / 2, stateMap.size());

        // Verify remaining
        for (long i = count / 2; i < count; i++) {
            assertEquals("val" + i, stateMap.get(i, ns));
        }
    }

    @Test
    void testNegativeKeys() {
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put(-1L, ns, "negative1");
        stateMap.put(-1000000L, ns, "negative2");
        stateMap.put(Long.MIN_VALUE, ns, "min");

        assertEquals("negative1", stateMap.get(-1L, ns));
        assertEquals("negative2", stateMap.get(-1000000L, ns));
        assertEquals("min", stateMap.get(Long.MIN_VALUE, ns));
    }

    @Test
    void testEdgeCaseKeys() {
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put(0L, ns, "zero");
        stateMap.put(Long.MAX_VALUE, ns, "max");
        stateMap.put(Long.MIN_VALUE, ns, "min");

        assertEquals("zero", stateMap.get(0L, ns));
        assertEquals("max", stateMap.get(Long.MAX_VALUE, ns));
        assertEquals("min", stateMap.get(Long.MIN_VALUE, ns));
        assertEquals(3, stateMap.size());
    }

    @Test
    void testIteration() {
        VoidNamespace ns = VoidNamespace.INSTANCE;

        stateMap.put(1L, ns, "one");
        stateMap.put(2L, ns, "two");
        stateMap.put(3L, ns, "three");

        int count = 0;
        for (StateEntry<Long, VoidNamespace, String> entry : stateMap) {
            count++;
            assertNotNull(entry.getKey());
            assertEquals(VoidNamespace.INSTANCE, entry.getNamespace());
            assertNotNull(entry.getState());
        }
        assertEquals(3, count);
    }
}
