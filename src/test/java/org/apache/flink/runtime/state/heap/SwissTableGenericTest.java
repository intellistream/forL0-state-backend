package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SwissTableGeneric fallback implementation.
 * 
 * <p>Tests the generic implementation used for unsupported type combinations.
 */
class SwissTableGenericTest {

    private ForL0StateMap<Integer, String, Double> stateMap;

    @BeforeEach
    void setUp() {
        // Use no-arg constructor which falls back to Generic
        stateMap = new ForL0StateMap<>();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (stateMap != null) {
            stateMap.close();
        }
    }

    @Test
    void testBasicPutAndGet() {
        Integer key = 123;
        String ns = "namespace1";
        Double value = 3.14;

        assertNull(stateMap.get(key, ns));
        stateMap.put(key, ns, value);
        assertEquals(value, stateMap.get(key, ns));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testMultiplePutAndGet() {
        for (int i = 0; i < 100; i++) {
            stateMap.put(i, "ns" + i, (double) i);
        }

        assertEquals(100, stateMap.size());

        for (int i = 0; i < 100; i++) {
            assertEquals((double) i, stateMap.get(i, "ns" + i));
        }
    }

    @Test
    void testSameKeyDifferentNamespace() {
        Integer key = 42;

        stateMap.put(key, "ns1", 1.0);
        stateMap.put(key, "ns2", 2.0);
        stateMap.put(key, "ns3", 3.0);

        assertEquals(3, stateMap.size());
        assertEquals(1.0, stateMap.get(key, "ns1"));
        assertEquals(2.0, stateMap.get(key, "ns2"));
        assertEquals(3.0, stateMap.get(key, "ns3"));
    }

    @Test
    void testUpdate() {
        Integer key = 999;
        String ns = "testNs";

        stateMap.put(key, ns, 1.0);
        assertEquals(1.0, stateMap.get(key, ns));

        stateMap.put(key, ns, 2.0);
        assertEquals(2.0, stateMap.get(key, ns));
        assertEquals(1, stateMap.size());
    }

    @Test
    void testRemove() {
        Integer key = 42;
        String ns = "removeNs";

        stateMap.put(key, ns, 99.9);
        assertEquals(1, stateMap.size());

        stateMap.remove(key, ns);
        assertNull(stateMap.get(key, ns));
        assertEquals(0, stateMap.size());
    }

    @Test
    void testContainsKey() {
        Integer key = 777;
        String ns = "existsNs";

        assertFalse(stateMap.containsKey(key, ns));
        stateMap.put(key, ns, 123.456);
        assertTrue(stateMap.containsKey(key, ns));
    }

    @Test
    void testLargeScale() {
        int count = 10000;

        for (int i = 0; i < count; i++) {
            stateMap.put(i, "ns", (double) i);
        }

        assertEquals(count, stateMap.size());

        for (int i = 0; i < count; i++) {
            assertEquals((double) i, stateMap.get(i, "ns"));
        }

        // Remove half
        for (int i = 0; i < count / 2; i++) {
            stateMap.remove(i, "ns");
        }

        assertEquals(count / 2, stateMap.size());
    }

    @Test
    void testNullValue() {
        Integer key = 1;
        String ns = "ns";

        // Put null value - should work, get returns null
        stateMap.put(key, ns, null);
        assertNull(stateMap.get(key, ns));
    }

    @Test
    void testIteration() {
        stateMap.put(1, "a", 1.0);
        stateMap.put(2, "b", 2.0);
        stateMap.put(3, "c", 3.0);

        int count = 0;
        for (StateEntry<Integer, String, Double> entry : stateMap) {
            count++;
            assertNotNull(entry.getKey());
            assertNotNull(entry.getNamespace());
            assertNotNull(entry.getState());
        }
        assertEquals(3, count);
    }

    @Nested
    class CustomTypeTests {
        
        @Test
        void testWithCustomKey() {
            // Test with a custom key type
            ForL0StateMap<CustomKey, String, String> customMap = new ForL0StateMap<>();
            
            CustomKey key1 = new CustomKey(1, "a");
            CustomKey key2 = new CustomKey(2, "b");
            
            customMap.put(key1, "ns", "value1");
            customMap.put(key2, "ns", "value2");
            
            assertEquals("value1", customMap.get(key1, "ns"));
            assertEquals("value2", customMap.get(key2, "ns"));
            
            // Test with equal key (different instance)
            CustomKey key1Copy = new CustomKey(1, "a");
            assertEquals("value1", customMap.get(key1Copy, "ns"));
        }

        @Test
        void testWithCustomNamespace() {
            ForL0StateMap<String, CustomNamespace, Integer> customMap = new ForL0StateMap<>();
            
            CustomNamespace ns1 = new CustomNamespace("region1", 100);
            CustomNamespace ns2 = new CustomNamespace("region2", 200);
            
            customMap.put("key", ns1, 1);
            customMap.put("key", ns2, 2);
            
            assertEquals(2, customMap.size());
            assertEquals(1, customMap.get("key", ns1));
            assertEquals(2, customMap.get("key", ns2));
        }
    }

    // Custom key class for testing
    static class CustomKey {
        final int id;
        final String name;

        CustomKey(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CustomKey customKey = (CustomKey) o;
            return id == customKey.id && name.equals(customKey.name);
        }

        @Override
        public int hashCode() {
            return 31 * id + name.hashCode();
        }
    }

    // Custom namespace class for testing
    static class CustomNamespace {
        final String region;
        final int partition;

        CustomNamespace(String region, int partition) {
            this.region = region;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CustomNamespace that = (CustomNamespace) o;
            return partition == that.partition && region.equals(that.region);
        }

        @Override
        public int hashCode() {
            return 31 * region.hashCode() + partition;
        }
    }
}
