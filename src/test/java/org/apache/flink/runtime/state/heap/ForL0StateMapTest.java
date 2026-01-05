package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.internal.InternalKvState;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ForL0StateMap implementation with Swiss Tables architecture.
 */
class ForL0StateMapTest {

    private ForL0StateMap<String, Integer, String> stateMap;

    @BeforeEach
    void setUp() {
        stateMap = new ForL0StateMap<>();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (stateMap != null) {
            stateMap.close();
        }
    }

    @Nested
    class BasicFunctionalityTests {

        @Test
        void testPutAndGet() {
            // Test basic put and get operations
            String key = "testKey";
            Integer namespace = 1;
            String value = "testValue";

            // Initially should be empty
            assertEquals(0, stateMap.size());
            assertNull(stateMap.get(key, namespace));

            // Put a value
            stateMap.put(key, namespace, value);
            assertEquals(1, stateMap.size());

            // Get the value back
            String retrievedValue = stateMap.get(key, namespace);
            assertEquals(value, retrievedValue);
        }

        @Test
        void testPutAndGetMultiple() {
            // Test multiple key-value pairs
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                Integer namespace = i;
                String value = "value" + i;

                stateMap.put(key, namespace, value);
                assertEquals(value, stateMap.get(key, namespace));
            }

            assertEquals(10, stateMap.size());

            // Verify all values are still there
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                Integer namespace = i;
                String expectedValue = "value" + i;
                assertEquals(expectedValue, stateMap.get(key, namespace));
            }
        }

        @Test
        void testUpdate() {
            String key = "updateKey";
            Integer namespace = 100;
            String initialValue = "initialValue";
            String updatedValue = "updatedValue";

            // Put initial value
            stateMap.put(key, namespace, initialValue);
            assertEquals(1, stateMap.size());
            assertEquals(initialValue, stateMap.get(key, namespace));

            // Update the value
            stateMap.put(key, namespace, updatedValue);
            assertEquals(1, stateMap.size()); // Size should remain the same
            assertEquals(updatedValue, stateMap.get(key, namespace));
        }

        @Test
        void testContainsKey() {
            String key = "containsKey";
            Integer namespace = 200;
            String value = "containsValue";

            // Initially should not contain the key
            assertFalse(stateMap.containsKey(key, namespace));

            // Put a value
            stateMap.put(key, namespace, value);

            // Now should contain the key
            assertTrue(stateMap.containsKey(key, namespace));
        }

        @Test
        void testRemove() {
            String key = "removeKey";
            Integer namespace = 300;
            String value = "removeValue";

            // Put a value
            stateMap.put(key, namespace, value);
            assertEquals(1, stateMap.size());
            assertTrue(stateMap.containsKey(key, namespace));

            // Remove the value
            stateMap.remove(key, namespace);
            assertEquals(0, stateMap.size());
            assertFalse(stateMap.containsKey(key, namespace));
            assertNull(stateMap.get(key, namespace));
        }

        @Test
        void testPutAndGetOld() {
            String key = "putAndGetOldKey";
            Integer namespace = 400;
            String initialValue = "initialValue";
            String newValue = "newValue";

            // Put initial value
            stateMap.put(key, namespace, initialValue);

            // Update and get old value
            String oldValue = stateMap.putAndGetOld(key, namespace, newValue);
            assertEquals(initialValue, oldValue);
            assertEquals(newValue, stateMap.get(key, namespace));

            // Test with non-existent key
            String nonExistentOld = stateMap.putAndGetOld("nonExistent", 999, "someValue");
            assertNull(nonExistentOld);
            assertEquals("someValue", stateMap.get("nonExistent", 999));
        }

        @Test
        void testRemoveAndGetOld() {
            String key = "removeAndGetOldKey";
            Integer namespace = 500;
            String value = "removeAndGetOldValue";

            // Put a value
            stateMap.put(key, namespace, value);

            // Remove and get old value
            String oldValue = stateMap.removeAndGetOld(key, namespace);
            assertEquals(value, oldValue);
            assertNull(stateMap.get(key, namespace));
            assertEquals(0, stateMap.size());

            // Test with non-existent key
            String nonExistentOld = stateMap.removeAndGetOld("nonExistent", 999);
            assertNull(nonExistentOld);
        }
    }

    // Note: CacheTests removed - SwissMap architecture replaces L0Table caching
    // with a unified directory-based Swiss Table structure

    @Nested
    class EdgeCaseTests {

        @Test
        void testEmptyStringValues() {
            String key = "emptyKey";
            Integer namespace = 900;
            String emptyValue = "";

            stateMap.put(key, namespace, emptyValue);
            assertEquals(emptyValue, stateMap.get(key, namespace));
            assertTrue(stateMap.containsKey(key, namespace));
        }

        @Test
        void testNamespaceIsolation() {
            String key = "sameKey";
            String value1 = "value1";
            String value2 = "value2";

            // Put same key in different namespaces
            stateMap.put(key, 1, value1);
            stateMap.put(key, 2, value2);

            assertEquals(2, stateMap.size());
            assertEquals(value1, stateMap.get(key, 1));
            assertEquals(value2, stateMap.get(key, 2));

            // Remove one namespace
            stateMap.remove(key, 1);
            assertEquals(1, stateMap.size());
            assertNull(stateMap.get(key, 1));
            assertEquals(value2, stateMap.get(key, 2));
        }
    }

    @Nested
    class StatisticsTests {

        @Test
        void testSwissMapBasicStats() {
            // Put some values
            for (int i = 0; i < 5; i++) {
                stateMap.put("key" + i, i, "value" + i);
            }

            // Verify all entries are accessible
            for (int i = 0; i < 5; i++) {
                assertEquals("value" + i, stateMap.get("key" + i, i));
            }

            // Verify size
            assertEquals(5, stateMap.size());
        }

        @Test
        void testDetailedStatsToString() {
            stateMap.put("testKey", 1, "testValue");
            stateMap.get("testKey", 1);

            ForL0StateMap.DetailedStats stats = stateMap.getDetailedStats();
            String statsString = stats.toString();

            assertNotNull(statsString);
            assertTrue(statsString.contains("DetailedStats"));
            assertTrue(statsString.contains("entries"));
        }
    }

    // Note: AutoResizeTests removed - MainTable now has fixed initial size,
    // making small-scale resize tests impractical. Resize functionality is tested in stress tests.

    @Nested
    class LifecycleTests {

        @Test
        void testClose() {
            // Put some data
            stateMap.put("testKey", 1, "testValue");
            assertEquals(1, stateMap.size());

            // Close should not throw exception
            assertDoesNotThrow(() -> stateMap.close());
        }

        @Test
        void testMultipleClose() {
            // Multiple close calls should be safe
            assertDoesNotThrow(() -> {
                stateMap.close();
                stateMap.close();
            });
        }
    }

    @Nested
    class IterationAndNamespaceTests {

        @Test
        void testGetKeysByNamespace() {
            // ns 1
            stateMap.put("a", 1, "v1");
            stateMap.put("b", 1, "v2");
            // ns 2
            stateMap.put("c", 2, "v3");

            java.util.Set<String> keysNs1 = stateMap.getKeys(1)
                    .collect(java.util.stream.Collectors.toSet());

            assertEquals(2, keysNs1.size());
            assertTrue(keysNs1.contains("a"));
            assertTrue(keysNs1.contains("b"));
            assertFalse(keysNs1.contains("c"));
        }

        @Test
        void testSizeOfNamespace() {
            // ns 10
            stateMap.put("k1", 10, "v1");
            stateMap.put("k2", 10, "v2");
            stateMap.put("k3", 10, "v3");
            // ns 11
            stateMap.put("k4", 11, "v4");

            assertEquals(3, stateMap.sizeOfNamespace(10));
            assertEquals(1, stateMap.sizeOfNamespace(11));
            assertEquals(0, stateMap.sizeOfNamespace(12));
        }

        @Test
        void testIncrementalVisitorBatches() {
            // prepare 7 entries
            for (int i = 0; i < 7; i++) {
                stateMap.put("k" + i, i, "v" + i);
            }
            int batchSize = 3;
            InternalKvState.StateIncrementalVisitor<String, Integer, String> vis =
                    stateMap.getStateIncrementalVisitor(batchSize);

            java.util.Set<String> seen = new java.util.HashSet<>();
            int total = 0;
            while (vis.hasNext()) {
                java.util.Collection<org.apache.flink.runtime.state.StateEntry<String, Integer, String>> batch = vis.nextEntries();
                assertFalse(batch.isEmpty());
                assertTrue(batch.size() <= batchSize);
                for (org.apache.flink.runtime.state.StateEntry<String, Integer, String> e : batch) {
                    assertNotNull(e.getKey());
                    assertNotNull(e.getNamespace());
                    // value may be null, but in our data it isn't
                    seen.add(e.getKey() + "#" + e.getNamespace());
                }
                total += batch.size();
            }
            // vis.close(); // interface may not define close; our implementation is no-op

            assertEquals(7, total);
            assertEquals(7, seen.size());
        }
    }

    @Nested
    class TransformTests {
        // Note: Some transform tests are disabled since we assume the caller is responsible for null checks.

        @Test
        void testTransformNewEntry() throws Exception {
            String key = "transformNewKey";
            Integer namespace = 1000;
            String suffix = "_transformed";

            // Transform on non-existent entry (previous state should be null)
            stateMap.transform(key, namespace, suffix, (previous, value) -> {
                assertNull(previous, "Previous state should be null for new entry");
                return "new" + value;
            });

            // Verify the entry was created with transformed value
            assertEquals(1, stateMap.size());
            assertEquals("new_transformed", stateMap.get(key, namespace));
            assertTrue(stateMap.containsKey(key, namespace));
        }

        @Test
        void testTransformExistingEntry() throws Exception {
            String key = "transformExistingKey";
            Integer namespace = 1001;
            String initialValue = "initial";
            String transformValue = "_updated";

            // Put initial value
            stateMap.put(key, namespace, initialValue);
            assertEquals(1, stateMap.size());

            // Transform existing entry
            stateMap.transform(key, namespace, transformValue, (previous, value) -> {
                assertEquals(initialValue, previous, "Previous state should match initial value");
                return previous + value;
            });

            // Verify the entry was updated
            assertEquals(1, stateMap.size()); // Size should remain the same
            assertEquals("initial_updated", stateMap.get(key, namespace));
        }

        @Test
        void testTransformMultipleEntries() throws Exception {
            // Setup multiple entries
            for (int i = 0; i < 5; i++) {
                stateMap.put("key" + i, i, "value" + i);
            }
            assertEquals(5, stateMap.size());

            // Transform all entries
            for (int i = 0; i < 5; i++) {
                final int index = i;
                stateMap.transform("key" + i, i, "_transformed", (previous, value) -> {
                    assertEquals("value" + index, previous);
                    return previous + value;
                });
            }

            // Verify all transformations
            assertEquals(5, stateMap.size());
            for (int i = 0; i < 5; i++) {
                assertEquals("value" + i + "_transformed", stateMap.get("key" + i, i));
            }
        }

        @Test
        void testTransformCacheConsistency() throws Exception {
            String key = "transformCacheKey";
            Integer namespace = 1005;
            String initialValue = "cache_test";

            // Put initial value
            stateMap.put(key, namespace, initialValue);

            // Transform the entry
            stateMap.transform(key, namespace, "_cached", (previous, value) -> {
                assertEquals(initialValue, previous);
                return previous + value;
            });

            // Verify the transformation worked
            assertEquals("cache_test_cached", stateMap.get(key, namespace));
        }

        @Test
        void testTransformCorrectness() throws Exception {
            String key = "correctnessKey";
            Integer namespace = 1006;
            String initialValue = "performance";

            // Put initial value
            stateMap.put(key, namespace, initialValue);

            // Test transform method correctness
            stateMap.transform(key, namespace, "_fast", (prev, val) -> prev + val);

            // Verify transform result
            assertEquals("performance_fast", stateMap.get(key, namespace));

            // Use a different key for get+put comparison to verify same behavior
            String key2 = "correctnessKey2";
            stateMap.put(key2, namespace, initialValue);

            // Equivalent get+put operation
            String oldValue = stateMap.get(key2, namespace);
            stateMap.put(key2, namespace, oldValue + "_slow");

            // Verify both approaches produce equivalent results (different suffixes but same logic)
            assertEquals("performance_slow", stateMap.get(key2, namespace));

            // Both entries should exist with correct values
            assertEquals(2, stateMap.size());
        }

        @Test
        void testTransformNamespaceIsolation() throws Exception {
            String key = "isolationKey";
            String baseValue = "base";

            // Transform same key in different namespaces
            stateMap.transform(key, 1, "_ns1", (prev, val) -> {
                assertNull(prev);
                return baseValue + val;
            });

            stateMap.transform(key, 2, "_ns2", (prev, val) -> {
                assertNull(prev);
                return baseValue + val;
            });

            // Verify namespace isolation
            assertEquals(2, stateMap.size());
            assertEquals("base_ns1", stateMap.get(key, 1));
            assertEquals("base_ns2", stateMap.get(key, 2));

            // Transform one namespace should not affect the other
            stateMap.transform(key, 1, "_updated", (prev, val) -> prev + val);

            assertEquals("base_ns1_updated", stateMap.get(key, 1));
            assertEquals("base_ns2", stateMap.get(key, 2)); // Should remain unchanged
        }
    }
}
