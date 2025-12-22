package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.heap.space.NativeL0MemoryAllocator;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ForL0StateMap implementation.
 * Tests the core KV functionality including cache behavior and statistics.
 */
class ForL0StateMapTest {

    private L0MemoryAllocator l0Allocator;
    private ForL0StateMap<String, Integer, String> stateMap;

    @BeforeEach
    void setUp() {
        l0Allocator = new NativeL0MemoryAllocator();

        // Create ForL0StateMap with L0 cache enabled
        stateMap = new ForL0StateMap<>(
            l0Allocator,
            3, // L0Table: 8 buckets
            StringSerializer.INSTANCE,
            IntSerializer.INSTANCE,
            StringSerializer.INSTANCE,
            true // L0 cache enabled
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (stateMap != null) {
            stateMap.close();
        }
        if (l0Allocator != null && !l0Allocator.isClosed()) {
            l0Allocator.close();
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

    @Nested
    class CacheTests {

        @Test
        void testL0CacheHit() {
            String key = "cacheKey";
            Integer namespace = 600;
            String value = "cacheValue";

            // Put a value (should go to both MainTable and L0Cache)
            stateMap.put(key, namespace, value);

            // Get L0 statistics before first get
            L0Table.L0TableStats statsBefore = stateMap.getL0Stats();

            // Get the value (should hit L0 cache)
            String retrievedValue = stateMap.get(key, namespace);
            assertEquals(value, retrievedValue);

            // Check L0 statistics after get
            L0Table.L0TableStats statsAfter = stateMap.getL0Stats();
            assertTrue(statsAfter.hitCount > statsBefore.hitCount);
            assertTrue(statsAfter.hitRate > 0);
        }

        @Test
        void testMainTableHitWithL0Promotion() {
            // Create a state map without L0 cache first
            try (ForL0StateMap<String, Integer, String> noCacheMap = new ForL0StateMap<>(
                    null, 3, StringSerializer.INSTANCE, IntSerializer.INSTANCE, StringSerializer.INSTANCE, false)) {

                String key = "promotionKey";
                Integer namespace = 700;
                String value = "promotionValue";

                // Put in no-cache map
                noCacheMap.put(key, namespace, value);
                assertEquals(value, noCacheMap.get(key, namespace));
                
                // Verify no L0 stats for no-cache map
                assertNull(noCacheMap.getL0Stats());
            } catch (Exception e) {
                fail("Exception in no-cache map test: " + e.getMessage());
            }

            // Now test with cache enabled
            String key2 = "promotionKey2";
            Integer namespace2 = 701;
            String value2 = "promotionValue2";

            stateMap.put(key2, namespace2, value2);
            String retrieved = stateMap.get(key2, namespace2);
            assertEquals(value2, retrieved);

            L0Table.L0TableStats stats = stateMap.getL0Stats();
            assertNotNull(stats);
            assertTrue(stats.accessCount > 0);
        }

        @Test
        void testConstructWithCustomL0Policy() throws Exception {
            try (ForL0StateMap<String, Integer, String> custom = new ForL0StateMap<>(
                    l0Allocator,
                    3,
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    true,
                    L0Table.ReplacementPolicy.CLOCK
            )) {
                custom.put("pKey", 42, "v1");
                assertEquals("v1", custom.get("pKey", 42));
                custom.put("pKey", 42, "v2");
                assertEquals("v2", custom.get("pKey", 42));
            }
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testNullValue() {
            String key = "nullValueKey";
            Integer namespace = 800;

            // Put null value
            stateMap.put(key, namespace, null);
            assertEquals(1, stateMap.size());

            // Get null value
            String retrievedValue = stateMap.get(key, namespace);
            assertNull(retrievedValue);
            assertTrue(stateMap.containsKey(key, namespace));
        }

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
        void testL0Statistics() {
            // Put some values
            for (int i = 0; i < 5; i++) {
                stateMap.put("key" + i, i, "value" + i);
            }

            // Get some values (should increase hit counts)
            for (int i = 0; i < 5; i++) {
                stateMap.get("key" + i, i);
            }

            L0Table.L0TableStats l0Stats = stateMap.getL0Stats();
            assertNotNull(l0Stats);
            assertTrue(l0Stats.accessCount > 0);
            assertTrue(l0Stats.hitRate > 0);
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

    // Note: AutoResizeTests removed - MainTable now has fixed initial size of 65536 buckets,
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
        void testTransformWithException() {
            String key = "transformExceptionKey";
            Integer namespace = 1004;

            // Test that exceptions from transformation function are propagated
            assertThrows(RuntimeException.class, () -> {
                stateMap.transform(key, namespace, "error", (previous, value) -> {
                    throw new RuntimeException("Transform error");
                });
            });

            // After exception, behavior matches Flink's CopyOnWriteStateMap:
            // The entry is created (size incremented) but state remains null
            // because transformation.apply() threw before setting state.
            assertEquals(1, stateMap.size());
            assertNull(stateMap.get(key, namespace));  // state is null
            assertTrue(stateMap.containsKey(key, namespace));  // but entry exists
        }

        @Test
        void testTransformCacheConsistency() throws Exception {
            String key = "transformCacheKey";
            Integer namespace = 1005;
            String initialValue = "cache_test";

            // Put initial value (should be in L0 cache)
            stateMap.put(key, namespace, initialValue);

            // Get L0 statistics before transform
            L0Table.L0TableStats statsBefore = stateMap.getL0Stats();

            // Transform the entry
            stateMap.transform(key, namespace, "_cached", (previous, value) -> {
                assertEquals(initialValue, previous);
                return previous + value;
            });

            // Verify the transformation worked
            assertEquals("cache_test_cached", stateMap.get(key, namespace));

            // Get L0 statistics after transform and get
            L0Table.L0TableStats statsAfter = stateMap.getL0Stats();

            // Verify L0 cache was accessed
            assertTrue(statsAfter.accessCount > statsBefore.accessCount);
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

    /**
     * Tests for fast-path optimization with fixed-length types.
     */
    @Nested
    class FastPathTests {

        @Test
        void testFastPathWithLongState() throws Exception {
            // Create a ForL0StateMap with Long state type (should trigger fast path)
            try (ForL0StateMap<String, Integer, Long> longStateMap = new ForL0StateMap<>(
                    l0Allocator,
                    3, // L0Table: 8 buckets
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE,
                    true // L0 cache enabled
            )) {
                // Test put and get
                longStateMap.put("key1", 1, 12345L);
                assertEquals(12345L, (long) longStateMap.get("key1", 1));

                // Test transform
                longStateMap.transform("key1", 1, 100L, (prev, val) -> prev + val);
                assertEquals(12445L, (long) longStateMap.get("key1", 1));

                // Test new entry via transform
                longStateMap.transform("key2", 1, 50L, (prev, val) -> {
                    assertNull(prev);
                    return val * 2;
                });
                assertEquals(100L, (long) longStateMap.get("key2", 1));
            }
        }

        @Test
        void testFastPathWithIntState() throws Exception {
            // Create a ForL0StateMap with Integer state type (should trigger fast path)
            try (ForL0StateMap<String, Integer, Integer> intStateMap = new ForL0StateMap<>(
                    l0Allocator,
                    3, // L0Table: 8 buckets
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    true // L0 cache enabled
            )) {
                // Test put and get
                intStateMap.put("key1", 1, 42);
                assertEquals(42, (int) intStateMap.get("key1", 1));

                // Test transform (simulating word count)
                for (int i = 0; i < 100; i++) {
                    intStateMap.transform("counter", 1, 1, (prev, val) -> {
                        return prev == null ? val : prev + val;
                    });
                }
                assertEquals(100, (int) intStateMap.get("counter", 1));
            }
        }

        @Test
        void testFastPathWithDoubleState() throws Exception {
            // Create a ForL0StateMap with Double state type (should trigger fast path)
            try (ForL0StateMap<String, Integer, Double> doubleStateMap = new ForL0StateMap<>(
                    l0Allocator,
                    3, // L0Table: 8 buckets
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    org.apache.flink.api.common.typeutils.base.DoubleSerializer.INSTANCE,
                    true // L0 cache enabled
            )) {
                // Test put and get
                doubleStateMap.put("pi", 1, 3.14159);
                assertEquals(3.14159, doubleStateMap.get("pi", 1), 0.0001);

                // Test transform (accumulating)
                doubleStateMap.transform("sum", 1, 1.5, (prev, val) -> prev == null ? val : prev + val);
                doubleStateMap.transform("sum", 1, 2.5, (prev, val) -> prev + val);
                doubleStateMap.transform("sum", 1, 3.0, (prev, val) -> prev + val);
                assertEquals(7.0, doubleStateMap.get("sum", 1), 0.0001);
            }
        }

        @Test
        void testFastPathLargeScale() throws Exception {
            // Test fast path with larger data set to ensure stability
            int numEntries = 10000;
            try (ForL0StateMap<String, Integer, Long> longStateMap = new ForL0StateMap<>(
                    l0Allocator,
                    6,  // L0Table: 64 buckets  
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE,
                    true
            )) {
                // Insert many entries
                for (int i = 0; i < numEntries; i++) {
                    longStateMap.put("key" + i, 1, (long) i * i);
                }
                assertEquals(numEntries, longStateMap.size());

                // Verify all entries
                for (int i = 0; i < numEntries; i++) {
                    assertEquals((long) i * i, (long) longStateMap.get("key" + i, 1));
                }

                // Transform all entries
                for (int i = 0; i < numEntries; i++) {
                    final int idx = i;
                    longStateMap.transform("key" + i, 1, 1L, (prev, val) -> {
                        assertEquals((long) idx * idx, (long) prev);
                        return prev + val;
                    });
                }

                // Verify transformed entries
                for (int i = 0; i < numEntries; i++) {
                    assertEquals((long) i * i + 1, (long) longStateMap.get("key" + i, 1));
                }
            }
        }
    }
}
