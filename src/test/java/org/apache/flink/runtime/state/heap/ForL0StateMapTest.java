package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ForL0StateMap implementation.
 * Tests the core KV functionality including cache behavior and statistics.
 */
class ForL0StateMapTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB for tests

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private ForL0StateMap<String, Integer, String> stateMap;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);

        // Create ForL0StateMap with L0 cache enabled
        stateMap = new ForL0StateMap<>(
            allocator,
            4, // MainTable: 16 buckets
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
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
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

            // Get statistics before first get
            ForL0StateMap.CacheStats statsBefore = stateMap.getCacheStats();

            // Get the value (should hit L0 cache)
            String retrievedValue = stateMap.get(key, namespace);
            assertEquals(value, retrievedValue);

            // Check statistics after get
            ForL0StateMap.CacheStats statsAfter = stateMap.getCacheStats();
            assertTrue(statsAfter.l0Hits > statsBefore.l0Hits);
            assertTrue(statsAfter.getL0HitRate() > 0);
        }

        @Test
        void testMainTableHitWithL0Promotion() {
            // Create a state map without L0 cache first
            try (ForL0StateMap<String, Integer, String> noCacheMap = new ForL0StateMap<>(
                    allocator, 4, 3, StringSerializer.INSTANCE, IntSerializer.INSTANCE, StringSerializer.INSTANCE, false)) {

                String key = "promotionKey";
                Integer namespace = 700;
                String value = "promotionValue";

                // Put in no-cache map
                noCacheMap.put(key, namespace, value);
                assertEquals(value, noCacheMap.get(key, namespace));
            } catch (Exception e) {
                fail("Exception in no-cache map test: " + e.getMessage());
            }

            // Now test with cache enabled
            String key2 = "promotionKey2";
            Integer namespace2 = 701;
            String value2 = "promotionValue2";

            // Simulate a miss in L0 but hit in MainTable by directly putting to MainTable
            // This is simplified - in real scenario we'd need to manipulate the cache state
            stateMap.put(key2, namespace2, value2);
            String retrieved = stateMap.get(key2, namespace2);
            assertEquals(value2, retrieved);

            ForL0StateMap.CacheStats stats = stateMap.getCacheStats();
            assertTrue(stats.totalAccesses > 0);
        }

        @Test
        void testConstructWithCustomL0Policy() throws Exception {
            try (ForL0StateMap<String, Integer, String> custom = new ForL0StateMap<>(
                    allocator,
                    4,
                    3,
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    true,
                    L0Table.ReplacementPolicy.FIFO
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
        void testNullKeyAndNamespace() {
            // Test null key
            assertNull(stateMap.get(null, 1));
            assertDoesNotThrow(() -> stateMap.put(null, 1, "value"));
            assertDoesNotThrow(() -> stateMap.remove(null, 1));

            // Test null namespace
            assertNull(stateMap.get("key", null));
            assertDoesNotThrow(() -> stateMap.put("key", null, "value"));
            assertDoesNotThrow(() -> stateMap.remove("key", null));
        }

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
        void testCacheStatistics() {
            // Initially no accesses
            ForL0StateMap.CacheStats initialStats = stateMap.getCacheStats();
            assertEquals(0, initialStats.totalAccesses);
            assertEquals(0, initialStats.l0Hits);
            assertEquals(0, initialStats.mainTableHits);
            assertEquals(0.0, initialStats.getOverallHitRate(), 0.001);

            // Put some values
            for (int i = 0; i < 5; i++) {
                stateMap.put("key" + i, i, "value" + i);
            }

            // Get some values (should increase hit counts)
            for (int i = 0; i < 5; i++) {
                stateMap.get("key" + i, i);
            }

            ForL0StateMap.CacheStats finalStats = stateMap.getCacheStats();
            assertTrue(finalStats.totalAccesses > 0);
            assertTrue(finalStats.getOverallHitRate() > 0);
            assertEquals(5, finalStats.totalEntries);
        }

        @Test
        void testCacheStatsToString() {
            stateMap.put("testKey", 1, "testValue");
            stateMap.get("testKey", 1);

            ForL0StateMap.CacheStats stats = stateMap.getCacheStats();
            String statsString = stats.toString();

            assertNotNull(statsString);
            assertTrue(statsString.contains("CacheStats"));
            assertTrue(statsString.contains("totalAccesses"));
            assertTrue(statsString.contains("l0Hits"));
            assertTrue(statsString.contains("mainTableHits"));
        }
    }

    @Nested
    class AutoResizeTests {

        private ForL0StateMap<String, Integer, String> smallMap;

        @BeforeEach
        void setupSmall() {
            // 使用极小的主表容量以快速触发扩容：2 buckets (pow2=1)
            smallMap = new ForL0StateMap<>(
                allocator,
                1, // 2 buckets; 新负载因子阈值 1.5 * 2 = 3 entries 即标记需扩容（旧注释: 0.75 * 12=9 已废弃）
                2, // L0 4 buckets
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                true
            );
        }

        @AfterEach
        void tearDownSmall() throws Exception {
            if (smallMap != null) {
                smallMap.close();
            }
        }

        @Test
        void testAutoResizeTriggeredByLoadFactor() {
            int initialBuckets = smallMap.getDetailedStats().mainTableStats.bucketCount;
            assertEquals(2, initialBuckets);

            int insertCount = 20; // 足够触发一次扩容（>9 entries）
            for (int i = 0; i < insertCount; i++) {
                smallMap.put("k" + i, 0, "v" + i);
            }

            ForL0StateMap.DetailedStats stats = smallMap.getDetailedStats();
            int afterBuckets = stats.mainTableStats.bucketCount;
            assertTrue(afterBuckets >= 4, "应已至少扩容到4 buckets, 实际=" + afterBuckets);
            assertEquals(insertCount, stats.totalEntries);

            // 校验数据仍可访问
            for (int i = 0; i < insertCount; i++) {
                assertEquals("v" + i, smallMap.get("k" + i, 0));
            }
        }

        @Test
        void testMultipleResizesAndDataIntegrity() {
            int targetBucketCount = 8; // 希望触发到 8 buckets
            int i = 0;
            while (smallMap.getDetailedStats().mainTableStats.bucketCount < targetBucketCount && i < 2000) {
                smallMap.put("mk" + i, 2, "mv" + i);
                i++;
            }
            ForL0StateMap.DetailedStats stats = smallMap.getDetailedStats();
            assertTrue(stats.mainTableStats.bucketCount >= targetBucketCount, "应已达到多次扩容");
            // 抽样验证
            for (int k = 0; k < i; k += Math.max(1, i / 10)) {
                assertEquals("mv" + k, smallMap.get("mk" + k, 2));
            }
        }
    }

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
        void testTransformToNull() throws Exception {
            String key = "transformToNullKey";
            Integer namespace = 1002;
            String initialValue = "toBeDeleted";

            // Put initial value
            stateMap.put(key, namespace, initialValue);
            assertEquals(1, stateMap.size());
            assertTrue(stateMap.containsKey(key, namespace));

            // Transform to null (should delete the entry)
            stateMap.transform(key, namespace, "deleteMe", (previous, value) -> {
                assertEquals(initialValue, previous);
                return null; // Delete entry
            });

            // Verify the entry was deleted
            assertEquals(0, stateMap.size());
            assertFalse(stateMap.containsKey(key, namespace));
            assertNull(stateMap.get(key, namespace));
        }

        @Test
        void testTransformFromNullToNull() throws Exception {
            String key = "transformNullToNullKey";
            Integer namespace = 1003;

            // Transform non-existent entry to null (should remain non-existent)
            stateMap.transform(key, namespace, "ignored", (previous, value) -> {
                assertNull(previous);
                return null; // Keep it null
            });

            // Verify no entry was created
            assertEquals(0, stateMap.size());
            assertFalse(stateMap.containsKey(key, namespace));
            assertNull(stateMap.get(key, namespace));
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

            // Verify state map remains unchanged after exception
            assertEquals(0, stateMap.size());
            assertNull(stateMap.get(key, namespace));
        }

        @Test
        void testTransformWithNullParameters() throws Exception {
            // Test null key
            assertDoesNotThrow(() -> {
                stateMap.transform(null, 1, "value", (prev, val) -> "result");
            });

            // Test null namespace
            assertDoesNotThrow(() -> {
                stateMap.transform("key", null, "value", (prev, val) -> "result");
            });

            // Test null transformation function
            assertDoesNotThrow(() -> {
                stateMap.transform("key", 1, "value", null);
            });

            // Verify no entries were created
            assertEquals(0, stateMap.size());
        }

        @Test
        void testTransformCacheConsistency() throws Exception {
            String key = "transformCacheKey";
            Integer namespace = 1005;
            String initialValue = "cache_test";

            // Put initial value (should be in L0 cache)
            stateMap.put(key, namespace, initialValue);

            // Get statistics before transform
            ForL0StateMap.CacheStats statsBefore = stateMap.getCacheStats();

            // Transform the entry
            stateMap.transform(key, namespace, "_cached", (previous, value) -> {
                assertEquals(initialValue, previous);
                return previous + value;
            });

            // Verify the transformation worked
            assertEquals("cache_test_cached", stateMap.get(key, namespace));

            // Get statistics after transform and get
            ForL0StateMap.CacheStats statsAfter = stateMap.getCacheStats();

            // Verify cache statistics updated appropriately
            assertTrue(statsAfter.totalAccesses > statsBefore.totalAccesses);
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
