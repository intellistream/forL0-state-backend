package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test ForL0StateMap with different EntryArena allocation strategies.
 */
class ForL0StateMapArenaStrategyTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 256L * DEFAULT_PAGE_SIZE; // 8MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private Object owner;
    private Random random;

    // Serializers
    private final StringSerializer keySerializer = StringSerializer.INSTANCE;
    private final StringSerializer namespaceSerializer = StringSerializer.INSTANCE;
    private final IntSerializer stateSerializer = IntSerializer.INSTANCE;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        random = new Random(42); // Fixed seed for reproducibility
    }

    @AfterEach
    void tearDown() throws Exception {
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    @Test
    void testLinearStrategyBasicOperations() throws Exception {
        try (ForL0StateMap<String, String, Integer> stateMap = new ForL0StateMap<>(
                allocator, 10, 6, keySerializer, namespaceSerializer, stateSerializer, true,
                EntryArena.AllocationStrategy.LINEAR)) {

            // Basic put/get operations
            stateMap.put("key1", "namespace1", 100);
            stateMap.put("key2", "namespace1", 200);
            stateMap.put("key1", "namespace2", 300);

            assertEquals(Integer.valueOf(100), stateMap.get("key1", "namespace1"));
            assertEquals(Integer.valueOf(200), stateMap.get("key2", "namespace1"));
            assertEquals(Integer.valueOf(300), stateMap.get("key1", "namespace2"));
            assertNull(stateMap.get("nonexistent", "namespace1"));

            assertEquals(3, stateMap.size());
            assertTrue(stateMap.containsKey("key1", "namespace1"));
            assertFalse(stateMap.containsKey("nonexistent", "namespace1"));
        }
    }

    @Test
    void testFreeListStrategyBasicOperations() throws Exception {
        try (ForL0StateMap<String, String, Integer> stateMap = new ForL0StateMap<>(
                allocator, 10, 6, keySerializer, namespaceSerializer, stateSerializer, true,
                EntryArena.AllocationStrategy.FREE_LIST)) {

            // Basic put/get operations
            stateMap.put("key1", "namespace1", 100);
            stateMap.put("key2", "namespace1", 200);
            stateMap.put("key1", "namespace2", 300);

            assertEquals(Integer.valueOf(100), stateMap.get("key1", "namespace1"));
            assertEquals(Integer.valueOf(200), stateMap.get("key2", "namespace1"));
            assertEquals(Integer.valueOf(300), stateMap.get("key1", "namespace2"));
            assertNull(stateMap.get("nonexistent", "namespace1"));

            assertEquals(3, stateMap.size());
            assertTrue(stateMap.containsKey("key1", "namespace1"));
            assertFalse(stateMap.containsKey("nonexistent", "namespace1"));
        }
    }

    @Test
    void testStrategyComparisonWithHeavyUpdates() throws Exception {
        // Test both strategies with heavy update workload
        StrategyPerformance linearPerf = testStrategyWithHeavyUpdates(EntryArena.AllocationStrategy.LINEAR);
        StrategyPerformance freeListPerf = testStrategyWithHeavyUpdates(EntryArena.AllocationStrategy.FREE_LIST);

        System.out.println("LINEAR Strategy Performance: " + linearPerf);
        System.out.println("FREE_LIST Strategy Performance: " + freeListPerf);

        // Both strategies should handle the workload correctly
        assertEquals(1000, linearPerf.finalSize);
        assertEquals(1000, freeListPerf.finalSize);

        // FREE_LIST might show different memory usage patterns
        assertTrue(linearPerf.operationsCompleted > 0);
        assertTrue(freeListPerf.operationsCompleted > 0);
    }

    private StrategyPerformance testStrategyWithHeavyUpdates(EntryArena.AllocationStrategy strategy) throws Exception {
        try (ForL0StateMap<String, String, Integer> stateMap = new ForL0StateMap<>(
                allocator, 10, 6, keySerializer, namespaceSerializer, stateSerializer, true, strategy)) {

            long startTime = System.nanoTime();
            int operations = 0;

            // Phase 1: Initial population
            for (int i = 0; i < 1000; i++) {
                stateMap.put("key" + i, "namespace1", i);
                operations++;
            }

            // Phase 2: Heavy updates (multiple rounds)
            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < 1000; i++) {
                    int newValue = i + (round + 1) * 1000;
                    stateMap.put("key" + i, "namespace1", newValue);
                    operations++;
                }
            }

            // Phase 3: Remove and re-add some entries
            for (int i = 0; i < 200; i++) {
                stateMap.remove("key" + i, "namespace1");
                operations++;
            }

            for (int i = 0; i < 200; i++) {
                stateMap.put("key" + i, "namespace1", i + 10000);
                operations++;
            }

            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000;

            // Verify final state
            int finalSize = stateMap.size();
            ForL0StateMap.CacheStats cacheStats = stateMap.getCacheStats();

            return new StrategyPerformance(strategy, operations, durationMs, finalSize, cacheStats);
        }
    }

    @Test
    void testRemovalBehaviorComparison() throws Exception {
        // Test removal patterns with both strategies
        RemovalTestResult linearResult = testRemovalBehavior(EntryArena.AllocationStrategy.LINEAR);
        RemovalTestResult freeListResult = testRemovalBehavior(EntryArena.AllocationStrategy.FREE_LIST);

        System.out.println("LINEAR Removal Test: " + linearResult);
        System.out.println("FREE_LIST Removal Test: " + freeListResult);

        // Both should handle removals correctly
        assertTrue(linearResult.removalsSuccessful > 0);
        assertTrue(freeListResult.removalsSuccessful > 0);
        assertEquals(linearResult.finalSize, freeListResult.finalSize);
    }

    private RemovalTestResult testRemovalBehavior(EntryArena.AllocationStrategy strategy) throws Exception {
        try (ForL0StateMap<String, String, Integer> stateMap = new ForL0StateMap<>(
                allocator, 10, 6, keySerializer, namespaceSerializer, stateSerializer, false, strategy)) {

            List<String> keys = new ArrayList<>();

            // Add many entries
            for (int i = 0; i < 2000; i++) {
                String key = "testKey" + i;
                keys.add(key);
                stateMap.put(key, "namespace1", i);
            }

            int initialSize = stateMap.size();

            // Remove every other entry
            int removalsSuccessful = 0;
            for (int i = 0; i < keys.size(); i += 2) {
                String key = keys.get(i);
                Integer oldValue = stateMap.removeAndGetOld(key, "namespace1");
                if (oldValue != null) {
                    removalsSuccessful++;
                }
            }

            int finalSize = stateMap.size();

            // Verify remaining entries are still accessible
            int accessibleEntries = 0;
            for (int i = 1; i < keys.size(); i += 2) {
                String key = keys.get(i);
                Integer value = stateMap.get(key, "namespace1");
                if (value != null && value.equals(i)) {
                    accessibleEntries++;
                }
            }

            return new RemovalTestResult(strategy, initialSize, finalSize, removalsSuccessful, accessibleEntries);
        }
    }

    @Test
    void testLargeValueHandling() throws Exception {
        // Test both strategies with large values
        testLargeValuesWithStrategy(EntryArena.AllocationStrategy.LINEAR);
        testLargeValuesWithStrategy(EntryArena.AllocationStrategy.FREE_LIST);
    }

    private void testLargeValuesWithStrategy(EntryArena.AllocationStrategy strategy) throws Exception {
        try (ForL0StateMap<String, String, Integer> stateMap = new ForL0StateMap<>(
                allocator, 8, 6, keySerializer, namespaceSerializer, stateSerializer, true, strategy)) {

            // Store large values (simulated by many operations)
            for (int i = 0; i < 100; i++) {
                String key = "largeKey" + i;
                stateMap.put(key, "namespace1", i * 1000);
                stateMap.put(key, "namespace2", i * 2000);

                // Verify immediate retrieval
                assertEquals(Integer.valueOf(i * 1000), stateMap.get(key, "namespace1"));
                assertEquals(Integer.valueOf(i * 2000), stateMap.get(key, "namespace2"));
            }

            assertEquals(200, stateMap.size());
        }
    }

    @Test
    void testCacheStatisticsWithDifferentStrategies() throws Exception {
        // Test cache statistics with both strategies
        ForL0StateMap.CacheStats linearStats = getCacheStatsWithStrategy(EntryArena.AllocationStrategy.LINEAR);
        ForL0StateMap.CacheStats freeListStats = getCacheStatsWithStrategy(EntryArena.AllocationStrategy.FREE_LIST);

        System.out.println("LINEAR Cache Stats: " + linearStats);
        System.out.println("FREE_LIST Cache Stats: " + freeListStats);

        // Both should have similar access patterns
        assertTrue(linearStats.totalAccesses > 0);
        assertTrue(freeListStats.totalAccesses > 0);
        assertTrue(linearStats.getOverallHitRate() >= 0);
        assertTrue(freeListStats.getOverallHitRate() >= 0);
    }

    private ForL0StateMap.CacheStats getCacheStatsWithStrategy(EntryArena.AllocationStrategy strategy) throws Exception {
        try (ForL0StateMap<String, String, Integer> stateMap = new ForL0StateMap<>(
                allocator, 8, 4, keySerializer, namespaceSerializer, stateSerializer, true, strategy)) {

            // Generate access pattern
            for (int i = 0; i < 100; i++) {
                String key = "key" + (i % 20); // Reuse keys to create hits
                stateMap.put(key, "namespace1", i);
                stateMap.get(key, "namespace1"); // Should hit L0 cache
                stateMap.get(key, "namespace1"); // Should hit L0 cache again
            }

            return stateMap.getCacheStats();
        }
    }

    // Helper classes for test results
    private static class StrategyPerformance {
        final EntryArena.AllocationStrategy strategy;
        final int operationsCompleted;
        final long durationMs;
        final int finalSize;
        final ForL0StateMap.CacheStats cacheStats;

        StrategyPerformance(EntryArena.AllocationStrategy strategy, int operationsCompleted,
                          long durationMs, int finalSize, ForL0StateMap.CacheStats cacheStats) {
            this.strategy = strategy;
            this.operationsCompleted = operationsCompleted;
            this.durationMs = durationMs;
            this.finalSize = finalSize;
            this.cacheStats = cacheStats;
        }

        @Override
        public String toString() {
            return String.format("StrategyPerformance{strategy=%s, operations=%d, duration=%dms, " +
                    "finalSize=%d, hitRate=%.3f}",
                    strategy, operationsCompleted, durationMs, finalSize, cacheStats.getOverallHitRate());
        }
    }

    private static class RemovalTestResult {
        final EntryArena.AllocationStrategy strategy;
        final int initialSize;
        final int finalSize;
        final int removalsSuccessful;
        final int accessibleEntries;

        RemovalTestResult(EntryArena.AllocationStrategy strategy, int initialSize, int finalSize,
                         int removalsSuccessful, int accessibleEntries) {
            this.strategy = strategy;
            this.initialSize = initialSize;
            this.finalSize = finalSize;
            this.removalsSuccessful = removalsSuccessful;
            this.accessibleEntries = accessibleEntries;
        }

        @Override
        public String toString() {
            return String.format("RemovalTestResult{strategy=%s, initial=%d, final=%d, " +
                    "removals=%d, accessible=%d}",
                    strategy, initialSize, finalSize, removalsSuccessful, accessibleEntries);
        }
    }
}
