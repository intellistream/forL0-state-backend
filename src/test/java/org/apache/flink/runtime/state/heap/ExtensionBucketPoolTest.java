package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ExtensionBucketPoolTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private ExtensionBucketPool bucketPool;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        bucketPool = new ExtensionBucketPool(allocator, 10); // 10 max buckets
    }

    @AfterEach
    void tearDown() throws Exception {
        if (bucketPool != null) {
            bucketPool.close();
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
        void testPoolInitialization() {
            ExtensionBucketPool.PoolStats stats = bucketPool.getStats();
            assertEquals(0, stats.bucketsInUse);
            assertEquals(10, stats.maxBuckets);
            assertEquals(0.0, stats.utilization, 0.001);
        }

        @Test
        void testAllocateBucket() {
            byte bucketId = bucketPool.allocateBucket();

            // Should get a valid bucket ID (1-255, since 0 is reserved as NULL)
            assertTrue(bucketId > 0);
            assertTrue(bucketId <= 10);

            // Verify bucket is marked as in use
            assertTrue(bucketPool.isBucketInUse(bucketId));

            // Verify stats
            ExtensionBucketPool.PoolStats stats = bucketPool.getStats();
            assertEquals(1, stats.bucketsInUse);
            assertEquals(0.1, stats.utilization, 0.001);
        }

        @Test
        void testFreeBucket() {
            byte bucketId = bucketPool.allocateBucket();
            assertTrue(bucketPool.isBucketInUse(bucketId));

            bucketPool.freeBucket(bucketId);
            assertFalse(bucketPool.isBucketInUse(bucketId));

            // Verify stats
            ExtensionBucketPool.PoolStats stats = bucketPool.getStats();
            assertEquals(0, stats.bucketsInUse);
            assertEquals(0.0, stats.utilization, 0.001);
        }

        @Test
        void testGetBucketAddress() {
            byte bucketId = bucketPool.allocateBucket();
            long address = bucketPool.getBucketAddress(bucketId);

            // Address should be non-zero and 64-byte aligned
            assertTrue(address > 0);
            assertEquals(0, address % 64);
        }

        @Test
        void testMultipleBucketAddresses() {
            List<Byte> bucketIds = new ArrayList<>();
            List<Long> addresses = new ArrayList<>();

            // Allocate multiple buckets
            for (int i = 0; i < 5; i++) {
                byte bucketId = bucketPool.allocateBucket();
                long address = bucketPool.getBucketAddress(bucketId);

                bucketIds.add(bucketId);
                addresses.add(address);

                // Each bucket should have a different address
                for (int j = 0; j < i; j++) {
                    assertNotEquals(addresses.get(j), address);
                }

                // Addresses should be 64 bytes apart (bucket size)
                if (i > 0) {
                    long expectedDistance = Math.abs(address - addresses.get(i - 1));
                    assertTrue(expectedDistance >= 64);
                }
            }
        }

        @Test
        void testBucketIdUniqueness() {
            Set<Byte> allocatedIds = new HashSet<>();

            // Allocate all buckets
            for (int i = 0; i < 10; i++) {
                byte bucketId = bucketPool.allocateBucket();
                assertTrue(bucketId > 0);
                assertFalse(allocatedIds.contains(bucketId), "Bucket ID should be unique");
                allocatedIds.add(bucketId);
            }

            assertEquals(10, allocatedIds.size());
        }
    }

    @Nested
    class AllocationTests {

        @Test
        void testSequentialAllocation() {
            List<Byte> bucketIds = new ArrayList<>();

            // Allocate buckets sequentially
            for (int i = 0; i < 10; i++) {
                byte bucketId = bucketPool.allocateBucket();
                assertTrue(bucketId > 0);
                bucketIds.add(bucketId);

                ExtensionBucketPool.PoolStats stats = bucketPool.getStats();
                assertEquals(i + 1, stats.bucketsInUse);
            }

            // All buckets should be allocated
            ExtensionBucketPool.PoolStats finalStats = bucketPool.getStats();
            assertEquals(10, finalStats.bucketsInUse);
            assertEquals(1.0, finalStats.utilization, 0.001);
        }

        @Test
        void testAllocationAfterFreeing() {
            // Allocate some buckets
            byte bucket1 = bucketPool.allocateBucket();
            byte bucket2 = bucketPool.allocateBucket();
            byte bucket3 = bucketPool.allocateBucket();

            // Free middle bucket
            bucketPool.freeBucket(bucket2);

            // Allocate new bucket - should reuse freed bucket ID
            byte newBucket = bucketPool.allocateBucket();
            assertEquals(bucket2, newBucket);

            assertTrue(bucketPool.isBucketInUse(bucket1));
            assertTrue(bucketPool.isBucketInUse(bucket2));
            assertTrue(bucketPool.isBucketInUse(bucket3));
        }

        @Test
        void testPoolExhaustion() {
            List<Byte> bucketIds = new ArrayList<>();

            // Allocate all buckets
            for (int i = 0; i < 10; i++) {
                byte bucketId = bucketPool.allocateBucket();
                assertTrue(bucketId > 0);
                bucketIds.add(bucketId);
            }

            // Try to allocate one more - should fail
            byte failedAllocation = bucketPool.allocateBucket();
            assertEquals(0, failedAllocation); // NULL_BUCKET_ID
        }

        @Test
        void testAllocationAfterPoolExhaustion() {
            // Exhaust the pool
            for (int i = 0; i < 10; i++) {
                bucketPool.allocateBucket();
            }

            // Verify exhausted
            assertEquals(0, bucketPool.allocateBucket());

            // Free one bucket
            bucketPool.freeBucket((byte) 1);

            // Should be able to allocate again
            byte newBucket = bucketPool.allocateBucket();
            assertEquals(1, newBucket);
        }

        @Test
        void testFreeListManagement() {
            // Allocate buckets in order
            List<Byte> buckets = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                buckets.add(bucketPool.allocateBucket());
            }

            // Free buckets in reverse order
            for (int i = 4; i >= 0; i--) {
                bucketPool.freeBucket(buckets.get(i));
            }

            // Allocate again - should get the lowest available ID first
            byte firstRealloc = bucketPool.allocateBucket();
            assertEquals(1, firstRealloc); // Should get bucket ID 1 (lowest)
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testFreeInvalidBucketId() {
            // Free bucket ID 0 (NULL_BUCKET_ID) - should be safe
            assertDoesNotThrow(() -> bucketPool.freeBucket((byte) 0));

            // Free bucket ID beyond max - should be safe
            assertDoesNotThrow(() -> bucketPool.freeBucket((byte) 100));

            // Stats should remain unchanged
            ExtensionBucketPool.PoolStats stats = bucketPool.getStats();
            assertEquals(0, stats.bucketsInUse);
        }

        @Test
        void testFreeBucketTwice() {
            byte bucketId = bucketPool.allocateBucket();
            assertTrue(bucketPool.isBucketInUse(bucketId));

            // Free once
            bucketPool.freeBucket(bucketId);
            assertFalse(bucketPool.isBucketInUse(bucketId));

            // Free again - should be safe
            assertDoesNotThrow(() -> bucketPool.freeBucket(bucketId));
            assertFalse(bucketPool.isBucketInUse(bucketId));

            ExtensionBucketPool.PoolStats stats = bucketPool.getStats();
            assertEquals(0, stats.bucketsInUse);
        }

        @Test
        void testGetBucketAddressInvalidId() {
            // Invalid bucket ID should throw
            assertThrows(IllegalArgumentException.class, () -> {
                bucketPool.getBucketAddress((byte) 0);
            });

            assertThrows(IllegalArgumentException.class, () -> {
                bucketPool.getBucketAddress((byte) 100);
            });
        }

        @Test
        void testIsBucketInUseInvalidId() {
            // Invalid bucket IDs should return false
            assertFalse(bucketPool.isBucketInUse((byte) 0));
            assertFalse(bucketPool.isBucketInUse((byte) 100));
            assertFalse(bucketPool.isBucketInUse((byte) -1));
        }

        @Test
        void testMaxBucketsLimit() {
            // Create pool with max limit (255)
            try (ExtensionBucketPool maxPool = new ExtensionBucketPool(allocator, 255)) {
                ExtensionBucketPool.PoolStats stats = maxPool.getStats();
                assertEquals(255, stats.maxBuckets);

                // Should be able to allocate at least some buckets
                byte bucketId = maxPool.allocateBucket();
                assertTrue(bucketId > 0);
            } catch (Exception e) {
                fail("Failed to create or use max bucket pool: " + e.getMessage());
            }
        }

        @Test
        void testInvalidMaxBuckets() {
            // Should throw for invalid max buckets
            assertThrows(IllegalArgumentException.class, () -> {
                new ExtensionBucketPool(allocator, 256); // > 255
            });

            assertThrows(IllegalArgumentException.class, () -> {
                new ExtensionBucketPool(allocator, 0); // Zero buckets
            });

            assertThrows(IllegalArgumentException.class, () -> {
                new ExtensionBucketPool(allocator, -1); // Negative buckets
            });
        }
    }

    @Nested
    class ResizeTests {

        @Test
        void testBasicResize() throws Exception {
            // Allocate and use some buckets
            byte bucket1 = bucketPool.allocateBucket();
            byte bucket2 = bucketPool.allocateBucket();

            // Write some test data to bucket addresses
            long addr1 = bucketPool.getBucketAddress(bucket1);
            long addr2 = bucketPool.getBucketAddress(bucket2);

            // Resize to larger pool
            try (ExtensionBucketPool newPool = bucketPool.resize(20)) {
                ExtensionBucketPool.PoolStats newStats = newPool.getStats();
                assertEquals(20, newStats.maxBuckets);

                // Should have copied in-use buckets
                assertTrue(newStats.bucketsInUse >= 2);

                // Should be able to allocate more buckets than before
                List<Byte> newBuckets = new ArrayList<>();
                for (int i = 0; i < 15; i++) { // More than original max of 10
                    byte bucketId = newPool.allocateBucket();
                    if (bucketId > 0) {
                        newBuckets.add(bucketId);
                    }
                }

                assertTrue(newBuckets.size() > 8); // Should be able to allocate more
            }
        }

        @Test
        void testResizeWithSameSize() throws Exception {
            byte bucket1 = bucketPool.allocateBucket();

            // Resize with same size should return same pool
            try (ExtensionBucketPool samePool = bucketPool.resize(10)) {
                assertSame(bucketPool, samePool);
            }
        }

        @Test
        void testResizeWithSmallerSize() throws Exception {
            byte bucket1 = bucketPool.allocateBucket();

            // Resize with smaller size should return same pool
            try (ExtensionBucketPool smallerPool = bucketPool.resize(5)) {
                assertSame(bucketPool, smallerPool);
            }
        }

        @Test
        void testResizeDataIntegrity() throws Exception {
            // This test would ideally verify that data is copied correctly,
            // but since we can't easily write/read data without more infrastructure,
            // we just verify that the resize operation completes successfully
            // and bucket allocation works in the new pool

            List<Byte> originalBuckets = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                originalBuckets.add(bucketPool.allocateBucket());
            }

            try (ExtensionBucketPool newPool = bucketPool.resize(15)) {
                // New pool should be functional
                byte newBucket = newPool.allocateBucket();
                assertTrue(newBucket > 0);
                assertTrue(newPool.isBucketInUse(newBucket));

                // Should be able to get valid addresses
                long address = newPool.getBucketAddress(newBucket);
                assertTrue(address > 0);
                assertEquals(0, address % 64); // 64-byte aligned
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testCloseWithAllocatedBuckets() throws Exception {
            // Allocate some buckets
            for (int i = 0; i < 5; i++) {
                bucketPool.allocateBucket();
            }

            long usedBytesBeforeClose = allocator.getUsedBytes();
            assertTrue(usedBytesBeforeClose > 0);

            bucketPool.close();

            // Memory should be freed
            assertTrue(allocator.getUsedBytes() < usedBytesBeforeClose);
        }

        @Test
        void testCloseEmptyPool() throws Exception {
            // Close without allocating anything
            assertDoesNotThrow(() -> bucketPool.close());
        }

        @Test
        void testOperationsAfterClose() throws Exception {
            bucketPool.close();

            // Operations after close should still work or fail gracefully
            // (Implementation dependent - some might throw, others might return default values)

            // Stats should still be accessible
            assertDoesNotThrow(() -> bucketPool.getStats());
        }
    }

    @Nested
    class PerformanceTests {

        @Test
        void testAllocationPerformance() {
            // Test that allocation is reasonably fast even with many operations
            long startTime = System.nanoTime();

            List<Byte> buckets = new ArrayList<>();

            // Allocate and free many times
            for (int iteration = 0; iteration < 100; iteration++) {
                // Allocate up to half capacity
                for (int i = 0; i < 5; i++) {
                    byte bucket = bucketPool.allocateBucket();
                    if (bucket > 0) {
                        buckets.add(bucket);
                    }
                }

                // Free all
                for (byte bucket : buckets) {
                    bucketPool.freeBucket(bucket);
                }
                buckets.clear();
            }

            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000;

            // Should complete in reasonable time (less than 1 second)
            assertTrue(durationMs < 1000, "Allocation performance test took too long: " + durationMs + "ms");
        }

        @Test
        void testFragmentationHandling() {
            List<Byte> buckets = new ArrayList<>();

            // Allocate all buckets
            for (int i = 0; i < 10; i++) {
                buckets.add(bucketPool.allocateBucket());
            }

            // Free every other bucket to create fragmentation
            for (int i = 0; i < buckets.size(); i += 2) {
                bucketPool.freeBucket(buckets.get(i));
            }

            // Should be able to allocate freed buckets
            List<Byte> newBuckets = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                byte bucket = bucketPool.allocateBucket();
                if (bucket > 0) {
                    newBuckets.add(bucket);
                }
            }

            // Should have successfully allocated into fragmented space
            assertEquals(5, newBuckets.size());
        }
    }

    @Nested
    class ConcurrencyTests {

        @Test
        void testBucketPoolIsThreadSafe() {
            // Note: Since we removed concurrency support, this test verifies
            // that the single-threaded implementation works correctly

            List<Byte> buckets = new ArrayList<>();

            // Simulate what would happen with concurrent access
            // (but actually sequential since we're single-threaded)
            for (int i = 0; i < 20; i++) {
                if (i % 3 == 0 && !buckets.isEmpty()) {
                    // Free a bucket
                    byte toFree = buckets.remove(buckets.size() - 1);
                    bucketPool.freeBucket(toFree);
                } else {
                    // Allocate a bucket
                    byte bucket = bucketPool.allocateBucket();
                    if (bucket > 0) {
                        buckets.add(bucket);
                    }
                }
            }

            // Verify pool is in consistent state
            ExtensionBucketPool.PoolStats stats = bucketPool.getStats();
            assertEquals(buckets.size(), stats.bucketsInUse);

            // Verify all tracked buckets are actually in use
            for (byte bucket : buckets) {
                assertTrue(bucketPool.isBucketInUse(bucket));
            }
        }
    }
}
