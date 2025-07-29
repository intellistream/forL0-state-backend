package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MemoryManagerAllocatorTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
    }

    @AfterEach
    void tearDown() {
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
        void testConstructorAndInitialization() {
            assertEquals(DEFAULT_PAGE_SIZE, allocator.getPageSize());
            assertEquals(owner, allocator.getOwner());
            assertEquals(0, allocator.outstandingBytes());
            assertFalse(allocator.isClosed());
        }

        @Test
        void testSimpleAllocation() throws MemoryAllocationException {
            int requestBytes = 1024;
            List<MemorySegment> segments = allocator.allocate(requestBytes);

            assertNotNull(segments);
            assertFalse(segments.isEmpty());
            assertEquals(1, segments.size()); // Should allocate 1 page for 1KB request
            assertEquals(DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            // Verify segment properties
            MemorySegment segment = segments.get(0);
            assertEquals(DEFAULT_PAGE_SIZE, segment.size());
            assertFalse(segment.isFreed());
        }

        @Test
        void testMultiplePageAllocation() throws MemoryAllocationException {
            int requestBytes = DEFAULT_PAGE_SIZE * 3 + 1024; // 3+ pages
            List<MemorySegment> segments = allocator.allocate(requestBytes);

            assertNotNull(segments);
            assertEquals(4, segments.size()); // Should allocate 4 pages
            assertEquals(4L * DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            // Verify all segments
            for (MemorySegment segment : segments) {
                assertEquals(DEFAULT_PAGE_SIZE, segment.size());
                assertFalse(segment.isFreed());
            }
        }

        @Test
        void testFreeMemory() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
            assertEquals(DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            allocator.free(segments);
            assertEquals(0, allocator.outstandingBytes());
        }

        @Test
        void testFreeSingleSegment() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
            MemorySegment segment = segments.get(0);

            allocator.free(segment);
            assertEquals(0, allocator.outstandingBytes());
        }

        @Test
        void testMultipleAllocationsAndFrees() throws MemoryAllocationException {
            List<List<MemorySegment>> allocations = new ArrayList<>();

            // Allocate multiple chunks
            for (int i = 0; i < 5; i++) {
                List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
                allocations.add(segments);
            }

            assertEquals(5L * DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            // Free some chunks
            allocator.free(allocations.get(0));
            allocator.free(allocations.get(2));
            allocator.free(allocations.get(4));

            assertEquals(2L * DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            // Free remaining chunks
            allocator.free(allocations.get(1));
            allocator.free(allocations.get(3));

            assertEquals(0, allocator.outstandingBytes());
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testZeroByteAllocation() {
            assertThrows(IllegalArgumentException.class, () -> {
                allocator.allocate(0);
            });
        }

        @Test
        void testNegativeByteAllocation() {
            assertThrows(IllegalArgumentException.class, () -> {
                allocator.allocate(-100);
            });
        }

        @Test
        void testFreeNullSegments() {
            assertDoesNotThrow(() -> {
                allocator.free((List<MemorySegment>) null);
                allocator.free((MemorySegment) null);
            });
        }

        @Test
        void testFreeEmptySegmentList() {
            assertDoesNotThrow(() -> {
                allocator.free(new ArrayList<>());
            });
        }

        @Test
        void testVeryLargeAllocation() {
            // Request more memory than available
            long totalAvailable = memoryManager.getMemorySize();
            long requestBytes = totalAvailable + DEFAULT_PAGE_SIZE;

            assertThrows(MemoryAllocationException.class, () -> {
                allocator.allocate((int) Math.min(requestBytes, Integer.MAX_VALUE));
            });
        }

        @Test
        void testExactPageSizeAllocation() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);

            assertEquals(1, segments.size());
            assertEquals(DEFAULT_PAGE_SIZE, allocator.outstandingBytes());
        }

        @Test
        void testSlightlyOverPageSizeAllocation() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE + 1);

            assertEquals(2, segments.size()); // Should round up to 2 pages
            assertEquals(2L * DEFAULT_PAGE_SIZE, allocator.outstandingBytes());
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClose() throws MemoryAllocationException {
            // Allocate some memory
            List<MemorySegment> segments1 = allocator.allocate(DEFAULT_PAGE_SIZE);
            List<MemorySegment> segments2 = allocator.allocate(DEFAULT_PAGE_SIZE * 2);

            assertTrue(allocator.outstandingBytes() > 0);
            assertFalse(allocator.isClosed());

            // Close should release all memory
            allocator.close();

            assertTrue(allocator.isClosed());
            assertEquals(0, allocator.outstandingBytes());
        }

        @Test
        void testDoubleClose() throws MemoryAllocationException {
            allocator.allocate(DEFAULT_PAGE_SIZE);

            allocator.close();
            assertTrue(allocator.isClosed());

            // Second close should be safe
            assertDoesNotThrow(() -> allocator.close());
            assertTrue(allocator.isClosed());
        }

        @Test
        void testOperationsAfterClose() throws MemoryAllocationException {
            allocator.close();

            assertThrows(IllegalStateException.class, () -> {
                allocator.allocate(DEFAULT_PAGE_SIZE);
            });
        }

        @Test
        void testFreeAfterClose() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
            allocator.close();

            // Free after close should not throw
            assertDoesNotThrow(() -> allocator.free(segments));
        }
    }

    @Nested
    class ConcurrencyTests {

        @Test
        void testConcurrentAllocations() throws InterruptedException {
            int numThreads = 4;
            int allocationsPerThread = 10;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);
            AtomicInteger successCount = new AtomicInteger(0);

            List<Future<List<List<MemorySegment>>>> futures = new ArrayList<>();

            for (int i = 0; i < numThreads; i++) {
                futures.add(executor.submit(() -> {
                    List<List<MemorySegment>> allocations = new ArrayList<>();
                    try {
                        startLatch.await();
                        for (int j = 0; j < allocationsPerThread; j++) {
                            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
                            allocations.add(segments);
                        }
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        // Expected for some threads due to memory limit
                    } finally {
                        doneLatch.countDown();
                    }
                    return allocations;
                }));
            }

            startLatch.countDown();
            doneLatch.await(10, TimeUnit.SECONDS);

            assertTrue(successCount.get() > 0, "At least some allocations should succeed");

            // Clean up
            for (Future<List<List<MemorySegment>>> future : futures) {
                try {
                    List<List<MemorySegment>> allocations = future.get();
                    for (List<MemorySegment> segments : allocations) {
                        allocator.free(segments);
                    }
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }

            executor.shutdown();
        }

        @Test
        void testConcurrentAllocationAndFree() throws InterruptedException {
            int numThreads = 2;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);

            // Shared list for segments
            ConcurrentLinkedQueue<List<MemorySegment>> sharedSegments = new ConcurrentLinkedQueue<>();

            // Allocator thread
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 20; i++) {
                        try {
                            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
                            sharedSegments.offer(segments);
                            Thread.sleep(10);
                        } catch (Exception e) {
                            // Ignore allocation failures
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });

            // Free thread
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 20; i++) {
                        List<MemorySegment> segments = sharedSegments.poll();
                        if (segments != null) {
                            allocator.free(segments);
                        }
                        Thread.sleep(15);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });

            startLatch.countDown();
            assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

            // Clean up remaining segments
            List<MemorySegment> remaining;
            while ((remaining = sharedSegments.poll()) != null) {
                allocator.free(remaining);
            }

            executor.shutdown();
        }
    }

    @Nested
    class MemoryTrackingTests {

        @Test
        void testOutstandingBytesAccuracy() throws MemoryAllocationException {
            assertEquals(0, allocator.outstandingBytes());

            List<MemorySegment> segments1 = allocator.allocate(1024);
            assertEquals(DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            List<MemorySegment> segments2 = allocator.allocate(DEFAULT_PAGE_SIZE * 2);
            assertEquals(3L * DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            allocator.free(segments1);
            assertEquals(2L * DEFAULT_PAGE_SIZE, allocator.outstandingBytes());

            allocator.free(segments2);
            assertEquals(0, allocator.outstandingBytes());
        }

        @Test
        void testOutstandingBytesNeverNegative() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);

            // Free twice (second free should be ignored)
            allocator.free(segments);
            allocator.free(segments);

            assertTrue(allocator.outstandingBytes() >= 0);
        }

        @Test
        void testMemoryTrackingAfterClose() throws MemoryAllocationException {
            allocator.allocate(DEFAULT_PAGE_SIZE);
            allocator.allocate(DEFAULT_PAGE_SIZE * 2);

            assertTrue(allocator.outstandingBytes() > 0);

            allocator.close();

            assertEquals(0, allocator.outstandingBytes());
        }
    }

    @Nested
    class IntegrationTests {

        @Test
        void testWithDifferentMemoryManagerConfigurations() {
            // Test with smaller page size
            MemoryManager smallPageMemoryManager = MemoryManagerBuilder.newBuilder()
                    .setPageSize(4 * 1024) // 4KB pages
                    .setMemorySize(64 * 4 * 1024) // 256KB total
                    .build();

            try (MemoryManagerAllocator smallPageAllocator =
                    new MemoryManagerAllocator(smallPageMemoryManager, new Object())) {

                assertEquals(4 * 1024, smallPageAllocator.getPageSize());

                List<MemorySegment> segments = smallPageAllocator.allocate(10 * 1024); // 10KB
                assertEquals(3, segments.size()); // Should need 3 pages of 4KB each
                assertEquals(3L * 4 * 1024, smallPageAllocator.outstandingBytes());

            } catch (Exception e) {
                fail("Test should not throw exception: " + e.getMessage());
            } finally {
                smallPageMemoryManager.shutdown();
            }
        }

        @Test
        void testMultipleAllocatorsWithSameMemoryManager() {
            Object owner1 = new Object();
            Object owner2 = new Object();

            try (MemoryManagerAllocator allocator1 = new MemoryManagerAllocator(memoryManager, owner1);
                 MemoryManagerAllocator allocator2 = new MemoryManagerAllocator(memoryManager, owner2)) {

                List<MemorySegment> segments1 = allocator1.allocate(DEFAULT_PAGE_SIZE);
                List<MemorySegment> segments2 = allocator2.allocate(DEFAULT_PAGE_SIZE);

                assertEquals(DEFAULT_PAGE_SIZE, allocator1.outstandingBytes());
                assertEquals(DEFAULT_PAGE_SIZE, allocator2.outstandingBytes());

                // Closing one allocator should not affect the other
                allocator1.close();
                assertEquals(0, allocator1.outstandingBytes());
                assertEquals(DEFAULT_PAGE_SIZE, allocator2.outstandingBytes());

            } catch (Exception e) {
                fail("Test should not throw exception: " + e.getMessage());
            }
        }
    }
}
