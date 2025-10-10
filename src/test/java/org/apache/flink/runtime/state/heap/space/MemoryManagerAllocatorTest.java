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
            assertEquals(0, allocator.getUsedBytes());
            assertFalse(allocator.isClosed());
        }

        @Test
        void testSimpleAllocation() throws MemoryAllocationException {
            int requestBytes = 1024;
            List<MemorySegment> segments = allocator.allocate(requestBytes);

            assertNotNull(segments);
            assertFalse(segments.isEmpty());
            assertEquals(1, segments.size()); // Should allocate 1 page for 1KB request
            assertEquals(DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

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
            assertEquals(4L * DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

            // Verify all segments
            for (MemorySegment segment : segments) {
                assertEquals(DEFAULT_PAGE_SIZE, segment.size());
                assertFalse(segment.isFreed());
            }
        }

        @Test
        void testReleaseMemory() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
            assertEquals(DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

            allocator.release(segments);
            assertEquals(0, allocator.getUsedBytes());
        }

        @Test
        void testMultipleAllocationsAndReleases() throws MemoryAllocationException {
            List<List<MemorySegment>> allocations = new ArrayList<>();

            // Allocate multiple chunks
            for (int i = 0; i < 5; i++) {
                List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
                allocations.add(segments);
            }

            assertEquals(5L * DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

            // Release some chunks
            allocator.release(allocations.get(0));
            allocator.release(allocations.get(2));
            allocator.release(allocations.get(4));

            assertEquals(2L * DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

            // Release remaining chunks
            allocator.release(allocations.get(1));
            allocator.release(allocations.get(3));

            assertEquals(0, allocator.getUsedBytes());
        }

        @Test
        void testAlignedMemoryAllocation() throws MemoryAllocationException {
            long size = 1024;
            int alignment = 64;

            long alignedAddress = allocator.allocateAligned(size, alignment);

            assertTrue(alignedAddress != 0);
            assertEquals(0, alignedAddress % alignment, "Address should be aligned");
            assertTrue(allocator.getUsedBytes() > 0);
            assertEquals(1, allocator.getAllocatedAlignedBlocks());
        }

        @Test
        void testAlignedMemoryDeallocation() throws MemoryAllocationException {
            long size = 1024;
            int alignment = 64;

            long alignedAddress = allocator.allocateAligned(size, alignment);
            @SuppressWarnings("unused")
            long usedBytesAfterAllocation = allocator.getUsedBytes();

            allocator.deallocate(alignedAddress, size);

            assertEquals(0, allocator.getUsedBytes());
            assertEquals(0, allocator.getAllocatedAlignedBlocks());
        }

        @Test
        void testMultipleAlignedAllocations() throws MemoryAllocationException {
            List<Long> addresses = new ArrayList<>();
            long size = 512;
            int alignment = 64;

            // Allocate multiple aligned blocks
            for (int i = 0; i < 5; i++) {
                long address = allocator.allocateAligned(size, alignment);
                addresses.add(address);
                assertEquals(0, address % alignment, "Address should be aligned");
            }

            assertEquals(5, allocator.getAllocatedAlignedBlocks());

            // Deallocate all blocks
            for (long address : addresses) {
                allocator.deallocate(address, size);
            }

            assertEquals(0, allocator.getAllocatedAlignedBlocks());
            assertEquals(0, allocator.getUsedBytes());
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
        void testZeroSizeAlignedAllocation() {
            assertThrows(IllegalArgumentException.class, () -> {
                allocator.allocateAligned(0, 64);
            });
        }

        @Test
        void testNegativeSizeAlignedAllocation() {
            assertThrows(IllegalArgumentException.class, () -> {
                allocator.allocateAligned(-100, 64);
            });
        }

        @Test
        void testInvalidAlignment() {
            assertThrows(IllegalArgumentException.class, () -> {
                allocator.allocateAligned(1024, 63); // Not power of 2
            });
        }

        @Test
        void testReleaseNullSegments() {
            assertDoesNotThrow(() -> {
                allocator.release(null);
            });
        }

        @Test
        void testReleaseEmptySegmentList() {
            assertDoesNotThrow(() -> {
                allocator.release(new ArrayList<>());
            });
        }

        @Test
        void testDeallocateZeroAddress() {
            assertDoesNotThrow(() -> {
                allocator.deallocate(0, 1024);
            });
        }

        @Test
        void testDeallocateUnknownAddress() {
            // This should log a warning but not throw
            assertDoesNotThrow(() -> {
                allocator.deallocate(0x12345678L, 1024);
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
            assertEquals(DEFAULT_PAGE_SIZE, allocator.getUsedBytes());
        }

        @Test
        void testSlightlyOverPageSizeAllocation() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE + 1);

            assertEquals(2, segments.size()); // Should round up to 2 pages
            assertEquals(2L * DEFAULT_PAGE_SIZE, allocator.getUsedBytes());
        }

        @Test
        void testLargeAlignmentValues() throws MemoryAllocationException {
            long size = 1024;
            int[] alignments = {64, 128, 256, 512, 1024, 2048, 4096};

            for (int alignment : alignments) {
                long address = allocator.allocateAligned(size, alignment);
                assertEquals(0, address % alignment, "Address should be aligned to " + alignment);
                allocator.deallocate(address, size);
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClose() throws MemoryAllocationException {
            // Allocate some regular memory
            @SuppressWarnings("unused")
            List<MemorySegment> segments1 = allocator.allocate(DEFAULT_PAGE_SIZE);
            @SuppressWarnings("unused")
            List<MemorySegment> segments2 = allocator.allocate(DEFAULT_PAGE_SIZE * 2);

            // Allocate some aligned memory
            @SuppressWarnings("unused")
            long alignedAddress = allocator.allocateAligned(1024, 64);

            assertTrue(allocator.getUsedBytes() > 0);
            assertFalse(allocator.isClosed());

            // Close should release all memory
            allocator.close();

            assertTrue(allocator.isClosed());
            assertEquals(0, allocator.getUsedBytes());
            assertEquals(0, allocator.getAllocatedSegmentLists());
            assertEquals(0, allocator.getAllocatedAlignedBlocks());
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

            assertThrows(IllegalStateException.class, () -> {
                allocator.allocateAligned(1024, 64);
            });
        }

        @Test
        void testReleaseAfterClose() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
            long alignedAddress = allocator.allocateAligned(1024, 64);

            allocator.close();

            // Release after close should not throw
            assertDoesNotThrow(() -> allocator.release(segments));
            assertDoesNotThrow(() -> allocator.deallocate(alignedAddress, 1024));
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
                        allocator.release(segments);
                    }
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }

            executor.shutdown();
        }

        @Test
        void testConcurrentAllocationAndRelease() throws InterruptedException {
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

            // Release thread
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 20; i++) {
                        List<MemorySegment> segments = sharedSegments.poll();
                        if (segments != null) {
                            allocator.release(segments);
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
                allocator.release(remaining);
            }

            executor.shutdown();
        }

        @Test
        void testConcurrentAlignedAllocations() throws InterruptedException {
            int numThreads = 4;
            int allocationsPerThread = 5;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);
            AtomicInteger successCount = new AtomicInteger(0);

            List<Future<List<Long>>> futures = new ArrayList<>();

            for (int i = 0; i < numThreads; i++) {
                futures.add(executor.submit(() -> {
                    List<Long> addresses = new ArrayList<>();
                    try {
                        startLatch.await();
                        for (int j = 0; j < allocationsPerThread; j++) {
                            long address = allocator.allocateAligned(1024, 64);
                            addresses.add(address);
                        }
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        // Expected for some threads due to memory limit
                    } finally {
                        doneLatch.countDown();
                    }
                    return addresses;
                }));
            }

            startLatch.countDown();
            doneLatch.await(10, TimeUnit.SECONDS);

            assertTrue(successCount.get() > 0, "At least some allocations should succeed");

            // Clean up
            for (Future<List<Long>> future : futures) {
                try {
                    List<Long> addresses = future.get();
                    for (Long address : addresses) {
                        allocator.deallocate(address, 1024);
                    }
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }

            executor.shutdown();
        }
    }

    @Nested
    class MemoryTrackingTests {

        @Test
        void testUsedBytesAccuracy() throws MemoryAllocationException {
            assertEquals(0, allocator.getUsedBytes());

            List<MemorySegment> segments1 = allocator.allocate(1024);
            assertEquals(DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

            List<MemorySegment> segments2 = allocator.allocate(DEFAULT_PAGE_SIZE * 2);
            assertEquals(3L * DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

            allocator.release(segments1);
            assertEquals(2L * DEFAULT_PAGE_SIZE, allocator.getUsedBytes());

            allocator.release(segments2);
            assertEquals(0, allocator.getUsedBytes());
        }

        @Test
        void testUsedBytesNeverNegative() throws MemoryAllocationException {
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);

            // Release twice (second release should be ignored)
            allocator.release(segments);
            allocator.release(segments);

            assertTrue(allocator.getUsedBytes() >= 0);
        }

        @Test
        void testMemoryTrackingAfterClose() throws MemoryAllocationException {
            allocator.allocate(DEFAULT_PAGE_SIZE);
            allocator.allocateAligned(1024, 64);

            assertTrue(allocator.getUsedBytes() > 0);

            allocator.close();

            assertEquals(0, allocator.getUsedBytes());
        }

        @Test
        void testAllocationCounters() throws MemoryAllocationException {
            assertEquals(0, allocator.getAllocatedSegmentLists());
            assertEquals(0, allocator.getAllocatedAlignedBlocks());

            List<MemorySegment> segments1 = allocator.allocate(DEFAULT_PAGE_SIZE);
            List<MemorySegment> segments2 = allocator.allocate(DEFAULT_PAGE_SIZE);
            long address1 = allocator.allocateAligned(1024, 64);
            long address2 = allocator.allocateAligned(2048, 128);

            assertEquals(2, allocator.getAllocatedSegmentLists());
            assertEquals(2, allocator.getAllocatedAlignedBlocks());

            allocator.release(segments1);
            allocator.deallocate(address1, 1024);

            assertEquals(1, allocator.getAllocatedSegmentLists());
            assertEquals(1, allocator.getAllocatedAlignedBlocks());

            allocator.release(segments2);
            allocator.deallocate(address2, 2048);

            assertEquals(0, allocator.getAllocatedSegmentLists());
            assertEquals(0, allocator.getAllocatedAlignedBlocks());
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
                assertEquals(3L * 4 * 1024, smallPageAllocator.getUsedBytes());

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

                @SuppressWarnings("unused")
                List<MemorySegment> segments1 = allocator1.allocate(DEFAULT_PAGE_SIZE);
                @SuppressWarnings("unused")
                List<MemorySegment> segments2 = allocator2.allocate(DEFAULT_PAGE_SIZE);

                assertEquals(DEFAULT_PAGE_SIZE, allocator1.getUsedBytes());
                assertEquals(DEFAULT_PAGE_SIZE, allocator2.getUsedBytes());

                // Closing one allocator should not affect the other
                allocator1.close();
                assertEquals(0, allocator1.getUsedBytes());
                assertEquals(DEFAULT_PAGE_SIZE, allocator2.getUsedBytes());

            } catch (Exception e) {
                fail("Test should not throw exception: " + e.getMessage());
            }
        }

        @Test
        void testMixedAllocationTypes() throws MemoryAllocationException {
            // Mix regular and aligned allocations
            List<MemorySegment> segments = allocator.allocate(DEFAULT_PAGE_SIZE);
            long alignedAddress1 = allocator.allocateAligned(1024, 64);
            long alignedAddress2 = allocator.allocateAligned(2048, 128);

            assertTrue(allocator.getUsedBytes() > 0);
            assertEquals(1, allocator.getAllocatedSegmentLists());
            assertEquals(2, allocator.getAllocatedAlignedBlocks());

            // Release in mixed order
            allocator.deallocate(alignedAddress1, 1024);
            allocator.release(segments);
            allocator.deallocate(alignedAddress2, 2048);

            assertEquals(0, allocator.getUsedBytes());
            assertEquals(0, allocator.getAllocatedSegmentLists());
            assertEquals(0, allocator.getAllocatedAlignedBlocks());
        }
    }
}
