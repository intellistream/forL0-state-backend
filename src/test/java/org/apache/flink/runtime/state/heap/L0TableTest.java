package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

class L0TableTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private L0Table l0Table;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        l0Table = new L0Table(allocator, 4); // 16 buckets (2^4)
    }

    @AfterEach
    void tearDown() throws Exception {
        if (l0Table != null) {
            l0Table.close();
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
        void testTableInitialization() {
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(0, stats.validSlots);
            assertEquals(16 * 4, stats.totalSlots); // 16 buckets * 4 slots each
            assertEquals(16, stats.bucketCount);
            assertEquals(0.0, stats.loadFactor);
        }

        @Test
        void testPutAndGet() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            // Mock KV matcher that always returns true
            L0Table.KVMatcher alwaysMatch = (pointer) -> pointer == kvPointer;

            // Put entry
            assertTrue(l0Table.put(keyHash, tag, kvPointer));

            // Get entry back
            long result = l0Table.get(keyHash, tag, alwaysMatch);
            assertEquals(kvPointer, result);

            // Verify stats
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(1, stats.validSlots);
            assertTrue(stats.loadFactor > 0);
        }

        @Test
        void testGetNonExistentEntry() {
            int keyHash = 12345;
            short tag = (short) 0x1234;

            L0Table.KVMatcher alwaysMatch = (pointer) -> true;

            // Try to get non-existent entry
            long result = l0Table.get(keyHash, tag, alwaysMatch);
            assertEquals(0, result);
        }

        @Test
        void testRemoveEntry() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            // Put and verify
            assertTrue(l0Table.put(keyHash, tag, kvPointer));
            assertEquals(kvPointer, l0Table.get(keyHash, tag, matcher));

            // Remove
            l0Table.remove(keyHash, tag, kvPointer);

            // Verify removed
            assertEquals(0, l0Table.get(keyHash, tag, matcher));

            // Verify stats
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(0, stats.validSlots);
        }

        @Test
        void testUpdateExistingEntry() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer1 = 0x111111111111111L;
            long kvPointer2 = 0x222222222222222L;

            L0Table.KVMatcher matcher1 = (pointer) -> pointer == kvPointer1;
            L0Table.KVMatcher matcher2 = (pointer) -> pointer == kvPointer2;

            // Put first entry
            assertTrue(l0Table.put(keyHash, tag, kvPointer1));
            assertEquals(kvPointer1, l0Table.get(keyHash, tag, matcher1));

            // Update with same key hash and tag
            assertTrue(l0Table.put(keyHash, tag, kvPointer2));
            assertEquals(kvPointer2, l0Table.get(keyHash, tag, matcher2));
            assertEquals(0, l0Table.get(keyHash, tag, matcher1)); // Old pointer should not match

            // Stats should still show only 1 slot (updated, not new)
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(1, stats.validSlots);
        }
    }

    @Nested
    class CollisionHandlingTests {

        @Test
        void testSameBucketMultipleEntries() {
            // Use same hash but different tags to force same bucket
            int keyHash = 0;
            short tag1 = (short) 0x1111;
            short tag2 = (short) 0x2222;
            short tag3 = (short) 0x3333;
            short tag4 = (short) 0x4444;

            long pointer1 = 0x111L;
            long pointer2 = 0x222L;
            long pointer3 = 0x333L;
            long pointer4 = 0x444L;

            L0Table.KVMatcher matcher1 = (p) -> p == pointer1;
            L0Table.KVMatcher matcher2 = (p) -> p == pointer2;
            L0Table.KVMatcher matcher3 = (p) -> p == pointer3;
            L0Table.KVMatcher matcher4 = (p) -> p == pointer4;

            // Fill all 4 slots in bucket 0
            assertTrue(l0Table.put(keyHash, tag1, pointer1));
            assertTrue(l0Table.put(keyHash, tag2, pointer2));
            assertTrue(l0Table.put(keyHash, tag3, pointer3));
            assertTrue(l0Table.put(keyHash, tag4, pointer4));

            // Verify all entries
            assertEquals(pointer1, l0Table.get(keyHash, tag1, matcher1));
            assertEquals(pointer2, l0Table.get(keyHash, tag2, matcher2));
            assertEquals(pointer3, l0Table.get(keyHash, tag3, matcher3));
            assertEquals(pointer4, l0Table.get(keyHash, tag4, matcher4));

            // Stats should show 4 valid slots
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(4, stats.validSlots);
        }

        @Test
        void testBucketEviction() {
            // Fill a bucket and add one more to trigger eviction
            int keyHash = 0;
            short tag1 = (short) 0x1111;
            short tag2 = (short) 0x2222;
            short tag3 = (short) 0x3333;
            short tag4 = (short) 0x4444;
            short tag5 = (short) 0x5555; // This should trigger eviction

            long pointer1 = 0x111L;
            long pointer2 = 0x222L;
            long pointer3 = 0x333L;
            long pointer4 = 0x444L;
            long pointer5 = 0x555L;

            L0Table.KVMatcher matcher1 = (p) -> p == pointer1;
            L0Table.KVMatcher matcher5 = (p) -> p == pointer5;

            // Fill all 4 slots
            assertTrue(l0Table.put(keyHash, tag1, pointer1));
            assertTrue(l0Table.put(keyHash, tag2, pointer2));
            assertTrue(l0Table.put(keyHash, tag3, pointer3));
            assertTrue(l0Table.put(keyHash, tag4, pointer4));

            // Add delay to ensure different timestamps for LRU
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Add 5th entry (should evict one of the existing)
            assertTrue(l0Table.put(keyHash, tag5, pointer5));

            // New entry should be findable
            assertEquals(pointer5, l0Table.get(keyHash, tag5, matcher5));

            // Should still have 4 valid slots (eviction happened)
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(4, stats.validSlots);
        }

        @Test
        void testTagMismatch() {
            int keyHash = 12345;
            short tag1 = (short) 0x1234;
            short tag2 = (short) 0x5678;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            // Put with tag1
            assertTrue(l0Table.put(keyHash, tag1, kvPointer));

            // Try to get with tag2 (should not find)
            assertEquals(0, l0Table.get(keyHash, tag2, matcher));

            // Get with correct tag should work
            assertEquals(kvPointer, l0Table.get(keyHash, tag1, matcher));
        }
    }

    @Nested
    class HashDistributionTests {

        @Test
        void testDifferentBucketsForDifferentHashes() {
            short tag = (short) 0x1234;
            long basePointer = 0x1000000000000000L;

            // Add entries to different buckets
            for (int i = 0; i < 16; i++) {
                int keyHash = i; // This should map to different buckets
                long pointer = basePointer + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;

                assertTrue(l0Table.put(keyHash, tag, pointer));
                assertEquals(pointer, l0Table.get(keyHash, tag, matcher));
            }

            // Should have 16 valid slots (one per bucket)
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(16, stats.validSlots);
            assertEquals(16, stats.bucketCount);
            assertEquals(1.0 / 4.0, stats.loadFactor, 0.001); // 16 slots out of 64 total
        }

        @Test
        void testHashMasking() {
            // Test that hash values properly map to bucket indices
            short tag = (short) 0x1234;

            // Hash that should map to bucket 0
            int hash0 = 16; // 16 & 15 = 0
            long pointer0 = 0x1000L;

            // Hash that should map to bucket 1
            int hash1 = 17; // 17 & 15 = 1
            long pointer1 = 0x2000L;

            L0Table.KVMatcher matcher0 = (p) -> p == pointer0;
            L0Table.KVMatcher matcher1 = (p) -> p == pointer1;

            assertTrue(l0Table.put(hash0, tag, pointer0));
            assertTrue(l0Table.put(hash1, tag, pointer1));

            assertEquals(pointer0, l0Table.get(hash0, tag, matcher0));
            assertEquals(pointer1, l0Table.get(hash1, tag, matcher1));
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testZeroPointer() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0L; // Zero pointer

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            // Should be able to store zero pointer
            assertTrue(l0Table.put(keyHash, tag, kvPointer));
            assertEquals(kvPointer, l0Table.get(keyHash, tag, matcher));
        }

        @Test
        void testNegativeHash() {
            int keyHash = -12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            assertTrue(l0Table.put(keyHash, tag, kvPointer));
            assertEquals(kvPointer, l0Table.get(keyHash, tag, matcher));
        }

        @Test
        void testMaxValues() {
            int keyHash = Integer.MAX_VALUE;
            short tag = Short.MAX_VALUE;
            long kvPointer = Long.MAX_VALUE;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            assertTrue(l0Table.put(keyHash, tag, kvPointer));
            assertEquals(kvPointer, l0Table.get(keyHash, tag, matcher));
        }

        @Test
        void testRemoveNonExistentEntry() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            // Remove non-existent entry should not crash
            assertDoesNotThrow(() -> l0Table.remove(keyHash, tag, kvPointer));

            // Stats should remain at 0
            L0Table.L0TableStats stats = l0Table.getStats();
            assertEquals(0, stats.validSlots);
        }

        @Test
        void testKVMatcherReturnsFalse() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            // Matcher that always returns false
            L0Table.KVMatcher neverMatch = (pointer) -> false;

            assertTrue(l0Table.put(keyHash, tag, kvPointer));

            // Should not find entry due to matcher
            assertEquals(0, l0Table.get(keyHash, tag, neverMatch));
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testClearTable() {
            // Add some entries
            for (int i = 0; i < 8; i++) {
                short tag = (short) i;
                long pointer = 0x1000L + i;
                assertTrue(l0Table.put(i, tag, pointer));
            }

            // Verify entries exist
            L0Table.L0TableStats statsBefore = l0Table.getStats();
            assertEquals(8, statsBefore.validSlots);

            // Clear table
            l0Table.clear();

            // Verify all entries are gone
            L0Table.L0TableStats statsAfter = l0Table.getStats();
            assertEquals(0, statsAfter.validSlots);
            assertEquals(0.0, statsAfter.loadFactor);
        }

        @Test
        void testTableAfterClear() {
            // Add entry, clear, then add again
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            // Add entry
            assertTrue(l0Table.put(keyHash, tag, kvPointer));
            assertEquals(kvPointer, l0Table.get(keyHash, tag, matcher));

            // Clear
            l0Table.clear();
            assertEquals(0, l0Table.get(keyHash, tag, matcher));

            // Add again
            assertTrue(l0Table.put(keyHash, tag, kvPointer));
            assertEquals(kvPointer, l0Table.get(keyHash, tag, matcher));
        }

        @Test
        void testCloseAndResourceCleanup() throws Exception {
            long initialUsedBytes = allocator.getUsedBytes();

            l0Table.close();

            // Memory should be freed
            assertTrue(allocator.getUsedBytes() < initialUsedBytes);
        }
    }

    @Nested
    class PerformanceTests {

        @Test
        void testLoadFactorCalculation() {
            int totalSlots = 16 * 4; // 16 buckets * 4 slots

            // Add entries and check load factor progression
            for (int i = 1; i <= 32; i++) {
                short tag = (short) i;
                long pointer = 0x1000L + i;
                int hash = i * 17; // Spread across buckets

                l0Table.put(hash, tag, pointer);

                L0Table.L0TableStats stats = l0Table.getStats();
                assertTrue(stats.validSlots <= totalSlots);
                assertTrue(stats.loadFactor >= 0.0 && stats.loadFactor <= 1.0);
            }
        }

        @Test
        void testHighLoadScenario() {
            // Try to fill the table beyond capacity
            int attempts = 100;
            int successful = 0;

            for (int i = 0; i < attempts; i++) {
                short tag = (short) (i & 0xFFFF);
                long pointer = 0x1000L + i;
                int hash = i;

                if (l0Table.put(hash, tag, pointer)) {
                    successful++;
                }
            }

            L0Table.L0TableStats stats = l0Table.getStats();

            // Should not exceed total capacity
            assertTrue(stats.validSlots <= stats.totalSlots);

            // Should have some successful insertions
            assertTrue(successful > 0);

            // Load factor should be reasonable
            assertTrue(stats.loadFactor <= 1.0);
        }
    }

    @Nested
    class TableSizeTests {

        @Test
        void testDifferentTableSizes() throws Exception {
            // Test with different bucket counts
            int[] bucketPowers = {2, 3, 4, 5, 6}; // 4, 8, 16, 32, 64 buckets

            for (int power : bucketPowers) {
                try (L0Table table = new L0Table(allocator, power)) {
                    int expectedBuckets = 1 << power;
                    int expectedTotalSlots = expectedBuckets * 4;

                    L0Table.L0TableStats stats = table.getStats();
                    assertEquals(expectedBuckets, stats.bucketCount);
                    assertEquals(expectedTotalSlots, stats.totalSlots);
                    assertEquals(0, stats.validSlots);
                    assertEquals(0.0, stats.loadFactor);

                    // Test basic functionality
                    short tag = (short) 0x1234;
                    long pointer = 0x5678L;
                    L0Table.KVMatcher matcher = (p) -> p == pointer;

                    assertTrue(table.put(12345, tag, pointer));
                    assertEquals(pointer, table.get(12345, tag, matcher));
                }
            }
        }

        @Test
        void testMinimalTable() throws Exception {
            // Test with smallest possible table (1 bucket)
            try (L0Table table = new L0Table(allocator, 0)) {
                L0Table.L0TableStats stats = table.getStats();
                assertEquals(1, stats.bucketCount);
                assertEquals(4, stats.totalSlots);

                // All hashes should map to bucket 0
                for (int hash : new int[]{0, 1, 15, 255, 12345}) {
                    short tag = (short) hash;
                    long pointer = 0x1000L + hash;
                    L0Table.KVMatcher matcher = (p) -> p == pointer;

                    assertTrue(table.put(hash, tag, pointer));
                    assertEquals(pointer, table.get(hash, tag, matcher));
                }
            }
        }
    }
}
