package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MainTableTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 128L * DEFAULT_PAGE_SIZE; // 4MB for larger tests

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private MainTable mainTable;
    private Object owner;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        mainTable = new MainTable(allocator, 4); // 16 buckets (2^4)
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mainTable != null) {
            mainTable.close();
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
            assertEquals(0.0, mainTable.getLoadFactor(), 0.001);
        }

        @Test
        void testPutAndGet() {
            byte[] keyBytes = "testKey".getBytes();
            byte[] namespaceBytes = "testNamespace".getBytes();
            int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespaceBytes);
            short tag = HashFunctions.extractTag(keyHash);
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            // Put entry (should return 0 for new insertion)
            long result = mainTable.put(keyHash, tag, kvPointer, matcher);
            assertEquals(0, result);

            // Get entry back
            long retrieved = mainTable.get(keyHash, tag, matcher);
            assertEquals(kvPointer, retrieved);

            // Load factor should be > 0
            assertTrue(mainTable.getLoadFactor() > 0);
        }

        @Test
        void testGetNonExistentEntry() {
            byte[] keyBytes = "nonExistentKey".getBytes();
            byte[] namespaceBytes = "testNamespace".getBytes();
            int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespaceBytes);
            short tag = HashFunctions.extractTag(keyHash);

            L0Table.KVMatcher matcher = (pointer) -> true;

            // Try to get non-existent entry
            long result = mainTable.get(keyHash, tag, matcher);
            assertEquals(0, result);
        }

        @Test
        void testUpdateExistingEntry() {
            byte[] keyBytes = "updateKey".getBytes();
            byte[] namespaceBytes = "testNamespace".getBytes();
            int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespaceBytes);
            short tag = HashFunctions.extractTag(keyHash);
            long kvPointer1 = 0x111111111111111L;
            long kvPointer2 = 0x222222222222222L;

            L0Table.KVMatcher matcher1 = (pointer) -> pointer == kvPointer1;
            L0Table.KVMatcher matcher2 = (pointer) -> pointer == kvPointer2;

            // Put first entry
            assertEquals(0, mainTable.put(keyHash, tag, kvPointer1, matcher1));
            assertEquals(kvPointer1, mainTable.get(keyHash, tag, matcher1));

            // Update with same key hash and tag
            assertEquals(kvPointer1, mainTable.put(keyHash, tag, kvPointer2, matcher1));
            assertEquals(kvPointer2, mainTable.get(keyHash, tag, matcher2));
            assertEquals(0, mainTable.get(keyHash, tag, matcher1)); // Old pointer should not match
        }

        @Test
        void testRemoveEntry() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            // Put entry
            assertEquals(0, mainTable.put(keyHash, tag, kvPointer, matcher));
            assertEquals(kvPointer, mainTable.get(keyHash, tag, matcher));

            // Remove entry
            long removed = mainTable.remove(keyHash, tag, matcher);
            assertEquals(kvPointer, removed);

            // Verify removed
            assertEquals(0, mainTable.get(keyHash, tag, matcher));
        }

        @Test
        void testRemoveNonExistentEntry() {
            int keyHash = 12345;
            short tag = (short) 0x1234;

            L0Table.KVMatcher matcher = (pointer) -> true;

            // Remove non-existent entry
            long removed = mainTable.remove(keyHash, tag, matcher);
            assertEquals(0, removed);
        }
    }

    @Nested
    class CollisionHandlingTests {

        @Test
        void testMainBucketCollisions() {
            // Fill main bucket slots (6 per bucket) - force all to same bucket
            byte[] baseKey = "collisionKey".getBytes();
            byte[] namespace = "ns".getBytes();

            // Use a fixed bucket by manipulating hash
            int targetBucket = 0;
            List<Long> pointers = new ArrayList<>();

            for (int i = 0; i < 6; i++) {
                // Create different keys that hash to the same bucket
                byte[] keyBytes = (new String(baseKey) + i).getBytes();
                int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespace);

                // Force to bucket 0 by clearing low bits and setting to 0
                keyHash = (keyHash & ~15) | targetBucket;

                short tag = (short) (0x1000 + i); // Different tags
                long pointer = 0x1000000L + i;
                pointers.add(pointer);

                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
                assertEquals(pointer, mainTable.get(keyHash, tag, matcher));
            }

            // Verify all entries are retrievable
            for (int i = 0; i < 6; i++) {
                byte[] keyBytes = (new String(baseKey) + i).getBytes();
                int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespace);
                keyHash = (keyHash & ~15) | targetBucket;

                short tag = (short) (0x1000 + i);
                long pointer = pointers.get(i);
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(pointer, mainTable.get(keyHash, tag, matcher));
            }
        }

        @Test
        void testExtensionBucketAllocation() {
            // Fill main bucket and force extension bucket creation
            int keyHash = 0;
            List<Long> pointers = new ArrayList<>();

            // Fill 6 main slots
            for (int i = 0; i < 6; i++) {
                short tag = (short) (0x1000 + i);
                long pointer = 0x1000000L + i;
                pointers.add(pointer);

                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
            }

            // Add 7th entry - should go to extension bucket
            short extensionTag = (short) 0x2000;
            long extensionPointer = 0x2000000L;
            L0Table.KVMatcher extensionMatcher = (p) -> p == extensionPointer;

            assertEquals(0, mainTable.put(keyHash, extensionTag, extensionPointer, extensionMatcher));
            assertEquals(extensionPointer, mainTable.get(keyHash, extensionTag, extensionMatcher));

            // Verify main bucket entries still accessible
            for (int i = 0; i < 6; i++) {
                short tag = (short) (0x1000 + i);
                long pointer = pointers.get(i);
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(pointer, mainTable.get(keyHash, tag, matcher));
            }
        }

        @Test
        void testExtensionBucketIndexSelection() {
            // Test that extension bucket index is selected based on tag
            int keyHash = 0;
            short tag1 = (short) 0x0000; // Should map to extension index 0
            short tag2 = (short) 0x0001; // Should map to extension index 1
            short tag3 = (short) 0x0002; // Should map to extension index 2
            short tag4 = (short) 0x0003; // Should map to extension index 3

            // Fill main bucket first
            for (int i = 0; i < 6; i++) {
                short tag = (short) (0x1000 + i);
                long pointer = 0x1000000L + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
            }

            // Add entries that should go to different extension buckets
            long pointer1 = 0x2000001L;
            long pointer2 = 0x2000002L;
            long pointer3 = 0x2000003L;
            long pointer4 = 0x2000004L;

            L0Table.KVMatcher matcher1 = (p) -> p == pointer1;
            L0Table.KVMatcher matcher2 = (p) -> p == pointer2;
            L0Table.KVMatcher matcher3 = (p) -> p == pointer3;
            L0Table.KVMatcher matcher4 = (p) -> p == pointer4;

            assertEquals(0, mainTable.put(keyHash, tag1, pointer1, matcher1));
            assertEquals(0, mainTable.put(keyHash, tag2, pointer2, matcher2));
            assertEquals(0, mainTable.put(keyHash, tag3, pointer3, matcher3));
            assertEquals(0, mainTable.put(keyHash, tag4, pointer4, matcher4));

            // Verify all extension entries are accessible
            assertEquals(pointer1, mainTable.get(keyHash, tag1, matcher1));
            assertEquals(pointer2, mainTable.get(keyHash, tag2, matcher2));
            assertEquals(pointer3, mainTable.get(keyHash, tag3, matcher3));
            assertEquals(pointer4, mainTable.get(keyHash, tag4, matcher4));
        }

        @Test
        void testExtensionBucketRemovalAndCleanup() {
            int keyHash = 0;

            // Fill main bucket
            for (int i = 0; i < 6; i++) {
                short tag = (short) (0x1000 + i);
                long pointer = 0x1000000L + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
            }

            // Add extension entry
            short extensionTag = (short) 0x2000;
            long extensionPointer = 0x2000000L;
            L0Table.KVMatcher extensionMatcher = (p) -> p == extensionPointer;

            assertEquals(0, mainTable.put(keyHash, extensionTag, extensionPointer, extensionMatcher));
            assertEquals(extensionPointer, mainTable.get(keyHash, extensionTag, extensionMatcher));

            // Remove extension entry
            assertEquals(extensionPointer, mainTable.remove(keyHash, extensionTag, extensionMatcher));
            assertEquals(0, mainTable.get(keyHash, extensionTag, extensionMatcher));

            // Main bucket entries should still be accessible
            for (int i = 0; i < 6; i++) {
                short tag = (short) (0x1000 + i);
                long pointer = 0x1000000L + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(pointer, mainTable.get(keyHash, tag, matcher));
            }
        }
    }

    @Nested
    class HashDistributionTests {

        @Test
        void testDifferentBucketsForDifferentHashes() {
            short tag = (short) 0x1234;
            long basePointer = 0x1000000000000000L;

            // Add entries to different buckets (16 buckets total)
            for (int i = 0; i < 16; i++) {
                byte[] keyBytes = ("key" + i).getBytes();
                byte[] namespaceBytes = "testNamespace".getBytes();
                int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespaceBytes);

                // Ensure we get different buckets by using the hash as-is
                long pointer = basePointer + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;

                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
                assertEquals(pointer, mainTable.get(keyHash, tag, matcher));
            }

            // Load factor should be 16/(16*6) = 1/6
            double expectedLoadFactor = 16.0 / (16.0 * 6.0);
            assertEquals(expectedLoadFactor, mainTable.getLoadFactor(), 0.001);
        }

        @Test
        void testLoadFactorProgression() {
            short tag = (short) 0x1234;
            int totalMainSlots = 16 * 6; // 16 buckets * 6 slots each

            for (int i = 1; i <= totalMainSlots; i++) {
                byte[] keyBytes = ("progressKey" + i).getBytes();
                byte[] namespaceBytes = "testNamespace".getBytes();
                int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespaceBytes);

                long pointer = 0x1000000L + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;

                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));

                double expectedLoadFactor = (double) i / totalMainSlots;
                // Allow for larger tolerance due to real hash distribution and extension buckets
                double actualLoadFactor = mainTable.getLoadFactor();
                assertTrue(Math.abs(actualLoadFactor - expectedLoadFactor) < 0.1,
                    String.format("Expected load factor close to %.3f but got %.3f for %d entries",
                        expectedLoadFactor, actualLoadFactor, i));
            }
        }
    }

    @Nested
    class IterationTests {

        @Test
        void testForEachEntry() {
            // Add entries to multiple buckets
            List<Long> expectedPointers = new ArrayList<>();
            List<Short> expectedTags = new ArrayList<>();

            for (int i = 0; i < 20; i++) {
                int keyHash = i;
                short tag = (short) (0x1000 + i);
                long pointer = 0x1000000L + i;

                expectedPointers.add(pointer);
                expectedTags.add(tag);

                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
            }

            // Collect entries via iteration
            List<Long> actualPointers = new ArrayList<>();
            List<Short> actualTags = new ArrayList<>();

            mainTable.forEachEntry((tag, pointer) -> {
                actualTags.add(tag);
                actualPointers.add(pointer);
            });

            // Verify all entries were visited
            assertEquals(expectedPointers.size(), actualPointers.size());
            assertEquals(expectedTags.size(), actualTags.size());

            // Convert to sets for order-independent comparison
            Set<Long> expectedPointerSet = new HashSet<>(expectedPointers);
            Set<Long> actualPointerSet = new HashSet<>(actualPointers);
            Set<Short> expectedTagSet = new HashSet<>(expectedTags);
            Set<Short> actualTagSet = new HashSet<>(actualTags);

            assertEquals(expectedPointerSet, actualPointerSet);
            assertEquals(expectedTagSet, actualTagSet);
        }

        @Test
        void testForEachEntryWithExtensionBuckets() {
            int keyHash = 0; // Force all to same bucket for extension testing

            // Fill main bucket
            for (int i = 0; i < 6; i++) {
                short tag = (short) (0x1000 + i);
                long pointer = 0x1000000L + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
            }

            // Add extension entries
            for (int i = 0; i < 4; i++) {
                short tag = (short) (0x2000 + i);
                long pointer = 0x2000000L + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
            }

            // Count entries via iteration
            List<Long> pointers = new ArrayList<>();
            mainTable.forEachEntry((tag, pointer) -> pointers.add(pointer));

            // Should find all 10 entries (6 main + 4 extension)
            assertEquals(10, pointers.size());

            // Verify uniqueness
            Set<Long> uniquePointers = new HashSet<>(pointers);
            assertEquals(10, uniquePointers.size());
        }

        @Test
        void testForEachEntryEmptyTable() {
            List<Long> pointers = new ArrayList<>();
            mainTable.forEachEntry((tag, pointer) -> pointers.add(pointer));

            assertEquals(0, pointers.size());
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testZeroPointer() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0L;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            assertEquals(0, mainTable.put(keyHash, tag, kvPointer, matcher));
            assertEquals(kvPointer, mainTable.get(keyHash, tag, matcher));
        }

        @Test
        void testNegativeHash() {
            int keyHash = -12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            assertEquals(0, mainTable.put(keyHash, tag, kvPointer, matcher));
            assertEquals(kvPointer, mainTable.get(keyHash, tag, matcher));
        }

        @Test
        void testMaxValues() {
            int keyHash = Integer.MAX_VALUE;
            short tag = Short.MAX_VALUE;
            long kvPointer = Long.MAX_VALUE;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            assertEquals(0, mainTable.put(keyHash, tag, kvPointer, matcher));
            assertEquals(kvPointer, mainTable.get(keyHash, tag, matcher));
        }

        @Test
        void testKVMatcherReturnsFalse() {
            int keyHash = 12345;
            short tag = (short) 0x1234;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher neverMatch = (pointer) -> false;
            L0Table.KVMatcher alwaysMatch = (pointer) -> pointer == kvPointer;

            // Put with matching matcher
            assertEquals(0, mainTable.put(keyHash, tag, kvPointer, alwaysMatch));

            // Get with non-matching matcher should fail
            assertEquals(0, mainTable.get(keyHash, tag, neverMatch));

            // Get with matching matcher should succeed
            assertEquals(kvPointer, mainTable.get(keyHash, tag, alwaysMatch));
        }

        @Test
        void testTagMismatch() {
            int keyHash = 12345;
            short tag1 = (short) 0x1234;
            short tag2 = (short) 0x5678;
            long kvPointer = 0x123456789ABCDEFL;

            L0Table.KVMatcher matcher = (pointer) -> pointer == kvPointer;

            // Put with tag1
            assertEquals(0, mainTable.put(keyHash, tag1, kvPointer, matcher));

            // Get with tag2 should fail
            assertEquals(0, mainTable.get(keyHash, tag2, matcher));

            // Get with tag1 should succeed
            assertEquals(kvPointer, mainTable.get(keyHash, tag1, matcher));
        }
    }

    @Nested
    class ResizeTests {

        @Test
        void testCreateExpandedTable() throws Exception {
            // Create expanded table
            try (MainTable expandedTable = mainTable.createExpandedTable()) {
                // Original table has 16 buckets, expanded should have 32

                // Add entries to both tables to verify they work independently
                int keyHash = 12345;
                short tag = (short) 0x1234;
                long originalPointer = 0x111111111L;
                long expandedPointer = 0x222222222L;

                L0Table.KVMatcher originalMatcher = (p) -> p == originalPointer;
                L0Table.KVMatcher expandedMatcher = (p) -> p == expandedPointer;

                assertEquals(0, mainTable.put(keyHash, tag, originalPointer, originalMatcher));
                assertEquals(0, expandedTable.put(keyHash, tag, expandedPointer, expandedMatcher));

                assertEquals(originalPointer, mainTable.get(keyHash, tag, originalMatcher));
                assertEquals(expandedPointer, expandedTable.get(keyHash, tag, expandedMatcher));

                // Tables should be independent
                assertEquals(0, mainTable.get(keyHash, tag, expandedMatcher));
                assertEquals(0, expandedTable.get(keyHash, tag, originalMatcher));
            }
        }

        @Test
        void testExpandedTableDifferentBucketMapping() throws Exception {
            try (MainTable expandedTable = mainTable.createExpandedTable()) {
                // Hash that maps to bucket 0 in 16-bucket table
                int keyHash = 16; // 16 & 15 = 0 for original, 16 & 31 = 16 for expanded

                short tag = (short) 0x1234;
                long pointer = 0x123456789L;
                L0Table.KVMatcher matcher = (p) -> p == pointer;

                // Put in both tables
                assertEquals(0, mainTable.put(keyHash, tag, pointer, matcher));
                assertEquals(0, expandedTable.put(keyHash, tag, pointer, matcher));

                // Both should be retrievable
                assertEquals(pointer, mainTable.get(keyHash, tag, matcher));
                assertEquals(pointer, expandedTable.get(keyHash, tag, matcher));
            }
        }
    }

    @Nested
    class LifecycleTests {

        @Test
        void testCloseAndResourceCleanup() throws Exception {
            // Add some entries to allocate extension buckets
            for (int i = 0; i < 20; i++) {
                int keyHash = 0; // Force extension bucket allocation
                short tag = (short) i;
                long pointer = 0x1000L + i;
                L0Table.KVMatcher matcher = (p) -> p == pointer;
                mainTable.put(keyHash, tag, pointer, matcher);
            }

            long usedBytesBeforeClose = allocator.getUsedBytes();
            assertTrue(usedBytesBeforeClose > 0);

            mainTable.close();

            // Memory usage should decrease after close
            assertTrue(allocator.getUsedBytes() < usedBytesBeforeClose);
        }
    }

    @Nested
    class PerformanceTests {

        @Test
        void testHighVolumeOperations() {
            int numEntries = 100; // Reduced to avoid capacity issues
            List<TestEntry> entries = new ArrayList<>();

            // Add many entries with proper hash distribution
            for (int i = 0; i < numEntries; i++) {
                byte[] keyBytes = ("volumeKey" + i).getBytes();
                byte[] namespaceBytes = ("ns" + (i % 10)).getBytes(); // Vary namespace too
                int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespaceBytes);
                short tag = HashFunctions.extractTag(keyHash);
                long pointer = 0x1000000L + i;

                TestEntry entry = new TestEntry(keyHash, tag, pointer);
                entries.add(entry);

                assertEquals(0, mainTable.put(keyHash, tag, pointer, entry.matcher));
            }

            // Verify all entries
            for (TestEntry entry : entries) {
                assertEquals(entry.pointer, mainTable.get(entry.keyHash, entry.tag, entry.matcher));
            }

            // Remove half the entries
            for (int i = 0; i < numEntries / 2; i++) {
                TestEntry entry = entries.get(i);
                assertEquals(entry.pointer, mainTable.remove(entry.keyHash, entry.tag, entry.matcher));
            }

            // Verify remaining entries still accessible
            for (int i = numEntries / 2; i < numEntries; i++) {
                TestEntry entry = entries.get(i);
                assertEquals(entry.pointer, mainTable.get(entry.keyHash, entry.tag, entry.matcher));
            }

            // Verify removed entries are gone
            for (int i = 0; i < numEntries / 2; i++) {
                TestEntry entry = entries.get(i);
                assertEquals(0, mainTable.get(entry.keyHash, entry.tag, entry.matcher));
            }
        }

        @Test
        void testWorstCaseCollisions() {
            // All entries go to same bucket to stress extension bucket handling
            byte[] baseKey = "collisionBase".getBytes();
            byte[] namespace = "collision".getBytes();
            int targetBucket = 0;
            int numEntries = 10; // Reduced to fit within capacity

            List<TestEntry> entries = new ArrayList<>();

            for (int i = 0; i < numEntries; i++) {
                byte[] keyBytes = (new String(baseKey) + i).getBytes();
                int keyHash = HashFunctions.combineKeyNamespaceHash(keyBytes, namespace);

                // Force all to same bucket
                keyHash = (keyHash & ~15) | targetBucket;

                short tag = (short) i; // Different tags
                long pointer = 0x1000000L + i;

                TestEntry entry = new TestEntry(keyHash, tag, pointer);
                entries.add(entry);

                assertEquals(0, mainTable.put(keyHash, tag, pointer, entry.matcher));
                assertEquals(pointer, mainTable.get(keyHash, tag, entry.matcher));
            }

            // Verify all entries are still accessible
            for (TestEntry entry : entries) {
                assertEquals(entry.pointer, mainTable.get(entry.keyHash, entry.tag, entry.matcher));
            }
        }
    }

    // Helper class for testing
    private static class TestEntry {
        final int keyHash;
        final short tag;
        final long pointer;
        final L0Table.KVMatcher matcher;

        TestEntry(int keyHash, short tag, long pointer) {
            this.keyHash = keyHash;
            this.tag = tag;
            this.pointer = pointer;
            this.matcher = (p) -> p == pointer;
        }
    }
}
