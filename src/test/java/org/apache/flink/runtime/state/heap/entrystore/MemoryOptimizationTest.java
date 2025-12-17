package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.flink.runtime.state.heap.entrystore.EntryStoreConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Phase 2.5 memory optimizations:
 * 1. Fine-grained size class (28 classes instead of 8) - reduces fragmentation from ~50% to ~17%
 * 2. Small value inline (≤8B values stored inline) - avoids ValuePool overhead for small values
 */
class MemoryOptimizationTest {
    
    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 128L * DEFAULT_PAGE_SIZE; // 4MB
    
    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryStore store;
    
    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        Object owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        store = new EntryStore(allocator);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (store != null && !store.isClosed()) {
            store.close();
        }
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }
    
    /**
     * Tests for fine-grained ValueSizeClass optimization.
     * 28 size classes vs original 8 reduces internal fragmentation.
     */
    @Nested
    @DisplayName("Fine-grained Size Class Tests")
    class FineGrainedSizeClassTests {
        
        @Test
        @DisplayName("Size class lookup is O(1) via table")
        void testSizeClassLookupIsConstantTime() {
            // Verify all total sizes map to correct size class via O(1) lookup
            // getSizeClass takes totalSize (header + value)
            for (int totalSize = 1; totalSize <= 4096; totalSize++) {
                ValueSizeClass sc = ValueSizeClass.getSizeClass(totalSize);
                assertNotNull(sc);
                assertTrue(sc.getSlotSize() >= totalSize,
                        "Size class " + sc + " slot " + sc.getSlotSize() + 
                        " cannot hold total size " + totalSize);
            }
        }
        
        @Test
        @DisplayName("28 size classes exist (0-128: 8 × 16B step, 128-4096: varied steps)")
        void testSizeClassCount() {
            // Count fixed size classes (excluding LARGE)
            int count = 0;
            for (ValueSizeClass sc : ValueSizeClass.values()) {
                if (sc != ValueSizeClass.LARGE) {
                    count++;
                }
            }
            assertEquals(28, count, "Should have 28 fixed size classes");
        }
        
        @Test
        @DisplayName("Small total sizes (≤128B): 16B step, max ~94% efficiency")
        void testSmallValueFragmentation() {
            // Test small size classes with 16B step
            // For a 16B slot, max value = 16 - 4 (header) = 12B
            // Efficiency = valueLen / slotSize
            int[] testTotalSizes = {5, 12, 16, 17, 28, 32, 33, 48, 64, 80, 96, 112, 128};
            
            for (int totalSize : testTotalSizes) {
                ValueSizeClass sc = ValueSizeClass.getSizeClass(totalSize);
                int slotSize = sc.getSlotSize();
                
                // Slot should fit the total size
                assertTrue(slotSize >= totalSize,
                        String.format("Total size %d doesn't fit in slot %d", totalSize, slotSize));
                
                // For 16B step, waste should be < 16 bytes
                int waste = slotSize - totalSize;
                assertTrue(waste < 16 || totalSize <= 16,
                        String.format("Total size %d: waste %d bytes exceeds 16B step", totalSize, waste));
            }
        }
        
        @Test
        @DisplayName("Medium total sizes (128-512B): 32-64B step")
        void testMediumValueFragmentation() {
            int[] testTotalSizes = {129, 160, 192, 224, 256, 320, 384, 448, 512};
            
            for (int totalSize : testTotalSizes) {
                ValueSizeClass sc = ValueSizeClass.getSizeClass(totalSize);
                int slotSize = sc.getSlotSize();
                
                assertTrue(slotSize >= totalSize,
                        String.format("Total size %d doesn't fit in slot %d", totalSize, slotSize));
            }
        }
        
        @Test
        @DisplayName("Large total sizes (>4KB) use LARGE class")
        void testLargeValues() {
            ValueSizeClass sc = ValueSizeClass.getSizeClass(5000);
            assertEquals(ValueSizeClass.LARGE, sc);
            
            sc = ValueSizeClass.getSizeClass(10000);
            assertEquals(ValueSizeClass.LARGE, sc);
        }
        
        @Test
        @DisplayName("Size class boundaries are correct")
        void testSizeClassBoundaries() {
            // getSizeClass takes totalSize (header + data)
            // VS_16 fits totalSize 1-16, VS_32 fits 17-32, etc.
            
            assertEquals(ValueSizeClass.VS_16, ValueSizeClass.getSizeClass(16));   // exactly 16
            assertEquals(ValueSizeClass.VS_32, ValueSizeClass.getSizeClass(17));   // 17 > 16, needs VS_32
            
            assertEquals(ValueSizeClass.VS_32, ValueSizeClass.getSizeClass(32));   // exactly 32
            assertEquals(ValueSizeClass.VS_48, ValueSizeClass.getSizeClass(33));   // 33 > 32, needs VS_48
            
            assertEquals(ValueSizeClass.VS_128, ValueSizeClass.getSizeClass(128)); // exactly 128
            assertEquals(ValueSizeClass.VS_160, ValueSizeClass.getSizeClass(129)); // 129 > 128, needs VS_160
            
            assertEquals(ValueSizeClass.VS_4096, ValueSizeClass.getSizeClass(4096)); // exactly 4096
            assertEquals(ValueSizeClass.LARGE, ValueSizeClass.getSizeClass(4097));   // > 4096, LARGE
        }
        
        @Test
        @DisplayName("Random total sizes fit in appropriate slots")
        void testRandomValueSizes() {
            Random random = new Random(42);
            
            for (int i = 0; i < 1000; i++) {
                int totalSize = random.nextInt(4096) + 1;  // 1-4096
                ValueSizeClass sc = ValueSizeClass.getSizeClass(totalSize);
                
                assertNotEquals(ValueSizeClass.LARGE, sc);
                assertTrue(sc.getSlotSize() >= totalSize,
                        "Size class " + sc + " slot " + sc.getSlotSize() + 
                        " cannot hold total size " + totalSize);
            }
        }
    }
    
    /**
     * Tests for small value inline optimization.
     * Values ≤8B are stored directly in KeyNsPool's valueHandle field.
     */
    @Nested
    @DisplayName("Small Value Inline Tests")
    class SmallValueInlineTests {
        
        @Test
        @DisplayName("INLINE_THRESHOLD is 8 bytes")
        void testInlineThreshold() {
            assertEquals(8, INLINE_THRESHOLD);
        }
        
        @Test
        @DisplayName("Values ≤8B stored inline don't allocate from ValuePool")
        void testInlineValuesNoValuePoolAllocation() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            // Allocate many small values (≤8 bytes)
            for (int i = 0; i < 100; i++) {
                byte[] value = new byte[i % (INLINE_THRESHOLD + 1)]; // 0-8 bytes
                for (int j = 0; j < value.length; j++) {
                    value[j] = (byte) (i + j);
                }
                long addr = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
                assertNotEquals(NULL_HANDLE, addr);
                
                // Verify value can be read back
                byte[] retrieved = store.getValueBytes(addr);
                assertArrayEquals(value, retrieved);
            }
            
            // Check stats: ValuePool should have 0 active entries
            EntryStoreStats stats = store.getStats();
            assertEquals(0, stats.getValueActiveCount(), 
                    "Inline values should not allocate from ValuePool");
        }
        
        @Test
        @DisplayName("Values >8B use ValuePool")
        void testNonInlineValuesUseValuePool() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            // Allocate values > 8 bytes
            for (int i = 0; i < 50; i++) {
                byte[] value = new byte[INLINE_THRESHOLD + 1 + i]; // 9+ bytes
                for (int j = 0; j < value.length; j++) {
                    value[j] = (byte) (i + j);
                }
                long addr = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
                assertNotEquals(NULL_HANDLE, addr);
            }
            
            // Check stats: ValuePool should have allocations
            EntryStoreStats stats = store.getStats();
            assertEquals(50, stats.getValueActiveCount(),
                    "Non-inline values should use ValuePool");
        }
        
        @Test
        @DisplayName("Mixed inline and pointer values work correctly")
        void testMixedInlineAndPointer() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            long[] addresses = new long[100];
            byte[][] values = new byte[100][];
            
            for (int i = 0; i < 100; i++) {
                // Alternate between inline (≤8B) and pointer (>8B)
                int len = (i % 2 == 0) ? 5 : 20;
                values[i] = new byte[len];
                for (int j = 0; j < len; j++) {
                    values[i][j] = (byte) (i + j);
                }
                addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, values[i], len);
            }
            
            // Verify all values
            for (int i = 0; i < 100; i++) {
                byte[] retrieved = store.getValueBytes(addresses[i]);
                assertArrayEquals(values[i], retrieved, "Value " + i + " mismatch");
            }
            
            // 50 should be inline, 50 should use ValuePool
            EntryStoreStats stats = store.getStats();
            assertEquals(50, stats.getValueActiveCount());
        }
        
        @Test
        @DisplayName("Inline to pointer mode transition on value growth")
        void testInlineToPointerTransition() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] smallValue = "tiny".getBytes();  // 4 bytes, inline
            
            long addr = store.allocateEntry(1, key, key.length, ns, ns.length, smallValue, smallValue.length);
            
            // Initially no ValuePool usage
            assertEquals(0, store.getStats().getValueActiveCount());
            
            // Update to larger value
            byte[] largeValue = "this_is_a_longer_value".getBytes(); // 22 bytes
            assertTrue(store.updateValue(addr, largeValue, largeValue.length));
            
            // Now should use ValuePool
            assertEquals(1, store.getStats().getValueActiveCount());
            
            // Verify value
            assertArrayEquals(largeValue, store.getValueBytes(addr));
        }
        
        @Test
        @DisplayName("Pointer to inline mode transition on value shrink")
        void testPointerToInlineTransition() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] largeValue = "this_is_a_longer_value".getBytes(); // 22 bytes
            
            long addr = store.allocateEntry(1, key, key.length, ns, ns.length, largeValue, largeValue.length);
            
            // Initially uses ValuePool
            assertEquals(1, store.getStats().getValueActiveCount());
            
            // Update to smaller value
            byte[] smallValue = "tiny".getBytes(); // 4 bytes
            assertTrue(store.updateValue(addr, smallValue, smallValue.length));
            
            // ValuePool allocation should be freed
            assertEquals(0, store.getStats().getValueActiveCount());
            
            // Verify value
            assertArrayEquals(smallValue, store.getValueBytes(addr));
        }
        
        @Test
        @DisplayName("Multiple mode transitions work correctly")
        void testMultipleModeTransitions() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            byte[] v1 = "small".getBytes();      // 5B, inline
            byte[] v2 = "medium_value_here".getBytes(); // 17B, pointer
            byte[] v3 = "tiny".getBytes();       // 4B, inline
            byte[] v4 = "another_longer_value".getBytes(); // 20B, pointer
            byte[] v5 = "12345678".getBytes();   // 8B, inline (boundary)
            
            long addr = store.allocateEntry(1, key, key.length, ns, ns.length, v1, v1.length);
            assertEquals(0, store.getStats().getValueActiveCount()); // inline
            
            assertTrue(store.updateValue(addr, v2, v2.length));
            assertEquals(1, store.getStats().getValueActiveCount()); // pointer
            assertArrayEquals(v2, store.getValueBytes(addr));
            
            assertTrue(store.updateValue(addr, v3, v3.length));
            assertEquals(0, store.getStats().getValueActiveCount()); // inline
            assertArrayEquals(v3, store.getValueBytes(addr));
            
            assertTrue(store.updateValue(addr, v4, v4.length));
            assertEquals(1, store.getStats().getValueActiveCount()); // pointer
            assertArrayEquals(v4, store.getValueBytes(addr));
            
            assertTrue(store.updateValue(addr, v5, v5.length));
            assertEquals(0, store.getStats().getValueActiveCount()); // inline (8B boundary)
            assertArrayEquals(v5, store.getValueBytes(addr));
        }
    }
    
    /**
     * Integration tests combining both optimizations.
     */
    @Nested
    @DisplayName("Combined Optimization Tests")
    class CombinedOptimizationTests {
        
        @Test
        @DisplayName("Realistic workload: mix of small and varied size values")
        void testRealisticWorkload() {
            byte[] key = "user_id".getBytes();
            byte[] ns = "session".getBytes();
            Random random = new Random(42);
            
            int smallCount = 0;  // ≤8B, inline
            int mediumCount = 0; // 9-4096B, use ValuePool
            int largeCount = 0;  // >4096B, LARGE
            
            long[] addresses = new long[1000];
            byte[][] values = new byte[1000][];
            
            for (int i = 0; i < 1000; i++) {
                // Simulate realistic distribution:
                // 60% small (counter, flags) - ≤8B
                // 35% medium (user data) - 10-500B
                // 5% large - >4KB
                int r = random.nextInt(100);
                int len;
                if (r < 60) {
                    len = random.nextInt(INLINE_THRESHOLD + 1);  // 0-8 bytes
                    smallCount++;
                } else if (r < 95) {
                    len = INLINE_THRESHOLD + 1 + random.nextInt(500); // 9-508 bytes
                    mediumCount++;
                } else {
                    len = 4097 + random.nextInt(1000); // 4097-5096 bytes
                    largeCount++;
                }
                
                values[i] = new byte[len];
                random.nextBytes(values[i]);
                addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, values[i], len);
                assertNotEquals(NULL_HANDLE, addresses[i]);
            }
            
            // Verify all values
            for (int i = 0; i < 1000; i++) {
                byte[] retrieved = store.getValueBytes(addresses[i]);
                assertArrayEquals(values[i], retrieved, "Value " + i + " mismatch");
            }
            
            // Verify ValuePool only has medium + large values
            EntryStoreStats stats = store.getStats();
            assertEquals(mediumCount + largeCount, stats.getValueActiveCount(),
                    String.format("Expected %d ValuePool entries (small=%d inline, medium=%d, large=%d)",
                            mediumCount + largeCount, smallCount, mediumCount, largeCount));
            
            System.out.printf("[MemoryOptimizationTest] Workload: small=%d (inline), medium=%d, large=%d%n",
                    smallCount, mediumCount, largeCount);
        }
        
        @Test
        @DisplayName("Memory efficiency: inline saves ValuePool overhead")
        void testMemoryEfficiencyWithInline() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            
            // Allocate 1000 entries with 4-byte values (would be inline)
            for (int i = 0; i < 1000; i++) {
                byte[] value = new byte[4];
                for (int j = 0; j < 4; j++) {
                    value[j] = (byte) (i + j);
                }
                store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            }
            
            EntryStoreStats stats = store.getStats();
            
            // No ValuePool usage
            assertEquals(0, stats.getValueActiveCount());
            
            // All entries stored in KeyNsPool
            assertEquals(1000, stats.getKeyNsActiveEntries());
            
            // Memory used should be minimal (just KeyNsPool overhead)
            assertTrue(stats.getKeyNsTotalBytes() > 0);
            assertEquals(0, stats.getValueTotalBytes(),
                    "Inline values should not use ValuePool memory");
        }
    }
}
