package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.state.heap.entrystore.EntryStoreConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ValuePool.
 */
class ValuePoolTest {
    
    private static final int DEFAULT_PAGE_SIZE = 64 * 1024; // 64KB
    private static final long DEFAULT_MEMORY_SIZE = 128L * DEFAULT_PAGE_SIZE; // 8MB
    
    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private ValuePool pool;
    
    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        Object owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        pool = new ValuePool(allocator);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (pool != null && !pool.isClosed()) {
            pool.close();
        }
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }
    
    @Nested
    class BasicAllocationTests {
        
        @Test
        void testPoolInitialization() {
            assertEquals(0, pool.getActiveValues());
            assertEquals(0, pool.getLargeObjectCount());
            assertFalse(pool.isClosed());
        }
        
        @Test
        void testSimpleAllocateWriteRead() {
            byte[] value = "testValue".getBytes();
            
            long handle = pool.allocate(value.length);
            assertTrue(handle != NULL_HANDLE);
            
            pool.write(handle, value, value.length);
            
            byte[] readValue = pool.read(handle);
            assertArrayEquals(value, readValue);
            
            assertEquals(1, pool.getActiveValues());
        }
        
        @Test
        void testAllocateEmptyValue() {
            long handle = pool.allocate(0);
            assertTrue(handle != NULL_HANDLE);
            
            pool.write(handle, new byte[0], 0);
            
            byte[] readValue = pool.read(handle);
            assertNotNull(readValue);
            assertEquals(0, readValue.length);
        }
        
        @Test
        void testMultipleAllocations() {
            int count = 100;
            long[] handles = new long[count];
            
            for (int i = 0; i < count; i++) {
                byte[] value = ("value" + i).getBytes();
                handles[i] = pool.allocate(value.length);
                assertTrue(handles[i] != NULL_HANDLE, "Allocation " + i + " failed");
                pool.write(handles[i], value, value.length);
            }
            
            assertEquals(count, pool.getActiveValues());
            
            // Verify all values
            for (int i = 0; i < count; i++) {
                byte[] expected = ("value" + i).getBytes();
                byte[] actual = pool.read(handles[i]);
                assertArrayEquals(expected, actual, "Value " + i + " mismatch");
            }
        }
    }
    
    @Nested
    class SizeClassTests {
        
        @Test
        void testSizeClassSelection() {
            // Test each size class boundary
            assertEquals(ValueSizeClass.VS_32, ValueSizeClass.getSizeClass(1));
            assertEquals(ValueSizeClass.VS_32, ValueSizeClass.getSizeClass(32));
            assertEquals(ValueSizeClass.VS_64, ValueSizeClass.getSizeClass(33));
            assertEquals(ValueSizeClass.VS_64, ValueSizeClass.getSizeClass(64));
            assertEquals(ValueSizeClass.VS_128, ValueSizeClass.getSizeClass(65));
            assertEquals(ValueSizeClass.VS_128, ValueSizeClass.getSizeClass(128));
            assertEquals(ValueSizeClass.VS_256, ValueSizeClass.getSizeClass(129));
            assertEquals(ValueSizeClass.VS_512, ValueSizeClass.getSizeClass(257));
            assertEquals(ValueSizeClass.VS_1K, ValueSizeClass.getSizeClass(513));
            assertEquals(ValueSizeClass.VS_2K, ValueSizeClass.getSizeClass(1025));
            assertEquals(ValueSizeClass.VS_4K, ValueSizeClass.getSizeClass(2049));
            assertEquals(ValueSizeClass.LARGE, ValueSizeClass.getSizeClass(4097));
        }
        
        @Test
        void testAllocateSmallValues() {
            // Allocate values in each small size class
            int[] sizes = {10, 50, 100, 200, 400, 800, 1500, 3000};
            long[] handles = new long[sizes.length];
            
            for (int i = 0; i < sizes.length; i++) {
                byte[] value = new byte[sizes[i]];
                for (int j = 0; j < value.length; j++) {
                    value[j] = (byte) (j & 0xFF);
                }
                
                handles[i] = pool.allocate(value.length);
                assertTrue(handles[i] != NULL_HANDLE, "Allocation for size " + sizes[i] + " failed");
                pool.write(handles[i], value, value.length);
                
                // Verify immediately
                byte[] readValue = pool.read(handles[i]);
                assertArrayEquals(value, readValue, "Value of size " + sizes[i] + " mismatch");
            }
        }
        
        @Test
        void testSlotSize() {
            // Allocate values and check slot sizes
            byte[] small = new byte[20];  // Should use VS_32 (slot=32)
            long handle = pool.allocate(small.length);
            pool.write(handle, small, small.length);
            
            int slotSize = pool.getSlotSize(handle);
            // Total size = 4 (header) + 20 = 24, fits in 32-byte slot
            assertEquals(32, slotSize);
        }
    }
    
    @Nested
    class LargeObjectTests {
        
        @Test
        void testAllocateLargeObject() {
            // Allocate a large object (> 4KB)
            byte[] largeValue = new byte[5000];
            for (int i = 0; i < largeValue.length; i++) {
                largeValue[i] = (byte) (i & 0xFF);
            }
            
            long handle = pool.allocate(largeValue.length);
            assertTrue(handle != NULL_HANDLE);
            assertTrue(pool.isLargeObject(handle));
            
            pool.write(handle, largeValue, largeValue.length);
            
            byte[] readValue = pool.read(handle);
            assertArrayEquals(largeValue, readValue);
            
            assertEquals(1, pool.getLargeObjectCount());
            assertEquals(1, pool.getActiveValues());
        }
        
        @Test
        void testFreeLargeObject() {
            byte[] largeValue = new byte[5000];
            long handle = pool.allocate(largeValue.length);
            pool.write(handle, largeValue, largeValue.length);
            
            assertEquals(1, pool.getLargeObjectCount());
            
            pool.free(handle);
            
            assertEquals(0, pool.getLargeObjectCount());
            assertEquals(0, pool.getActiveValues());
        }
        
        @Test
        void testMultipleLargeObjects() {
            int count = 10;
            long[] handles = new long[count];
            
            for (int i = 0; i < count; i++) {
                byte[] value = new byte[5000 + i * 1000];
                handles[i] = pool.allocate(value.length);
                pool.write(handles[i], value, value.length);
            }
            
            assertEquals(count, pool.getLargeObjectCount());
            
            // Free half
            for (int i = 0; i < count / 2; i++) {
                pool.free(handles[i]);
            }
            
            assertEquals(count / 2, pool.getLargeObjectCount());
        }
    }
    
    @Nested
    class FreeAndReuseTests {
        
        @Test
        void testFreeDecreasesActiveCount() {
            byte[] value = "test".getBytes();
            
            long handle1 = pool.allocate(value.length);
            pool.write(handle1, value, value.length);
            
            long handle2 = pool.allocate(value.length);
            pool.write(handle2, value, value.length);
            
            assertEquals(2, pool.getActiveValues());
            
            pool.free(handle1);
            assertEquals(1, pool.getActiveValues());
            
            pool.free(handle2);
            assertEquals(0, pool.getActiveValues());
        }
        
        @Test
        void testFreeNullHandleIsNoOp() {
            pool.free(NULL_HANDLE);
            assertEquals(0, pool.getActiveValues());
        }
        
        @Test
        void testSlotReuse() {
            byte[] value = "reuse test".getBytes();
            
            // Allocate and free many times
            for (int round = 0; round < 10; round++) {
                long[] handles = new long[100];
                
                for (int i = 0; i < 100; i++) {
                    handles[i] = pool.allocate(value.length);
                    pool.write(handles[i], value, value.length);
                }
                
                assertEquals(100, pool.getActiveValues());
                
                // Free all
                for (long h : handles) {
                    pool.free(h);
                }
                
                assertEquals(0, pool.getActiveValues());
            }
            
            // Runs should be reused, not constantly growing
            assertTrue(pool.getRunCount() < 20, "Too many runs allocated: " + pool.getRunCount());
        }
    }
    
    @Nested
    class InPlaceUpdateTests {
        
        @Test
        void testUpdateInPlaceSameSize() {
            byte[] original = "original".getBytes();
            byte[] updated = "updated!".getBytes();  // Same length
            
            long handle = pool.allocate(original.length);
            pool.write(handle, original, original.length);
            
            assertTrue(pool.updateInPlace(handle, updated, updated.length));
            
            byte[] readValue = pool.read(handle);
            assertArrayEquals(updated, readValue);
        }
        
        @Test
        void testUpdateInPlaceSmallerValue() {
            byte[] original = "original value".getBytes();
            byte[] smaller = "small".getBytes();
            
            long handle = pool.allocate(original.length);
            pool.write(handle, original, original.length);
            
            assertTrue(pool.updateInPlace(handle, smaller, smaller.length));
            
            byte[] readValue = pool.read(handle);
            assertArrayEquals(smaller, readValue);
        }
        
        @Test
        void testUpdateInPlaceFitsInSlot() {
            // Allocate small value, update to larger but still fits in slot
            byte[] original = new byte[10];
            byte[] larger = new byte[25];  // Still fits in 32-byte slot (with 4-byte header)
            
            for (int i = 0; i < larger.length; i++) {
                larger[i] = (byte) (i + 1);
            }
            
            long handle = pool.allocate(original.length);
            pool.write(handle, original, original.length);
            
            // Should succeed because 4 + 25 = 29 <= 32
            assertTrue(pool.updateInPlace(handle, larger, larger.length));
            
            byte[] readValue = pool.read(handle);
            assertArrayEquals(larger, readValue);
        }
        
        @Test
        void testUpdateInPlaceExceedsSlot() {
            byte[] original = new byte[10];  // Fits in 32-byte slot
            byte[] tooLarge = new byte[50];   // Needs 64-byte slot (4 + 50 = 54 > 32)
            
            long handle = pool.allocate(original.length);
            pool.write(handle, original, original.length);
            
            // Should fail because new value doesn't fit
            assertFalse(pool.updateInPlace(handle, tooLarge, tooLarge.length));
            
            // Original value should be unchanged
            byte[] readValue = pool.read(handle);
            assertArrayEquals(original, readValue);
        }
        
        @Test
        void testUpdateInPlaceLargeObject() {
            byte[] original = new byte[5000];
            byte[] updated = new byte[4000];  // Smaller, should fit
            
            for (int i = 0; i < updated.length; i++) {
                updated[i] = (byte) (i & 0xFF);
            }
            
            long handle = pool.allocate(original.length);
            pool.write(handle, original, original.length);
            
            assertTrue(pool.isLargeObject(handle));
            assertTrue(pool.updateInPlace(handle, updated, updated.length));
            
            byte[] readValue = pool.read(handle);
            assertArrayEquals(updated, readValue);
        }
    }
    
    @Nested
    class ZeroCopySliceTests {
        
        @Test
        void testGetSlice() {
            byte[] value = "slice test value".getBytes();
            
            long handle = pool.allocate(value.length);
            pool.write(handle, value, value.length);
            
            MemorySegmentSlice slice = pool.getSlice(handle);
            assertNotNull(slice);
            assertEquals(value.length, slice.length);
            
            // Read bytes from slice
            byte[] readValue = new byte[slice.length];
            slice.segment.get(slice.offset, readValue);
            assertArrayEquals(value, readValue);
        }
        
        @Test
        void testGetSliceLargeObject() {
            byte[] value = new byte[5000];
            for (int i = 0; i < value.length; i++) {
                value[i] = (byte) (i & 0xFF);
            }
            
            long handle = pool.allocate(value.length);
            pool.write(handle, value, value.length);
            
            MemorySegmentSlice slice = pool.getSlice(handle);
            assertNotNull(slice);
            assertEquals(value.length, slice.length);
            
            // Read bytes from slice
            byte[] readValue = new byte[slice.length];
            slice.segment.get(slice.offset, readValue);
            assertArrayEquals(value, readValue);
        }
    }
    
    @Nested
    class BitmapTests {
        
        @Test
        void testBitmapAllocation() {
            // Allocate many values to test bitmap operations
            byte[] value = new byte[20];  // Small value, uses VS_32
            
            // Allocate enough to fill multiple bitmap words (64 bits each)
            int count = 200;
            long[] handles = new long[count];
            
            for (int i = 0; i < count; i++) {
                handles[i] = pool.allocate(value.length);
                assertTrue(handles[i] != NULL_HANDLE, "Allocation " + i + " failed");
                pool.write(handles[i], value, value.length);
            }
            
            assertEquals(count, pool.getActiveValues());
            
            // Free every other slot
            for (int i = 0; i < count; i += 2) {
                pool.free(handles[i]);
            }
            
            assertEquals(count / 2, pool.getActiveValues());
            
            // Allocate again - should reuse freed slots
            for (int i = 0; i < count / 2; i++) {
                long newHandle = pool.allocate(value.length);
                assertTrue(newHandle != NULL_HANDLE);
            }
            
            assertEquals(count, pool.getActiveValues());
        }
    }
    
    @Nested
    class LifecycleTests {
        
        @Test
        void testCloseReleasesAllMemory() {
            // Allocate various sizes
            for (int size : new int[]{10, 100, 1000, 5000}) {
                byte[] value = new byte[size];
                long handle = pool.allocate(value.length);
                pool.write(handle, value, value.length);
            }
            
            pool.close();
            
            assertTrue(pool.isClosed());
            assertEquals(0, pool.getActiveValues());
            assertEquals(0, pool.getLargeObjectCount());
            assertEquals(0, pool.getRunCount());
        }
        
        @Test
        void testDoubleCloseIsNoOp() {
            pool.close();
            pool.close();  // Should not throw
            assertTrue(pool.isClosed());
        }
        
        @Test
        void testAllocateAfterCloseReturnsNull() {
            pool.close();
            assertEquals(NULL_HANDLE, pool.allocate(100));
        }
        
        @Test
        void testReleaseEmptyRuns() {
            byte[] value = new byte[20];
            
            // Allocate many values
            long[] handles = new long[1000];
            for (int i = 0; i < handles.length; i++) {
                handles[i] = pool.allocate(value.length);
                pool.write(handles[i], value, value.length);
            }
            
            int runsBefore = pool.getRunCount();
            
            // Free all
            for (long h : handles) {
                pool.free(h);
            }
            
            // Release empty runs
            long released = pool.releaseEmptyRuns();
            
            assertTrue(released > 0);
            assertTrue(pool.getRunCount() < runsBefore);
        }
    }
    
    @Nested
    class StressTests {
        
        @Test
        void testHighVolumeAllocations() {
            int count = 5000;
            long[] handles = new long[count];
            
            // Allocate with varying sizes
            for (int i = 0; i < count; i++) {
                int size = 10 + (i % 500);  // 10 to 509 bytes
                byte[] value = new byte[size];
                for (int j = 0; j < size; j++) {
                    value[j] = (byte) ((i + j) & 0xFF);
                }
                
                handles[i] = pool.allocate(size);
                assertTrue(handles[i] != NULL_HANDLE, "Allocation " + i + " failed");
                pool.write(handles[i], value, value.length);
            }
            
            assertEquals(count, pool.getActiveValues());
            
            // Verify random samples
            java.util.Random random = new java.util.Random(42);
            for (int i = 0; i < 100; i++) {
                int idx = random.nextInt(count);
                int expectedSize = 10 + (idx % 500);
                byte[] value = pool.read(handles[idx]);
                assertNotNull(value);
                assertEquals(expectedSize, value.length);
            }
        }
        
        @Test
        void testMixedOperations() {
            java.util.List<Long> activeHandles = new java.util.ArrayList<>();
            java.util.Random random = new java.util.Random(42);
            
            for (int i = 0; i < 5000; i++) {
                double op = random.nextDouble();
                
                if (op < 0.5 || activeHandles.isEmpty()) {
                    // 50% allocate
                    int size = 10 + random.nextInt(500);
                    byte[] value = new byte[size];
                    long handle = pool.allocate(size);
                    if (handle != NULL_HANDLE) {
                        pool.write(handle, value, value.length);
                        activeHandles.add(handle);
                    }
                } else if (op < 0.7) {
                    // 20% free
                    int idx = random.nextInt(activeHandles.size());
                    pool.free(activeHandles.remove(idx));
                } else {
                    // 30% update in place
                    int idx = random.nextInt(activeHandles.size());
                    long handle = activeHandles.get(idx);
                    int slotSize = pool.getSlotSize(handle);
                    if (slotSize > VALUE_ENTRY_HEADER_SIZE) {
                        int newSize = Math.min(random.nextInt(slotSize - VALUE_ENTRY_HEADER_SIZE + 1), 
                                               slotSize - VALUE_ENTRY_HEADER_SIZE);
                        byte[] newValue = new byte[newSize];
                        pool.updateInPlace(handle, newValue, newValue.length);
                    }
                }
            }
            
            assertEquals(activeHandles.size(), pool.getActiveValues());
            
            // Cleanup
            for (long h : activeHandles) {
                pool.free(h);
            }
            
            assertEquals(0, pool.getActiveValues());
        }
    }
}
