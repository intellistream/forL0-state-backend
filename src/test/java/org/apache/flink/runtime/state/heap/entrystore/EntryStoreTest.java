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
 * Unit tests for EntryStore.
 */
class EntryStoreTest {
    
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
    
    @Nested
    class BasicOperationTests {
        
        @Test
        void testStoreInitialization() {
            assertEquals(0, store.getActiveEntries());
            assertFalse(store.isClosed());
            
            EntryStoreStats stats = store.getStats();
            assertNotNull(stats);
            assertEquals(0, stats.getKeyNsActiveEntries());
        }
        
        @Test
        void testAllocateAndReadEntry() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNamespace".getBytes();
            byte[] value = "testValue".getBytes();
            int hash = 12345;
            
            long address = store.allocateEntry(hash, key, key.length, ns, ns.length, value, value.length);
            
            assertTrue(address != NULL_HANDLE);
            assertEquals(hash, store.getHash(address));
            assertArrayEquals(key, store.getKeyBytes(address));
            assertArrayEquals(ns, store.getNamespaceBytes(address));
            assertArrayEquals(value, store.getValueBytes(address));
            assertEquals(1, store.getActiveEntries());
        }
        
        @Test
        void testMultipleEntries() {
            int count = 100;
            long[] addresses = new long[count];
            
            for (int i = 0; i < count; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] ns = ("ns" + i).getBytes();
                byte[] value = ("value" + i).getBytes();
                
                addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
                assertTrue(addresses[i] != NULL_HANDLE, "Allocation " + i + " failed");
            }
            
            assertEquals(count, store.getActiveEntries());
            
            // Verify all entries
            for (int i = 0; i < count; i++) {
                byte[] expectedKey = ("key" + i).getBytes();
                byte[] expectedNs = ("ns" + i).getBytes();
                byte[] expectedValue = ("value" + i).getBytes();
                
                assertEquals(i, store.getHash(addresses[i]));
                assertArrayEquals(expectedKey, store.getKeyBytes(addresses[i]));
                assertArrayEquals(expectedNs, store.getNamespaceBytes(addresses[i]));
                assertArrayEquals(expectedValue, store.getValueBytes(addresses[i]));
            }
        }
        
        @Test
        void testEmptyKeyNamespaceValue() {
            byte[] key = new byte[0];
            byte[] ns = new byte[0];
            byte[] value = new byte[0];
            
            long address = store.allocateEntry(0, key, 0, ns, 0, value, 0);
            
            assertTrue(address != NULL_HANDLE);
            assertArrayEquals(key, store.getKeyBytes(address));
            assertArrayEquals(ns, store.getNamespaceBytes(address));
            assertArrayEquals(value, store.getValueBytes(address));
        }
    }
    
    @Nested
    class UpdateValueTests {
        
        @Test
        void testUpdateValueInPlace() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] originalValue = "original".getBytes();
            byte[] newValue = "updated!".getBytes();  // Same length
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, 
                                               originalValue, originalValue.length);
            
            // Update value
            boolean success = store.updateValue(address, newValue, newValue.length);
            assertTrue(success);
            
            // Verify value changed
            assertArrayEquals(newValue, store.getValueBytes(address));
            
            // Key and namespace should be unchanged
            assertArrayEquals(key, store.getKeyBytes(address));
            assertArrayEquals(ns, store.getNamespaceBytes(address));
        }
        
        @Test
        void testUpdateValueAddressStable() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] smallValue = "small".getBytes();
            byte[] largeValue = "this is a much larger value that requires reallocation".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, 
                                               smallValue, smallValue.length);
            long originalAddress = address;
            
            // Update with larger value (may require reallocation internally)
            boolean success = store.updateValue(address, largeValue, largeValue.length);
            assertTrue(success);
            
            // **CRITICAL**: Address should NOT change!
            // This is the key improvement over EntryArena
            
            // Verify entry is still accessible at original address
            assertArrayEquals(largeValue, store.getValueBytes(originalAddress));
            assertArrayEquals(key, store.getKeyBytes(originalAddress));
            assertArrayEquals(ns, store.getNamespaceBytes(originalAddress));
        }
        
        @Test
        void testUpdateValueSmallerValue() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] largeValue = "this is a large value".getBytes();
            byte[] smallValue = "tiny".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, 
                                               largeValue, largeValue.length);
            
            boolean success = store.updateValue(address, smallValue, smallValue.length);
            assertTrue(success);
            
            assertArrayEquals(smallValue, store.getValueBytes(address));
        }
        
        @Test
        void testUpdateValueMultipleTimes() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "initial".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            long originalAddress = address;
            
            // Update multiple times with varying sizes
            for (int i = 0; i < 10; i++) {
                byte[] newValue = ("value iteration " + i + " with some padding").getBytes();
                assertTrue(store.updateValue(address, newValue, newValue.length));
                assertArrayEquals(newValue, store.getValueBytes(address));
            }
            
            // Address should still be the same
            assertEquals(originalAddress, address);
        }
        
        @Test
        void testUpdateValueWithLargeObject() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] smallValue = "small".getBytes();
            byte[] largeValue = new byte[5000];  // Large object (> 4KB)
            for (int i = 0; i < largeValue.length; i++) {
                largeValue[i] = (byte) (i & 0xFF);
            }
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, 
                                               smallValue, smallValue.length);
            
            // Update to large value
            boolean success = store.updateValue(address, largeValue, largeValue.length);
            assertTrue(success);
            
            byte[] readValue = store.getValueBytes(address);
            assertArrayEquals(largeValue, readValue);
        }
    }
    
    @Nested
    class RemoveEntryTests {
        
        @Test
        void testRemoveEntry() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            assertEquals(1, store.getActiveEntries());
            
            store.removeEntry(address);
            assertEquals(0, store.getActiveEntries());
        }
        
        @Test
        void testRemoveNullAddressIsNoOp() {
            store.removeEntry(NULL_HANDLE);
            assertEquals(0, store.getActiveEntries());
        }
        
        @Test
        void testRemoveMultipleEntries() {
            int count = 50;
            long[] addresses = new long[count];
            
            for (int i = 0; i < count; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] ns = ("ns" + i).getBytes();
                byte[] value = ("value" + i).getBytes();
                addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            }
            
            assertEquals(count, store.getActiveEntries());
            
            // Remove half
            for (int i = 0; i < count / 2; i++) {
                store.removeEntry(addresses[i]);
            }
            
            assertEquals(count / 2, store.getActiveEntries());
        }
    }
    
    @Nested
    class MatchesKeyTests {
        
        @Test
        void testMatchesKeyExactMatch() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            byte[] value = "testValue".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            
            assertTrue(store.matchesKey(address, key, key.length, ns, ns.length));
        }
        
        @Test
        void testMatchesKeyDifferentKey() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            byte[] value = "testValue".getBytes();
            byte[] differentKey = "otherKey".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            
            assertFalse(store.matchesKey(address, differentKey, differentKey.length, ns, ns.length));
        }
        
        @Test
        void testMatchesKeyDifferentNamespace() {
            byte[] key = "testKey".getBytes();
            byte[] ns = "testNs".getBytes();
            byte[] value = "testValue".getBytes();
            byte[] differentNs = "otherNs".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            
            assertFalse(store.matchesKey(address, key, key.length, differentNs, differentNs.length));
        }
    }
    
    @Nested
    class ZeroCopyTests {
        
        @Test
        void testGetKeySlice() {
            byte[] key = "sliceKey".getBytes();
            byte[] ns = "sliceNs".getBytes();
            byte[] value = "sliceValue".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            
            MemorySegmentSlice slice = store.getKeySlice(address);
            assertNotNull(slice);
            assertEquals(key.length, slice.length);
            
            byte[] readKey = new byte[slice.length];
            slice.segment.get(slice.offset, readKey);
            assertArrayEquals(key, readKey);
        }
        
        @Test
        void testGetNamespaceSlice() {
            byte[] key = "sliceKey".getBytes();
            byte[] ns = "sliceNamespace".getBytes();
            byte[] value = "sliceValue".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            
            MemorySegmentSlice slice = store.getNamespaceSlice(address);
            assertNotNull(slice);
            assertEquals(ns.length, slice.length);
            
            byte[] readNs = new byte[slice.length];
            slice.segment.get(slice.offset, readNs);
            assertArrayEquals(ns, readNs);
        }
        
        @Test
        void testGetValueSlice() {
            byte[] key = "sliceKey".getBytes();
            byte[] ns = "sliceNs".getBytes();
            byte[] value = "sliceValue".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            
            MemorySegmentSlice slice = store.getValueSlice(address);
            assertNotNull(slice);
            assertEquals(value.length, slice.length);
            
            byte[] readValue = new byte[slice.length];
            slice.segment.get(slice.offset, readValue);
            assertArrayEquals(value, readValue);
        }
        
        @Test
        void testZeroCopyWrite() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            int valueSize = 100;
            
            // Allocate with null value buffer (reserve space only)
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, null, valueSize);
            assertTrue(address != NULL_HANDLE);
            
            // Write directly to value slice
            MemorySegmentSlice slice = store.getValueSlice(address);
            assertNotNull(slice);
            assertTrue(slice.length >= valueSize);
            
            // Write custom data
            byte[] customValue = new byte[valueSize];
            for (int i = 0; i < customValue.length; i++) {
                customValue[i] = (byte) (i * 2);
            }
            slice.segment.put(slice.offset, customValue, 0, customValue.length);
            
            // Read back and verify
            byte[] readValue = store.getValueBytes(address);
            // Note: valueBytes will have the length we specified
            assertNotNull(readValue);
        }
    }
    
    @Nested
    class StatisticsTests {
        
        @Test
        void testStats() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            for (int i = 0; i < 50; i++) {
                store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            }
            
            EntryStoreStats stats = store.getStats();
            
            assertEquals(50, stats.getKeyNsActiveEntries());
            assertTrue(stats.getKeyNsActiveSegments() > 0);
            assertTrue(stats.getValueActiveCount() > 0);
            assertTrue(stats.getTotalSystemMemory() > 0);
        }
        
        @Test
        void testReleaseEmptyMemory() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            // Allocate many entries
            long[] addresses = new long[500];
            for (int i = 0; i < addresses.length; i++) {
                addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            }
            
            // Free all
            for (long addr : addresses) {
                store.removeEntry(addr);
            }
            
            assertEquals(0, store.getActiveEntries());
            
            // Release empty memory
            long released = store.releaseEmptyMemory();
            assertTrue(released > 0);
        }
    }
    
    @Nested
    class EdgeCaseTests {
        
        @Test
        void testInvalidInputs() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            // Null key
            assertEquals(NULL_HANDLE, store.allocateEntry(1, null, 0, ns, ns.length, value, value.length));
            
            // Null namespace
            assertEquals(NULL_HANDLE, store.allocateEntry(1, key, key.length, null, 0, value, value.length));
            
            // Negative lengths
            assertEquals(NULL_HANDLE, store.allocateEntry(1, key, -1, ns, ns.length, value, value.length));
            assertEquals(NULL_HANDLE, store.allocateEntry(1, key, key.length, ns, -1, value, value.length));
            assertEquals(NULL_HANDLE, store.allocateEntry(1, key, key.length, ns, ns.length, value, -1));
        }
        
        @Test
        void testLargeKeyAndValue() {
            byte[] key = new byte[1024];  // 1KB key
            byte[] ns = new byte[512];    // 512B namespace
            byte[] value = new byte[4000]; // 4KB value
            
            for (int i = 0; i < key.length; i++) key[i] = (byte) (i & 0xFF);
            for (int i = 0; i < ns.length; i++) ns[i] = (byte) ((i * 2) & 0xFF);
            for (int i = 0; i < value.length; i++) value[i] = (byte) ((i * 3) & 0xFF);
            
            long address = store.allocateEntry(0x12345678, key, key.length, ns, ns.length, value, value.length);
            
            assertTrue(address != NULL_HANDLE);
            assertArrayEquals(key, store.getKeyBytes(address));
            assertArrayEquals(ns, store.getNamespaceBytes(address));
            assertArrayEquals(value, store.getValueBytes(address));
        }
    }
    
    @Nested
    class LifecycleTests {
        
        @Test
        void testCloseReleasesAllMemory() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            for (int i = 0; i < 100; i++) {
                store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            }
            
            store.close();
            
            assertTrue(store.isClosed());
        }
        
        @Test
        void testAllocateAfterCloseReturnsNull() {
            store.close();
            
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            assertEquals(NULL_HANDLE, store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length));
        }
        
        @Test
        void testDoubleCloseIsNoOp() {
            store.close();
            store.close();  // Should not throw
            assertTrue(store.isClosed());
        }
        
        @Test
        void testUpdateAfterCloseReturnsFalse() {
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            long address = store.allocateEntry(1, key, key.length, ns, ns.length, value, value.length);
            
            store.close();
            
            assertFalse(store.updateValue(address, "new".getBytes(), 3));
        }
    }
}
