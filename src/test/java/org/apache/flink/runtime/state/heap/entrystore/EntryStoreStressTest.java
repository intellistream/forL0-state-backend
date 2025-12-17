package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.runtime.state.heap.entrystore.EntryStoreConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for EntryStore.
 * Tests high-volume operations, mixed workloads, and memory efficiency.
 */
class EntryStoreStressTest {
    
    private static final int DEFAULT_PAGE_SIZE = 64 * 1024; // 64KB
    private static final long DEFAULT_MEMORY_SIZE = 256L * DEFAULT_PAGE_SIZE; // 16MB
    
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
    
    @Test
    void testHighVolumeAllocations() {
        int count = 10000;
        long[] addresses = new long[count];
        
        // Allocate many entries with varying sizes
        for (int i = 0; i < count; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] ns = ("namespace" + (i % 10)).getBytes();
            int valueSize = 10 + (i % 200);  // 10 to 209 bytes
            byte[] value = new byte[valueSize];
            for (int j = 0; j < valueSize; j++) {
                value[j] = (byte) ((i + j) & 0xFF);
            }
            
            addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            assertTrue(addresses[i] != NULL_HANDLE, "Allocation " + i + " failed");
        }
        
        assertEquals(count, store.getActiveEntries());
        
        // Verify random samples
        Random random = new Random(42);
        for (int sample = 0; sample < 100; sample++) {
            int i = random.nextInt(count);
            byte[] expectedKey = ("key" + i).getBytes();
            byte[] expectedNs = ("namespace" + (i % 10)).getBytes();
            int expectedValueSize = 10 + (i % 200);
            
            assertEquals(i, store.getHash(addresses[i]));
            assertArrayEquals(expectedKey, store.getKeyBytes(addresses[i]));
            assertArrayEquals(expectedNs, store.getNamespaceBytes(addresses[i]));
            
            byte[] value = store.getValueBytes(addresses[i]);
            assertEquals(expectedValueSize, value.length);
        }
        
        // Free all
        for (long addr : addresses) {
            store.removeEntry(addr);
        }
        
        assertEquals(0, store.getActiveEntries());
    }
    
    @Test
    void testMixedAllocateUpdateRemove() {
        List<Long> activeAddresses = new ArrayList<>();
        Map<Long, byte[]> expectedValues = new HashMap<>();
        Random random = new Random(42);
        
        int operations = 10000;
        
        for (int op = 0; op < operations; op++) {
            double choice = random.nextDouble();
            
            if (choice < 0.4 || activeAddresses.isEmpty()) {
                // 40% allocate
                byte[] key = ("key" + op).getBytes();
                byte[] ns = ("ns" + (op % 5)).getBytes();
                int valueSize = 10 + random.nextInt(500);
                byte[] value = new byte[valueSize];
                random.nextBytes(value);
                
                long addr = store.allocateEntry(op, key, key.length, ns, ns.length, value, value.length);
                if (addr != NULL_HANDLE) {
                    activeAddresses.add(addr);
                    expectedValues.put(addr, value);
                }
                
            } else if (choice < 0.7) {
                // 30% update
                int idx = random.nextInt(activeAddresses.size());
                long addr = activeAddresses.get(idx);
                
                int newValueSize = 10 + random.nextInt(500);
                byte[] newValue = new byte[newValueSize];
                random.nextBytes(newValue);
                
                boolean success = store.updateValue(addr, newValue, newValue.length);
                if (success) {
                    expectedValues.put(addr, newValue);
                }
                
            } else {
                // 30% remove
                int idx = random.nextInt(activeAddresses.size());
                long addr = activeAddresses.remove(idx);
                store.removeEntry(addr);
                expectedValues.remove(addr);
            }
        }
        
        assertEquals(activeAddresses.size(), store.getActiveEntries());
        
        // Verify remaining entries
        for (long addr : activeAddresses) {
            byte[] expectedValue = expectedValues.get(addr);
            byte[] actualValue = store.getValueBytes(addr);
            assertArrayEquals(expectedValue, actualValue, "Value mismatch for address " + addr);
        }
        
        // Cleanup
        for (long addr : activeAddresses) {
            store.removeEntry(addr);
        }
        
        assertEquals(0, store.getActiveEntries());
    }
    
    @Test
    void testAddressStabilityUnderUpdates() {
        // This test verifies the key design goal: addresses remain stable during updates
        int count = 1000;
        long[] addresses = new long[count];
        
        // Allocate with small values
        byte[] key = "key".getBytes();
        byte[] ns = "ns".getBytes();
        byte[] smallValue = "initial".getBytes();
        
        for (int i = 0; i < count; i++) {
            addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, smallValue, smallValue.length);
            assertTrue(addresses[i] != NULL_HANDLE);
        }
        
        // Record original addresses
        long[] originalAddresses = addresses.clone();
        
        // Update all entries with larger values (forces reallocation in ValuePool)
        byte[] largeValue = new byte[200];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) i;
        }
        
        for (int i = 0; i < count; i++) {
            boolean success = store.updateValue(addresses[i], largeValue, largeValue.length);
            assertTrue(success, "Update " + i + " failed");
            
            // Verify address is unchanged
            assertEquals(originalAddresses[i], addresses[i], 
                    "Address changed for entry " + i + "! This should NOT happen.");
        }
        
        // Verify all entries are still accessible at original addresses
        for (int i = 0; i < count; i++) {
            assertArrayEquals(largeValue, store.getValueBytes(originalAddresses[i]));
        }
        
        // Update again with even larger values
        byte[] veryLargeValue = new byte[1000];
        for (int i = 0; i < veryLargeValue.length; i++) {
            veryLargeValue[i] = (byte) (i * 2);
        }
        
        for (int i = 0; i < count; i++) {
            boolean success = store.updateValue(addresses[i], veryLargeValue, veryLargeValue.length);
            assertTrue(success, "Second update " + i + " failed");
        }
        
        // Verify addresses are still unchanged
        for (int i = 0; i < count; i++) {
            assertEquals(originalAddresses[i], addresses[i], 
                    "Address changed after second update for entry " + i);
            assertArrayEquals(veryLargeValue, store.getValueBytes(originalAddresses[i]));
        }
    }
    
    @Test
    void testMemoryEfficiencyUnderChurn() {
        // Test that memory is efficiently reused under high churn
        byte[] key = "key".getBytes();
        byte[] ns = "ns".getBytes();
        
        // Baseline: allocate and free many times
        for (int round = 0; round < 10; round++) {
            long[] addresses = new long[1000];
            
            // Allocate with varying value sizes
            for (int i = 0; i < addresses.length; i++) {
                int valueSize = 50 + (i % 100);
                byte[] value = new byte[valueSize];
                addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
                assertTrue(addresses[i] != NULL_HANDLE);
            }
            
            // Free all
            for (long addr : addresses) {
                store.removeEntry(addr);
            }
        }
        
        assertEquals(0, store.getActiveEntries());
        
        // Release empty memory
        long released = store.releaseEmptyMemory();
        
        // Get final stats
        EntryStoreStats stats = store.getStats();
        
        // Memory should be efficiently managed
        // After releasing empty memory, usage should be minimal
        System.out.println("Stats after churn: " + stats);
        System.out.println("Released bytes: " + released);
    }
    
    @Test
    void testLargeObjectHandling() {
        // Test with large objects (> 4KB)
        int count = 100;
        long[] addresses = new long[count];
        int[] valueSizes = new int[count];
        
        byte[] key = "largeKey".getBytes();
        byte[] ns = "largeNs".getBytes();
        Random random = new Random(42);
        
        for (int i = 0; i < count; i++) {
            valueSizes[i] = 5000 + random.nextInt(10000);  // 5KB to 15KB
            byte[] value = new byte[valueSizes[i]];
            random.nextBytes(value);
            
            addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            assertTrue(addresses[i] != NULL_HANDLE, "Large allocation " + i + " failed");
        }
        
        assertEquals(count, store.getActiveEntries());
        
        EntryStoreStats stats = store.getStats();
        assertTrue(stats.getLargeObjectCount() > 0, "Should have large objects");
        
        // Free half
        for (int i = 0; i < count / 2; i++) {
            store.removeEntry(addresses[i]);
        }
        
        assertEquals(count / 2, store.getActiveEntries());
        
        // Cleanup
        for (int i = count / 2; i < count; i++) {
            store.removeEntry(addresses[i]);
        }
        
        assertEquals(0, store.getActiveEntries());
    }
    
    @Test
    void testPreAllocation() {
        // Create store with pre-allocation
        EntryStore preAllocStore = new EntryStore(allocator, DEFAULT_PAGE_SIZE * 4);
        
        try {
            EntryStoreStats stats = preAllocStore.getStats();
            
            // Should have pre-allocated segments/runs
            assertTrue(stats.getKeyNsSegmentCount() >= 1);
            
            // Allocations should still work normally
            byte[] key = "key".getBytes();
            byte[] ns = "ns".getBytes();
            byte[] value = "value".getBytes();
            
            for (int i = 0; i < 100; i++) {
                long addr = preAllocStore.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
                assertTrue(addr != NULL_HANDLE);
            }
            
            assertEquals(100, preAllocStore.getActiveEntries());
        } finally {
            preAllocStore.close();
        }
    }
    
    @Test
    void testConcurrentStyleAccess() {
        // Simulate concurrent-style access patterns (single-threaded but interleaved)
        int numKeys = 100;
        Map<Integer, Long> keyToAddress = new HashMap<>();
        Random random = new Random(42);
        
        byte[] ns = "namespace".getBytes();
        
        for (int op = 0; op < 5000; op++) {
            int keyId = random.nextInt(numKeys);
            byte[] key = ("key" + keyId).getBytes();
            
            if (keyToAddress.containsKey(keyId)) {
                double action = random.nextDouble();
                
                if (action < 0.3) {
                    // Read
                    long addr = keyToAddress.get(keyId);
                    byte[] value = store.getValueBytes(addr);
                    assertNotNull(value);
                    
                } else if (action < 0.7) {
                    // Update
                    long addr = keyToAddress.get(keyId);
                    byte[] newValue = new byte[10 + random.nextInt(100)];
                    random.nextBytes(newValue);
                    assertTrue(store.updateValue(addr, newValue, newValue.length));
                    
                } else {
                    // Delete and re-insert
                    long oldAddr = keyToAddress.remove(keyId);
                    store.removeEntry(oldAddr);
                    
                    byte[] value = new byte[10 + random.nextInt(100)];
                    random.nextBytes(value);
                    long newAddr = store.allocateEntry(keyId, key, key.length, ns, ns.length, value, value.length);
                    assertTrue(newAddr != NULL_HANDLE);
                    keyToAddress.put(keyId, newAddr);
                }
            } else {
                // Insert new key
                byte[] value = new byte[10 + random.nextInt(100)];
                random.nextBytes(value);
                long addr = store.allocateEntry(keyId, key, key.length, ns, ns.length, value, value.length);
                assertTrue(addr != NULL_HANDLE);
                keyToAddress.put(keyId, addr);
            }
        }
        
        assertEquals(keyToAddress.size(), store.getActiveEntries());
        
        // Cleanup
        for (long addr : keyToAddress.values()) {
            store.removeEntry(addr);
        }
        
        assertEquals(0, store.getActiveEntries());
    }
    
    @Test
    void testVariedValueSizes() {
        // Test with a wide range of value sizes
        int[] sizes = {0, 1, 10, 31, 32, 33, 63, 64, 65, 127, 128, 129, 
                       255, 256, 257, 511, 512, 513, 1023, 1024, 1025,
                       2047, 2048, 2049, 4095, 4096, 4097, 5000, 10000};
        
        long[] addresses = new long[sizes.length];
        byte[] key = "key".getBytes();
        byte[] ns = "ns".getBytes();
        
        // Allocate entries with each size
        for (int i = 0; i < sizes.length; i++) {
            byte[] value = new byte[sizes[i]];
            for (int j = 0; j < value.length; j++) {
                value[j] = (byte) ((i + j) & 0xFF);
            }
            
            addresses[i] = store.allocateEntry(i, key, key.length, ns, ns.length, value, value.length);
            assertTrue(addresses[i] != NULL_HANDLE, "Allocation for size " + sizes[i] + " failed");
        }
        
        // Verify each
        for (int i = 0; i < sizes.length; i++) {
            byte[] value = store.getValueBytes(addresses[i]);
            assertNotNull(value, "Read failed for size " + sizes[i]);
            assertEquals(sizes[i], value.length, "Size mismatch for size " + sizes[i]);
            
            // Verify content
            for (int j = 0; j < value.length; j++) {
                assertEquals((byte) ((i + j) & 0xFF), value[j], 
                        "Content mismatch at position " + j + " for size " + sizes[i]);
            }
        }
        
        // Update each to a different size
        for (int i = 0; i < sizes.length; i++) {
            int newSize = sizes[(i + 5) % sizes.length];
            byte[] newValue = new byte[newSize];
            for (int j = 0; j < newValue.length; j++) {
                newValue[j] = (byte) ((i * 2 + j) & 0xFF);
            }
            
            boolean success = store.updateValue(addresses[i], newValue, newValue.length);
            assertTrue(success, "Update to size " + newSize + " failed for entry " + i);
            
            // Verify
            byte[] readValue = store.getValueBytes(addresses[i]);
            assertArrayEquals(newValue, readValue);
        }
        
        // Cleanup
        for (long addr : addresses) {
            store.removeEntry(addr);
        }
        
        assertEquals(0, store.getActiveEntries());
    }
}
