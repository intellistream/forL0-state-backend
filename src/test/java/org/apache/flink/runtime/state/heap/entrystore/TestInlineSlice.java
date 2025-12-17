package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;

public class TestInlineSlice {
    public static void main(String[] args) throws Exception {
        int PAGE_SIZE = 32 * 1024;
        long MEMORY_SIZE = 16L * PAGE_SIZE;
        
        MemoryManager mm = MemoryManagerBuilder.newBuilder()
            .setMemorySize(MEMORY_SIZE).setPageSize(PAGE_SIZE).build();
        MemoryManagerAllocator alloc = new MemoryManagerAllocator(mm, new Object());
        EntryStore store = new EntryStore(alloc);
        
        byte[] key = "testKey".getBytes();
        byte[] ns = "testNs".getBytes();
        
        // Test: allocate with null valueBuffer but valueLen=4 (should use inline)
        System.out.println("=== Test: allocateEntry with null valueBuffer, valueLen=4 ===");
        long addr = store.allocateEntry(12345, key, key.length, ns, ns.length, null, 4);
        System.out.println("Address: " + addr);
        System.out.println("Is inline mode: " + store.isInlineMode(addr));
        
        MemorySegmentSlice slice = store.getValueSlice(addr);
        System.out.println("Slice: " + slice);
        if (slice != null) {
            System.out.println("Slice segment: " + slice.segment);
            System.out.println("Slice offset: " + slice.offset);
            System.out.println("Slice length: " + slice.length);
        } else {
            System.out.println("ERROR: slice is null!");
        }
        
        store.close();
        alloc.close();
        mm.shutdown();
    }
}
