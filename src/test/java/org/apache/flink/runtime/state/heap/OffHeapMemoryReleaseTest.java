package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates off-heap memory is properly reused and released:
 * - Updates reuse EntryArena free blocks (no continuous growth during updates)
 * - close() releases all managed memory back to MemoryManager
 */
public class OffHeapMemoryReleaseTest {

    private static final int PAGE_SIZE = 32 * 1024; // 32KB
    private static final long TOTAL_MEMORY = 64L * PAGE_SIZE; // 2MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;

    @BeforeEach
    void setup() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setPageSize(PAGE_SIZE)
                .setMemorySize(TOTAL_MEMORY)
                .build();
        allocator = new MemoryManagerAllocator(memoryManager, this);
    }

    @AfterEach
    void teardown() {
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    @Test
    void testOffHeapMemoryReuseAndRelease() throws Exception {
        // Build a ForL0StateMap with default FREE_LIST Arena
        ForL0StateMap<Integer, String, Integer> map = new ForL0StateMap<>(
                allocator,
                10, // main table buckets: 1<<10
                10, // l0 cache buckets: 1<<10
                IntSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                true
        );

        final String ns = "ns";
        final int keys = 5000;

        // Phase 1: insert
        for (int k = 0; k < keys; k++) {
            map.put(k, ns, 1);
        }
        long usedAfterInsert = allocator.getUsedBytes();
        assertTrue(usedAfterInsert > 0, "should allocate some managed memory after inserts");

        // Phase 2: repeated updates should NOT grow managed memory unboundedly
        long maxUsedDuringUpdates = usedAfterInsert;
        for (int round = 0; round < 10; round++) {
            for (int k = 0; k < keys; k++) {
                map.put(k, ns, 2 + round);
            }
            long u = allocator.getUsedBytes();
            if (u > maxUsedDuringUpdates) {
                maxUsedDuringUpdates = u;
            }
        }

        // Allow a small slack (fragmentation may trigger 1-2 extra pages), but no runaway growth
        long slack = 4L * PAGE_SIZE;
        assertTrue(maxUsedDuringUpdates <= usedAfterInsert + slack,
                String.format("managed memory grew too much during updates: insert=%d, peak=%d", usedAfterInsert, maxUsedDuringUpdates));

        // Phase 3: remove all entries
        for (int k = 0; k < keys; k++) {
            map.remove(k, ns);
        }

        // Memory may not drop immediately (pages kept by arena), but close() must free everything
        map.close();
        assertEquals(0L, allocator.getUsedBytes(), "allocator should release all managed memory on close");
    }
}
