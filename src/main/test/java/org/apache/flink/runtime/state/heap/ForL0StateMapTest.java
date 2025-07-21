package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic contract tests for {@link ForL0StateMap}.
 *
 * <p>Key = Integer  Namespace = String  Value = Integer</p>
 */
class ForL0StateMapTest {

    private static MemoryManager mm;
    private MemoryManagerAllocator allocator;
    private ForL0StateMap<Integer,String,Integer> map;

    // ---------------------------------------------------------------------
    @BeforeAll
    static void initMM() {
        mm = MemoryManagerBuilder
                .newBuilder()
                .setPageSize(4 * 1024)
                .setMemorySize(4L * 1024 * 1024)      // 4 MB
                .build();
    }

    @BeforeEach
    void open() throws Exception {
        allocator = new MemoryManagerAllocator(mm, this);
        map = new ForL0StateMap<>(
                10,                              // 2^10 = 1 k top buckets
                IntSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                allocator);
    }

    @AfterEach
    void close() throws Exception {
        map.close();
        assertEquals(0L, allocator.outstandingBytes(), "all memory freed");
    }

    // ---------------------------------------------------------------------
    @Test
    void basicPutGetRemove() {
        assertNull(map.get(1, "ns1"));
        map.put(1, "ns1", 42);
        assertEquals(42, map.get(1, "ns1"));
        assertTrue(map.containsKey(1, "ns1"));
        assertEquals(1, map.size());

        map.remove(1, "ns1");
        assertNull(map.get(1, "ns1"));
        assertEquals(0, map.size());
    }

    // ---------------------------------------------------------------------
    @Test
    void putAndGetOld() {
        map.put(2, "ns", 10);
        assertEquals(10, map.putAndGetOld(2, "ns", 20));
        assertEquals(20, map.get(2, "ns"));
        assertEquals(1, map.size());
    }

    // ---------------------------------------------------------------------
    @Test
    void bulkInsertSize() {
        final int CNT = 100;
        for (int i = 0; i < CNT; i++) {
            map.put(i, "N", i);
        }
        assertEquals(CNT, map.size());
        for (int i = 0; i < CNT; i += 311) {
            assertEquals(i, map.get(i, "N"));
        }
    }
}
