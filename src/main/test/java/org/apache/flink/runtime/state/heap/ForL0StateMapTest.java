package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class ForL0StateMapTest {
    private ForL0StateMap<String, String, Integer> map;

    /* ---------- lifecycle ---------- */

    @BeforeEach
    void setUp() {
        // 8MB for tests
        // 64KB segment-size
        MemoryManager memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(8 * 1024 * 1024L)    // 8MB for tests
                .setPageSize(64 * 1024)             // 64KB segment-size
                .build();
        MemoryManagerAllocator allocator = new MemoryManagerAllocator(memoryManager, this);
        // 2^8 = 256 根 bucket，足够覆盖批量测试
        map = new ForL0StateMap<>(
                8,                                 // initPow2
                StringSerializer.INSTANCE,         // key serializer
                StringSerializer.INSTANCE,         // namespace serializer
                IntSerializer.INSTANCE,
                allocator);           // state serializer
    }

    @AfterEach
    void tearDown() throws Exception {
        map.close();                              // 释放堆外内存
    }

    /* ---------- test cases ---------- */

    @Test
    void testPutGetContainsSize() {
        assertEquals(0, map.size());

        map.put("k1", "ns", 42);
        assertEquals(1, map.size());
        assertTrue(map.containsKey("k1", "ns"));
        assertEquals(Integer.valueOf(42), map.get("k1", "ns"));
    }

    @Test
    void testOverwriteKeepsSize() {
        map.put("dup", "ns", 1);
        map.put("dup", "ns", 2);                  // 覆盖
        assertEquals(1, map.size(),
                "size must not grow on overwrite");
        assertEquals(Integer.valueOf(2), map.get("dup", "ns"),
                "value should be updated");
    }

    @Test
    void testBulkInsertAndRandomAccess() {
        final int total = 72; // ??????????????????????????
        for (int i = 0; i < total; i++) {
            map.put("key" + i, "ns", i);
        }
        assertEquals(total, map.size());

        Random rnd = new Random(1234);
        for (int t = 0; t < 200; t++) {
            int idx = rnd.nextInt(total);
            String k = "key" + idx;
            assertTrue(map.containsKey(k, "ns"));
            assertEquals(Integer.valueOf(idx), map.get(k, "ns"));
        }
    }

    @Test
    void testMissingKey() {
        assertNull(map.get("noSuchKey", "ns"));
        assertFalse(map.containsKey("noSuchKey", "ns"));
    }
}
