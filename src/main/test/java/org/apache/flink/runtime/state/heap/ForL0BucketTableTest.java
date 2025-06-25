package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.MemorySlice;
import org.apache.flink.runtime.state.heap.space.SimpleUnsafeMemoryAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ForL0BucketTableTest {
    private SimpleUnsafeMemoryAllocator allocator;
    private ForL0BucketTable table;

    /* ========== 辅助工具 ========== */

    /** 创建一条最小合法条目，写入 key/ns 字节和 header，返回原生地址 */
    private long createEntry(byte[] key, byte[] ns) {
        int len = ForL0EntryAccess.HEADER + key.length + ns.length;
        MemorySlice slice = allocator.allocate(len);
        long addr = slice.address();
        ForL0EntryAccess.hash(addr, 0);                       // 测试里统一用 hash=0
        UnsafeUtils.unsafe().putInt(addr + ForL0EntryAccess.KL, key.length);
        UnsafeUtils.unsafe().putInt(addr + ForL0EntryAccess.NL, ns.length);
        UnsafeUtils.unsafe().putInt(addr + ForL0EntryAccess.VL, 0); // 无 value
        ForL0EntryAccess.next(addr, 0);
        long p = addr + ForL0EntryAccess.HEADER;
        long ba = sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
        UnsafeUtils.unsafe().copyMemory(key, ba, null, p, key.length);
        UnsafeUtils.unsafe().copyMemory(ns,  ba, null, p + key.length, ns.length);
        return addr;
    }

    /** 与 ForL0StateMap 相同的 tag 计算，仅用 key */
    private short tag16(byte[] key) {
        int h = 0;
        for (byte b : key) { h ^= b; h *= 0x5bd1e995; h ^= h >>> 15; }
        return (short) (h & 0xFFFF);
    }

    /* ========== JUnit 生命周期 ========== */

    @BeforeEach
    void setup() {
        allocator = new SimpleUnsafeMemoryAllocator();
        /*
         * 创建根 bucket 数量 = 2^2 = 4，可让所有 hash=0 的条目都落在根 bucket[0]，
         * 方便测试满载及子桶扩容。
         */
        table = new ForL0BucketTable(2, allocator);
    }

    @AfterEach
    void teardown() {
        allocator.close();
//        assertEquals(0L, allocator.outstandingBytes(),
//                "native memory should be fully released");
    }

    /* ========== 测试 ========== */

    @Test
    void testSingleInsertLookup() {
        byte[] k = "k1".getBytes(), n = "ns".getBytes();
        long ptr = createEntry(k, n);
        table.insert(0, tag16(k), ptr);

        long found = table.lookup(k, n, 0, tag16(k));
        assertEquals(ptr, found, "lookup should return the exact pointer inserted");
    }

    @Test
    void testCollisionWithinRootBucket() {
        // 填满 6 slot
        for (int i = 0; i < 6; i++) {
            byte[] k = ("k" + i).getBytes();
            long p = createEntry(k, "ns".getBytes());
            table.insert(0, tag16(k), p);
        }
        // 确认全部命中
        for (int i = 0; i < 6; i++) {
            byte[] k = ("k" + i).getBytes();
            long found = table.lookup(k, "ns".getBytes(), 0, tag16(k));
            assertNotEquals(0, found, "each key should be found in root bucket slots");
        }
    }

    @Test
    void testLocalExpansionCreatesChildBucket() {
        // 先插入 6 条填满根 bucket
        for (int i = 0; i < 6; i++) {
            byte[] k = ("root" + i).getBytes();
            table.insert(0, tag16(k), createEntry(k, "ns".getBytes()));
        }
        // 再插入额外条目 -> 触发子桶扩容 (tag 决定 child index)
        byte[] extraKey = "extra".getBytes();
        long extraPtr = createEntry(extraKey, "ns".getBytes());
        table.insert(0, tag16(extraKey), extraPtr);

        long found = table.lookup(extraKey, "ns".getBytes(), 0, tag16(extraKey));
        assertEquals(extraPtr, found, "key should reside in the newly created child bucket");
    }

    @Test
    void testLookupMissReturnsZero() {
        byte[] k = "foo".getBytes();
        long res = table.lookup(k, "ns".getBytes(), 0, tag16(k));
        assertEquals(0, res, "lookup on empty table should return 0");
    }
}
