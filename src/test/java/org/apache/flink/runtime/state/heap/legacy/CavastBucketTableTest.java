package org.apache.flink.runtime.state.heap.legacy;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.legacy.CavastEntryAccess;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CavastBucketTable}.
 *
 * <p>The tests focus on:
 * <ul>
 *   <li>basic insert / lookup semantics</li>
 *   <li>collision handling inside the root bucket</li>
 *   <li>local expansion that creates child buckets</li>
 *   <li>slab growth when child‑bucket count exceeds initial capacity</li>
 *   <li>native memory accounting &amp; leak detection</li>
 * </ul>
 *
 * <p>The helper methods intentionally bypass higher‑level serializers and operate
 * directly on the entry layout to keep the tests deterministic and fast.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CavastBucketTableTest {

    /* --------------------------------------------------------------------- */
    /*  Test constants                                                        */
    /* --------------------------------------------------------------------- */

    private static final int ROOT_BUCKET_POW2 = 2;      // 2^2 = 4 root buckets
    private static final int PAGE_SIZE        = 64 * 1024;
    private static final long MEM_SIZE        = 8 * 1024 * 1024L; // 8MB

    /* --------------------------------------------------------------------- */
    /*  Flink managed memory                                                 */
    /* --------------------------------------------------------------------- */

    private MemoryManager         memoryManager;
    private MemoryManagerAllocator allocator;
    private CavastBucketTable table;

    /* --------------------------------------------------------------------- */
    /*  Test lifecycle                                                       */
    /* --------------------------------------------------------------------- */

    @BeforeAll
    void initManager() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                                            .setMemorySize(MEM_SIZE)
                                            .setPageSize(PAGE_SIZE)
                                            .build();
    }

    @BeforeEach
    void setUpTable() {
        allocator = new MemoryManagerAllocator(memoryManager, this);
        table     = new CavastBucketTable(ROOT_BUCKET_POW2, allocator);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
        memoryManager.verifyEmpty();  // fail test if native memory leaked
    }

    @AfterAll
    void closeManager() {
        memoryManager.shutdown();
    }

    /* --------------------------------------------------------------------- */
    /*  Helper utilities                                                     */
    /* --------------------------------------------------------------------- */

    /** Creates the minimal entry layout (no value) and returns native address. */
    private long createEntry(byte[] key, byte[] ns) {
        int len = CavastEntryAccess.HEADER + key.length + ns.length;
        List<MemorySegment> pages = null;
        try {
            pages = allocator.allocate(len);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
        MemorySegment seg = pages.get(0);

        long addr = seg.getAddress();
        UnsafeUtils.unsafe().putInt(addr + CavastEntryAccess.HASH, 0);           // use hash=0
        UnsafeUtils.unsafe().putInt(addr + CavastEntryAccess.KL,   key.length);
        UnsafeUtils.unsafe().putInt(addr + CavastEntryAccess.NL,   ns.length);
        UnsafeUtils.unsafe().putInt(addr + CavastEntryAccess.VL,   0);           // no value
        CavastEntryAccess.next(addr, 0);

        long off = addr + CavastEntryAccess.HEADER;
        long aba = sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
        UnsafeUtils.unsafe().copyMemory(key, aba, null, off,               key.length);
        UnsafeUtils.unsafe().copyMemory(ns,  aba, null, off + key.length,  ns.length);
        return addr;
    }

    private static short tag16(byte[] key) {
        int h = 0;
        for (byte b : key) { h ^= b; h *= 0x5bd1e995; h ^= h >>> 15; }
        return (short) (h & 0xFFFF);
    }

    /* --------------------------------------------------------------------- */
    /*  Tests                                                                */
    /* --------------------------------------------------------------------- */

    @Test
    void insertAndLookup_acrossRootBuckets() {
        byte[] ns = "ns".getBytes();

        // keys designed to land in different root buckets (hash=0, tag decides slot)
        byte[] k1 = "alpha".getBytes();
        byte[] k2 = "beta".getBytes();

        long p1 = createEntry(k1, ns);
        long p2 = createEntry(k2, ns);

        table.insert(0, tag16(k1), p1);
        table.insert(0, tag16(k2), p2);

        assertEquals(p1, table.lookup(k1, ns, 0, tag16(k1)));
        assertEquals(p2, table.lookup(k2, ns, 0, tag16(k2)));
    }

    @Test
    void collisionWithinRootBucket_allSlotsUtilised() {
        // Fill 6 slots in root bucket[0]
        byte[] ns = "ns".getBytes();
        for (int i = 0; i < 6; i++) {
            byte[] k = ("k" + i).getBytes();
            table.insert(0, tag16(k), createEntry(k, ns));
        }
        // verify they are all reachable
        for (int i = 0; i < 6; i++) {
            byte[] k = ("k" + i).getBytes();
            assertNotEquals(0, table.lookup(k, ns, 0, tag16(k)));
        }
    }

//    @Test
//    void localExpansion_createsAndFindsChildBucket() {
//        byte[] ns = "ns".getBytes();
//
//        // Fill root bucket
//        for (int i = 0; i < 6; i++) {
//            byte[] k = ("root" + i).getBytes();
//            table.insert(0, tag16(k), createEntry(k, ns));
//        }
//
//        // Insert one more -> should go to child
//        byte[] extra = "extra".getBytes();
//        long extraPtr = createEntry(extra, ns);
//        table.insert(0, tag16(extra), extraPtr);
//
//        long found = table.lookup(extra, ns, 0, tag16(extra));
//        assertEquals(extraPtr, found, "entry should be located in child bucket");
//    }

    @Test
    void slabGrows_whenChildBucketsExceedCapacity() throws Exception {
        byte[] ns = "ns".getBytes();

        // Continuously cause local expansions until slab doubles
        int initialTotalBuckets = getTotalBuckets(table);

        int inserts = 0;
        while (getTotalBuckets(table) == initialTotalBuckets) {
            byte[] k = ("bulk" + inserts).getBytes();
            table.insert(0, tag16(k), createEntry(k, ns));
            inserts++;
        }
        int grownBuckets = getTotalBuckets(table);
        assertEquals(initialTotalBuckets * 2, grownBuckets,
                     "totalBuckets should double after slab grow");
    }

    @Test
    void afterClose_noOutstandingBytes() {
        allocator.close();
        assertEquals(0, allocator.getUsedBytes(),
                     "allocator should report zero outstanding bytes after close");
    }

    /* --------------------------------------------------------------------- */
    /*  Reflection helpers                                                   */
    /* --------------------------------------------------------------------- */

    private static int getTotalBuckets(CavastBucketTable t) throws Exception {
        Field f = CavastBucketTable.class.getDeclaredField("totalBuckets");
        f.setAccessible(true);
        return (int) f.get(t);
    }
}
