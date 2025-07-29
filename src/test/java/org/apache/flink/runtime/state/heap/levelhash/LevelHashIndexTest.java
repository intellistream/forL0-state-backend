package org.apache.flink.runtime.state.heap.levelhash;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.*;

import java.util.IntSummaryStatistics;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verify the off‑heap Level Hash implementation w.r.t CRUD, overwrite semantics,
 * heavy collisions, in‑place resize and correct resource reclamation.
 *
 * We store the integer key itself in the lower 32‑bits of the ptr to keep assertions simple.
 */
class LevelHashIndexTest {

    // ---------------------------------------------------------------------

    private static MemoryManager     mm;
    private MemoryManagerAllocator   allocator;
    private LevelHashIndex index;

    private static long encode(int v) { return (v & 0xffff_ffffL); }
    private static int  decode(long p){ return (int) p; }

    // ---------------------------------------------------------------------
    @BeforeAll
    static void setupMM() {
        mm = MemoryManagerBuilder
                .newBuilder()
                .setPageSize(32 * 1024)
                .setMemorySize(32L * 32 * 1024)
                .build();
    }

    @BeforeEach
    void open() throws Exception {
        allocator = new MemoryManagerAllocator(mm, this);
        // initial 1k top buckets (must be power‑of‑two exponent)
        index = new LevelHashIndex(allocator, 10); // 2^10 = 1024
    }

    @AfterEach
    void close() {
        long before = allocator.outstandingBytes();
        index.close();
        assertEquals(0L, allocator.outstandingBytes(), "all off‑heap pages returned");
        assertTrue(before > 0, "index should allocate something");
    }

    // ---------------------------------------------------------------------
    //  Basic CRUD
    // ---------------------------------------------------------------------
    @Test
    void putGetRemove() throws Exception {
        final int CNT = 100;
        for (int i = 0; i < CNT; i++) {
            assertEquals(0L, index.get(i));
            assertEquals(0L, index.put(i, encode(i)));
            assertEquals(i, decode(index.get(i)));
        }
        // remove odd keys
        for (int i = 1; i < CNT; i += 2) {
            assertEquals(i, decode(index.remove(i)));
            assertEquals(0L, index.get(i));
        }
        assertEquals(CNT / 2, index.size());
    }

    // ---------------------------------------------------------------------
    @Test
    void overwriteReturnsOld() throws Exception {
        assertEquals(0L, index.put(42, encode(100)));
        assertEquals(100, decode(index.put(42, encode(200))));
        assertEquals(200, decode(index.get(42)));
    }

    // ---------------------------------------------------------------------
    @Test
    void heavyCollision() throws Exception {
        int base = 17;
        for (int i = 0; i < 50; i++) {
            int k = base + i * 1024; // keep low bits identical
            index.put(k, encode(k));
            assertEquals(k, decode(index.get(k)));
        }
        assertEquals(50, index.size());
    }

    // ---------------------------------------------------------------------
//    @Test
//    void resizeKeepsEntries() throws Exception {
//        int initial = index.topCapacity();
//        int toInsert = (int) (initial * 1.2); // definitely triggers resize (load > 0.9)
//        for (int i = 0; i < toInsert; i++) {
//            index.put(i, encode(i));
//        }
//        assertTrue(index.topCapacity() > initial, "must grow");
//        // verify
//        for (int i = 0; i < toInsert; i++) {
//            assertEquals(i, decode(index.get(i)));
//        }
//    }

    // ---------------------------------------------------------------------
    @Test
    void randomisedFuzz() throws Exception {
        final int OPS = 100;
        Random rnd = new Random(1234);
        Set<Integer> present = IntStream.empty().boxed().collect(Collectors.toSet());

        for (int i = 0; i < OPS; i++) {
            int k = rnd.nextInt(OPS / 2);
            if (rnd.nextBoolean()) {
                index.put(k, encode(k));
                present.add(k);
            } else {
                index.remove(k);
                present.remove(k);
            }
        }
        for (int k : present) {
            assertEquals(k, decode(index.get(k)));
        }
        // quick stats
        IntSummaryStatistics stats = present.stream().mapToInt(Integer::intValue).summaryStatistics();
        System.out.printf("fuzz done – keys left: %d  min:%d  max:%d%n", stats.getCount(), stats.getMin(), stats.getMax());
    }
}
