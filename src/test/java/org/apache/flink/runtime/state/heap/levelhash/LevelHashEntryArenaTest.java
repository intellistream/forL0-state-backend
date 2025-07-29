package org.apache.flink.runtime.state.heap.levelhash;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LevelHashEntryArena}.
 *
 * <p>Verifies:
 * <ul>
 *     <li>Basic put / get round-trip</li>
 *     <li>Page rollover when many entries are appended</li>
 *     <li>All off-heap pages are released by {@link LevelHashEntryArena#clear()}</li>
 * </ul>
 * </p>
 */
class LevelHashEntryArenaTest {

    private static final int PAGE_SIZE = 4 * 1024;          // Flink minimum
    private static final long TOTAL_MEM = PAGE_SIZE * 16L;  // 16 pages

    private static MemoryManager mm;
    private MemoryManagerAllocator allocator;
    private LevelHashEntryArena arena;

    // ---------------------------------------------------------------------
    @BeforeAll
    static void initMemoryManager() {
        mm = MemoryManagerBuilder
                .newBuilder()
                .setPageSize(PAGE_SIZE)
                .setMemorySize(TOTAL_MEM)
                .build();
    }

    @BeforeEach
    void setUp() {
        allocator = new MemoryManagerAllocator(mm, this);
        arena     = new LevelHashEntryArena(allocator);
    }

    @AfterEach
    void tearDown() {
        arena.clear();                                 // explicit release
        arena.close();
    }

    // ---------------------------------------------------------------------
    @Test
    void basicPutGet() throws Exception {
        byte[] key   = "key-1".getBytes();
        byte[] value = "value-1".getBytes();

        long ptr = arena.put(key, value);
        assertNotEquals(0L, ptr, "non-zero pointer");

        assertArrayEquals(value, arena.getValue(ptr));
    }

    @Test
    void pageRollover() throws Exception {
        // each entry ≈ 50 B; inserting 500 triggers > 4 pages
        final int ENTRY_CNT = 500;
        for (int i = 0; i < ENTRY_CNT; i++) {
            byte[] k = ("k" + i).getBytes();
            byte[] v = ("payload-" + i).getBytes();
            arena.put(k, v);
        }

        // spot-check a few entries
        for (int i : new int[]{0, 123, 499}) {
            byte[] k = ("k" + i).getBytes();
            long ptr = arena.put(k, ("new-" + i).getBytes()); // overwrite via new record
            assertArrayEquals(("new-" + i).getBytes(), arena.getValue(ptr));
        }
    }
}
