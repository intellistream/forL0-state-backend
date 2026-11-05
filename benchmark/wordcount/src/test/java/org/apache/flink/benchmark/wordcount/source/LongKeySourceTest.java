package org.apache.flink.benchmark.wordcount.source;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
public class LongKeySourceTest {
    @Test void coversKeySpaceAcrossUnevenParallelPartitions() {
        int keys = 10003;
        Set<Long> seen = new HashSet<>();
        for (int subtask = 0; subtask < 8; subtask++) {
            SplittableRandom random = new SplittableRandom(subtask);
            for (long ordinal = subtask; ordinal < keys; ordinal += 8)
                assertTrue(seen.add(LongKeySource.keyFor(ordinal, keys, random, null)));
        }
        assertEquals(keys, seen.size());
        SplittableRandom random = new SplittableRandom(42);
        for (int i = 0; i < 10000; i++) {
            long key = LongKeySource.keyFor(keys + i, keys, random, null);
            assertTrue(key >= 0 && key < keys);
        }
    }
}
