package org.apache.flink.benchmark.wordcount.source;

import java.util.SplittableRandom;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** Direct Long keys. The first pass covers every key, without hash collisions. */
public class LongKeySource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
    private final int numKeys;
    private final long numRecords;
    private final double skew;
    private final int arrivalRate;
    private volatile boolean running = true;

    public LongKeySource(int numKeys, long numRecords, double skew, int arrivalRate) {
        if (numKeys <= 0 || numRecords <= 0 || arrivalRate < 0) {
            throw new IllegalArgumentException("Invalid source size/rate");
        }
        this.numKeys = numKeys;
        this.numRecords = numRecords;
        this.skew = skew;
        this.arrivalRate = arrivalRate;
    }

    static long keyFor(long ordinal, int numKeys, SplittableRandom random, ZipfDistribution zipf) {
        if (ordinal < numKeys) return ordinal;
        return zipf == null ? random.nextInt(numKeys) : zipf.sample() - 1L;
    }

    @Override public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
        int index = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        SplittableRandom random = new SplittableRandom(20261102L + index);
        ZipfDistribution zipf = skew > 0 ? new ZipfDistribution(numKeys, skew) : null;
        if (zipf != null) zipf.reseedRandomGenerator(20261102L + index);
        long count = 0;
        int batch = arrivalRate > 0 ? Math.max(1, arrivalRate / parallelism / 100) : 10000;
        long start = System.nanoTime();
        for (long ordinal = index; running && ordinal < numRecords; ordinal += parallelism) {
            long key = keyFor(ordinal, numKeys, random, zipf);
            synchronized (ctx.getCheckpointLock()) { ctx.collect(Tuple2.of(key, 1L)); }
            count++;
            if (arrivalRate > 0 && count % batch == 0) {
                long remaining = count * 1000000000L * parallelism / arrivalRate - (System.nanoTime() - start);
                if (remaining > 0) Thread.sleep(remaining / 1000000L, (int)(remaining % 1000000L));
            }
        }
        System.out.println("LongKeySource subtask=" + index + " records=" + count);
    }
    @Override public void cancel() { running = false; }
}
