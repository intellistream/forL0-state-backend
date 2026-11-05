package org.apache.flink.benchmark.benchset;

import java.util.SplittableRandom;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** First pass covers the entire key space; subsequent passes use uniform random access. */
public class BenchsetSource extends RichParallelSourceFunction<BenchsetEvent> {
    private final int keys;
    private final long records;
    private volatile boolean running=true;
    public BenchsetSource(int keys,long records) { this.keys=keys;this.records=records; }
    @Override public void run(SourceContext<BenchsetEvent> ctx) throws Exception {
        int index=getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism=getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        SplittableRandom random=new SplittableRandom(20261102L+index);
        for(long i=index;running && i<records;i+=parallelism) {
            BenchsetEvent e=new BenchsetEvent();
            e.key=i<keys?i:random.nextInt(keys);
            e.peer=(e.key+1)%keys;
            e.sequence=i; e.sample=1+(BenchsetEvent.mix(i)&1023); e.metric=e.sample;
            e.kind=(int)(i&15);
            synchronized(ctx.getCheckpointLock()) { ctx.collect(e); }
        }
    }
    @Override public void cancel() { running=false; }
}
