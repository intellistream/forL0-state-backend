package org.apache.flink.benchmark.benchset;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.google.gson.Gson;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** Shared key-coverage barrier, then the original seeded uniform access stream. */
public class BenchsetSource extends RichParallelSourceFunction<BenchsetEvent> {
    private final int keys; private final long records; private final String control;
    private volatile boolean running=true;
    public BenchsetSource(int keys,long records){this(keys,records,"");}
    public BenchsetSource(int keys,long records,String control){this.keys=keys;this.records=records;this.control=control;}
    @Override public void run(SourceContext<BenchsetEvent> ctx)throws Exception {
        int index=getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism=getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        SplittableRandom random=new SplittableRandom(20261102L+index);
        Path base=control.isEmpty()?null:Paths.get(control);
        long produced=0,signature=0,sequence=index;
        try {
            for(;running && (base!=null || sequence<records);sequence+=parallelism) {
                if(base!=null && sequence>=keys && produced==keys/parallelism+(index<keys%parallelism?1:0)) {
                    Files.write(base.resolve("ready-"+index),Long.toString(produced).getBytes(StandardCharsets.UTF_8));
                    while(running && !Files.exists(base.resolve("release")))Thread.sleep(10);
                }
                if(base!=null && (produced&1023)==0) {
                    if(Files.exists(base.resolve("stop")))break;
                    if(Files.exists(base.resolve("pause"))) {
                        Map<String,Object> paused=new LinkedHashMap<>();paused.put("records",produced);paused.put("signature",signature);
                        Path temp=base.resolve("paused-"+index+".json.tmp");
                        Files.write(temp,new Gson().toJson(paused).getBytes(StandardCharsets.UTF_8));
                        Files.move(temp,base.resolve("paused-"+index+".json"));
                        while(running && Files.exists(base.resolve("pause")) && !Files.exists(base.resolve("stop")))Thread.sleep(10);
                        if(Files.exists(base.resolve("stop")))break;
                    }
                }
                BenchsetEvent e=new BenchsetEvent();
                e.key=sequence<keys?sequence:random.nextInt(keys);e.peer=(e.key+1)%keys;
                e.sequence=sequence;e.sample=1+(BenchsetEvent.mix(sequence)&1023);e.metric=e.sample;e.kind=(int)(sequence&15);
                synchronized(ctx.getCheckpointLock()){ctx.collect(e);}
                produced++;signature+=BenchsetEvent.mix(sequence);
            }
        }finally {
            if(base!=null){
                Map<String,Object> row=new LinkedHashMap<>();row.put("subtask",index);row.put("records",produced);row.put("signature",signature);row.put("next_sequence",sequence);
                Files.write(base.resolve("source-"+index+".json"),new Gson().toJson(row).getBytes(StandardCharsets.UTF_8));
            }
        }
    }
    @Override public void cancel(){running=false;}
}
