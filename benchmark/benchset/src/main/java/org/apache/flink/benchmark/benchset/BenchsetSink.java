package org.apache.flink.benchmark.benchset;

import com.google.gson.Gson;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class BenchsetSink extends RichSinkFunction<BenchsetEvent> {
    private final String directory,name;
    private transient long records,projection,signature;
    public BenchsetSink(String directory,String name) {this.directory=directory;this.name=name;}
    @Override public void open(Configuration c) {records=projection=signature=0;}
    @Override public void invoke(BenchsetEvent e,Context c) {
        records++;projection+=e.features;signature+=BenchsetEvent.mix(e.sequence);
    }
    @Override public void close() throws Exception {
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("sink",name);result.put("subtask",getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        result.put("records",records);result.put("feature_projection",projection);result.put("source_signature",signature);
        String text=new Gson().toJson(result);
        Path p=Paths.get(directory,"sinks");Files.createDirectories(p);
        Files.write(p.resolve(name.replace(' ','_')+"-"+result.get("subtask")+".json"),text.getBytes(StandardCharsets.UTF_8));
    }
}
