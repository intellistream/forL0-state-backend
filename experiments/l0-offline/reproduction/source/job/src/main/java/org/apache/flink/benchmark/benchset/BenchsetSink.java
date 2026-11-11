package org.apache.flink.benchmark.benchset;

import com.google.gson.Gson;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class BenchsetSink extends RichSinkFunction<BenchsetEvent> {
    private final String directory,name,control;
    private transient long measuredRecords,measuredProjection,measuredSignature,startMs,endMs,nextCheck;
    private transient long[] secondCounts;
    private transient long records,projection,signature;
    public BenchsetSink(String directory,String name) {this(directory,name,"");}
    public BenchsetSink(String directory,String name,String control) {this.directory=directory;this.name=name;this.control=control;}
    @Override public void open(Configuration c) {records=projection=signature=0;}
    @Override public void invoke(BenchsetEvent e,Context c) throws Exception {
        records++;projection+=e.features;signature+=BenchsetEvent.mix(e.sequence);
        if(!control.isEmpty()) {
            long now=System.currentTimeMillis();
            if(endMs==0 && now>=nextCheck) {
                nextCheck=now+250;
                Path window=Paths.get(control,"window.json");
                if(Files.exists(window)) {
                    long[] times=new Gson().fromJson(new String(Files.readAllBytes(window),StandardCharsets.UTF_8),long[].class);
                    startMs=times[0];endMs=times[1];secondCounts=new long[(int)((endMs-startMs+999)/1000)];
                }
            }
            if(now>=startMs && now<endMs) {
                measuredRecords++;measuredProjection+=e.features;measuredSignature+=BenchsetEvent.mix(e.sequence);
                secondCounts[(int)((now-startMs)/1000)]++;
            }
        }
    }
    @Override public void close() throws Exception {
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("sink",name);result.put("subtask",getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        result.put("records",records);
        result.put("measured_records",measuredRecords);result.put("measured_projection",measuredProjection);
        result.put("measured_signature",measuredSignature);result.put("measurement_start_ms",startMs);result.put("measurement_end_ms",endMs);result.put("second_counts",secondCounts);result.put("feature_projection",projection);result.put("source_signature",signature);
        String text=new Gson().toJson(result);
        Path p=Paths.get(directory,"sinks");Files.createDirectories(p);
        Files.write(p.resolve(name.replace(' ','_')+"-"+result.get("subtask")+".json"),text.getBytes(StandardCharsets.UTF_8));
    }
}
