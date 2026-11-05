package org.apache.flink.benchmark.benchset;

import com.google.gson.GsonBuilder;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.*;
import org.apache.flink.util.*;

/** Numeric state-pressure adaptation of ICDE 2017 Figure 5, rebuilt from scratch. */
public class BenchsetMain {
    static final OutputTag<BenchsetEvent> CALLED=new OutputTag<BenchsetEvent>("called-party"){};
    static final KeySelector<BenchsetEvent,Long> KEY=e->e.key;
    private final StreamExecutionEnvironment env;
    private final int width,touches,window,p;
    private final String output;
    private final String control;
    private final boolean materializeHistory;
    BenchsetMain(StreamExecutionEnvironment env,ParameterTool args) {
        this.env=env;p=args.getInt("parallelism",8);
        width=args.getInt("featureWidth",64);touches=args.getInt("touches",32);window=args.getInt("window",16);
        if(width<1||(width&(width-1))!=0||width>4096||touches<1||touches>4096||window<1)
            throw new IllegalArgumentException("width must be a power of two; dimensions must be positive and bounded");
        output=args.getRequired("outputDirectory");
        control=args.get("controlDirectory","");
        materializeHistory=!control.isEmpty() || args.getBoolean("materializeHistory",false);
    }
    DataStream<BenchsetEvent> task(DataStream<BenchsetEvent> input,String name) {
        return input.keyBy(KEY).process(new BenchsetStage(name,width,touches,window,materializeHistory))
                .name(name).uid(name).setParallelism(p);
    }
    void sink(DataStream<BenchsetEvent> input,String name) {
        input.addSink(new BenchsetSink(output,name,control)).name(name).uid(name).setParallelism(p);
    }
    void build(String workload,int keys,long records) {
        DataStream<BenchsetEvent> source=env.addSource(new BenchsetSource(keys,records,control))
                .name("Data Source").uid("Data Source").setParallelism(p);
        switch(workload) {
            case "wc":
                DataStream<BenchsetEvent> split=source.map(e->{BenchsetEvent x=e.copy();x.metric=1;return x;})
                    .returns(BenchsetEvent.class).name("Split").uid("Split").setParallelism(p);
                sink(task(split,"Count"),"Sink");break;
            case "fd":sink(task(source,"Predict"),"Sink");break;
            case "sd":sink(task(task(source,"Moving Average"),"Spike Detection"),"Sink");break;
            case "tm":sink(task(task(source,"Map Match"),"Speed Calculate"),"Sink");break;
            case "lg":
                sink(task(task(source,"Geo Finder"),"Geo Status"),"Geo Sink");
                sink(task(source,"Status Counter"),"Status Sink");
                sink(task(source,"Volume Counter"),"Count Sink");break;
            case "vs":
                SingleOutputStreamOperator<BenchsetEvent> dispatcher=source.process(new ProcessFunction<BenchsetEvent,BenchsetEvent>() {
                    @Override public void processElement(BenchsetEvent e,Context c,Collector<BenchsetEvent> out) {
                        out.collect(e);
                        BenchsetEvent called=e.copy();called.key=e.peer;c.output(CALLED,called);
                    }
                }).name("Voice Dispatcher").uid("Voice Dispatcher").setParallelism(p);
                DataStream<BenchsetEvent> rcrf=task(dispatcher.union(dispatcher.getSideOutput(CALLED)),"RCRF");
                DataStream<BenchsetEvent> ecrf=task(dispatcher,"ECRF");
                DataStream<BenchsetEvent> encr=task(dispatcher,"ENCR");
                DataStream<BenchsetEvent> ct24=task(dispatcher,"CT24");
                DataStream<BenchsetEvent> ecr24=task(dispatcher,"ECR24");
                DataStream<BenchsetEvent> global=dispatcher.keyBy((KeySelector<BenchsetEvent,Long>)(e->0L))
                    .process(new BenchsetStage("Global ACD",4,1,window,materializeHistory))
                    .name("Global ACD").uid("Global ACD").setParallelism(1);
                DataStream<BenchsetEvent> fofir=task(rcrf.union(ecrf),"FoFIR");
                DataStream<BenchsetEvent> url=task(ecrf.union(encr),"URL");
                DataStream<BenchsetEvent> acd=task(ct24.union(ecr24).union(global),"ACD");
                sink(task(fofir.union(url).union(acd),"Score"),"Sink");break;
            case "lr":
                DataStream<BenchsetEvent> dispatch=source.map(e->e.copy()).returns(BenchsetEvent.class)
                    .name("Dispatcher").uid("Dispatcher").setParallelism(p);
                DataStream<BenchsetEvent> avg=task(dispatch,"Average Speed");
                DataStream<BenchsetEvent> last=task(avg,"Last Average Speed");
                DataStream<BenchsetEvent> accident=task(dispatch,"Accident Detection");
                DataStream<BenchsetEvent> count=task(dispatch,"Count Vehicles");
                DataStream<BenchsetEvent> notification=task(dispatch.union(accident),"Accident Notification");
                DataStream<BenchsetEvent> daily=task(dispatch,"Daily Expenses");
                DataStream<BenchsetEvent> toll=task(dispatch.union(last).union(accident).union(count),"Toll Notification");
                DataStream<BenchsetEvent> account=task(dispatch.union(toll),"Account Balance");
                sink(notification.union(daily).union(toll).union(account),"Sink");break;
            default:throw new IllegalArgumentException("Unknown benchmark: "+workload);
        }
    }
    static String normalized(String s) {
        return s.replaceFirst("^Source: ","").replaceFirst("^Sink: ","");
    }
    static Map<String,Object> topology(StreamGraph graph) {
        List<Map<String,Object>> nodes=new ArrayList<>();List<Map<String,String>> edges=new ArrayList<>();
        for(StreamNode node:graph.getStreamNodes()) {
            Map<String,Object> n=new LinkedHashMap<>();
            n.put("name",normalized(node.getOperatorName()));n.put("parallelism",node.getParallelism());nodes.add(n);
            for(StreamEdge edge:node.getOutEdges()) {
                Map<String,String> e=new LinkedHashMap<>();
                e.put("from",normalized(node.getOperatorName()));
                e.put("to",normalized(graph.getStreamNode(edge.getTargetId()).getOperatorName()));
                edges.add(e);
            }
        }
        Map<String,Object> result=new LinkedHashMap<>();result.put("nodes",nodes);result.put("edges",edges);return result;
    }
    public static void main(String[] argv) throws Exception {
        ParameterTool args=ParameterTool.fromArgs(argv);
        if(args.getInt("checkpointInterval",0)!=0) throw new IllegalArgumentException("This suite runs without checkpointing");
        int keys=args.getInt("numKeys",524288);long records=args.getLong("numRecords",8000000);
        if(keys<1||records<keys)throw new IllegalArgumentException("records must cover the entire positive key space");
        String benchmark=args.get("benchmark","wc").toLowerCase(Locale.ROOT);
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(args.getInt("parallelism",8));env.disableOperatorChaining();
        env.getCheckpointConfig().disableCheckpointing();
        env.getConfig().disableGenericTypes();
        BenchsetMain job=new BenchsetMain(env,args);job.build(benchmark,keys,records);
        StreamGraph graph=env.getStreamGraph(false);graph.setJobName("ICDE standard state API "+benchmark);
        Path directory=Paths.get(job.output);Files.createDirectories(directory);
        Files.write(directory.resolve("topology.json"),new GsonBuilder().setPrettyPrinting().create()
                .toJson(topology(graph)).getBytes(StandardCharsets.UTF_8));
        Files.write(directory.resolve("plan.json"),graph.getStreamingPlanAsJSON().getBytes(StandardCharsets.UTF_8));
        if(!args.getBoolean("planOnly",false))env.execute(graph);
    }
}
