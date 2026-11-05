package org.apache.flink.benchmark.benchset;

import org.junit.jupiter.api.Test;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import java.lang.reflect.Proxy;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class TaskSemanticsTest {
    private BenchsetStage stage(String role,int window) throws Exception {
        Map<String,ValueState<Long>> values=new HashMap<>();
        Map<String,MapState<Long,Long>> maps=new HashMap<>();
        RuntimeContext runtime=(RuntimeContext)Proxy.newProxyInstance(RuntimeContext.class.getClassLoader(),new Class<?>[]{RuntimeContext.class},(p,m,args)->{
            StateDescriptor<?,?> descriptor=(StateDescriptor<?,?>)args[0];
            if(m.getName().equals("getState"))return values.computeIfAbsent(descriptor.getName(),k->new ValueState<Long>() {
                Long value;public Long value(){return value;}public void update(Long v){value=v;}public void clear(){value=null;}
            });
            if(m.getName().equals("getMapState"))return maps.computeIfAbsent(descriptor.getName(),k->new BenchsetTest.HeapMap());
            throw new UnsupportedOperationException(m.getName());
        });
        BenchsetStage stage=new BenchsetStage(role,64,32,window);
        stage.setRuntimeContext(runtime);stage.open(new Configuration());return stage;
    }
    private BenchsetEvent apply(BenchsetStage stage,long sample,long metric,int kind,long seq) throws Exception {
        BenchsetEvent e=new BenchsetEvent();e.key=1;e.sample=sample;e.metric=metric;e.kind=kind;e.sequence=seq;
        List<BenchsetEvent> results=new ArrayList<>();
        stage.processElement(e,null,new Collector<BenchsetEvent>() {
            public void collect(BenchsetEvent x){results.add(x);}public void close(){}
        });
        assertEquals(1,results.size());assertTrue(results.get(0).features>0);return results.get(0);
    }
    @Test void wordCountRemembersEveryOccurrence() throws Exception {
        BenchsetStage stage=stage("Count",2);
        for(int i=1;i<=10;i++)assertEquals(i,apply(stage,1,1,0,i).metric);
    }
    @Test void movingAverageRetiresTheOldestMeasurement() throws Exception {
        BenchsetStage stage=stage("Moving Average",2);
        assertEquals(10,apply(stage,10,0,0,1).metric);assertEquals(15,apply(stage,20,0,0,2).metric);
        assertEquals(25,apply(stage,30,0,0,3).metric);assertEquals(35,apply(stage,40,0,0,4).metric);
    }
    @Test void predictorUsesASequenceOfTwoEventTypes() throws Exception {
        BenchsetStage stage=stage("Predict",2);
        assertEquals(0,apply(stage,0,0,1,1).metric);assertEquals(0,apply(stage,0,0,2,2).metric);
        assertEquals(0,apply(stage,0,0,1,3).metric);
        assertEquals(500000,apply(stage,0,0,3,4).metric);
    }
    @Test void spikeThresholdIsThreePercent() throws Exception {
        BenchsetStage stage=stage("Spike Detection",2);
        assertEquals(0,apply(stage,102,100,0,1).flags);assertEquals(1,apply(stage,104,100,0,2).flags);
    }
    @Test void benchmarkRejectsCheckpoints() {
        assertThrows(IllegalArgumentException.class,()->BenchsetMain.main(new String[]{"--checkpointInterval","1000"}));
    }
}
