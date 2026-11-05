package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** A named original task, with numeric state and context-feature history. */
public class BenchsetStage extends KeyedProcessFunction<Long,BenchsetEvent,BenchsetEvent> {
    private final String role;
    private final int width,touches,window;
    private transient LongStateAccess.Counter count,total,last;
    private transient LongStateAccess.Vector features;
    private transient MapState<Long,Long> history;
    public BenchsetStage(String role,int width,int touches,int window) {
        this.role=role;this.width=width;this.touches=touches;this.window=window;
    }
    @Override public void open(Configuration ignored) throws Exception {
        count=new LongStateAccess.Counter(getRuntimeContext().getState(new ValueStateDescriptor<>(role+".count",Long.class)));
        total=new LongStateAccess.Counter(getRuntimeContext().getState(new ValueStateDescriptor<>(role+".total",Long.class)));
        last=new LongStateAccess.Counter(getRuntimeContext().getState(new ValueStateDescriptor<>(role+".last",Long.class)));
        features=new LongStateAccess.Vector(getRuntimeContext().getMapState(
                new MapStateDescriptor<>(role+".contextHistory",Long.class,Long.class)));
        history=getRuntimeContext().getMapState(new MapStateDescriptor<>(role+".window",Long.class,Long.class));
        System.out.println("STANDARD_STATE_API role="+role+" width="+width+" touches="+touches);
    }
    @Override public void processElement(BenchsetEvent input,Context ctx,Collector<BenchsetEvent> out) throws Exception {
        BenchsetEvent e=input.copy();
        long n=count.add(1);
        long previous=0;
        switch(role) {
            case "Count": e.metric=n; break;
            case "Predict":
                previous=last.get();
                if(n==1) { e.metric=0; }
                else {
                    long transition=previous*16+e.kind;
                    Long oldTransition=history.get(transition);
                    long transitionCount=oldTransition==null?1:oldTransition+1;
                    history.put(transition,transitionCount);
                    long outgoing=256+previous;
                    Long oldOutgoing=history.get(outgoing);
                    long outgoingCount=oldOutgoing==null?1:oldOutgoing+1;
                    history.put(outgoing,outgoingCount);
                    e.metric=1000000L-transitionCount*1000000L/outgoingCount;
                }
                e.flags=e.metric>750000?1:0;
                last.set(e.kind); break;
            case "Moving Average":
                long slot=(n-1)%window;
                Long old=history.get(slot);
                long sum=total.add(e.sample-(old==null?0:old));
                history.put(slot,e.sample);
                e.metric=sum/Math.min(n,window); break;
            case "Spike Detection":
                e.flags=Math.abs(e.sample-e.metric)*100 > Math.max(1,e.metric)*3 ? 1:0;
                total.add(e.flags); break;
            case "Map Match":
                // Numeric road dictionary: full key-space identity, no cardinality collapse.
                last.set(e.key); e.metric=e.sample; break;
            case "Speed Calculate": case "Average Speed": case "Global ACD":
                e.metric=total.add(e.sample)/n; break;
            case "Geo Finder":
                // Numeric city/country dictionary, retaining per-client history.
                e.metric=e.key%251; last.set(e.peer%65521); break;
            case "Geo Status": total.add(e.metric); e.metric=n; break;
            case "Status Counter": total.add(e.kind%5+200); e.metric=n; break;
            case "Volume Counter": e.metric=total.add(e.sample); break;
            case "RCRF": case "ECRF": case "ENCR": case "ECR24":
                e.metric=total.add(e.kind%2==0?1:0); break;
            case "CT24": e.metric=total.add(e.sample); break;
            case "FoFIR": case "URL": case "ACD":
                e.metric=total.add(e.metric)/n;
                e.flags=e.metric>512?1:0; break;
            case "Score": e.metric=total.add(e.flags+e.metric); break;
            case "Last Average Speed": previous=last.get(); last.set(e.metric); e.metric-=previous; break;
            case "Accident Detection":
                previous=last.get();
                e.flags=e.sample<20 && previous==e.peer?1:0;
                last.set(e.peer); e.metric=total.add(e.flags); break;
            case "Count Vehicles": e.metric=n; break;
            case "Accident Notification": e.metric=total.add(e.flags); break;
            case "Daily Expenses": e.metric=total.add(e.sample%50); break;
            case "Toll Notification": e.metric=total.add(e.metric>40?e.sample%25:0); break;
            case "Account Balance": e.metric=total.add(e.metric); break;
            default: throw new IllegalArgumentException(role);
        }
        int start=(int)((e.sequence*17+role.hashCode())&(width-1));
        // Context weights form a task-local feature history with observable projection.
        // The common 64-bit increment keeps values outside the boxed Long cache.
        e.features=features.increment(start,touches,width);
        out.collect(e);
    }
}
