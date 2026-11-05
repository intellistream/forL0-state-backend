package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.StateBackendType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Bounded Long-only subset of flink-state-benchmark; all timed calls use Flink public APIs. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations=3, time=3)
@Measurement(iterations=5, time=5)
@Fork(2)
public class LongStateMicrobenchmarks {
    public static final int BATCH=1024;
    public static final long BASE=1_000_000_007L;
    @State(Scope.Thread)
    public abstract static class Fixture {
        @Param({"HEAP","FORL0"}) public StateBackendType backendType;
        @Param({"1048576"}) public int keyCount;
        KeyedStateBackend<Long> backend;
        Long[] keys=new Long[BATCH];
        Long[] values=new Long[BATCH];
        int cursor;
        String operation;
        void open(BenchmarkParams p) throws Exception {
            operation=p.getBenchmark().substring(p.getBenchmark().lastIndexOf('.')+1);
            backend=StateBackendBenchmarkUtils.createKeyedStateBackend(backendType);
        }
        void nextKeys(boolean insertion) {
            for(int i=0;i<BATCH;i++) {
                int k=((cursor+i)*65537)&(keyCount-1);
                keys[i]=(long)k+(insertion?keyCount:0);
                values[i]=BASE+cursor+i;
            }
            cursor=(cursor+BATCH)&(keyCount-1);
        }
        @TearDown(Level.Trial) public void close() throws Exception {
            StateBackendBenchmarkUtils.cleanUp(backend);
        }
    }
    @State(Scope.Thread)
    public static class Values extends Fixture {
        ValueState<Long> state;
        @Setup(Level.Trial) public void setup(BenchmarkParams p) throws Exception {
            open(p);
            state=StateBackendBenchmarkUtils.getValueState(backend,new ValueStateDescriptor<>("value",LongSerializer.INSTANCE));
            for(long k=0;k<keyCount;k++){backend.setCurrentKey(k);state.update(BASE+k);}
        }
        @Setup(Level.Invocation) public void prepare() throws Exception {
            boolean add=operation.equals("valueAdd");nextKeys(add);
            if(add)for(Long k:keys){backend.setCurrentKey(k);state.clear();}
        }
    }
    @State(Scope.Thread)
    public static class Lists extends Fixture {
        ListState<Long> state;
        final List<Long> batchValues=new ArrayList<>();
        @Setup(Level.Trial) public void setup(BenchmarkParams p) throws Exception {
            open(p);
            state=StateBackendBenchmarkUtils.getListState(backend,new ListStateDescriptor<>("list",LongSerializer.INSTANCE));
            for(int j=0;j<16;j++)batchValues.add(BASE+j);
            for(long k=0;k<keyCount;k++) {
                List<Long> initial=new ArrayList<>(16);
                for(int j=0;j<16;j++)initial.add(BASE+k*16+j);
                backend.setCurrentKey(k);state.update(initial);
            }
        }
        @Setup(Level.Invocation) public void prepare() throws Exception {
            nextKeys(false);
            if(operation.equals("listAdd")||operation.equals("listAddAll")||operation.equals("listAddThenIterate"))
                for(Long k:keys){backend.setCurrentKey(k);state.update(batchValues);}
        }
    }
    @State(Scope.Thread)
    public static class Maps extends Fixture {
        @Param({"64"}) public int touchesPerKey;
        MapState<Long,Long> state;
        Long[] inner=new Long[BATCH];
        final Map<Long,Long> bulk=new LinkedHashMap<>();
        @Setup(Level.Trial) public void setup(BenchmarkParams p) throws Exception {
            open(p);
            state=StateBackendBenchmarkUtils.getMapState(backend,new MapStateDescriptor<>("map",LongSerializer.INSTANCE,LongSerializer.INSTANCE));
            for(long j=0;j<16;j++)bulk.put(j,BASE+j);
            for(long k=0;k<keyCount;k++) {
                backend.setCurrentKey(k);
                for(long j=0;j<64;j++)state.put(j,BASE+k*64+j);
            }
        }
        @Setup(Level.Invocation) public void prepare() throws Exception {
            boolean add=operation.equals("mapAdd");
            boolean point=operation.equals("mapGet")||operation.equals("mapUpdate");
            if(point) {
                for(int i=0;i<BATCH;i++) {
                    keys[i]=(long)(((cursor+i/touchesPerKey)*65537)&(keyCount-1));
                    inner[i]=(long)(i&63);values[i]=BASE+cursor+i;
                }
                cursor=(cursor+BATCH/touchesPerKey)&(keyCount-1);
            } else {
                nextKeys(add);
                for(int i=0;i<BATCH;i++)inner[i]=(long)((cursor/BATCH+i)&63);
            }
            if(add)for(Long k:keys){backend.setCurrentKey(k);state.clear();}
            if(operation.equals("mapRemove"))for(int i=0;i<BATCH;i++){
                backend.setCurrentKey(keys[i]);state.put(inner[i],values[i]);
            }
        }
    }
    @Benchmark @OperationsPerInvocation(BATCH)
    public void valueAdd(Values s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.update(s.values[i]);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public long valueGet(Values s)throws Exception{long n=0;for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);n+=s.state.value();}return n;}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void valueUpdate(Values s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.update(s.values[i]);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void listAdd(Lists s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.add(s.values[i]);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void listAddAll(Lists s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.addAll(s.batchValues);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void listGet(Lists s,Blackhole bh)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);bh.consume(s.state.get());}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public long listIterate(Lists s)throws Exception{long n=0;for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);for(Long v:s.state.get())n+=v;}return n;}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void listUpdate(Lists s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.update(s.batchValues);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void mapAdd(Maps s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.put(s.inner[i],s.values[i]);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public long mapGet(Maps s)throws Exception {
        long n=0;
        for(int begin=0;begin<BATCH;begin+=s.touchesPerKey) {
            s.backend.setCurrentKey(s.keys[begin]);
            for(int i=begin;i<begin+s.touchesPerKey;i++)n+=s.state.get(s.inner[i]);
        }
        return n;
    }
    @Benchmark @OperationsPerInvocation(BATCH)
    public void mapPutAll(Maps s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.putAll(s.bulk);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void mapRemove(Maps s)throws Exception{for(int i=0;i<BATCH;i++){s.backend.setCurrentKey(s.keys[i]);s.state.remove(s.inner[i]);}}
    @Benchmark @OperationsPerInvocation(BATCH)
    public void mapUpdate(Maps s)throws Exception {
        for(int begin=0;begin<BATCH;begin+=s.touchesPerKey) {
            s.backend.setCurrentKey(s.keys[begin]);
            for(int i=begin;i<begin+s.touchesPerKey;i++)s.state.put(s.inner[i],s.values[i]);
        }
    }
    @Benchmark @OperationsPerInvocation(BATCH)
    public long listUpdateThenIterate(Lists s)throws Exception {
        long sum=0;
        for(int i=0;i<BATCH;i++) {
            s.backend.setCurrentKey(s.keys[i]);s.state.update(s.batchValues);
            for(Long v:s.state.get())sum+=v;
        }
        return sum;
    }
    @Benchmark @OperationsPerInvocation(BATCH)
    public long listAddThenIterate(Lists s)throws Exception {
        long sum=0;
        for(int i=0;i<BATCH;i++) {
            s.backend.setCurrentKey(s.keys[i]);s.state.add(s.values[i]);
            for(Long v:s.state.get())sum+=v;
        }
        return sum;
    }
}
