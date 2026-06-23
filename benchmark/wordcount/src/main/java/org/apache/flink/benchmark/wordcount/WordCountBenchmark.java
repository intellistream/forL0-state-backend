/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark.wordcount;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.wordcount.sink.MetricsSink;
import org.apache.flink.benchmark.wordcount.source.SkewedWordSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

/**
 * Stateful WordCount Benchmark (State-Backend Focused).
 * 
 * <p>This benchmark measures the performance of Flink StateBackend implementations
 * using a stateful word count workload that maximizes state access overhead.
 * 
 * <p>Key design for StateBackend benchmarking:
 * <ul>
 *   <li>Uses KeyedProcessFunction with ValueState (VoidNamespace) instead of windows</li>
 *   <li>No Timer overhead - pure state access</li>
 *   <li>Each record triggers one state read + one state write</li>
 *   <li>High key cardinality to stress the state backend</li>
 * </ul>
 * 
 * <p>Metrics collected:
 * <ul>
 *   <li>Throughput (records/s)</li>
 *   <li>Throughput per core</li>
 * </ul>
 * 
 * <p>Usage:
 * <pre>
 * flink run wordcount-benchmark.jar \
 *   --numKeys 1000000 \
 *   --numRecords 100000000 \
 *   --skewFactor 1.1 \
 *   --parallelism 8
 * </pre>
 */
public class WordCountBenchmark {

    public static void main(String[] args) throws Exception {
        // Parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        
        int numKeys = params.getInt("numKeys", 1_000_000);
        long numRecords = params.getLong("numRecords", 100_000_000L);
        double skewFactor = params.getDouble("skewFactor", 1.1);
        int arrivalRate = params.getInt("arrivalRate", 0);  // records/s, 0 = unlimited (default: unlimited for max throughput)
        String workloadMode = params.get("workloadMode", "stateful_counter").toLowerCase();
        String keyType = params.get("keyType", "string").toLowerCase();  // "string" or "long"
        int windowSize = params.getInt("windowSize", 5);
        int slideSizeMs = params.getInt("slideSize", 200);
        int parallelism = params.getInt("parallelism", 8);
        String outputPath = params.get("output", null);
        String backend = params.get("backend", "unknown");
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        
        // Disable checkpointing by default to avoid Copy-on-Write overhead
        // This gives cleaner state backend performance comparison
        int checkpointInterval = params.getInt("checkpointInterval", 0);
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }
        
        // When keyType=long, use Long keys to exercise ForL0's primitive fast paths
        boolean useLongKeys = "long".equals(keyType);

        DataStream<Tuple2<String, Long>> result;
        if (useLongKeys && "stateful_counter".equals(workloadMode)) {
            // Long-key stateful counter — exercises ForL0 LONG_VOID + addAndGetLong fast path
            DataStream<Tuple2<Long, Long>> longSource = env
                .addSource(new SkewedWordSource(numKeys, numRecords, skewFactor, arrivalRate))
                .map(t -> Tuple2.of((long) t.f0.hashCode(), t.f1))
                .returns(new TypeHint<Tuple2<Long, Long>>() {})
                .name("LongKeySource");
            DataStream<Tuple2<String, Long>> longResult = longSource
                .keyBy(t -> t.f0)
                .process(new LongKeyStatefulCounter())
                .name("LongKeyStatefulCounter");
            longResult.addSink(new MetricsSink(outputPath, parallelism, backend, numRecords))
                .name("MetricsSink")
                .setParallelism(1);
            System.out.println("=== WordCount Benchmark (Long keys) ===");
            System.out.println("workloadMode: " + workloadMode);
            System.out.println("keyType: long");
            System.out.println("numKeys: " + numKeys);
            System.out.println("numRecords: " + numRecords);
            System.out.println("parallelism: " + parallelism);
            env.execute("Stateful WordCount Benchmark (Long keys)");
            return;
        }

        // Create skewed key source - outputs Tuple2<key, 1L>
        DataStream<Tuple2<String, Long>> source = env
            .addSource(new SkewedWordSource(numKeys, numRecords, skewFactor, arrivalRate))
            .name("SkewedKeySource");
        
        if ("sliding_window".equals(workloadMode)) {
            // Sliding processing-time window to match contract scenario.
            result = source
                .keyBy(t -> t.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.milliseconds(slideSizeMs)))
                .sum(1)
                .name("SlidingWindowWordCount");
        } else {
            // Stateful count using KeyedProcessFunction with ValueState
            // This uses VoidNamespace and has no Timer overhead
            result = source
                .keyBy(t -> t.f0)
                .process(new StatefulCounter())
                .name("StatefulCounter");
        }
        
        // Metrics sink
        result.addSink(new MetricsSink(outputPath, parallelism, backend, numRecords))
            .name("MetricsSink")
            .setParallelism(1);
        
        // Execute
        System.out.println("=== WordCount Benchmark ===");
        System.out.println("workloadMode: " + workloadMode);
        System.out.println("numKeys: " + numKeys);
        System.out.println("numRecords: " + numRecords);
        System.out.println("keyType: " + keyType);
        System.out.println("skewFactor: " + skewFactor);
        System.out.println("arrivalRate: " + (arrivalRate > 0 ? arrivalRate + " records/s" : "unlimited"));
        if ("sliding_window".equals(workloadMode)) {
            System.out.println("windowSize: " + windowSize + "s");
            System.out.println("slideSize: " + slideSizeMs + "ms");
        }
        System.out.println("parallelism: " + parallelism);
        System.out.println("checkpointing: " + (checkpointInterval > 0 ? checkpointInterval + "ms" : "disabled"));
        System.out.println("============================================================");
        
        env.execute("Stateful WordCount Benchmark");
    }
    
    /**
     * Long-key stateful counter using ValueState<Long> with Long key.
     *
     * <p>Exercises ForL0's LONG_VOID + addAndGetLong fused JNI fast path.
     * No String serialization, no GC from key objects.
     */
    public static class LongKeyStatefulCounter
            extends KeyedProcessFunction<Long, Tuple2<Long, Long>, Tuple2<String, Long>> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> countState;
        private transient LongAddAndGetFastPath countStateFastPath;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("count", Long.class));
            countStateFastPath = resolveFastPath(countState);
        }

        @Override
        public void processElement(
            Tuple2<Long, Long> value,
                Context ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {
            long newCount;
            if (countStateFastPath != null) {
                newCount = countStateFastPath.addAndGet(value.f1);
            } else {
                Long currentCount = countState.value();
                newCount = (currentCount == null) ? value.f1 : currentCount + value.f1;
                countState.update(newCount);
            }
            out.collect(Tuple2.of(String.valueOf(value.f0), newCount));
        }
    }

    @FunctionalInterface
    interface LongAddAndGetFastPath {
        long addAndGet(long delta) throws Exception;
    }

    private static LongAddAndGetFastPath resolveFastPath(ValueState<Long> state) {
        try {
            Method method = state.getClass().getMethod("addAndGetLong", long.class);
            MethodHandle handle = MethodHandles.publicLookup().unreflect(method).bindTo(state);
            return delta -> {
                try {
                    return (long) handle.invokeExact(delta);
                } catch (Throwable throwable) {
                    throw new RuntimeException("ForL0 addAndGetLong fast path failed", throwable);
                }
            };
        } catch (NoSuchMethodException | IllegalAccessException ignored) {
            return null;
        }
    }

    /**
         * Stateful counter using ValueState.
     * 
     * <p>Each record triggers:
     * <ul>
         *   <li>One state read: countState.value()</li>
         *   <li>One state write: countState.update()</li>
     * </ul>
     * 
         * <p>Uses VoidNamespace (default for ValueState), which allows ForL0 to use
         * direct SwissTable access without HashMap overhead.
     */
    public static class StatefulCounter 
            extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {
        
        private static final long serialVersionUID = 1L;
        
        private transient ValueState<Long> countState;
        private transient LongAddAndGetFastPath countStateFastPath;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("count", Long.class));
            countStateFastPath = resolveFastPath(countState);
        }
        
        @Override
        public void processElement(
            Tuple2<String, Long> value,
                Context ctx, 
            Collector<Tuple2<String, Long>> out) throws Exception {
            long newCount;
            if (countStateFastPath != null) {
                newCount = countStateFastPath.addAndGet(value.f1);
            } else {
                Long currentCount = countState.value();
                newCount = (currentCount == null) ? value.f1 : currentCount + value.f1;
                countState.update(newCount);
            }
            out.collect(Tuple2.of(value.f0, newCount));
        }
    }
}
