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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.wordcount.sink.MetricsSink;
import org.apache.flink.benchmark.wordcount.source.SkewedWordSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stateful WordCount Benchmark (State-Backend Focused).
 * 
 * <p>This benchmark measures the performance of Flink StateBackend implementations
 * using a stateful word count workload that maximizes state access overhead.
 * 
 * <p>Key design for StateBackend benchmarking:
 * <ul>
 *   <li>Uses KeyedProcessFunction with ReducingState instead of windows</li>
 *   <li>No Timer overhead - pure state access</li>
 *   <li>Each record triggers one incremental state update</li>
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
        
        // Create skewed key source - outputs Tuple2<key, 1L>
        DataStream<Tuple2<Long, Long>> source = env
            .addSource(new SkewedWordSource(numKeys, numRecords, skewFactor, arrivalRate))
            .name("SkewedKeySource");
        
        // Stateful count using KeyedProcessFunction with ReducingState
        // This keeps the benchmark focused on incremental state updates.
        DataStream<Tuple2<Long, Long>> result = source
            .keyBy(t -> t.f0)
            .process(new StatefulCounter())
            .name("StatefulCounter");
        
        // Metrics sink
        result.addSink(new MetricsSink(outputPath, parallelism, backend, numRecords))
            .name("MetricsSink")
            .setParallelism(1);
        
        // Execute
        System.out.println("=== Stateful WordCount Benchmark (StateBackend Focused) ===");
        System.out.println("numKeys: " + numKeys);
        System.out.println("numRecords: " + numRecords);
        System.out.println("skewFactor: " + skewFactor);
        System.out.println("arrivalRate: " + (arrivalRate > 0 ? arrivalRate + " records/s" : "unlimited"));
        System.out.println("parallelism: " + parallelism);
        System.out.println("checkpointing: " + (checkpointInterval > 0 ? checkpointInterval + "ms" : "disabled"));
        System.out.println("============================================================");
        
        env.execute("Stateful WordCount Benchmark");
    }
    
    /**
     * Stateful counter using ReducingState.
     * 
     * <p>Each record triggers:
     * <ul>
     *   <li>One state update: countState.add()</li>
     * </ul>
     * 
     * <p>The benchmark sink only measures end-to-end throughput, so it does not need
     * the per-record count value. This lets us model a common no-L0 optimization:
     * associative updates that avoid an explicit read-before-write on every record.
     */
    public static class StatefulCounter 
            extends KeyedProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        
        private static final long serialVersionUID = 1L;
        
        private transient ReducingState<Long> countState;

        private static final ReduceFunction<Long> SUM_REDUCER = new ReduceFunction<Long>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) {
                return value1 + value2;
            }
        };
        
        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getReducingState(
                new ReducingStateDescriptor<>("count", SUM_REDUCER, Long.class));
        }
        
        @Override
        public void processElement(
                Tuple2<Long, Long> value, 
                Context ctx, 
                Collector<Tuple2<Long, Long>> out) throws Exception {
            countState.add(value.f1);
            out.collect(value);
        }
    }
}
