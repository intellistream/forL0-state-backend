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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.wordcount.sink.MetricsSink;
import org.apache.flink.benchmark.wordcount.source.SkewedWordSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Sliding Window WordCount Benchmark.
 * 
 * <p>This benchmark measures the performance of Flink StateBackend implementations
 * using a sliding window word count workload with skewed data distribution.
 * 
 * <p>Metrics collected (aligned with NexMark):
 * <ul>
 *   <li>Throughput (records/s)</li>
 *   <li>Latency (P50, P95, P99, Max)</li>
 *   <li>Throughput per core</li>
 * </ul>
 * 
 * <p>Usage:
 * <pre>
 * flink run wordcount-benchmark.jar \
 *   --numKeys 1000000 \
 *   --numRecords 100000000 \
 *   --skewFactor 1.1 \
 *   --windowSize 60 \
 *   --slideSize 10 \
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
        int arrivalRate = params.getInt("arrivalRate", 230_000);  // records/s, 0 = unlimited
        int windowSizeMillis = params.getInt("windowSize", 5000);  // milliseconds
        int slideSizeMillis = params.getInt("slideSize", 200);    // milliseconds
        int parallelism = params.getInt("parallelism", 8);
        String outputPath = params.get("output", null);
        String latencyDir = params.get("latencyDir", System.getProperty("java.io.tmpdir"));
        String backend = params.get("backend", "unknown");
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        
        // Enable checkpointing if specified
        int checkpointInterval = params.getInt("checkpointInterval", 10000);
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }
        
        // Create skewed word source
        DataStream<Tuple2<String, Long>> source = env
            .addSource(new SkewedWordSource(numKeys, numRecords, skewFactor, arrivalRate))
            .name("SkewedWordSource")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> event.f1)
            )
            .name("Watermarks");
        
        // Sliding window word count
        DataStream<Tuple3<String, Long, Long>> result = source
            .keyBy(t -> t.f0)
            .window(SlidingEventTimeWindows.of(
                Duration.ofMillis(windowSizeMillis),
                Duration.ofMillis(slideSizeMillis)
            ))
            .aggregate(
                new CountAggregator(),
                new WindowResultFunction()
            )
            .name("SlidingWindowCount");
        
        // Metrics sink - pass numRecords for correct throughput calculation
        result.addSink(new MetricsSink(outputPath, parallelism, latencyDir, backend, numRecords))
            .name("MetricsSink")
            .setParallelism(1);
        
        // Execute
        System.out.println("=== WordCount Benchmark ===");
        System.out.println("numKeys: " + numKeys);
        System.out.println("numRecords: " + numRecords);
        System.out.println("skewFactor: " + skewFactor);
        System.out.println("arrivalRate: " + (arrivalRate > 0 ? arrivalRate + " records/s" : "unlimited"));
        System.out.println("windowSize: " + windowSizeMillis + "ms");
        System.out.println("slideSize: " + slideSizeMillis + "ms");
        System.out.println("parallelism: " + parallelism);
        System.out.println("===========================");
        
        env.execute("WordCount Benchmark");
    }
    
    /**
     * Aggregator that counts occurrences and tracks the latest record timestamp.
     * Accumulator: (count, latestTimestamp)
     */
    public static class CountAggregator 
            implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }
        
        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            // value.f1 is the processing time when the record was emitted from source
            return Tuple2.of(accumulator.f0 + 1, Math.max(accumulator.f1, value.f1));
        }
        
        @Override
        public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
            return accumulator;
        }
        
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return Tuple2.of(a.f0 + b.f0, Math.max(a.f1, b.f1));
        }
    }
    
    /**
     * Window function that produces (word, count, sourceTimestamp) tuples.
     * sourceTimestamp is the latest record's emit time from source, used for end-to-end latency.
     */
    public static class WindowResultFunction 
            extends ProcessWindowFunction<Tuple2<Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow> {
        
        @Override
        public void process(String key,
                          Context context,
                          Iterable<Tuple2<Long, Long>> results,
                          Collector<Tuple3<String, Long, Long>> out) {
            Tuple2<Long, Long> result = results.iterator().next();
            // result.f0 = count, result.f1 = latest source timestamp
            out.collect(Tuple3.of(key, result.f0, result.f1));
        }
    }
}
