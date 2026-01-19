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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.wordcount.sink.MetricsSink;
import org.apache.flink.benchmark.wordcount.source.SkewedWordSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;

import java.time.Duration;

/**
 * Sliding Window WordCount Benchmark.
 * 
 * <p>This benchmark measures the performance of Flink StateBackend implementations
 * using a sliding window word count workload with skewed data distribution.
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
 *   --windowSize 5000 \
 *   --slideSize 200 \
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
        String backend = params.get("backend", "unknown");
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        
        // Enable checkpointing if specified
        int checkpointInterval = params.getInt("checkpointInterval", 10000);
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }
        
        // Create skewed key source - outputs Tuple2<key, 1L>
        DataStream<Tuple2<Long, Long>> source = env
            .addSource(new SkewedWordSource(numKeys, numRecords, skewFactor, arrivalRate))
            .name("SkewedKeySource");
        
        // Sliding window key count
        DataStream<Tuple2<Long, Long>> result = source
            .keyBy(t -> t.f0)
            .window(SlidingProcessingTimeWindows.of(
                Duration.ofMillis(windowSizeMillis),
                Duration.ofMillis(slideSizeMillis)
            ))
            .sum(1)
            .name("SlidingWindowCount");
        
        // Metrics sink
        result.addSink(new MetricsSink(outputPath, parallelism, backend, numRecords))
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
}
