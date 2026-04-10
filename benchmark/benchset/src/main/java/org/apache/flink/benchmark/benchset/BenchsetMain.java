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

package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Main entry point for ForL0 Benchmark Set v3.
 *
 * <p>This benchmark set is designed to maximize state access density with
 * realistic business scenarios. All benchmarks use Long keys to trigger
 * SwissTableLong specialization.
 *
 * <p>Available benchmarks:
 * <ul>
 *   <li>fraud: Fraud Detection - 7 states, 23 ops/record</li>
 *   <li>recommend: Realtime Recommendation - 6 states, 18 ops/record</li>
 *   <li>metric: Multi-Metric Aggregation - 10 states, 20 ops/record</li>
 *   <li>session: Session Sequence Analysis - 5 states, 15 ops/record</li>
 *   <li>join: Multi-Stream Join - 8 states, 25 ops/record</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * flink run benchset.jar \
 *   --benchmark fraud \
 *   --numKeys 5000000 \
 *   --numRecords 100000000 \
 *   --parallelism 8
 * </pre>
 */
public class BenchsetMain {

    public static void main(String[] args) throws Exception {
        // Parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        String benchmark = params.get("benchmark", "fraud");
        int numKeys = params.getInt("numKeys", 1_000_000);
        long numRecords = params.getLong("numRecords", 100_000_000L);
        double skewFactor = params.getDouble("skewFactor", 0);
        int batchSize = params.getInt("batchSize", 10);
        int parallelism = params.getInt("parallelism", 8);
        int checkpointInterval = params.getInt("checkpointInterval", 0);
        String backend = params.get("backend", "unknown");

        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // Disable checkpointing by default
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }

        // Print configuration
        System.out.println("=== ForL0 Benchset v3 ===");
        System.out.println("Benchmark: " + benchmark);
        System.out.println("Backend: " + backend);
        System.out.println("numKeys: " + numKeys);
        System.out.println("numRecords: " + numRecords);
        System.out.println("skewFactor: " + skewFactor);
        System.out.println("batchSize: " + batchSize);
        System.out.println("parallelism: " + parallelism);
        System.out.println("checkpointing: " + (checkpointInterval > 0 ? checkpointInterval + "ms" : "disabled"));
        System.out.println("=========================");

        // Run selected benchmark
        switch (benchmark.toLowerCase()) {
            case "fraud":
                FraudDetectionBenchmark.run(env, numKeys, numRecords, skewFactor, parallelism, backend);
                break;
            case "recommend":
                RealtimeRecommendationBenchmark.run(env, numKeys, numRecords, skewFactor, parallelism, backend);
                break;
            case "metric":
                MultiMetricAggregationBenchmark.run(env, numKeys, numRecords, skewFactor, parallelism, backend);
                break;
            case "session":
                SessionSequenceBenchmark.run(env, numKeys, numRecords, skewFactor, batchSize, parallelism, backend);
                break;
            case "join":
                MultiStreamJoinBenchmark.run(env, numKeys, numRecords, skewFactor, parallelism, backend);
                break;
            default:
                System.err.println("Unknown benchmark: " + benchmark);
                System.err.println("Available: fraud, recommend, metric, session, join");
                System.exit(1);
        }
    }
}
