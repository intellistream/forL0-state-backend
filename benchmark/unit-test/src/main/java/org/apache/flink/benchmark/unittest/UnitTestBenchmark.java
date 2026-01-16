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

package org.apache.flink.benchmark.unittest;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Unit Test Benchmark for ForL0 StateBackend.
 * 
 * <p>A minimal and fully controllable benchmark for analyzing StateBackend behavior.
 * 
 * <p>Features:
 * <ul>
 *   <li>Single parallelism (parallelism=1, maxParallelism=1)</li>
 *   <li>Single KeyGroup for precise state access analysis</li>
 *   <li>Controllable state count, size, and key distribution</li>
 *   <li>Controllable arrival rate</li>
 * </ul>
 * 
 * <p>Job topology:
 * <pre>
 * ControlledSource -&gt; StatefulMap (ValueState: GET+PUT) -&gt; DiscardSink
 * </pre>
 * 
 * <p>Usage:
 * <pre>
 * flink run unit-test-benchmark.jar \
 *   --numKeys 1000 \
 *   --stateSize 100 \
 *   --numOperations 1000000 \
 *   --keyDistribution UNIFORM \
 *   --zipfExponent 1.1 \
 *   --arrivalRate 0
 * </pre>
 */
public class UnitTestBenchmark {

    public static void main(String[] args) throws Exception {
        // Parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        int numKeys = params.getInt("numKeys", 1000);
        int stateSize = params.getInt("stateSize", 100);
        long numOperations = params.getLong("numOperations", 1_000_000L);
        double zipfExponent = params.getDouble("zipfExponent", 0);
        int arrivalRate = params.getInt("arrivalRate", 0);

        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Single parallelism, single KeyGroup
        env.setParallelism(1);
        env.setMaxParallelism(1);

        // Print configuration
        System.out.println("=== Unit Test Benchmark ===");
        System.out.println("numKeys: " + numKeys);
        System.out.println("stateSize: " + stateSize + " bytes");
        System.out.println("numOperations: " + numOperations);
        System.out.println("zipfExponent: " + zipfExponent + " (raw value)");
        System.out.println("keyDistribution: " + (zipfExponent > 0 ? "ZIPF (s=" + zipfExponent + ")" : "UNIFORM"));
        System.out.println("arrivalRate: " + (arrivalRate > 0 ? arrivalRate + " ops/sec" : "unlimited"));
        System.out.println("===========================");

        // Build job topology
        env.addSource(new ControlledSource(numKeys, numOperations, zipfExponent, arrivalRate))
                .name("ControlledSource")
                .keyBy(keyIndex -> keyIndex)
                .map(new StatefulMapFunction(stateSize))
                .name("StatefulMap")
                .addSink(new DiscardSink())
                .name("DiscardSink");

        // Execute
        env.execute("Unit Test Benchmark");
    }
}
