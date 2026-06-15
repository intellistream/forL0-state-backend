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

package org.apache.flink.benchmark.wordcount.sink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * A sink that collects metrics during benchmark execution.
 * 
 * <p>Metrics collected:
 * <ul>
 *   <li>Throughput: records per second</li>
 *   <li>Throughput per core</li>
 *   <li>Total records processed</li>
 *   <li>Total execution time</li>
 * </ul>
 * 
 * <p>At the end of execution, metrics are printed to stdout and optionally
 * saved to a JSON file.
 */
public class MetricsSink extends RichSinkFunction<Tuple2<String, Long>> {
    
    private static final long serialVersionUID = 1L;
    
    private final String outputPath;
    private final int parallelism;
    private final String backend;
    /** Total input records (from Source), used for throughput calculation */
    private final long numRecords;
    
    // Metrics collection
    private transient long startTime;
    private transient long sinkRecordCount;
    
    public MetricsSink(String outputPath, int parallelism, String backend, long numRecords) {
        this.outputPath = outputPath;
        this.parallelism = parallelism;
        this.backend = backend;
        this.numRecords = numRecords;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        startTime = System.currentTimeMillis();
        sinkRecordCount = 0;
    }
    
    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        sinkRecordCount++;
    }
    
    @Override
    public void close() throws Exception {
        long endTime = System.currentTimeMillis();
        long totalTimeMs = endTime - startTime;
        double totalTimeSeconds = totalTimeMs / 1000.0;
        
        // Calculate metrics using INPUT records (from Source), not Sink records
        // Sink records are window aggregation outputs, not the actual input throughput
        double throughput = numRecords / totalTimeSeconds;
        double throughputPerCore = throughput / parallelism;
        
        // Build result map
        Map<String, Object> result = new HashMap<>();
        result.put("benchmark", "wordcount");
        result.put("backend", backend);
        result.put("total_records", numRecords);
        result.put("total_time_seconds", totalTimeSeconds);
        result.put("throughput", throughput);
        result.put("throughput_per_core", throughputPerCore);
        result.put("parallelism", parallelism);
        result.put("sink_records", sinkRecordCount);
        
        // Print results
        System.out.println("\n========================================");
        System.out.println("       BENCHMARK RESULTS");
        System.out.println("========================================");
        System.out.printf("Backend:            %s%n", backend);
        System.out.printf("Total records:      %,d%n", numRecords);
        System.out.printf("Total time:         %.2f seconds%n", totalTimeSeconds);
        System.out.printf("Throughput:         %,.0f records/s%n", throughput);
        System.out.printf("Throughput/core:    %,.0f records/s%n", throughputPerCore);
        System.out.printf("Parallelism:        %d%n", parallelism);
        System.out.println("========================================\n");
        
        // Output JSON for machine parsing
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String jsonResult = gson.toJson(result);
        
        System.out.println("JSON_RESULT_START");
        System.out.println(jsonResult);
        System.out.println("JSON_RESULT_END");
        
        // Save to file if path specified
        if (outputPath != null && !outputPath.isEmpty()) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
                writer.println(jsonResult);
            }
            System.out.println("Results saved to: " + outputPath);
        }
        
        super.close();
    }
}
