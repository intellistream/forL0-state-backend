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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A sink that collects metrics during benchmark execution.
 * 
 * <p>Metrics collected (aligned with NexMark):
 * <ul>
 *   <li>Throughput: records per second</li>
 *   <li>Latency: P50, P95, P99, Max (processing latency)</li>
 *   <li>Throughput per core</li>
 *   <li>Total records processed</li>
 *   <li>Total execution time</li>
 * </ul>
 * 
 * <p>Latency samples are saved to a CSV file for CDF plotting.
 * 
 * <p>At the end of execution, metrics are printed to stdout and optionally
 * saved to a JSON file.
 */
public class MetricsSink extends RichSinkFunction<Tuple3<String, Long, Long>> {
    
    private static final long serialVersionUID = 1L;
    
    private final String outputPath;
    private final int parallelism;
    private final String latencyDir;
    private final String backend;
    
    // Metrics collection
    private transient long startTime;
    private transient long recordCount;
    private transient List<Long> latencies;
    
    // For throughput calculation
    private transient long lastReportTime;
    private transient long lastReportCount;
    
    public MetricsSink(String outputPath, int parallelism, String latencyDir, String backend) {
        this.outputPath = outputPath;
        this.parallelism = parallelism;
        this.latencyDir = latencyDir;
        this.backend = backend;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        startTime = System.currentTimeMillis();
        recordCount = 0;
        latencies = new ArrayList<>();
        lastReportTime = startTime;
        lastReportCount = 0;
    }
    
    @Override
    public void invoke(Tuple3<String, Long, Long> value, Context context) throws Exception {
        recordCount++;
        
        // f2 is the source emit timestamp, calculate end-to-end latency
        long currentTime = System.currentTimeMillis();
        long latency = currentTime - value.f2;
        
        // Sample latencies to avoid memory issues with large datasets
        // Keep every 100th latency measurement for better resolution
        if (recordCount % 100 == 0 && latency >= 0) {
            latencies.add(latency);
        }
        
        // Progress report every 10 seconds
        if (currentTime - lastReportTime >= 10_000) {
            long elapsedSinceLastReport = currentTime - lastReportTime;
            long recordsSinceLastReport = recordCount - lastReportCount;
            double currentThroughput = recordsSinceLastReport * 1000.0 / elapsedSinceLastReport;
            
            System.out.printf("[Metrics] Records: %,d, Current throughput: %,.0f records/s%n",
                recordCount, currentThroughput);
            
            lastReportTime = currentTime;
            lastReportCount = recordCount;
        }
    }
    
    @Override
    public void close() throws Exception {
        long endTime = System.currentTimeMillis();
        long totalTimeMs = endTime - startTime;
        double totalTimeSeconds = totalTimeMs / 1000.0;
        
        // Calculate metrics
        double throughput = recordCount / totalTimeSeconds;
        double throughputPerCore = throughput / parallelism;
        
        // Calculate latency percentiles
        Collections.sort(latencies);
        long p50 = getPercentile(latencies, 50);
        long p95 = getPercentile(latencies, 95);
        long p99 = getPercentile(latencies, 99);
        long maxLatency = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);
        
        // Save latency samples to CSV for CDF plotting
        saveLatencySamples();
        
        // Build result map
        Map<String, Object> result = new HashMap<>();
        result.put("benchmark", "wordcount");
        result.put("total_records", recordCount);
        result.put("total_time_seconds", totalTimeSeconds);
        result.put("throughput", throughput);
        result.put("throughput_per_core", throughputPerCore);
        result.put("parallelism", parallelism);
        result.put("latency_samples_count", latencies.size());
        
        Map<String, Long> latencyMap = new HashMap<>();
        latencyMap.put("p50", p50);
        latencyMap.put("p95", p95);
        latencyMap.put("p99", p99);
        latencyMap.put("max", maxLatency);
        result.put("latency_ms", latencyMap);
        
        // Print results
        System.out.println("\n========================================");
        System.out.println("       BENCHMARK RESULTS");
        System.out.println("========================================");
        System.out.printf("Total records:      %,d%n", recordCount);
        System.out.printf("Total time:         %.2f seconds%n", totalTimeSeconds);
        System.out.printf("Throughput:         %,.0f records/s%n", throughput);
        System.out.printf("Throughput/core:    %,.0f records/s%n", throughputPerCore);
        System.out.printf("Parallelism:        %d%n", parallelism);
        System.out.println("----------------------------------------");
        System.out.println("Latency:");
        System.out.printf("  P50:              %d ms%n", p50);
        System.out.printf("  P95:              %d ms%n", p95);
        System.out.printf("  P99:              %d ms%n", p99);
        System.out.printf("  Max:              %d ms%n", maxLatency);
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
    
    /**
     * Save latency samples to CSV file for CDF plotting.
     * File is saved to the latency samples directory with timestamp.
     */
    private void saveLatencySamples() {
        if (latencies.isEmpty()) {
            System.out.println("[Metrics] No latency samples to save");
            return;
        }
        
        try {
            // Create directory if not exists
            Path dir = Paths.get(latencyDir);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            
            String timestamp = String.valueOf(System.currentTimeMillis());
            String filename = String.format("latency_samples_%s_%s.csv", backend, timestamp);
            Path filepath = dir.resolve(filename);
            
            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(filepath))) {
                writer.println("latency_ms");
                for (Long latency : latencies) {
                    writer.println(latency);
                }
            }
            
            System.out.println("[Metrics] Latency samples saved to: " + filepath);
            System.out.println("LATENCY_SAMPLES_FILE:" + filepath.toAbsolutePath());
        } catch (Exception e) {
            System.err.println("[Metrics] Failed to save latency samples: " + e.getMessage());
        }
    }
    
    private long getPercentile(List<Long> sortedList, int percentile) {
        if (sortedList.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil(percentile / 100.0 * sortedList.size()) - 1;
        index = Math.max(0, Math.min(index, sortedList.size() - 1));
        return sortedList.get(index);
    }
}
