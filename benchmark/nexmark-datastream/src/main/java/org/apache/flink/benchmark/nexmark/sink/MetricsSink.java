/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.sink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metrics sink for Nexmark benchmark.
 * Collects throughput and timing metrics and outputs JSON results.
 * 
 * <p>This sink should run with parallelism=1 to aggregate all results.
 * Results are output with JSON_RESULT_START/END markers for parsing by the runner script.
 */
public class MetricsSink<T> extends RichSinkFunction<T> {
    private static final long serialVersionUID = 1L;

    private final String queryName;
    private final String backend;
    private final long numEvents;
    private final int parallelism;

    private transient long startTime;
    private transient long recordCount;
    private transient Gson gson;

    public MetricsSink(String queryName, String backend, long numEvents, int parallelism) {
        this.queryName = queryName;
        this.backend = backend;
        this.numEvents = numEvents;
        this.parallelism = parallelism;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.startTime = System.currentTimeMillis();
        this.recordCount = 0;
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        recordCount++;
    }

    @Override
    public void close() throws Exception {
        long endTime = System.currentTimeMillis();
        double totalTimeSeconds = (endTime - startTime) / 1000.0;
        
        // Calculate throughput using input events (not sink records)
        double throughput = numEvents / totalTimeSeconds;
        double throughputPerCore = throughput / parallelism;
        
        // Build result map (ordered for consistent output)
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("benchmark", "nexmark");
        result.put("query", queryName);
        result.put("backend", backend);
        result.put("total_time_seconds", round(totalTimeSeconds, 3));
        result.put("total_events", numEvents);
        result.put("sink_records", recordCount);
        result.put("parallelism", parallelism);
        result.put("throughput", round(throughput, 2));
        result.put("throughput_per_core", round(throughputPerCore, 2));
        
        // Latency placeholders (can be extended with latency tracking)
        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("p50", null);
        latency.put("p95", null);
        latency.put("p99", null);
        latency.put("max", null);
        result.put("latency_ms", latency);
        
        // Print human-readable summary
        System.out.println();
        System.out.println("============================================================");
        System.out.println("                   Nexmark Benchmark Results                 ");
        System.out.println("============================================================");
        System.out.printf("Query:               %s%n", queryName);
        System.out.printf("Backend:             %s%n", backend);
        System.out.printf("Total Time:          %.3f seconds%n", totalTimeSeconds);
        System.out.printf("Total Events:        %,d%n", numEvents);
        System.out.printf("Sink Records:        %,d%n", recordCount);
        System.out.printf("Parallelism:         %d%n", parallelism);
        System.out.printf("Throughput:          %,.2f events/sec%n", throughput);
        System.out.printf("Throughput/Core:     %,.2f events/sec%n", throughputPerCore);
        System.out.println("============================================================");
        System.out.println();
        
        // Print JSON result with markers (for script parsing)
        System.out.println("JSON_RESULT_START");
        System.out.println(gson.toJson(result));
        System.out.println("JSON_RESULT_END");
        
        super.close();
    }

    private static double round(double value, int decimals) {
        double scale = Math.pow(10, decimals);
        return Math.round(value * scale) / scale;
    }
}
