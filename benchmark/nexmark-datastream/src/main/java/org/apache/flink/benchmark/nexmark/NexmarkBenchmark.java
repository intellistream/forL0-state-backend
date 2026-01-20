/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.query.*;
import org.apache.flink.benchmark.nexmark.source.NexmarkSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Main entry point for Nexmark DataStream Benchmark.
 * 
 * <p>This implementation is aligned with the original Nexmark benchmark but uses
 * DataStream API instead of SQL for more explicit state access patterns.
 * 
 * <p>Supported queries: q4, q5, q7, q8, q9, q11, q12, q18, q19, q20
 * 
 * <p>Usage:
 * <pre>
 * flink run nexmark-datastream.jar \
 *   --query q5 \
 *   --numEvents 10000000 \
 *   --tps 500000 \
 *   --parallelism 4 \
 *   --backend forl0 \
 *   --personProportion 1 \
 *   --auctionProportion 3 \
 *   --bidProportion 46
 * </pre>
 */
public class NexmarkBenchmark {

    public static void main(String[] args) throws Exception {
        // Parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        
        String query = params.get("query", "q5");
        long numEvents = params.getLong("numEvents", 10_000_000L);
        long tps = params.getLong("tps", 0); // 0 = unlimited
        int parallelism = params.getInt("parallelism", 4);
        String backend = params.get("backend", "unknown");
        long checkpointInterval = params.getLong("checkpointInterval", 0);
        
        // Event proportions (Nexmark default: 1:3:46)
        int personProportion = params.getInt("personProportion", 
                NexmarkSource.DEFAULT_PERSON_PROPORTION);
        int auctionProportion = params.getInt("auctionProportion", 
                NexmarkSource.DEFAULT_AUCTION_PROPORTION);
        int bidProportion = params.getInt("bidProportion", 
                NexmarkSource.DEFAULT_BID_PROPORTION);

        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        
        // Enable checkpointing if configured
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }

        // Create event source with configurable proportions
        DataStream<Event> events = env
                .addSource(new NexmarkSource(numEvents, tps, 
                        personProportion, auctionProportion, bidProportion))
                .name("NexmarkSource");

        // Run the specified query
        runQuery(env, events, query, backend, numEvents, parallelism);

        // Execute
        env.execute("Nexmark " + query.toUpperCase() + " Benchmark");
    }

    /**
     * Run the specified Nexmark query.
     */
    private static void runQuery(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String query,
            String backend,
            long numEvents,
            int parallelism) {
        
        switch (query.toLowerCase()) {
            case "q4":
                Query4.run(env, events, backend, numEvents, parallelism);
                break;
            case "q5":
                Query5.run(env, events, backend, numEvents, parallelism);
                break;
            case "q7":
                Query7.run(env, events, backend, numEvents, parallelism);
                break;
            case "q8":
                Query8.run(env, events, backend, numEvents, parallelism);
                break;
            case "q9":
                Query9.run(env, events, backend, numEvents, parallelism);
                break;
            case "q11":
                Query11.run(env, events, backend, numEvents, parallelism);
                break;
            case "q12":
                Query12.run(env, events, backend, numEvents, parallelism);
                break;
            case "q18":
                Query18.run(env, events, backend, numEvents, parallelism);
                break;
            case "q19":
                Query19.run(env, events, backend, numEvents, parallelism);
                break;
            case "q20":
                Query20.run(env, events, backend, numEvents, parallelism);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown query: " + query + 
                        ". Supported: q4, q5, q7, q8, q9, q11, q12, q18, q19, q20");
        }
    }
}
