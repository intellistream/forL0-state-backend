/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.benchmark.benchset.sink.BlackholeSink;
import org.apache.flink.benchmark.benchset.source.LongEventSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Realtime Recommendation Benchmark - User profile for personalization.
 *
 * <p>Real-world scenario: E-commerce recommendation engine.
 * Core states for user interest tracking:
 * - viewCount: Total page views (engagement level)
 * - clickCount: Total clicks (interest signal)
 * - purchaseCount: Total purchases (conversion)
 *
 * <p>3 ValueStates, 6 operations per record (3 read + 3 write)
 */
public class RealtimeRecommendationBenchmark {

    public static void run(
            StreamExecutionEnvironment env,
            int numKeys,
            long numRecords,
            double skewFactor,
            int parallelism,
            String backend) throws Exception {

        DataStream<Long> source = env
                .addSource(new LongEventSource(numKeys, numRecords, skewFactor))
                .setParallelism(parallelism)
                .name("UserEventSource");

        DataStream<Long> result = source
                .keyBy(userId -> userId)
                .process(new RecommendationFunction())
                .setParallelism(parallelism)
                .name("RecommendationEngine");

        result.addSink(new BlackholeSink<>())
                .setParallelism(parallelism)
                .name("Blackhole");

        env.execute("Realtime Recommendation Benchmark");
    }

    public static class RecommendationFunction
            extends KeyedProcessFunction<Long, Long, Long> {

        private static final long serialVersionUID = 1L;

        // 3 core states for user engagement tracking
        private transient ValueState<Long> viewCount;
        private transient ValueState<Long> clickCount;
        private transient ValueState<Long> purchaseCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            viewCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("viewCount", Long.class));
            clickCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("clickCount", Long.class));
            purchaseCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("purchaseCount", Long.class));
        }

        @Override
        public void processElement(Long userId, Context ctx, Collector<Long> out) throws Exception {
            // Read 3 states
            Long views = viewCount.value();
            Long clicks = clickCount.value();
            Long purchases = purchaseCount.value();

            // Simple increments
            long newViews = views == null ? 1L : views + 1;
            long newClicks = clicks == null ? 1L : clicks + 1;
            long newPurchases = purchases == null ? 1L : purchases + 1;

            // Write 3 states
            viewCount.update(newViews);
            clickCount.update(newClicks);
            purchaseCount.update(newPurchases);

            out.collect(newViews);
        }
    }
}
