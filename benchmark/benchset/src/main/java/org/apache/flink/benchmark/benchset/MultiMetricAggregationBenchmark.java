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
 * Multi-Metric Aggregation Benchmark - APM metrics aggregation.
 *
 * <p>Real-world scenario: System monitoring/APM platform.
 * Core states for request tracking:
 * - requestCount: Total requests
 * - errorCount: Error count for error rate
 * - latencySum: Sum for average latency
 * - latencyMax: Peak latency
 *
 * <p>4 ValueStates, 8 operations per record (4 read + 4 write)
 */
public class MultiMetricAggregationBenchmark {

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
                .name("MetricSource");

        DataStream<Long> result = source
                .keyBy(instanceId -> instanceId)
                .process(new MetricAggregationFunction())
                .setParallelism(parallelism)
                .name("MetricAggregation");

        result.addSink(new BlackholeSink<>())
                .setParallelism(parallelism)
                .name("Blackhole");

        env.execute("Multi-Metric Aggregation Benchmark");
    }

    public static class MetricAggregationFunction
            extends KeyedProcessFunction<Long, Long, Long> {

        private static final long serialVersionUID = 1L;

        // 4 core states for APM metrics
        private transient ValueState<Long> requestCount;
        private transient ValueState<Long> errorCount;
        private transient ValueState<Long> latencySum;
        private transient ValueState<Long> latencyMax;

        @Override
        public void open(Configuration parameters) throws Exception {
            requestCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("requestCount", Long.class));
            errorCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("errorCount", Long.class));
            latencySum = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latencySum", Long.class));
            latencyMax = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latencyMax", Long.class));
        }

        @Override
        public void processElement(Long instanceId, Context ctx, Collector<Long> out) throws Exception {
            // Read 4 states
            Long reqCnt = requestCount.value();
            Long errCnt = errorCount.value();
            Long latSum = latencySum.value();
            Long latMax = latencyMax.value();

            // Simple increments
            long newReqCnt = reqCnt == null ? 1L : reqCnt + 1;
            long newErrCnt = errCnt == null ? 1L : errCnt + 1;
            long newLatSum = latSum == null ? 1L : latSum + 1;
            long newLatMax = latMax == null ? 1L : latMax + 1;

            // Write 4 states
            requestCount.update(newReqCnt);
            errorCount.update(newErrCnt);
            latencySum.update(newLatSum);
            latencyMax.update(newLatMax);

            out.collect(newReqCnt);
        }
    }
}
