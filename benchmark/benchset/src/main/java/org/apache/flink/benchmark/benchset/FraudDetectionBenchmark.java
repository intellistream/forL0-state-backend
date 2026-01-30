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
 * Fraud Detection Benchmark - Real-time transaction risk assessment.
 *
 * <p>Real-world scenario: Credit card fraud detection.
 * Core states for velocity-based fraud detection:
 * - txnCount: Transaction count in current window (velocity check)
 * - txnSum: Total amount in window (amount spike detection)
 * - lastTxnTime: Time of last transaction (rapid-fire detection)
 * - riskScore: Cumulative risk score
 *
 * <p>4 ValueStates, 8 operations per record (4 read + 4 write)
 */
public class FraudDetectionBenchmark {

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
                .name("TransactionSource");

        DataStream<Long> result = source
                .keyBy(userId -> userId)
                .process(new FraudDetectionFunction())
                .setParallelism(parallelism)
                .name("FraudDetector");

        result.addSink(new BlackholeSink<>())
                .setParallelism(parallelism)
                .name("Blackhole");

        env.execute("Fraud Detection Benchmark");
    }

    public static class FraudDetectionFunction
            extends KeyedProcessFunction<Long, Long, Long> {

        private static final long serialVersionUID = 1L;

        // 4 core states for fraud detection
        private transient ValueState<Long> txnCount;      // Transaction count
        private transient ValueState<Long> txnSum;        // Total amount
        private transient ValueState<Long> lastTxnTime;   // Last transaction time
        private transient ValueState<Long> riskScore;     // Cumulative risk

        @Override
        public void open(Configuration parameters) throws Exception {
            txnCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("txnCount", Long.class));
            txnSum = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("txnSum", Long.class));
            lastTxnTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastTxnTime", Long.class));
            riskScore = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("riskScore", Long.class));
        }

        @Override
        public void processElement(Long userId, Context ctx, Collector<Long> out) throws Exception {
            // Read 4 states
            Long count = txnCount.value();
            Long sum = txnSum.value();
            Long lastTime = lastTxnTime.value();
            Long risk = riskScore.value();

            // Simple increments
            long newCount = count == null ? 1L : count + 1;
            long newSum = sum == null ? 1L : sum + 1;
            long newTime = lastTime == null ? 1L : lastTime + 1;
            long newRisk = risk == null ? 1L : risk + 1;

            // Write 4 states
            txnCount.update(newCount);
            txnSum.update(newSum);
            lastTxnTime.update(newTime);
            riskScore.update(newRisk);

            out.collect(newRisk);
        }
    }
}
