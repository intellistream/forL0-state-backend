/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.benchmark.benchset.sink.BlackholeSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.SplittableRandom;

/**
 * Multi-Stream Join Benchmark - Order-Payment matching.
 *
 * <p>Real-world scenario: E-commerce order-payment reconciliation.
 * Two streams arrive asynchronously, need to be matched by userId.
 * Core states:
 * - orderCount: Number of orders received
 * - paymentCount: Number of payments received
 * - matchCount: Successfully matched pairs
 * - pendingCount: Items awaiting match
 *
 * <p>4 ValueStates, 8 operations per record (4 read + 4 write)
 */
public class MultiStreamJoinBenchmark {

    public static void run(
            StreamExecutionEnvironment env,
            int numKeys,
            long numRecords,
            double skewFactor,
            int parallelism,
            String backend) throws Exception {

        long recordsPerSource = numRecords / 2;

        // Order stream
        DataStream<Tuple2<Long, Long>> orderStream = env
                .addSource(new OrderSource(numKeys, recordsPerSource, skewFactor, 17))
                .setParallelism(parallelism)
                .name("OrderSource");

        // Payment stream
        DataStream<Tuple2<Long, Long>> paymentStream = env
                .addSource(new OrderSource(numKeys, recordsPerSource, skewFactor, 31))
                .setParallelism(parallelism)
                .name("PaymentSource");

        DataStream<Long> result = orderStream
                .keyBy(t -> t.f0)
                .connect(paymentStream.keyBy(t -> t.f0))
                .process(new JoinFunction())
                .setParallelism(parallelism)
                .name("StreamJoin");

        result.addSink(new BlackholeSink<>())
                .setParallelism(parallelism)
                .name("Blackhole");

        env.execute("Multi-Stream Join Benchmark");
    }

    public static class OrderSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private static final long serialVersionUID = 1L;

        private final int numKeys;
        private final long numRecords;
        private final double skewFactor;
        private final long seed;
        private volatile boolean running = true;

        public OrderSource(int numKeys, long numRecords, double skewFactor, long seed) {
            this.numKeys = numKeys;
            this.numRecords = numRecords;
            this.skewFactor = skewFactor;
            this.seed = seed;
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

            long myRecords = numRecords / parallelism;
            if (subtaskIndex == parallelism - 1) {
                myRecords = numRecords - myRecords * (parallelism - 1);
            }

            SplittableRandom random = new SplittableRandom(subtaskIndex * seed);

            long count = 0;
            final Object lock = ctx.getCheckpointLock();

            while (running && count < myRecords) {
                long userId = random.nextLong(numKeys);
                long eventId = subtaskIndex * 1_000_000_000L + count;

                synchronized (lock) {
                    ctx.collect(Tuple2.of(userId, eventId));
                }
                count++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class JoinFunction
            extends KeyedCoProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Long> {

        private static final long serialVersionUID = 1L;

        // 4 core states for order-payment matching
        private transient ValueState<Long> orderCount;
        private transient ValueState<Long> paymentCount;
        private transient ValueState<Long> matchCount;
        private transient ValueState<Long> pendingCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("orderCount", Long.class));
            paymentCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("paymentCount", Long.class));
            matchCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("matchCount", Long.class));
            pendingCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pendingCount", Long.class));
        }

        @Override
        public void processElement1(Tuple2<Long, Long> order, Context ctx, Collector<Long> out) throws Exception {
            processEvent(out);
        }

        @Override
        public void processElement2(Tuple2<Long, Long> payment, Context ctx, Collector<Long> out) throws Exception {
            processEvent(out);
        }

        private void processEvent(Collector<Long> out) throws Exception {
            // Read 4 states
            Long ordCnt = orderCount.value();
            Long payCnt = paymentCount.value();
            Long matCnt = matchCount.value();
            Long pendCnt = pendingCount.value();

            // Simple increments
            long newOrdCnt = ordCnt == null ? 1L : ordCnt + 1;
            long newPayCnt = payCnt == null ? 1L : payCnt + 1;
            long newMatCnt = matCnt == null ? 1L : matCnt + 1;
            long newPendCnt = pendCnt == null ? 1L : pendCnt + 1;

            // Write 4 states
            orderCount.update(newOrdCnt);
            paymentCount.update(newPayCnt);
            matchCount.update(newMatCnt);
            pendingCount.update(newPendCnt);

            out.collect(newMatCnt);
        }
    }
}
