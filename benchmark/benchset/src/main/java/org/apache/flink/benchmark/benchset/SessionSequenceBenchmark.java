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
 * Session Sequence Benchmark - Web session funnel analysis.
 *
 * <p>Real-world scenario: E-commerce session tracking.
 * Core states for conversion funnel:
 * - eventCount: Total events in session
 * - funnelStage: Current stage (0=landing, 1=browse, 2=cart, 3=checkout, 4=purchase)
 * - lastEventTime: For session timeout detection
 * - conversionValue: Accumulated purchase value
 *
 * <p>4 ValueStates, 8 operations per record (4 read + 4 write)
 */
public class SessionSequenceBenchmark {

    public static void run(
            StreamExecutionEnvironment env,
            int numKeys,
            long numRecords,
            double skewFactor,
            int batchSize,
            int parallelism,
            String backend) throws Exception {

        DataStream<Long> source = env
                .addSource(new LongEventSource(numKeys, numRecords, skewFactor))
                .setParallelism(parallelism)
                .name("SessionEventSource");

        DataStream<Long> result = source
                .keyBy(sessionId -> sessionId)
                .process(new SessionSequenceFunction())
                .setParallelism(parallelism)
                .name("SessionSequenceAnalyzer");

        result.addSink(new BlackholeSink<>())
                .setParallelism(parallelism)
                .name("Blackhole");

        env.execute("Session Sequence Benchmark");
    }

    public static class SessionSequenceFunction
            extends KeyedProcessFunction<Long, Long, Long> {

        private static final long serialVersionUID = 1L;

        // 4 core states for session funnel tracking
        private transient ValueState<Long> eventCount;
        private transient ValueState<Long> funnelStage;
        private transient ValueState<Long> lastEventTime;
        private transient ValueState<Long> conversionValue;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("eventCount", Long.class));
            funnelStage = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("funnelStage", Long.class));
            lastEventTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastEventTime", Long.class));
            conversionValue = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("conversionValue", Long.class));
        }

        @Override
        public void processElement(Long sessionId, Context ctx, Collector<Long> out) throws Exception {
            // Read 4 states
            Long evtCnt = eventCount.value();
            Long stage = funnelStage.value();
            Long lastTime = lastEventTime.value();
            Long conversion = conversionValue.value();

            // Simple increments
            long newEvtCnt = evtCnt == null ? 1L : evtCnt + 1;
            long newStage = stage == null ? 1L : stage + 1;
            long newLastTime = lastTime == null ? 1L : lastTime + 1;
            long newConversion = conversion == null ? 1L : conversion + 1;

            // Write 4 states
            eventCount.update(newEvtCnt);
            funnelStage.update(newStage);
            lastEventTime.update(newLastTime);
            conversionValue.update(newConversion);

            out.collect(newEvtCnt);
        }
    }
}
