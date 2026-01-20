/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Query 11: User Sessions
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT B.bidder, count(*) as bid_count,
 *        SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,
 *        SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime
 * FROM bid B
 * GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);
 * </pre>
 * 
 * <p>Implementation:
 * Session window with 10s gap, count bids per bidder per session.
 */
public class Query11 {

    private static final long SESSION_GAP_SECONDS = 10;

    public static void run(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String backend,
            long numEvents,
            int parallelism) {

        DataStream<Bid> bids = events
                .filter(Event::isBid)
                .map(e -> e.bid)
                .returns(Bid.class);

        // Session window by bidder with 10s gap
        DataStream<Tuple4<Long, Long, Long, Long>> sessions = bids
                .keyBy(b -> b.bidder)
                .window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(SESSION_GAP_SECONDS)))
                .aggregate(new CountAggregate(), new SessionInfoFunction())
                .name("SessionBidCount");

        sessions.addSink(new MetricsSink<>("q11", backend, numEvents, parallelism))
                .name("Q11Sink")
                .setParallelism(1);
    }

    /**
     * Count bids in session.
     */
    private static class CountAggregate implements AggregateFunction<Bid, Long, Long> {
        @Override
        public Long createAccumulator() { return 0L; }

        @Override
        public Long add(Bid value, Long accumulator) { return accumulator + 1; }

        @Override
        public Long getResult(Long accumulator) { return accumulator; }

        @Override
        public Long merge(Long a, Long b) { return a + b; }
    }

    /**
     * Attach session info: (bidder, count, starttime, endtime).
     */
    private static class SessionInfoFunction 
            extends ProcessWindowFunction<Long, Tuple4<Long, Long, Long, Long>, Long, TimeWindow> {

        @Override
        public void process(Long bidder, Context ctx, Iterable<Long> elements, 
                Collector<Tuple4<Long, Long, Long, Long>> out) {
            Long count = elements.iterator().next();
            out.collect(Tuple4.of(bidder, count, ctx.window().getStart(), ctx.window().getEnd()));
        }
    }
}
