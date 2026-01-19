/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Query 11: User Sessions
 * 
 * <p>Count bids per user session, where a session is defined by a 10-second gap.
 * This query tests session window aggregation.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT bidder, COUNT(*) AS num, SESSION_START, SESSION_END
 * FROM bid
 * GROUP BY bidder, SESSION(dateTime, INTERVAL '10' SECOND)
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>Session window state: Merge and maintain session windows per bidder</li>
 *   <li>Aggregation state: Count per session</li>
 * </ul>
 */
public class Query11 {

    // Session gap: 10 seconds
    private static final long SESSION_GAP_SECONDS = 10;

    public static void run(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String backend,
            long numEvents,
            int parallelism) {

        // Filter to bids only
        DataStream<Bid> bids = events
                .filter(Event::isBid)
                .map(e -> e.bid)
                .returns(Bid.class);

        // Session window aggregation: count bids per bidder per session
        DataStream<Tuple3<Long, Long, Long>> sessions = bids
                .keyBy(b -> b.bidder)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(SESSION_GAP_SECONDS)))
                .aggregate(new SessionBidCountAggregate())
                .name("SessionBidCount");

        // Sink
        sessions.addSink(new MetricsSink<>("q11", backend, numEvents, parallelism))
                .name("Q11Sink")
                .setParallelism(1);
    }

    /**
     * Aggregate function to count bids per session.
     * Output: (bidder, count, sessionDuration)
     */
    private static class SessionBidCountAggregate 
            implements AggregateFunction<Bid, SessionAccumulator, Tuple3<Long, Long, Long>> {

        @Override
        public SessionAccumulator createAccumulator() {
            return new SessionAccumulator();
        }

        @Override
        public SessionAccumulator add(Bid bid, SessionAccumulator acc) {
            acc.bidder = bid.bidder;
            acc.count++;
            if (acc.minTime == 0 || bid.dateTime < acc.minTime) {
                acc.minTime = bid.dateTime;
            }
            if (bid.dateTime > acc.maxTime) {
                acc.maxTime = bid.dateTime;
            }
            return acc;
        }

        @Override
        public Tuple3<Long, Long, Long> getResult(SessionAccumulator acc) {
            long duration = acc.maxTime - acc.minTime;
            return Tuple3.of(acc.bidder, acc.count, duration);
        }

        @Override
        public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
            SessionAccumulator merged = new SessionAccumulator();
            merged.bidder = a.bidder;
            merged.count = a.count + b.count;
            merged.minTime = Math.min(a.minTime == 0 ? b.minTime : a.minTime, 
                    b.minTime == 0 ? a.minTime : b.minTime);
            merged.maxTime = Math.max(a.maxTime, b.maxTime);
            return merged;
        }
    }

    /**
     * Accumulator for session aggregation.
     */
    private static class SessionAccumulator {
        long bidder = -1;
        long count = 0;
        long minTime = 0;
        long maxTime = 0;
    }
}
