/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Query 5: Hot Items
 * 
 * <p>Find the auctions with the most bids over a sliding window.
 * This query tests sliding window aggregation with high cardinality keys.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT auction, num FROM (
 *     SELECT auction, COUNT(*) AS num, window_start, window_end
 *     FROM TABLE(HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
 *     GROUP BY auction, window_start, window_end
 * ) WHERE num >= max_of_all_auctions_in_window
 * </pre>
 * 
 * <p>For benchmarking purposes, we output all auction counts (without the final max filter)
 * to maximize state operations.
 * 
 * <p>State pattern:
 * <ul>
 *   <li>Window state: Aggregation accumulators per auction per window</li>
 * </ul>
 */
public class Query5 {

    // Window parameters (aligned with original Nexmark)
    private static final long WINDOW_SIZE_SECONDS = 10;
    private static final long SLIDE_SIZE_SECONDS = 2;

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

        // Sliding window aggregation: count bids per auction
        DataStream<Tuple2<Long, Long>> hotItems = bids
                .keyBy(b -> b.auction)
                .window(SlidingProcessingTimeWindows.of(
                        Time.seconds(WINDOW_SIZE_SECONDS),
                        Time.seconds(SLIDE_SIZE_SECONDS)))
                .aggregate(new BidCountAggregate())
                .name("SlidingBidCount");

        // Sink
        hotItems.addSink(new MetricsSink<>("q5", backend, numEvents, parallelism))
                .name("Q5Sink")
                .setParallelism(1);
    }

    /**
     * Aggregate function to count bids per auction.
     * Outputs (auction, count).
     */
    private static class BidCountAggregate 
            implements AggregateFunction<Bid, Long, Tuple2<Long, Long>> {

        private long currentAuction = -1;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Bid bid, Long accumulator) {
            currentAuction = bid.auction;
            return accumulator + 1;
        }

        @Override
        public Tuple2<Long, Long> getResult(Long accumulator) {
            return Tuple2.of(currentAuction, accumulator);
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
