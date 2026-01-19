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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Query 12: Processing Time Windows
 * 
 * <p>Count bids per bidder in tumbling processing-time windows.
 * This query tests processing-time tumbling window aggregation.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT bidder, COUNT(*) as num, window_start, window_end
 * FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(processingTime), INTERVAL '10' SECOND))
 * GROUP BY bidder, window_start, window_end
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>Window state: Aggregation accumulators per bidder per window</li>
 * </ul>
 */
public class Query12 {

    // Window size: 10 seconds
    private static final long WINDOW_SIZE_SECONDS = 10;

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

        // Tumbling window aggregation: count bids per bidder
        DataStream<Tuple2<Long, Long>> bidderCounts = bids
                .keyBy(b -> b.bidder)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .aggregate(new BidderCountAggregate())
                .name("TumblingBidderCount");

        // Sink
        bidderCounts.addSink(new MetricsSink<>("q12", backend, numEvents, parallelism))
                .name("Q12Sink")
                .setParallelism(1);
    }

    /**
     * Aggregate function to count bids per bidder.
     * Output: (bidder, count)
     */
    private static class BidderCountAggregate 
            implements AggregateFunction<Bid, BidderAccumulator, Tuple2<Long, Long>> {

        @Override
        public BidderAccumulator createAccumulator() {
            return new BidderAccumulator();
        }

        @Override
        public BidderAccumulator add(Bid bid, BidderAccumulator acc) {
            acc.bidder = bid.bidder;
            acc.count++;
            return acc;
        }

        @Override
        public Tuple2<Long, Long> getResult(BidderAccumulator acc) {
            return Tuple2.of(acc.bidder, acc.count);
        }

        @Override
        public BidderAccumulator merge(BidderAccumulator a, BidderAccumulator b) {
            BidderAccumulator merged = new BidderAccumulator();
            merged.bidder = a.bidder;
            merged.count = a.count + b.count;
            return merged;
        }
    }

    /**
     * Accumulator for bidder count aggregation.
     */
    private static class BidderAccumulator {
        long bidder = -1;
        long count = 0;
    }
}
