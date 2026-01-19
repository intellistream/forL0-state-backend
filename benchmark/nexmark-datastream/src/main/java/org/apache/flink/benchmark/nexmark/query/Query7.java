/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Query 7: Highest Bid
 * 
 * <p>Find the highest bid in each tumbling window.
 * This query tests tumbling window aggregation with global max tracking.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
 * FROM bid B
 * JOIN (
 *     SELECT MAX(price) AS maxprice, window_start, window_end
 *     FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
 *     GROUP BY window_start, window_end
 * ) S ON B.price = S.maxprice
 * WHERE B.dateTime >= S.window_start AND B.dateTime < S.window_end
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>Window state: Track max bid per window</li>
 * </ul>
 */
public class Query7 {

    // Window parameters
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

        // Tumbling window: find max bid per window
        // We use windowAll since we need global max
        DataStream<Bid> highestBids = bids
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(WINDOW_SIZE_SECONDS)))
                .process(new MaxBidWindowFunction())
                .name("TumblingMaxBid");

        // Sink
        highestBids.addSink(new MetricsSink<>("q7", backend, numEvents, parallelism))
                .name("Q7Sink")
                .setParallelism(1);
    }

    /**
     * Process window function to find the max bid in each window.
     */
    private static class MaxBidWindowFunction 
            extends ProcessAllWindowFunction<Bid, Bid, TimeWindow> {

        @Override
        public void process(Context context, 
                Iterable<Bid> elements, Collector<Bid> out) throws Exception {
            Bid maxBid = null;
            
            for (Bid bid : elements) {
                if (maxBid == null || bid.price > maxBid.price) {
                    maxBid = bid;
                }
            }
            
            if (maxBid != null) {
                out.collect(maxBid);
            }
        }
    }
}
