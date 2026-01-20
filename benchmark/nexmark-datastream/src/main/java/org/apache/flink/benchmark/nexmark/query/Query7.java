/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Query 7: Highest Bid
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra
 * FROM bid B
 * JOIN (
 *   SELECT MAX(price) AS maxprice, window_end as dateTime
 *   FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
 *   GROUP BY window_start, window_end
 * ) B1
 * ON B.price = B1.maxprice
 * WHERE B.dateTime BETWEEN B1.dateTime - INTERVAL '10' SECOND AND B1.dateTime;
 * </pre>
 * 
 * <p>Implementation:
 * 1. Tumbling window (10s) to find max price
 * 2. For each window, collect all bids
 * 3. Join: output all bids with price = max price in that window
 */
public class Query7 {

    private static final long WINDOW_SIZE_SECONDS = 10;

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

        // Tumbling window: find max bid and output all bids with that max price
        DataStream<Bid> highestBids = bids
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(WINDOW_SIZE_SECONDS)))
                .process(new MaxBidWindowFunction())
                .name("TumblingMaxBid");

        highestBids.addSink(new MetricsSink<>("q7", backend, numEvents, parallelism))
                .name("Q7Sink")
                .setParallelism(1);
    }

    /**
     * Find max price in window and emit all bids with that price.
     * This implements the self-join semantics of the SQL query.
     */
    private static class MaxBidWindowFunction 
            extends ProcessAllWindowFunction<Bid, Bid, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Bid> elements, 
                Collector<Bid> out) throws Exception {
            
            // First pass: find max price and collect all bids
            long maxPrice = Long.MIN_VALUE;
            List<Bid> allBids = new ArrayList<>();
            
            for (Bid bid : elements) {
                allBids.add(bid);
                if (bid.price > maxPrice) {
                    maxPrice = bid.price;
                }
            }
            
            // Second pass: emit all bids with max price
            for (Bid bid : allBids) {
                if (bid.price == maxPrice) {
                    out.collect(bid);
                }
            }
        }
    }
}
