/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Query 19: Auction TOP-10 Price
 * 
 * <p>Find the top 10 bids by price for each auction.
 * This query tests top-N aggregation with state maintenance.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT * FROM (
 *     SELECT *, ROW_NUMBER() OVER (
 *         PARTITION BY auction 
 *         ORDER BY price DESC
 *     ) AS rank_number
 *     FROM bid
 * ) WHERE rank_number <= 10
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>ListState per auction: Store top-10 bids sorted by price descending</li>
 * </ul>
 */
public class Query19 {

    private static final int TOP_N = 10;

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

        // Key by auction and maintain top-10
        DataStream<Bid> topBids = bids
                .keyBy(b -> b.auction)
                .process(new TopNBidsFunction(TOP_N))
                .name("TopNBids");

        // Sink
        topBids.addSink(new MetricsSink<>("q19", backend, numEvents, parallelism))
                .name("Q19Sink")
                .setParallelism(1);
    }

    /**
     * Maintain top-N bids per auction.
     * Emits a bid if it's in the top-N.
     */
    private static class TopNBidsFunction extends KeyedProcessFunction<Long, Bid, Bid> {

        private final int topN;

        // State: list of top bids (sorted by price descending)
        private transient ListState<Bid> topBidsState;

        public TopNBidsFunction(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            topBidsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("topBids", Bid.class));
        }

        @Override
        public void processElement(Bid bid, Context ctx, Collector<Bid> out) throws Exception {
            // Get current top bids
            List<Bid> topBids = new ArrayList<>();
            for (Bid b : topBidsState.get()) {
                topBids.add(b);
            }

            // Check if this bid should be in top-N
            boolean shouldAdd = topBids.size() < topN;
            if (!shouldAdd && !topBids.isEmpty()) {
                // Check if bid is higher than the lowest in top-N
                long minPrice = topBids.stream().mapToLong(b -> b.price).min().orElse(Long.MAX_VALUE);
                shouldAdd = bid.price > minPrice;
            }

            if (shouldAdd) {
                topBids.add(bid);
                
                // Sort by price descending
                topBids.sort(Comparator.comparingLong((Bid b) -> b.price).reversed());
                
                // Keep only top-N
                if (topBids.size() > topN) {
                    topBids = topBids.subList(0, topN);
                }
                
                // Update state
                topBidsState.update(topBids);
                
                // Emit the new bid (it's in top-N)
                out.collect(bid);
            }
        }
    }
}
