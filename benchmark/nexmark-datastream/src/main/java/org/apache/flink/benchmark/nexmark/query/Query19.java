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
 * <p>Original SQL:
 * <pre>
 * SELECT * FROM (
 *   SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number
 *   FROM bid
 * )
 * WHERE rank_number <= 10;
 * </pre>
 * 
 * <p>Implementation:
 * For each auction, maintain top 10 bids by price.
 * This is a TOP-N query.
 */
public class Query19 {

    private static final int TOP_N = 10;

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

        // Key by auction, maintain top 10
        DataStream<BidWithRank> topBids = bids
                .keyBy(b -> b.auction)
                .process(new TopNFunction())
                .name("TopN");

        topBids.addSink(new MetricsSink<>("q19", backend, numEvents, parallelism))
                .name("Q19Sink")
                .setParallelism(1);
    }

    /**
     * Bid with rank number.
     */
    public static class BidWithRank implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public Bid bid;
        public int rank;
        
        public BidWithRank() {}
        
        public BidWithRank(Bid bid, int rank) {
            this.bid = bid;
            this.rank = rank;
        }
        
        @Override
        public String toString() {
            return "BidWithRank{auction=" + bid.auction + ", price=" + bid.price + ", rank=" + rank + "}";
        }
    }

    /**
     * Maintain top N bids per auction.
     */
    private static class TopNFunction 
            extends KeyedProcessFunction<Long, Bid, BidWithRank> {

        private transient ListState<Bid> topBidsState;

        @Override
        public void open(Configuration parameters) {
            topBidsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("topBids", Bid.class));
        }

        @Override
        public void processElement(Bid bid, Context ctx, Collector<BidWithRank> out) throws Exception {
            // Get current top bids
            List<Bid> topBids = new ArrayList<>();
            for (Bid b : topBidsState.get()) {
                topBids.add(b);
            }
            
            // Add new bid
            topBids.add(bid);
            
            // Sort by price descending
            topBids.sort(Comparator.comparingLong((Bid b) -> b.price).reversed());
            
            // Keep only top N
            if (topBids.size() > TOP_N) {
                topBids = topBids.subList(0, TOP_N);
            }
            
            // Update state
            topBidsState.clear();
            for (Bid b : topBids) {
                topBidsState.add(b);
            }
            
            // Check if bid is in top N and emit with rank
            for (int i = 0; i < topBids.size(); i++) {
                if (topBids.get(i).equals(bid)) {
                    out.collect(new BidWithRank(bid, i + 1));
                    break;
                }
            }
        }
    }
}
