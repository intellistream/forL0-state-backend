/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.benchmark.nexmark.model.Auction;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Query 4: Average Price for a Category
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT Q.category, AVG(Q.final)
 * FROM (
 *     SELECT MAX(B.price) AS final, A.category
 *     FROM auction A, bid B
 *     WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
 *     GROUP BY A.id, A.category
 * ) Q
 * GROUP BY Q.category;
 * </pre>
 * 
 * <p>Implementation:
 * 1. Join bids with auctions (bid.auction = auction.id)
 * 2. Filter bids within auction time range
 * 3. For each auction, track max bid price (winning bid)
 * 4. For each category, compute running average of winning prices
 */
public class Query4 {

    public static void run(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String backend,
            long numEvents,
            int parallelism) {

        // Split into auction and bid streams
        DataStream<Auction> auctions = events
                .filter(Event::isAuction)
                .map(e -> e.auction)
                .returns(Auction.class);

        DataStream<Bid> bids = events
                .filter(Event::isBid)
                .map(e -> e.bid)
                .returns(Bid.class);

        // Join bids with auctions, keyed by auction id
        // Track max bid per auction, filter by time range
        SingleOutputStreamOperator<Tuple2<Long, Long>> winningBids = auctions
                .keyBy(a -> a.id)
                .connect(bids.keyBy(b -> b.auction))
                .process(new AuctionBidJoin())
                .name("AuctionBidJoin");

        // Compute running average per category
        DataStream<Tuple2<Long, Double>> categoryAvg = winningBids
                .keyBy(t -> t.f0) // category
                .process(new CategoryAvgFunction())
                .name("CategoryAvg");

        // Sink
        categoryAvg.addSink(new MetricsSink<>("q4", backend, numEvents, parallelism))
                .name("Q4Sink")
                .setParallelism(1);
    }

    /**
     * Join auctions with bids.
     * For each auction, track max bid price where bid time is within auction time range.
     * Emits (category, maxPrice) when auction expires.
     */
    private static class AuctionBidJoin 
            extends KeyedCoProcessFunction<Long, Auction, Bid, Tuple2<Long, Long>> {

        // Auction state
        private transient ValueState<Auction> auctionState;
        // Max bid price for this auction
        private transient ValueState<Long> maxBidState;

        @Override
        public void open(Configuration parameters) {
            auctionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auction", Auction.class));
            maxBidState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("maxBid", Long.class));
        }

        @Override
        public void processElement1(Auction auction, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
            auctionState.update(auction);
            // Register timer to emit result when auction expires
            ctx.timerService().registerProcessingTimeTimer(auction.expires);
        }

        @Override
        public void processElement2(Bid bid, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
            Auction auction = auctionState.value();
            if (auction != null) {
                // Check if bid is within auction time range
                if (bid.dateTime >= auction.dateTime && bid.dateTime <= auction.expires) {
                    Long currentMax = maxBidState.value();
                    if (currentMax == null || bid.price > currentMax) {
                        maxBidState.update(bid.price);
                    }
                }
            }
            // If auction not yet received, we could buffer bids, but for simplicity
            // we assume auctions arrive before their bids (as per Nexmark generator)
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
            Auction auction = auctionState.value();
            Long maxBid = maxBidState.value();
            
            if (auction != null && maxBid != null) {
                // Emit (category, winningPrice)
                out.collect(Tuple2.of(auction.category, maxBid));
            }
            
            // Clear state
            auctionState.clear();
            maxBidState.clear();
        }
    }

    /**
     * Compute running average of winning prices per category.
     * Emits updated average on each new winning bid.
     */
    private static class CategoryAvgFunction 
            extends KeyedProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Double>> {

        private transient ValueState<Long> sumState;
        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) {
            sumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("sum", Long.class));
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(Tuple2<Long, Long> value, Context ctx, 
                Collector<Tuple2<Long, Double>> out) throws Exception {
            Long category = value.f0;
            Long price = value.f1;
            
            Long sum = sumState.value();
            Long count = countState.value();
            
            sum = (sum == null ? 0L : sum) + price;
            count = (count == null ? 0L : count) + 1;
            
            sumState.update(sum);
            countState.update(count);
            
            double avg = (double) sum / count;
            out.collect(Tuple2.of(category, avg));
        }
    }
}
