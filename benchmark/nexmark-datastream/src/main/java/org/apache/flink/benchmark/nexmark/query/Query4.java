/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
 * <p>Finds the average selling price for each category of item.
 * This requires joining auctions with their winning bids and aggregating by category.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT category, AVG(final) 
 * FROM (
 *     SELECT MAX(B.price) AS final, A.category
 *     FROM auction A, bid B
 *     WHERE A.id = B.auction 
 *       AND B.dateTime BETWEEN A.dateTime AND A.expires
 *     GROUP BY A.id, A.category
 * ) 
 * GROUP BY category
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>MapState: Store auctions keyed by auction ID</li>
 *   <li>ValueState: Track max bid per auction</li>
 *   <li>MapState: Track running average per category (sum, count)</li>
 * </ul>
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

        // Join auctions and bids, compute max bid per auction, then average by category
        // We use a two-stage approach:
        // Stage 1: Join auction with bids, track max bid per auction
        // Stage 2: Aggregate by category

        // Stage 1: Co-process auction and bid streams keyed by auction ID
        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> auctionMaxBids = auctions
                .keyBy(a -> a.id)
                .connect(bids.keyBy(b -> b.auction))
                .process(new AuctionBidJoinFunction())
                .name("AuctionBidJoin");

        // Stage 2: Aggregate by category
        DataStream<Tuple2<Long, Double>> categoryAvg = auctionMaxBids
                .keyBy(t -> t.f1) // key by category
                .process(new CategoryAverageFunction())
                .name("CategoryAverage");

        // Sink
        categoryAvg.addSink(new MetricsSink<>("q4", backend, numEvents, parallelism))
                .name("Q4Sink")
                .setParallelism(1);
    }

    /**
     * Join auctions with bids and output (auctionId, category, maxBid) when auction expires.
     * 
     * <p>State:
     * <ul>
     *   <li>ValueState for auction details (category, expires)</li>
     *   <li>ValueState for max bid seen</li>
     * </ul>
     */
    private static class AuctionBidJoinFunction 
            extends KeyedCoProcessFunction<Long, Auction, Bid, Tuple3<Long, Long, Long>> {

        // Auction state: (category, dateTime, expires)
        private transient ValueState<Tuple3<Long, Long, Long>> auctionState;
        // Max bid state
        private transient ValueState<Long> maxBidState;

        @Override
        public void open(Configuration parameters) throws Exception {
            auctionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auction", Types.TUPLE(Types.LONG, Types.LONG, Types.LONG)));
            maxBidState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("maxBid", Types.LONG));
        }

        @Override
        public void processElement1(Auction auction, Context ctx, Collector<Tuple3<Long, Long, Long>> out) 
                throws Exception {
            // Store auction details
            auctionState.update(Tuple3.of(auction.category, auction.dateTime, auction.expires));
            
            // Register timer for auction expiration
            ctx.timerService().registerProcessingTimeTimer(auction.expires);
        }

        @Override
        public void processElement2(Bid bid, Context ctx, Collector<Tuple3<Long, Long, Long>> out) 
                throws Exception {
            Tuple3<Long, Long, Long> auction = auctionState.value();
            if (auction == null) {
                return; // No auction yet, ignore bid
            }
            
            // Check if bid is within auction time window
            long auctionStart = auction.f1;
            long auctionEnd = auction.f2;
            if (bid.dateTime >= auctionStart && bid.dateTime <= auctionEnd) {
                // Update max bid
                Long currentMax = maxBidState.value();
                if (currentMax == null || bid.price > currentMax) {
                    maxBidState.update(bid.price);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Long, Long>> out) 
                throws Exception {
            // Auction expired, emit result
            Tuple3<Long, Long, Long> auction = auctionState.value();
            Long maxBid = maxBidState.value();
            
            if (auction != null && maxBid != null) {
                // Output (auctionId, category, maxBid)
                out.collect(Tuple3.of(ctx.getCurrentKey(), auction.f0, maxBid));
            }
            
            // Clean up state
            auctionState.clear();
            maxBidState.clear();
        }
    }

    /**
     * Compute running average price per category.
     * 
     * <p>State: ValueState storing (sum, count) for running average.
     */
    private static class CategoryAverageFunction 
            extends KeyedProcessFunction<Long, Tuple3<Long, Long, Long>, Tuple2<Long, Double>> {

        // Running average state: (sum, count)
        private transient ValueState<Tuple2<Long, Long>> avgState;

        @Override
        public void open(Configuration parameters) throws Exception {
            avgState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("avg", Types.TUPLE(Types.LONG, Types.LONG)));
        }

        @Override
        public void processElement(Tuple3<Long, Long, Long> value, Context ctx, 
                Collector<Tuple2<Long, Double>> out) throws Exception {
            long category = value.f1;
            long maxBid = value.f2;
            
            Tuple2<Long, Long> current = avgState.value();
            if (current == null) {
                current = Tuple2.of(0L, 0L);
            }
            
            long newSum = current.f0 + maxBid;
            long newCount = current.f1 + 1;
            avgState.update(Tuple2.of(newSum, newCount));
            
            double avg = (double) newSum / newCount;
            out.collect(Tuple2.of(category, avg));
        }
    }
}
