/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.benchmark.nexmark.model.Auction;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Query 9: Winning Bids
 * 
 * <p>Find the winning bid (highest price, earliest time) for each auction.
 * This requires an interval join between auctions and bids, then selecting
 * the best bid per auction.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT * FROM (
 *     SELECT A.*, B.*, ROW_NUMBER() OVER (
 *         PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC
 *     ) AS rownum
 *     FROM auction A, bid B
 *     WHERE A.id = B.auction 
 *       AND B.dateTime BETWEEN A.dateTime AND A.expires
 * ) WHERE rownum <= 1
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>ValueState for auction details</li>
 *   <li>ValueState for best bid (highest price, earliest time)</li>
 * </ul>
 */
public class Query9 {

    public static void run(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String backend,
            long numEvents,
            int parallelism) {

        // Split streams
        DataStream<Auction> auctions = events
                .filter(Event::isAuction)
                .map(e -> e.auction)
                .returns(Auction.class);

        DataStream<Bid> bids = events
                .filter(Event::isBid)
                .map(e -> e.bid)
                .returns(Bid.class);

        // Join auctions and bids, track best bid per auction
        DataStream<Tuple4<Long, Long, Long, Long>> winningBids = auctions
                .keyBy(a -> a.id)
                .connect(bids.keyBy(b -> b.auction))
                .process(new WinningBidFunction())
                .name("WinningBidJoin");

        // Sink
        winningBids.addSink(new MetricsSink<>("q9", backend, numEvents, parallelism))
                .name("Q9Sink")
                .setParallelism(1);
    }

    /**
     * Join auctions with bids and track the winning bid.
     * Winning bid = highest price, with ties broken by earliest time.
     * 
     * Output: (auctionId, sellerId, bidderId, winningPrice)
     */
    private static class WinningBidFunction 
            extends KeyedCoProcessFunction<Long, Auction, Bid, Tuple4<Long, Long, Long, Long>> {

        // Auction state: (seller, dateTime, expires)
        private transient ValueState<Tuple2<Long, Tuple2<Long, Long>>> auctionState;
        // Best bid state: (price, dateTime, bidder)
        private transient ValueState<Tuple2<Long, Tuple2<Long, Long>>> bestBidState;

        @Override
        public void open(Configuration parameters) throws Exception {
            auctionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auction", 
                            Types.TUPLE(Types.LONG, Types.TUPLE(Types.LONG, Types.LONG))));
            bestBidState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("bestBid", 
                            Types.TUPLE(Types.LONG, Types.TUPLE(Types.LONG, Types.LONG))));
        }

        @Override
        public void processElement1(Auction auction, Context ctx, 
                Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // Store auction: (seller, (dateTime, expires))
            auctionState.update(Tuple2.of(auction.seller, Tuple2.of(auction.dateTime, auction.expires)));
            
            // Register timer for auction expiration
            ctx.timerService().registerProcessingTimeTimer(auction.expires);
        }

        @Override
        public void processElement2(Bid bid, Context ctx, 
                Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            Tuple2<Long, Tuple2<Long, Long>> auction = auctionState.value();
            if (auction == null) {
                return; // No auction yet
            }
            
            long auctionStart = auction.f1.f0;
            long auctionEnd = auction.f1.f1;
            
            // Check if bid is within auction time window
            if (bid.dateTime >= auctionStart && bid.dateTime <= auctionEnd) {
                Tuple2<Long, Tuple2<Long, Long>> currentBest = bestBidState.value();
                
                boolean isBetter = false;
                if (currentBest == null) {
                    isBetter = true;
                } else {
                    long bestPrice = currentBest.f0;
                    long bestTime = currentBest.f1.f0;
                    // Better if higher price, or same price but earlier time
                    isBetter = bid.price > bestPrice || 
                            (bid.price == bestPrice && bid.dateTime < bestTime);
                }
                
                if (isBetter) {
                    bestBidState.update(Tuple2.of(bid.price, Tuple2.of(bid.dateTime, bid.bidder)));
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, 
                Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // Auction expired, emit winning bid
            Tuple2<Long, Tuple2<Long, Long>> auction = auctionState.value();
            Tuple2<Long, Tuple2<Long, Long>> bestBid = bestBidState.value();
            
            if (auction != null && bestBid != null) {
                long auctionId = ctx.getCurrentKey();
                long seller = auction.f0;
                long bidder = bestBid.f1.f1;
                long price = bestBid.f0;
                out.collect(Tuple4.of(auctionId, seller, bidder, price));
            }
            
            // Clean up state
            auctionState.clear();
            bestBidState.clear();
        }
    }
}
