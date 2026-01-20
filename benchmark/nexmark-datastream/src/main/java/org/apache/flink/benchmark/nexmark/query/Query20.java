/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.benchmark.nexmark.model.Auction;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Query 20: Expand bid with auction
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT
 *     auction, bidder, price, channel, url, B.dateTime, B.extra,
 *     itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra
 * FROM
 *     bid AS B INNER JOIN auction AS A on B.auction = A.id
 * WHERE A.category = 10;
 * </pre>
 * 
 * <p>Implementation:
 * Join bids with auctions, filter by category = 10.
 */
public class Query20 {

    private static final long CATEGORY_FILTER = 10;

    public static void run(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String backend,
            long numEvents,
            int parallelism) {

        DataStream<Auction> auctions = events
                .filter(Event::isAuction)
                .map(e -> e.auction)
                .returns(Auction.class);

        DataStream<Bid> bids = events
                .filter(Event::isBid)
                .map(e -> e.bid)
                .returns(Bid.class);

        // Join bids with auctions, filter by category
        DataStream<BidAuctionJoinResult> joinedBids = auctions
                .keyBy(a -> a.id)
                .connect(bids.keyBy(b -> b.auction))
                .process(new BidAuctionJoin())
                .name("BidAuctionJoin");

        joinedBids.addSink(new MetricsSink<>("q20", backend, numEvents, parallelism))
                .name("Q20Sink")
                .setParallelism(1);
    }

    /**
     * Result of joining bid with auction.
     */
    public static class BidAuctionJoinResult implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public Bid bid;
        public Auction auction;
        
        public BidAuctionJoinResult() {}
        
        public BidAuctionJoinResult(Bid bid, Auction auction) {
            this.bid = bid;
            this.auction = auction;
        }
        
        @Override
        public String toString() {
            return "BidAuctionJoinResult{auction=" + auction.id + 
                   ", bidder=" + bid.bidder + ", price=" + bid.price + 
                   ", category=" + auction.category + "}";
        }
    }

    /**
     * Join bids with auctions, filter by category = 10.
     */
    private static class BidAuctionJoin 
            extends KeyedCoProcessFunction<Long, Auction, Bid, BidAuctionJoinResult> {

        private transient ValueState<Auction> auctionState;
        private transient org.apache.flink.api.common.state.ListState<Bid> pendingBidsState;

        @Override
        public void open(Configuration parameters) {
            auctionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auction", Auction.class));
            pendingBidsState = getRuntimeContext().getListState(
                    new org.apache.flink.api.common.state.ListStateDescriptor<>(
                        "pendingBids", Bid.class));
        }

        @Override
        public void processElement1(Auction auction, Context ctx, 
                Collector<BidAuctionJoinResult> out) throws Exception {
            
            // Only store auctions with category = 10
            if (auction.category == CATEGORY_FILTER) {
                auctionState.update(auction);
                
                // Process pending bids
                List<Bid> pending = new ArrayList<>();
                for (Bid bid : pendingBidsState.get()) {
                    pending.add(bid);
                }
                pendingBidsState.clear();
                
                for (Bid bid : pending) {
                    out.collect(new BidAuctionJoinResult(bid, auction));
                }
                
                // Register cleanup timer (auction expiry + buffer)
                ctx.timerService().registerProcessingTimeTimer(auction.expires + 60000);
            }
        }

        @Override
        public void processElement2(Bid bid, Context ctx, 
                Collector<BidAuctionJoinResult> out) throws Exception {
            Auction auction = auctionState.value();
            
            if (auction != null) {
                // Auction exists and has category = 10
                out.collect(new BidAuctionJoinResult(bid, auction));
            } else {
                // Buffer bid in case auction arrives later
                pendingBidsState.add(bid);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, 
                Collector<BidAuctionJoinResult> out) throws Exception {
            // Cleanup state after auction expires
            auctionState.clear();
            pendingBidsState.clear();
        }
    }
}
