/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * Query 9: Winning Bids
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra,
 *        auction, bidder, price, bid_dateTime, bid_extra
 * FROM (
 *    SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,
 *      ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
 *    FROM auction A, bid B
 *    WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
 * )
 * WHERE rownum <= 1;
 * </pre>
 * 
 * <p>Implementation:
 * 1. Join auctions with bids
 * 2. Filter bids within auction time range
 * 3. For each auction, track highest bid (tiebreak by earliest time)
 * 4. Emit winning bid when auction expires
 */
public class Query9 {

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

        // Join and find winning bid per auction
        DataStream<AuctionBidResult> winningBids = auctions
                .keyBy(a -> a.id)
                .connect(bids.keyBy(b -> b.auction))
                .process(new WinningBidJoin())
                .name("WinningBidJoin");

        winningBids.addSink(new MetricsSink<>("q9", backend, numEvents, parallelism))
                .name("Q9Sink")
                .setParallelism(1);
    }

    /**
     * Result type combining auction and winning bid.
     */
    public static class AuctionBidResult implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public Auction auction;
        public Bid winningBid;
        
        public AuctionBidResult() {}
        
        public AuctionBidResult(Auction auction, Bid winningBid) {
            this.auction = auction;
            this.winningBid = winningBid;
        }
        
        @Override
        public String toString() {
            return "AuctionBidResult{auction=" + auction.id + 
                   ", bidder=" + (winningBid != null ? winningBid.bidder : "null") + 
                   ", price=" + (winningBid != null ? winningBid.price : 0) + "}";
        }
    }

    /**
     * Join auctions with bids, tracking winning bid per auction.
     * Winning bid = highest price, tiebreak by earliest time.
     */
    private static class WinningBidJoin 
            extends KeyedCoProcessFunction<Long, Auction, Bid, AuctionBidResult> {

        private transient ValueState<Auction> auctionState;
        private transient ValueState<Bid> winningBidState;
        private transient ListState<Bid> pendingBidsState;

        @Override
        public void open(Configuration parameters) {
            auctionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auction", Auction.class));
            winningBidState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("winningBid", Bid.class));
            pendingBidsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("pendingBids", Bid.class));
        }

        @Override
        public void processElement1(Auction auction, Context ctx, 
                Collector<AuctionBidResult> out) throws Exception {
            auctionState.update(auction);
            
            // Process any pending bids
            List<Bid> pending = new ArrayList<>();
            for (Bid bid : pendingBidsState.get()) {
                pending.add(bid);
            }
            pendingBidsState.clear();
            
            for (Bid bid : pending) {
                processBid(auction, bid);
            }
            
            // Register timer for auction expiration
            ctx.timerService().registerProcessingTimeTimer(auction.expires);
        }

        @Override
        public void processElement2(Bid bid, Context ctx, 
                Collector<AuctionBidResult> out) throws Exception {
            Auction auction = auctionState.value();
            
            if (auction == null) {
                // Auction not yet received, buffer the bid
                pendingBidsState.add(bid);
            } else {
                processBid(auction, bid);
            }
        }

        private void processBid(Auction auction, Bid bid) throws Exception {
            // Check if bid is within auction time range
            if (bid.dateTime >= auction.dateTime && bid.dateTime <= auction.expires) {
                Bid currentWinner = winningBidState.value();
                
                if (currentWinner == null) {
                    winningBidState.update(bid);
                } else {
                    // Higher price wins, or earlier time if same price
                    if (bid.price > currentWinner.price || 
                        (bid.price == currentWinner.price && bid.dateTime < currentWinner.dateTime)) {
                        winningBidState.update(bid);
                    }
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, 
                Collector<AuctionBidResult> out) throws Exception {
            Auction auction = auctionState.value();
            Bid winningBid = winningBidState.value();
            
            if (auction != null && winningBid != null) {
                out.collect(new AuctionBidResult(auction, winningBid));
            }
            
            // Clear state
            auctionState.clear();
            winningBidState.clear();
            pendingBidsState.clear();
        }
    }
}
