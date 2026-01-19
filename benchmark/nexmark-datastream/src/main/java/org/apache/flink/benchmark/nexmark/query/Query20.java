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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Query 20: Expand Bid with Auction (Filter Join)
 * 
 * <p>Join bids with auctions, filtering for category 10.
 * This query tests regular equi-join with filter.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT bid.*, auction.* 
 * FROM bid INNER JOIN auction 
 * ON bid.auction = auction.id
 * WHERE auction.category = 10
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>ValueState for auctions: Cache auction details by ID</li>
 *   <li>ListState for bids: Buffer bids waiting for matching auction</li>
 * </ul>
 */
public class Query20 {

    private static final long TARGET_CATEGORY = 10;

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

        // Join bids with auctions on bid.auction = auction.id, filter category = 10
        DataStream<Tuple2<Bid, Auction>> joinedBids = auctions
                .keyBy(a -> a.id)
                .connect(bids.keyBy(b -> b.auction))
                .process(new BidAuctionJoinFunction(TARGET_CATEGORY))
                .name("BidAuctionJoin");

        // Sink
        joinedBids.addSink(new MetricsSink<>("q20", backend, numEvents, parallelism))
                .name("Q20Sink")
                .setParallelism(1);
    }

    /**
     * Join bids with auctions and filter by category.
     */
    private static class BidAuctionJoinFunction 
            extends KeyedCoProcessFunction<Long, Auction, Bid, Tuple2<Bid, Auction>> {

        private final long targetCategory;

        // Auction state: cached auction for this key
        private transient ValueState<Auction> auctionState;
        // Buffered bids waiting for auction
        private transient ValueState<List<Bid>> pendingBidsState;

        public BidAuctionJoinFunction(long targetCategory) {
            this.targetCategory = targetCategory;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            auctionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auction", Auction.class));
            pendingBidsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pendingBids", 
                            org.apache.flink.api.common.typeinfo.Types.LIST(
                                    org.apache.flink.api.common.typeinfo.TypeInformation.of(Bid.class))));
        }

        @Override
        public void processElement1(Auction auction, Context ctx, 
                Collector<Tuple2<Bid, Auction>> out) throws Exception {
            // Store auction
            auctionState.update(auction);
            
            // Check if this auction matches category filter
            if (auction.category == targetCategory) {
                // Process any pending bids
                List<Bid> pending = pendingBidsState.value();
                if (pending != null && !pending.isEmpty()) {
                    for (Bid bid : pending) {
                        out.collect(Tuple2.of(bid, auction));
                    }
                    pendingBidsState.clear();
                }
            }
        }

        @Override
        public void processElement2(Bid bid, Context ctx, 
                Collector<Tuple2<Bid, Auction>> out) throws Exception {
            Auction auction = auctionState.value();
            
            if (auction != null) {
                // Auction exists, check category filter
                if (auction.category == targetCategory) {
                    out.collect(Tuple2.of(bid, auction));
                }
            } else {
                // No auction yet, buffer bid
                List<Bid> pending = pendingBidsState.value();
                if (pending == null) {
                    pending = new ArrayList<>();
                }
                pending.add(bid);
                pendingBidsState.update(pending);
            }
        }
    }
}
