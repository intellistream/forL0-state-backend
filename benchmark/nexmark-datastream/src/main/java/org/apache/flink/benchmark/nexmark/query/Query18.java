/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Query 18: Find last bid (Deduplicate)
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT auction, bidder, price, channel, url, dateTime, extra
 * FROM (
 *   SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number
 *   FROM bid
 * )
 * WHERE rank_number <= 1;
 * </pre>
 * 
 * <p>Implementation:
 * For each (bidder, auction) pair, keep only the latest bid (by dateTime).
 * This is a deduplication query.
 */
public class Query18 {

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

        // Key by (bidder, auction), keep latest bid
        DataStream<Bid> latestBids = bids
                .keyBy(b -> b.bidder + "-" + b.auction)
                .process(new DeduplicateFunction())
                .name("Deduplicate");

        latestBids.addSink(new MetricsSink<>("q18", backend, numEvents, parallelism))
                .name("Q18Sink")
                .setParallelism(1);
    }

    /**
     * Keep only the latest bid per (bidder, auction) pair.
     * Emits the current latest bid on each update.
     */
    private static class DeduplicateFunction 
            extends KeyedProcessFunction<String, Bid, Bid> {

        private transient ValueState<Bid> latestBidState;

        @Override
        public void open(Configuration parameters) {
            latestBidState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latestBid", Bid.class));
        }

        @Override
        public void processElement(Bid bid, Context ctx, Collector<Bid> out) throws Exception {
            Bid current = latestBidState.value();
            
            // Keep the bid with latest dateTime
            if (current == null || bid.dateTime > current.dateTime) {
                latestBidState.update(bid);
                out.collect(bid);
            }
        }
    }
}
