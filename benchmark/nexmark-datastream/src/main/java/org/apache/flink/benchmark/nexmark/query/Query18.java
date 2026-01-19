/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Query 18: Find Last Bid (Deduplication)
 * 
 * <p>Keep only the latest bid per (bidder, auction) pair.
 * This query tests deduplication with composite keys.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT * FROM (
 *     SELECT *, ROW_NUMBER() OVER (
 *         PARTITION BY bidder, auction 
 *         ORDER BY dateTime DESC
 *     ) AS rank_number
 *     FROM bid
 * ) WHERE rank_number <= 1
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>ValueState per (bidder, auction) pair: Store the current latest bid</li>
 * </ul>
 */
public class Query18 {

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

        // Key by (bidder, auction) and keep latest bid
        DataStream<Bid> latestBids = bids
                .keyBy(b -> Tuple2.of(b.bidder, b.auction).toString())
                .process(new LastBidFunction())
                .name("LastBidDedup");

        // Sink
        latestBids.addSink(new MetricsSink<>("q18", backend, numEvents, parallelism))
                .name("Q18Sink")
                .setParallelism(1);
    }

    /**
     * Keep only the latest bid per (bidder, auction) key.
     * Emits the new latest bid whenever it changes.
     */
    private static class LastBidFunction extends KeyedProcessFunction<String, Bid, Bid> {

        // State: the latest bid for this key
        private transient ValueState<Bid> latestBidState;

        @Override
        public void open(Configuration parameters) throws Exception {
            latestBidState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latestBid", Bid.class));
        }

        @Override
        public void processElement(Bid bid, Context ctx, Collector<Bid> out) throws Exception {
            Bid current = latestBidState.value();
            
            // Update if this is a newer bid
            if (current == null || bid.dateTime > current.dateTime) {
                latestBidState.update(bid);
                out.collect(bid);
            }
        }
    }
}
