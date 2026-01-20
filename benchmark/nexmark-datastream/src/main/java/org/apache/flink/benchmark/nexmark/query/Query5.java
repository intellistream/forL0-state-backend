/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * Query 5: Hot Items
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT AuctionBids.auction, AuctionBids.num
 * FROM (
 *   SELECT auction, count(*) AS num, window_start, window_end
 *   FROM TABLE(HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
 *   GROUP BY auction, window_start, window_end
 * ) AS AuctionBids
 * JOIN (
 *   SELECT max(CountBids.num) AS maxn, starttime, endtime
 *   FROM (
 *     SELECT count(*) AS num, window_start, window_end
 *     FROM TABLE(HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
 *     GROUP BY auction, window_start, window_end
 *   ) AS CountBids
 *   GROUP BY starttime, endtime
 * ) AS MaxBids
 * ON AuctionBids.starttime = MaxBids.starttime AND AuctionBids.endtime = MaxBids.endtime 
 *    AND AuctionBids.num >= MaxBids.maxn;
 * </pre>
 * 
 * <p>Implementation:
 * 1. Sliding window (10s window, 2s slide) aggregation: count bids per auction
 * 2. Find max count per window
 * 3. Output only auctions with count == max
 */
public class Query5 {

    private static final long WINDOW_SIZE_SECONDS = 10;
    private static final long SLIDE_SIZE_SECONDS = 2;

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

        // First aggregate: count per auction per window
        // Then find max and filter - we use ProcessWindowFunction to do both in one pass
        DataStream<Tuple3<Long, Long, Long>> hotItems = bids
                .keyBy(b -> b.auction)
                .window(SlidingProcessingTimeWindows.of(
                        Duration.ofSeconds(WINDOW_SIZE_SECONDS),
                        Duration.ofSeconds(SLIDE_SIZE_SECONDS)))
                .aggregate(new CountAggregate(), new CountWithWindow())
                .name("SlidingBidCount")
                // Now group by window to find max
                .keyBy(t -> t.f1 + "-" + t.f2) // windowStart-windowEnd as key
                .process(new FindMaxAndEmit())
                .name("FindMaxBids");

        // Output format: (auction, count, windowEnd)
        hotItems.addSink(new MetricsSink<>("q5", backend, numEvents, parallelism))
                .name("Q5Sink")
                .setParallelism(1);
    }

    /**
     * Count bids per auction.
     */
    private static class CountAggregate implements AggregateFunction<Bid, Long, Long> {
        @Override
        public Long createAccumulator() { return 0L; }

        @Override
        public Long add(Bid value, Long accumulator) { return accumulator + 1; }

        @Override
        public Long getResult(Long accumulator) { return accumulator; }

        @Override
        public Long merge(Long a, Long b) { return a + b; }
    }

    /**
     * Attach window info to count result.
     * Output: (auction, count, windowStart, windowEnd)
     */
    private static class CountWithWindow 
            extends ProcessWindowFunction<Long, Tuple4<Long, Long, Long, Long>, Long, TimeWindow> {

        @Override
        public void process(Long auction, Context ctx, Iterable<Long> elements, 
                Collector<Tuple4<Long, Long, Long, Long>> out) {
            Long count = elements.iterator().next();
            out.collect(Tuple4.of(auction, count, ctx.window().getStart(), ctx.window().getEnd()));
        }
    }

    /**
     * For each window, find the max count and emit only auctions with that count.
     * This implements the self-join to find max.
     */
    private static class FindMaxAndEmit 
            extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<
                String, Tuple4<Long, Long, Long, Long>, Tuple3<Long, Long, Long>> {

        // Store auction counts for current window
        private transient ValueState<java.util.List<Tuple4<Long, Long, Long, Long>>> bufferedState;
        private transient ValueState<Long> maxCountState;
        private transient ValueState<Long> windowEndState;

        @Override
        public void open(Configuration parameters) {
            bufferedState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("buffered", 
                        org.apache.flink.api.common.typeinfo.Types.LIST(
                            org.apache.flink.api.common.typeinfo.Types.TUPLE(
                                org.apache.flink.api.common.typeinfo.Types.LONG,
                                org.apache.flink.api.common.typeinfo.Types.LONG,
                                org.apache.flink.api.common.typeinfo.Types.LONG,
                                org.apache.flink.api.common.typeinfo.Types.LONG))));
            maxCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("maxCount", Long.class));
            windowEndState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("windowEnd", Long.class));
        }

        @Override
        public void processElement(Tuple4<Long, Long, Long, Long> value, Context ctx,
                Collector<Tuple3<Long, Long, Long>> out) throws Exception {
            
            java.util.List<Tuple4<Long, Long, Long, Long>> buffered = bufferedState.value();
            if (buffered == null) {
                buffered = new java.util.ArrayList<>();
            }
            buffered.add(value);
            bufferedState.update(buffered);
            
            Long currentMax = maxCountState.value();
            if (currentMax == null || value.f1 > currentMax) {
                maxCountState.update(value.f1);
            }
            
            // Register timer to emit results after window closes
            Long windowEnd = value.f3;
            if (windowEndState.value() == null) {
                windowEndState.update(windowEnd);
                // Add small delay to ensure all data for window arrives
                ctx.timerService().registerProcessingTimeTimer(windowEnd + 100);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, 
                Collector<Tuple3<Long, Long, Long>> out) throws Exception {
            java.util.List<Tuple4<Long, Long, Long, Long>> buffered = bufferedState.value();
            Long maxCount = maxCountState.value();
            
            if (buffered != null && maxCount != null) {
                for (Tuple4<Long, Long, Long, Long> item : buffered) {
                    if (item.f1.equals(maxCount)) {
                        // Emit (auction, count, windowEnd)
                        out.collect(Tuple3.of(item.f0, item.f1, item.f3));
                    }
                }
            }
            
            // Clear state
            bufferedState.clear();
            maxCountState.clear();
            windowEndState.clear();
        }
    }
}
