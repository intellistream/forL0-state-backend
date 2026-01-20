/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Query 12: Processing Time Windows
 * 
 * <p>Original SQL:
 * <pre>
 * CREATE VIEW B AS SELECT *, PROCTIME() as p_time FROM bid;
 * 
 * SELECT bidder, count(*) as bid_count,
 *        window_start AS starttime, window_end AS endtime
 * FROM TABLE(TUMBLE(TABLE B, DESCRIPTOR(p_time), INTERVAL '10' SECOND))
 * GROUP BY bidder, window_start, window_end;
 * </pre>
 * 
 * <p>Implementation:
 * Tumbling processing time window (10s), count bids per bidder.
 */
public class Query12 {

    private static final long WINDOW_SIZE_SECONDS = 10;

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

        // Tumbling processing time window, count per bidder
        DataStream<Tuple4<Long, Long, Long, Long>> bidderCounts = bids
                .keyBy(b -> b.bidder)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(WINDOW_SIZE_SECONDS)))
                .aggregate(new CountAggregate(), new WindowInfoFunction())
                .name("TumblingBidCount");

        bidderCounts.addSink(new MetricsSink<>("q12", backend, numEvents, parallelism))
                .name("Q12Sink")
                .setParallelism(1);
    }

    /**
     * Count bids per bidder.
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
     * Attach window info: (bidder, count, starttime, endtime).
     */
    private static class WindowInfoFunction 
            extends ProcessWindowFunction<Long, Tuple4<Long, Long, Long, Long>, Long, TimeWindow> {

        @Override
        public void process(Long bidder, Context ctx, Iterable<Long> elements, 
                Collector<Tuple4<Long, Long, Long, Long>> out) {
            Long count = elements.iterator().next();
            out.collect(Tuple4.of(bidder, count, ctx.window().getStart(), ctx.window().getEnd()));
        }
    }
}
