/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.benchmark.nexmark.model.Auction;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.model.Person;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Query 8: Monitor New Users
 * 
 * <p>Original SQL:
 * <pre>
 * SELECT P.id, P.name, P.starttime
 * FROM (
 *   SELECT id, name, window_start AS starttime, window_end AS endtime
 *   FROM TABLE(TUMBLE(TABLE person, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
 *   GROUP BY id, name, window_start, window_end
 * ) P
 * JOIN (
 *   SELECT seller, window_start AS starttime, window_end AS endtime
 *   FROM TABLE(TUMBLE(TABLE auction, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
 *   GROUP BY seller, window_start, window_end
 * ) A
 * ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;
 * </pre>
 * 
 * <p>Implementation:
 * 1. Window persons by 10s tumbling window
 * 2. Window auctions by 10s tumbling window
 * 3. Join: person.id = auction.seller in same window
 * 4. Output persons who created auctions in the same window they registered
 */
public class Query8 {

    private static final long WINDOW_SIZE_SECONDS = 10;

    public static void run(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String backend,
            long numEvents,
            int parallelism) {

        DataStream<Person> persons = events
                .filter(Event::isPerson)
                .map(e -> e.person)
                .returns(Person.class);

        DataStream<Auction> auctions = events
                .filter(Event::isAuction)
                .map(e -> e.auction)
                .returns(Auction.class);

        // Window both streams by the same tumbling window, keyed by person.id / auction.seller
        // Then join within the same window
        
        // First, add window info to persons
        DataStream<Tuple3<Long, String, Long>> windowedPersons = persons
                .keyBy(p -> p.id)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(WINDOW_SIZE_SECONDS)))
                .process(new PersonWindowFunction())
                .name("PersonWindow");

        // Add window info to auctions (keyed by seller)
        DataStream<Tuple3<Long, Long, Long>> windowedAuctions = auctions
                .keyBy(a -> a.seller)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(WINDOW_SIZE_SECONDS)))
                .process(new AuctionWindowFunction())
                .name("AuctionWindow");

        // Join by composite key: (personId/seller, windowStart, windowEnd)
        DataStream<Tuple3<Long, String, Long>> joined = windowedPersons
                .keyBy(t -> t.f0 + "-" + t.f2) // id-windowEnd
                .connect(windowedAuctions.keyBy(t -> t.f0 + "-" + t.f2)) // seller-windowEnd
                .process(new PersonAuctionJoin())
                .name("PersonAuctionJoin");

        joined.addSink(new MetricsSink<>("q8", backend, numEvents, parallelism))
                .name("Q8Sink")
                .setParallelism(1);
    }

    /**
     * Extract (id, name, windowEnd) from windowed persons.
     */
    private static class PersonWindowFunction 
            extends ProcessWindowFunction<Person, Tuple3<Long, String, Long>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context ctx, Iterable<Person> elements, 
                Collector<Tuple3<Long, String, Long>> out) {
            // Deduplicate by id within window (GROUP BY id, name)
            Set<Long> seen = new HashSet<>();
            for (Person p : elements) {
                if (!seen.contains(p.id)) {
                    seen.add(p.id);
                    out.collect(Tuple3.of(p.id, p.name, ctx.window().getEnd()));
                }
            }
        }
    }

    /**
     * Extract (seller, auctionId, windowEnd) from windowed auctions.
     */
    private static class AuctionWindowFunction 
            extends ProcessWindowFunction<Auction, Tuple3<Long, Long, Long>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context ctx, Iterable<Auction> elements, 
                Collector<Tuple3<Long, Long, Long>> out) {
            // Deduplicate by seller within window (GROUP BY seller)
            Set<Long> seen = new HashSet<>();
            for (Auction a : elements) {
                if (!seen.contains(a.seller)) {
                    seen.add(a.seller);
                    out.collect(Tuple3.of(a.seller, a.id, ctx.window().getEnd()));
                }
            }
        }
    }

    /**
     * Join persons with auctions by id/seller in same window.
     */
    private static class PersonAuctionJoin 
            extends KeyedCoProcessFunction<String, Tuple3<Long, String, Long>, 
                    Tuple3<Long, Long, Long>, Tuple3<Long, String, Long>> {

        private transient ValueState<Tuple3<Long, String, Long>> personState;
        private transient ValueState<Boolean> auctionSeenState;

        @Override
        public void open(Configuration parameters) {
            personState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("person", 
                        org.apache.flink.api.common.typeinfo.Types.TUPLE(
                            org.apache.flink.api.common.typeinfo.Types.LONG,
                            org.apache.flink.api.common.typeinfo.Types.STRING,
                            org.apache.flink.api.common.typeinfo.Types.LONG)));
            auctionSeenState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("auctionSeen", Boolean.class));
        }

        @Override
        public void processElement1(Tuple3<Long, String, Long> person, Context ctx,
                Collector<Tuple3<Long, String, Long>> out) throws Exception {
            personState.update(person);
            
            // If we've already seen an auction for this key, emit
            if (Boolean.TRUE.equals(auctionSeenState.value())) {
                out.collect(person);
            }
            
            // Register cleanup timer
            ctx.timerService().registerProcessingTimeTimer(person.f2 + 1000);
        }

        @Override
        public void processElement2(Tuple3<Long, Long, Long> auction, Context ctx,
                Collector<Tuple3<Long, String, Long>> out) throws Exception {
            auctionSeenState.update(true);
            
            // If we've already seen the person, emit
            Tuple3<Long, String, Long> person = personState.value();
            if (person != null) {
                out.collect(person);
            }
            
            // Register cleanup timer
            ctx.timerService().registerProcessingTimeTimer(auction.f2 + 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                Collector<Tuple3<Long, String, Long>> out) throws Exception {
            personState.clear();
            auctionSeenState.clear();
        }
    }
}
