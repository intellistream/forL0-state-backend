/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.query;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.benchmark.nexmark.model.Auction;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.model.Person;
import org.apache.flink.benchmark.nexmark.sink.MetricsSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Query 8: Monitor New Users
 * 
 * <p>Find users who have created auctions within 12 hours of registering.
 * This query tests window join of two streams.
 * 
 * <p>SQL equivalent:
 * <pre>
 * SELECT P.id, P.name, A.reserve
 * FROM person P, auction A
 * WHERE P.id = A.seller
 *   AND A.dateTime >= P.dateTime
 *   AND A.dateTime <= P.dateTime + INTERVAL '12' HOUR
 * </pre>
 * 
 * <p>State pattern:
 * <ul>
 *   <li>ListState for persons: Buffer persons waiting for matching auctions</li>
 *   <li>ListState for auctions: Buffer auctions waiting for matching persons</li>
 * </ul>
 */
public class Query8 {

    // Join window: 12 hours in milliseconds
    private static final long JOIN_WINDOW_MS = 12 * 60 * 60 * 1000L;

    public static void run(
            StreamExecutionEnvironment env,
            DataStream<Event> events,
            String backend,
            long numEvents,
            int parallelism) {

        // Split streams
        DataStream<Person> persons = events
                .filter(Event::isPerson)
                .map(e -> e.person)
                .returns(Person.class);

        DataStream<Auction> auctions = events
                .filter(Event::isAuction)
                .map(e -> e.auction)
                .returns(Auction.class);

        // Join persons with auctions on seller = person.id
        DataStream<Tuple3<Long, String, Long>> newUserAuctions = persons
                .keyBy(p -> p.id)
                .connect(auctions.keyBy(a -> a.seller))
                .process(new PersonAuctionJoinFunction(JOIN_WINDOW_MS))
                .name("PersonAuctionJoin");

        // Sink
        newUserAuctions.addSink(new MetricsSink<>("q8", backend, numEvents, parallelism))
                .name("Q8Sink")
                .setParallelism(1);
    }

    /**
     * Join persons with auctions.
     * A match occurs when auction.seller = person.id and auction is created within
     * JOIN_WINDOW_MS of person registration.
     */
    private static class PersonAuctionJoinFunction 
            extends KeyedCoProcessFunction<Long, Person, Auction, Tuple3<Long, String, Long>> {

        private final long joinWindowMs;

        // Buffer persons
        private transient ListState<Person> personBuffer;
        // Buffer auctions
        private transient ListState<Auction> auctionBuffer;

        public PersonAuctionJoinFunction(long joinWindowMs) {
            this.joinWindowMs = joinWindowMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            personBuffer = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("persons", Person.class));
            auctionBuffer = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("auctions", Auction.class));
        }

        @Override
        public void processElement1(Person person, Context ctx, 
                Collector<Tuple3<Long, String, Long>> out) throws Exception {
            // Check for matching auctions already in buffer
            List<Auction> toRemove = new ArrayList<>();
            for (Auction auction : auctionBuffer.get()) {
                if (isMatch(person, auction)) {
                    out.collect(Tuple3.of(person.id, person.name, auction.reserve));
                }
                // Check if auction is too old
                if (auction.dateTime < person.dateTime - joinWindowMs) {
                    toRemove.add(auction);
                }
            }
            
            // Buffer the person
            personBuffer.add(person);
            
            // Register cleanup timer
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + joinWindowMs);
        }

        @Override
        public void processElement2(Auction auction, Context ctx, 
                Collector<Tuple3<Long, String, Long>> out) throws Exception {
            // Check for matching persons already in buffer
            for (Person person : personBuffer.get()) {
                if (isMatch(person, auction)) {
                    out.collect(Tuple3.of(person.id, person.name, auction.reserve));
                }
            }
            
            // Buffer the auction
            auctionBuffer.add(auction);
            
            // Register cleanup timer
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + joinWindowMs);
        }

        private boolean isMatch(Person person, Auction auction) {
            return auction.dateTime >= person.dateTime 
                    && auction.dateTime <= person.dateTime + joinWindowMs;
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, 
                Collector<Tuple3<Long, String, Long>> out) throws Exception {
            // Clean up old state
            long cutoff = timestamp - joinWindowMs;
            
            // Remove old persons
            List<Person> validPersons = new ArrayList<>();
            for (Person person : personBuffer.get()) {
                if (person.dateTime >= cutoff) {
                    validPersons.add(person);
                }
            }
            personBuffer.update(validPersons);
            
            // Remove old auctions
            List<Auction> validAuctions = new ArrayList<>();
            for (Auction auction : auctionBuffer.get()) {
                if (auction.dateTime >= cutoff) {
                    validAuctions.add(auction);
                }
            }
            auctionBuffer.update(validAuctions);
        }
    }
}
