/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.source;

import org.apache.flink.benchmark.nexmark.model.Auction;
import org.apache.flink.benchmark.nexmark.model.Bid;
import org.apache.flink.benchmark.nexmark.model.Event;
import org.apache.flink.benchmark.nexmark.model.Person;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Nexmark event source that generates Person, Auction, and Bid events.
 * 
 * <p>This is a DataStream implementation aligned with the original Nexmark benchmark.
 * Event proportions: Person(1) : Auction(3) : Bid(46) as per Nexmark spec.
 * 
 * <p>Supports:
 * <ul>
 *   <li>Hot sellers/bidders/auctions (99% of events target hot entities)</li>
 *   <li>Configurable TPS rate limiting</li>
 *   <li>Parallel event generation</li>
 * </ul>
 */
public class NexmarkSource extends RichParallelSourceFunction<Event> {
    private static final long serialVersionUID = 1L;

    // Event type proportions (from Nexmark spec)
    private static final int PERSON_PROPORTION = 1;
    private static final int AUCTION_PROPORTION = 3;
    private static final int BID_PROPORTION = 46;
    private static final int PROPORTION_TOTAL = PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;

    // Hot entity parameters (99% of events target hot entities)
    private static final int NUM_HOT_SELLERS = 100;
    private static final int NUM_HOT_BIDDERS = 100;
    private static final int NUM_HOT_AUCTIONS = 100;
    private static final double HOT_RATIO = 0.99;

    // Categories (Nexmark uses 10-14)
    private static final int NUM_CATEGORIES = 5;
    private static final int FIRST_CATEGORY_ID = 10;

    // Channels for bids
    private static final String[] CHANNELS = {"Google", "Facebook", "Baidu", "Bing", "Yahoo"};

    // US states
    private static final String[] US_STATES = {"AZ", "CA", "CO", "FL", "GA", "ID", "IL", "NY", "OR", "TX", "WA"};
    private static final String[] US_CITIES = {"Phoenix", "Los Angeles", "Denver", "Miami", "Atlanta", 
            "Boise", "Chicago", "New York", "Portland", "Dallas", "Seattle"};

    // Configuration
    private final long numEvents;
    private final long targetTps;
    private final int personProportion;
    private final int auctionProportion;
    private final int bidProportion;

    // Runtime state
    private volatile boolean running = true;
    private transient Random random;
    private transient long personIdGenerator;
    private transient long auctionIdGenerator;
    private transient long baseTime;

    /**
     * Create a Nexmark event source.
     *
     * @param numEvents Total number of events to generate
     * @param targetTps Target events per second (0 for unlimited)
     */
    public NexmarkSource(long numEvents, long targetTps) {
        this(numEvents, targetTps, PERSON_PROPORTION, AUCTION_PROPORTION, BID_PROPORTION);
    }

    /**
     * Create a Nexmark event source with custom proportions.
     */
    public NexmarkSource(long numEvents, long targetTps, int personProp, int auctionProp, int bidProp) {
        this.numEvents = numEvents;
        this.targetTps = targetTps;
        this.personProportion = personProp;
        this.auctionProportion = auctionProp;
        this.bidProportion = bidProp;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        
        // Each subtask has unique random seed
        this.random = new Random(subtaskIndex * 1000L + System.nanoTime());
        
        // Distribute ID space across subtasks
        long idSpacePerSubtask = Long.MAX_VALUE / parallelism / 2;
        this.personIdGenerator = subtaskIndex * idSpacePerSubtask;
        this.auctionIdGenerator = subtaskIndex * idSpacePerSubtask;
        
        this.baseTime = System.currentTimeMillis();
    }

    @Override
    public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Event> ctx) throws Exception {
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        long eventsPerSubtask = numEvents / parallelism;
        
        // Rate limiting setup
        long batchSize = targetTps > 0 ? Math.max(1000, targetTps / 100) : 10000;
        long nsPerBatch = targetTps > 0 ? (batchSize * 1_000_000_000L) / targetTps : 0;
        
        long eventCount = 0;
        long batchStart = System.nanoTime();
        long eventsInBatch = 0;

        while (running && eventCount < eventsPerSubtask) {
            Event event = generateNextEvent(eventCount);
            
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(event);
            }
            
            eventCount++;
            eventsInBatch++;
            
            // Rate limiting
            if (targetTps > 0 && eventsInBatch >= batchSize) {
                long elapsed = System.nanoTime() - batchStart;
                long sleepNs = nsPerBatch - elapsed;
                if (sleepNs > 1_000_000) { // More than 1ms
                    Thread.sleep(sleepNs / 1_000_000, (int) (sleepNs % 1_000_000));
                }
                batchStart = System.nanoTime();
                eventsInBatch = 0;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    /**
     * Generate the next event based on configured proportions.
     */
    private Event generateNextEvent(long eventNumber) {
        int proportionTotal = personProportion + auctionProportion + bidProportion;
        int rand = random.nextInt(proportionTotal);
        
        long timestamp = baseTime + (eventNumber / 1000); // Advance time slowly
        
        if (rand < personProportion) {
            return new Event(generatePerson(timestamp));
        } else if (rand < personProportion + auctionProportion) {
            return new Event(generateAuction(timestamp));
        } else {
            return new Event(generateBid(timestamp));
        }
    }

    private Person generatePerson(long timestamp) {
        long id = personIdGenerator++;
        int stateIdx = random.nextInt(US_STATES.length);
        
        return new Person(
                id,
                "Person_" + id,
                "person" + id + "@example.com",
                "1234-5678-" + (id % 10000),
                US_CITIES[stateIdx],
                US_STATES[stateIdx],
                timestamp,
                generateExtra()
        );
    }

    private Auction generateAuction(long timestamp) {
        long id = auctionIdGenerator++;
        long seller = getHotPersonId();
        long category = FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
        long initialBid = 100 + random.nextInt(10000);
        long reserve = initialBid + random.nextInt(5000);
        
        // Auction duration: 10 seconds to 10 minutes
        long duration = 10_000 + random.nextInt(590_000);
        long expires = timestamp + duration;
        
        return new Auction(
                id,
                "Item_" + id,
                "Description for item " + id,
                initialBid,
                reserve,
                timestamp,
                expires,
                seller,
                category,
                generateExtra()
        );
    }

    private Bid generateBid(long timestamp) {
        long auction = getHotAuctionId();
        long bidder = getHotPersonId();
        long price = 100 + random.nextInt(100000);
        String channel = CHANNELS[random.nextInt(CHANNELS.length)];
        
        return new Bid(
                auction,
                bidder,
                price,
                channel,
                "https://example.com/auction/" + auction,
                timestamp,
                generateExtra()
        );
    }

    /**
     * Get a person ID with hot distribution.
     * 99% of the time returns a "hot" person, 1% returns any person.
     */
    private long getHotPersonId() {
        if (random.nextDouble() < HOT_RATIO) {
            // Hot person (0 to NUM_HOT_SELLERS - 1)
            return random.nextInt(NUM_HOT_SELLERS);
        } else {
            // Any person
            return Math.abs(random.nextLong()) % Math.max(1, personIdGenerator);
        }
    }

    /**
     * Get an auction ID with hot distribution.
     * 99% of the time returns a "hot" auction, 1% returns any auction.
     */
    private long getHotAuctionId() {
        if (random.nextDouble() < HOT_RATIO) {
            // Hot auction (0 to NUM_HOT_AUCTIONS - 1)
            return random.nextInt(NUM_HOT_AUCTIONS);
        } else {
            // Any auction
            return Math.abs(random.nextLong()) % Math.max(1, auctionIdGenerator);
        }
    }

    /**
     * Generate extra padding string.
     */
    private String generateExtra() {
        // Short extra for performance (can be made configurable)
        return "";
    }
}
