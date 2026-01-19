/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.model;

import java.io.Serializable;

/**
 * Unified event wrapper for Nexmark events.
 * Contains exactly one of: Person, Auction, or Bid.
 */
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int PERSON = 0;
    public static final int AUCTION = 1;
    public static final int BID = 2;

    /** Event type: PERSON(0), AUCTION(1), or BID(2). */
    public int type;

    /** Person event data (only if type == PERSON). */
    public Person person;

    /** Auction event data (only if type == AUCTION). */
    public Auction auction;

    /** Bid event data (only if type == BID). */
    public Bid bid;

    public Event() {}

    public Event(Person person) {
        this.type = PERSON;
        this.person = person;
    }

    public Event(Auction auction) {
        this.type = AUCTION;
        this.auction = auction;
    }

    public Event(Bid bid) {
        this.type = BID;
        this.bid = bid;
    }

    public boolean isPerson() {
        return type == PERSON;
    }

    public boolean isAuction() {
        return type == AUCTION;
    }

    public boolean isBid() {
        return type == BID;
    }

    public long getTimestamp() {
        switch (type) {
            case PERSON:
                return person != null ? person.dateTime : 0;
            case AUCTION:
                return auction != null ? auction.dateTime : 0;
            case BID:
                return bid != null ? bid.dateTime : 0;
            default:
                return 0;
        }
    }

    @Override
    public String toString() {
        switch (type) {
            case PERSON:
                return "Event{person=" + person + '}';
            case AUCTION:
                return "Event{auction=" + auction + '}';
            case BID:
                return "Event{bid=" + bid + '}';
            default:
                return "Event{type=" + type + '}';
        }
    }
}
