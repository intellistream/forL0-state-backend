/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * A bid in the Nexmark auction system.
 * Corresponds to the "bid" table.
 */
public class Bid implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Auction ID this bid is for (Auction.id). */
    public long auction;

    /** Bidder ID (Person.id). */
    public long bidder;

    /** Bid price in cents. */
    public long price;

    /** Channel through which this bid was placed. */
    public String channel;

    /** URL associated with the bid. */
    public String url;

    /** Bid timestamp in milliseconds. */
    public long dateTime;

    /** Extra padding for performance testing. */
    public String extra;

    public Bid() {}

    public Bid(
            long auction,
            long bidder,
            long price,
            String channel,
            String url,
            long dateTime,
            String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bid bid = (Bid) o;
        return auction == bid.auction
                && bidder == bid.bidder
                && price == bid.price
                && dateTime == bid.dateTime
                && Objects.equals(channel, bid.channel)
                && Objects.equals(url, bid.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(auction, bidder, price, channel, url, dateTime);
    }

    @Override
    public String toString() {
        return "Bid{" +
                "auction=" + auction +
                ", bidder=" + bidder +
                ", price=" + price +
                ", dateTime=" + dateTime +
                '}';
    }
}
