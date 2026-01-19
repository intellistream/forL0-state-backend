/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * An auction in the Nexmark auction system.
 * Corresponds to the "auction" table.
 */
public class Auction implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Unique auction ID. */
    public long id;

    /** Name of item being auctioned. */
    public String itemName;

    /** Description of item. */
    public String description;

    /** Initial bid price in cents. */
    public long initialBid;

    /** Reserve price in cents. */
    public long reserve;

    /** Auction start time in milliseconds. */
    public long dateTime;

    /** Auction end time in milliseconds. */
    public long expires;

    /** ID of the seller (Person.id). */
    public long seller;

    /** Category ID (10-14). */
    public long category;

    /** Extra padding for performance testing. */
    public String extra;

    public Auction() {}

    public Auction(
            long id,
            String itemName,
            String description,
            long initialBid,
            long reserve,
            long dateTime,
            long expires,
            long seller,
            long category,
            String extra) {
        this.id = id;
        this.itemName = itemName;
        this.description = description;
        this.initialBid = initialBid;
        this.reserve = reserve;
        this.dateTime = dateTime;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
        this.extra = extra;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Auction auction = (Auction) o;
        return id == auction.id
                && initialBid == auction.initialBid
                && reserve == auction.reserve
                && dateTime == auction.dateTime
                && expires == auction.expires
                && seller == auction.seller
                && category == auction.category;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, initialBid, reserve, dateTime, expires, seller, category);
    }

    @Override
    public String toString() {
        return "Auction{" +
                "id=" + id +
                ", itemName='" + itemName + '\'' +
                ", initialBid=" + initialBid +
                ", seller=" + seller +
                ", category=" + category +
                ", dateTime=" + dateTime +
                ", expires=" + expires +
                '}';
    }
}
