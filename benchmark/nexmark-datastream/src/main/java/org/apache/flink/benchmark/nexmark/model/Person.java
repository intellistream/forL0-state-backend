/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */
package org.apache.flink.benchmark.nexmark.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * A person in the Nexmark auction system.
 * Corresponds to the "person" table.
 */
public class Person implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Unique person ID. */
    public long id;

    /** Person's name. */
    public String name;

    /** Person's email address. */
    public String emailAddress;

    /** Person's credit card number. */
    public String creditCard;

    /** Person's city. */
    public String city;

    /** Person's state. */
    public String state;

    /** Event timestamp in milliseconds. */
    public long dateTime;

    /** Extra padding for performance testing. */
    public String extra;

    public Person() {}

    public Person(
            long id,
            String name,
            String emailAddress,
            String creditCard,
            String city,
            String state,
            long dateTime,
            String extra) {
        this.id = id;
        this.name = name;
        this.emailAddress = emailAddress;
        this.creditCard = creditCard;
        this.city = city;
        this.state = state;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return id == person.id
                && dateTime == person.dateTime
                && Objects.equals(name, person.name)
                && Objects.equals(emailAddress, person.emailAddress)
                && Objects.equals(creditCard, person.creditCard)
                && Objects.equals(city, person.city)
                && Objects.equals(state, person.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, emailAddress, creditCard, city, state, dateTime);
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", dateTime=" + dateTime +
                '}';
    }
}
