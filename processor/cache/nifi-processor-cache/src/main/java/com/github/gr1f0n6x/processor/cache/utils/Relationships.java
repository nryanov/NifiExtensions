package com.github.gr1f0n6x.processor.cache.utils;

import org.apache.nifi.processor.Relationship;

public class Relationships {
    public static final Relationship SUCCESS = new Relationship.Builder()
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .build();

    public static final Relationship EXIST = new Relationship.Builder()
            .build();

    public static final Relationship NOT_EXIST = new Relationship.Builder()
            .build();
}
