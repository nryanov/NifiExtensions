package com.github.gr1f0n6x.processor.cache.utils;

import org.apache.nifi.processor.Relationship;

public class Relationships {
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .build();

    public static final Relationship EXIST = new Relationship.Builder()
            .name("exist")
            .build();

    public static final Relationship NOT_EXIST = new Relationship.Builder()
            .name("not exist")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .build();
}
