package com.github.gr1f0n6x.service.common;

@FunctionalInterface
public interface ValueJoiner<V1, V2, R> {
    R join(V1 v1, V2 v2);
}
