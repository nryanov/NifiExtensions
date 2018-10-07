package com.github.gr1f0n6x.nifi.service.common;


import java.io.IOException;

public interface Serializer<T> {
    byte[] serialize(T o) throws IOException;
}
