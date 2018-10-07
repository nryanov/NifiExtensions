package com.github.gr1f0n6x.nifi.service.common;


import java.io.IOException;

public interface Deserializer<T> {
    T deserialize(byte[] bytes) throws IOException;
}
