package com.github.gr1f0n6x.service.common;


import java.io.IOException;

public interface Deserializer<T> {
    T deserialize(byte[] bytes) throws IOException;
}
