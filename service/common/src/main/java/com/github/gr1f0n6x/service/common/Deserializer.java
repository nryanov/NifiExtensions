package com.github.gr1f0n6x.service.common;

public interface Deserializer<T> {
    T deserialize(byte[] bytes);
}
